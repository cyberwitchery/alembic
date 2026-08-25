//! validation utilities for the ir.

use crate::ir::{
    key_string, FieldFormat, FieldType, Inventory, Object, Schema, SourceLocation, TypeName, Uid,
};
use ipnet::IpNet;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::IpAddr;
use std::sync::OnceLock;
use thiserror::Error;

/// validation errors emitted during graph validation.
///
/// the serialized form is adjacently tagged, so a consumer switches on `kind`
/// and reads the named fields out of `detail` rather than parsing the message.
#[derive(Debug, Error, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "detail", rename_all = "snake_case")]
pub enum ValidationError {
    #[error("duplicate uid: {0}")]
    DuplicateUid(Uid),
    #[error("duplicate key: {0}")]
    DuplicateKey(String),
    #[error("missing type on object")]
    MissingType,
    #[error("missing key on object")]
    MissingKey,
    #[error("missing key field {type_name}.{field}")]
    MissingKeyField { type_name: String, field: String },
    #[error("extra key field {type_name}.{field}")]
    ExtraKeyField { type_name: String, field: String },
    #[error("missing attr field {type_name}.{field}")]
    MissingAttrField { type_name: String, field: String },
    #[error("extra attr field {type_name}.{field}")]
    ExtraAttrField { type_name: String, field: String },
    #[error("invalid value for {field}: expected {expected}, got {actual}")]
    InvalidValue {
        field: String,
        expected: String,
        actual: String,
    },
    #[error("unknown type: {0}")]
    UnknownType(String),
    #[error("missing reference {field} -> {target}")]
    MissingReference { field: String, target: Uid },
    #[error("reference type mismatch {field} -> {target} (expected {expected}, got {actual})")]
    ReferenceTypeMismatch {
        field: String,
        target: Uid,
        expected: String,
        actual: String,
    },
    #[error("unknown ref target {type_name}.{field} -> {target}")]
    UnknownRefTarget {
        type_name: String,
        field: String,
        target: String,
    },
    #[error("invalid pattern for {type_name}.{field}: {error} (pattern: {pattern})")]
    InvalidSchemaPattern {
        type_name: String,
        field: String,
        pattern: String,
        error: String,
    },
    #[error("{constraint} constraint on non-string field {type_name}.{field} (type {field_type})")]
    ConstraintOnNonStringField {
        type_name: String,
        field: String,
        constraint: String,
        field_type: String,
    },
    #[error(
        "conflicting format on {type_name}.{field}: declared {format}, but a {field_type} type already checks {type_check} (docs/ir.md)"
    )]
    ConflictingFormat {
        type_name: String,
        field: String,
        field_type: String,
        format: String,
        type_check: String,
    },
    #[error("empty enum for {type_name}.{field}: an enum with no values is unsatisfiable")]
    EmptyEnum { type_name: String, field: String },
    #[error(
        "non-scalar key field {type_name}.{field}: a {field_type} key has no scalar identity form (docs/ir.md)"
    )]
    NonScalarKeyField {
        type_name: String,
        field: String,
        field_type: String,
    },
    #[error(
        "nullable key field {type_name}.{field}: a null identity component has no stable identity (docs/ir.md)"
    )]
    NullableKeyField { type_name: String, field: String },
    #[error(
        "key field {type_name}.{field} is declared again in fields with a different schema; one name carries one schema (docs/ir.md)"
    )]
    KeyFieldDisagreement { type_name: String, field: String },
    #[error("scope names undeclared type {type_name} (docs/inventory.md)")]
    ScopeUnknownType { type_name: String },
    #[error("scope field {type_name}.{field} is not a key field of the type (docs/inventory.md)")]
    ScopeNonKeyField { type_name: String, field: String },
    #[error("invalid scope value for {type_name}.{field}: expected {expected}, got {actual}")]
    ScopeInvalidValue {
        type_name: String,
        field: String,
        expected: String,
        actual: String,
    },
}

impl ValidationError {
    /// return the uid associated with this error, if any.
    pub fn uid(&self) -> Option<Uid> {
        match self {
            ValidationError::DuplicateUid(uid) => Some(*uid),
            ValidationError::MissingReference { target, .. } => Some(*target),
            ValidationError::ReferenceTypeMismatch { target, .. } => Some(*target),
            // exhaustive on purpose (no `_`): a new variant that carries a uid must
            // be classified here rather than silently returning None.
            ValidationError::DuplicateKey(_)
            | ValidationError::MissingType
            | ValidationError::MissingKey
            | ValidationError::MissingKeyField { .. }
            | ValidationError::ExtraKeyField { .. }
            | ValidationError::MissingAttrField { .. }
            | ValidationError::ExtraAttrField { .. }
            | ValidationError::InvalidValue { .. }
            | ValidationError::UnknownType(_)
            | ValidationError::UnknownRefTarget { .. }
            | ValidationError::InvalidSchemaPattern { .. }
            | ValidationError::ConstraintOnNonStringField { .. }
            | ValidationError::ConflictingFormat { .. }
            | ValidationError::EmptyEnum { .. }
            | ValidationError::NonScalarKeyField { .. }
            | ValidationError::NullableKeyField { .. }
            | ValidationError::KeyFieldDisagreement { .. }
            | ValidationError::ScopeUnknownType { .. }
            | ValidationError::ScopeNonKeyField { .. }
            | ValidationError::ScopeInvalidValue { .. } => None,
        }
    }

    /// return a key-like string associated with this error, if any.
    pub fn key_hint(&self) -> Option<String> {
        match self {
            ValidationError::DuplicateKey(key) => {
                if let Some((_, k)) = key.split_once("::") {
                    Some(k.to_string())
                } else {
                    Some(key.clone())
                }
            }
            // exhaustive on purpose (see uid())
            ValidationError::DuplicateUid(_)
            | ValidationError::MissingType
            | ValidationError::MissingKey
            | ValidationError::MissingKeyField { .. }
            | ValidationError::ExtraKeyField { .. }
            | ValidationError::MissingAttrField { .. }
            | ValidationError::ExtraAttrField { .. }
            | ValidationError::InvalidValue { .. }
            | ValidationError::UnknownType(_)
            | ValidationError::MissingReference { .. }
            | ValidationError::ReferenceTypeMismatch { .. }
            | ValidationError::UnknownRefTarget { .. }
            | ValidationError::InvalidSchemaPattern { .. }
            | ValidationError::ConstraintOnNonStringField { .. }
            | ValidationError::ConflictingFormat { .. }
            | ValidationError::EmptyEnum { .. }
            | ValidationError::NonScalarKeyField { .. }
            | ValidationError::NullableKeyField { .. }
            | ValidationError::KeyFieldDisagreement { .. }
            | ValidationError::ScopeUnknownType { .. }
            | ValidationError::ScopeNonKeyField { .. }
            | ValidationError::ScopeInvalidValue { .. } => None,
        }
    }

    /// return the type name associated with this error, if any.
    pub fn type_hint(&self) -> Option<String> {
        match self {
            ValidationError::UnknownType(t) => Some(t.clone()),
            ValidationError::MissingKeyField { type_name, .. }
            | ValidationError::ExtraKeyField { type_name, .. }
            | ValidationError::MissingAttrField { type_name, .. }
            | ValidationError::ExtraAttrField { type_name, .. }
            | ValidationError::UnknownRefTarget { type_name, .. }
            | ValidationError::InvalidSchemaPattern { type_name, .. }
            | ValidationError::ConstraintOnNonStringField { type_name, .. }
            | ValidationError::ConflictingFormat { type_name, .. }
            | ValidationError::EmptyEnum { type_name, .. }
            | ValidationError::NonScalarKeyField { type_name, .. }
            | ValidationError::NullableKeyField { type_name, .. }
            | ValidationError::KeyFieldDisagreement { type_name, .. }
            | ValidationError::ScopeUnknownType { type_name }
            | ValidationError::ScopeNonKeyField { type_name, .. }
            | ValidationError::ScopeInvalidValue { type_name, .. } => Some(type_name.clone()),
            ValidationError::InvalidValue { field, .. } => {
                field.split('.').next().map(|s| s.to_string())
            }
            ValidationError::MissingReference { field, .. }
            | ValidationError::ReferenceTypeMismatch { field, .. } => {
                field.split('.').next().map(|s| s.to_string())
            }
            ValidationError::DuplicateKey(key) => key.split("::").next().map(|s| s.to_string()),
            // exhaustive on purpose (see uid())
            ValidationError::DuplicateUid(_)
            | ValidationError::MissingType
            | ValidationError::MissingKey => None,
        }
    }

    /// return the dotted field path for this error, if any.
    pub fn field(&self) -> Option<&str> {
        match self {
            ValidationError::InvalidValue { field, .. }
            | ValidationError::MissingReference { field, .. }
            | ValidationError::ReferenceTypeMismatch { field, .. } => Some(field),
            // exhaustive on purpose (see uid())
            ValidationError::DuplicateUid(_)
            | ValidationError::DuplicateKey(_)
            | ValidationError::MissingType
            | ValidationError::MissingKey
            | ValidationError::MissingKeyField { .. }
            | ValidationError::ExtraKeyField { .. }
            | ValidationError::MissingAttrField { .. }
            | ValidationError::ExtraAttrField { .. }
            | ValidationError::UnknownType(_)
            | ValidationError::UnknownRefTarget { .. }
            | ValidationError::InvalidSchemaPattern { .. }
            | ValidationError::ConstraintOnNonStringField { .. }
            | ValidationError::ConflictingFormat { .. }
            | ValidationError::EmptyEnum { .. }
            | ValidationError::NonScalarKeyField { .. }
            | ValidationError::NullableKeyField { .. }
            | ValidationError::KeyFieldDisagreement { .. }
            | ValidationError::ScopeUnknownType { .. }
            | ValidationError::ScopeNonKeyField { .. }
            | ValidationError::ScopeInvalidValue { .. } => None,
        }
    }
}

/// a validation error with optional source location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocatedError {
    pub error: ValidationError,
    pub source: Option<SourceLocation>,
}

impl LocatedError {
    pub fn new(error: ValidationError) -> Self {
        Self {
            error,
            source: None,
        }
    }

    pub fn with_source(error: ValidationError, source: Option<SourceLocation>) -> Self {
        Self { error, source }
    }
}

impl fmt::Display for LocatedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(source) = &self.source {
            write!(f, "{}: {}", source, self.error)
        } else {
            write!(f, "{}", self.error)
        }
    }
}

/// aggregated validation report.
#[derive(Debug, Default, Clone)]
pub struct ValidationReport {
    pub errors: Vec<ValidationError>,
}

/// a validation report whose errors carry their source location: the document
/// form of [`ValidationReport`], and what `alembic validate --output` writes.
///
/// `Deserialize` is here so a consumer can read a written report back; it is
/// never a way to feed errors into validation.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocatedReport {
    pub errors: Vec<LocatedError>,
}

impl ValidationReport {
    /// return true when no errors are present.
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }

    /// return true when errors are present.
    pub fn is_err(&self) -> bool {
        !self.errors.is_empty()
    }

    /// enrich errors with source locations from objects.
    ///
    /// this matches errors to objects based on UIDs, types, and keys,
    /// and attaches the object's source location to the error.
    pub fn with_sources(self, objects: &[Object]) -> Vec<LocatedError> {
        let uid_to_source: BTreeMap<Uid, Option<SourceLocation>> =
            objects.iter().map(|o| (o.uid, o.source.clone())).collect();
        let key_to_source: BTreeMap<String, Option<SourceLocation>> = objects
            .iter()
            .map(|o| {
                let key = format!("{}::{}", o.type_name, key_string(&o.key));
                (key, o.source.clone())
            })
            .collect();
        let type_to_source: BTreeMap<String, Option<SourceLocation>> = objects
            .iter()
            .filter_map(|o| o.source.clone().map(|s| (o.type_name.to_string(), Some(s))))
            .collect();
        let known_types: BTreeSet<&str> = objects.iter().map(|o| o.type_name.as_str()).collect();

        self.errors
            .into_iter()
            .map(|error| {
                let source = error
                    .uid()
                    // only DuplicateUid's uid is the offending object; ref errors' uid is the referent.
                    .filter(|_| matches!(error, ValidationError::DuplicateUid(_)))
                    .and_then(|uid| uid_to_source.get(&uid).cloned().flatten())
                    .or_else(|| {
                        error.key_hint().and_then(|_| {
                            if let ValidationError::DuplicateKey(key) = &error {
                                key_to_source.get(key).cloned().flatten()
                            } else {
                                None
                            }
                        })
                    })
                    .or_else(|| {
                        // type names contain dots, so match the longest known type that prefixes `field`.
                        if let Some(field) = error.field() {
                            known_types
                                .iter()
                                .filter(|t| {
                                    field.strip_prefix(**t).is_some_and(|rest| {
                                        rest.is_empty() || rest.starts_with('.')
                                    })
                                })
                                .max_by_key(|t| t.len())
                                .and_then(|t| type_to_source.get(*t).cloned().flatten())
                        } else {
                            error
                                .type_hint()
                                .and_then(|t| type_to_source.get(&t).cloned().flatten())
                        }
                    });
                LocatedError::with_source(error, source)
            })
            .collect()
    }

    /// the report as a located document, ready to serialize.
    ///
    /// an empty report locates to an empty `errors` list rather than to nothing,
    /// so a passing run still has a report to write.
    pub fn located(self, objects: &[Object]) -> LocatedReport {
        LocatedReport {
            errors: self.with_sources(objects),
        }
    }
}

/// validate uniqueness and reference integrity for the given inventory.
pub fn validate_inventory(inventory: &Inventory) -> ValidationReport {
    let mut report = ValidationReport::default();
    let mut seen_uids = BTreeSet::new();
    let mut seen_keys = BTreeSet::new();
    let mut uid_to_type = BTreeMap::new();

    for object in &inventory.objects {
        if object.key.is_empty() {
            report.errors.push(ValidationError::MissingKey);
        }
        if object.type_name.is_empty() {
            report.errors.push(ValidationError::MissingType);
        }
        if !seen_uids.insert(object.uid) {
            report
                .errors
                .push(ValidationError::DuplicateUid(object.uid));
        }
        let key = format!("{}::{}", object.type_name, key_string(&object.key));
        if !seen_keys.insert(key.clone()) {
            report.errors.push(ValidationError::DuplicateKey(key));
        }
        uid_to_type.insert(object.uid, object.type_name.clone());
    }

    validate_schema_ref_targets(&inventory.schema, &mut report);
    let pattern_cache = compile_schema_patterns(&inventory.schema, &mut report);
    validate_schema_constraint_types(&inventory.schema, &mut report);
    validate_schema_format_agreement(&inventory.schema, &mut report);
    validate_schema_enums(&inventory.schema, &mut report);
    validate_schema_key_scalar(&inventory.schema, &mut report);
    validate_schema_key_nullable(&inventory.schema, &mut report);
    validate_schema_key_field_agreement(&inventory.schema, &mut report);
    if let Some(scope) = &inventory.scope {
        validate_scope(scope, &inventory.schema, &pattern_cache, &mut report);
    }
    validate_schema_types(&inventory.schema, &inventory.objects, &mut report);
    for object in &inventory.objects {
        validate_object(
            object,
            &inventory.schema,
            &uid_to_type,
            &pattern_cache,
            &mut report,
        );
    }

    report
}

/// walk every declared field in the schema, visiting each type's key fields
/// (labeled `key.<field>`) then its attribute fields. the schema-level
/// validators share this traversal; centralizing it keeps the key-field
/// labeling convention in one place.
fn for_each_schema_field(
    schema: &Schema,
    mut visit: impl FnMut(&str, &str, &crate::ir::FieldSchema),
) {
    for (type_name, type_schema) in &schema.types {
        for (field, field_schema) in &type_schema.key {
            visit(type_name, &format!("key.{field}"), field_schema);
        }
        for (field, field_schema) in &type_schema.fields {
            visit(type_name, field, field_schema);
        }
    }
}

/// validate that every `ref`/`list_ref` target declared in the schema names a
/// declared type.
///
/// targets are free-form strings, so a typo (`tenant` for `tenancy.tenant`)
/// would otherwise pass schema validation and only surface later as misleading
/// per-object reference errors. this catches the mistake at the schema level,
/// attributed to the declaring type and field.
fn validate_schema_ref_targets(schema: &Schema, report: &mut ValidationReport) {
    for_each_schema_field(schema, |type_name, field, field_schema| {
        validate_field_ref_targets(schema, type_name, field, &field_schema.r#type, report);
    });
}

/// recursively check ref targets within a field type, descending into `list`
/// and `map` item types so refs nested inside them are validated too.
fn validate_field_ref_targets(
    schema: &Schema,
    type_name: &str,
    field: &str,
    field_type: &FieldType,
    report: &mut ValidationReport,
) {
    match field_type {
        FieldType::Ref { target } | FieldType::ListRef { target } => {
            if !schema.types.contains_key(target) {
                report.errors.push(ValidationError::UnknownRefTarget {
                    type_name: type_name.to_string(),
                    field: field.to_string(),
                    target: target.to_string(),
                });
            }
        }
        FieldType::List { item } => {
            validate_field_ref_targets(schema, type_name, field, item, report);
        }
        FieldType::Map { value } => {
            validate_field_ref_targets(schema, type_name, field, value, report);
        }
        FieldType::String
        | FieldType::Text
        | FieldType::Int
        | FieldType::Float
        | FieldType::Bool
        | FieldType::Uuid
        | FieldType::Date
        | FieldType::Datetime
        | FieldType::Time
        | FieldType::Json
        | FieldType::IpAddress
        | FieldType::Cidr
        | FieldType::Prefix
        | FieldType::Mac
        | FieldType::Slug
        | FieldType::Enum { .. } => {}
    }
}

/// compile every field `pattern:` once, keyed by pattern string. a malformed one is
/// reported here as `InvalidSchemaPattern` and left out, so the per-object pass skips it.
fn compile_schema_patterns(
    schema: &Schema,
    report: &mut ValidationReport,
) -> BTreeMap<String, Regex> {
    let mut cache = BTreeMap::new();
    for_each_schema_field(schema, |type_name, field, field_schema| {
        let Some(pattern) = &field_schema.pattern else {
            return;
        };
        if cache.contains_key(pattern) {
            return;
        }
        match Regex::new(pattern) {
            Ok(regex) => {
                cache.insert(pattern.clone(), regex);
            }
            Err(err) => report.errors.push(ValidationError::InvalidSchemaPattern {
                type_name: type_name.to_string(),
                field: field.to_string(),
                pattern: pattern.to_string(),
                error: err.to_string(),
            }),
        }
    });
    cache
}

/// reject a top-level `format:`/`pattern:` on a field whose type can never hold
/// a string; otherwise it is silently accepted at load and only fails per-object
/// as a misleading `expected string` error (never at all for an empty type).
///
/// `format`/`pattern` live only on the top-level `FieldSchema`, so a flat walk
/// over key and attr fields is complete.
fn validate_schema_constraint_types(schema: &Schema, report: &mut ValidationReport) {
    for_each_schema_field(schema, |type_name, field, field_schema| {
        validate_field_constraint_type(type_name, field, field_schema, report);
    });
}

/// flag each `format:`/`pattern:` present on a never-string field, one error per
/// constraint.
fn validate_field_constraint_type(
    type_name: &str,
    field: &str,
    field_schema: &crate::ir::FieldSchema,
    report: &mut ValidationReport,
) {
    if !is_never_string_type(&field_schema.r#type) {
        return;
    }
    let field_type = field_type_label(&field_schema.r#type);
    for (present, constraint) in [
        (field_schema.format.is_some(), "format"),
        (field_schema.pattern.is_some(), "pattern"),
    ] {
        if present {
            report
                .errors
                .push(ValidationError::ConstraintOnNonStringField {
                    type_name: type_name.to_string(),
                    field: field.to_string(),
                    constraint: constraint.to_string(),
                    field_type: field_type.clone(),
                });
        }
    }
}

/// true when a field's type can never hold a json string, so a `format:` or
/// `pattern:` on it is meaningless. `ref`/`json` can carry a string; only the
/// scalar non-string and collection types are rejected.
fn is_never_string_type(field_type: &FieldType) -> bool {
    match field_type {
        FieldType::Int
        | FieldType::Float
        | FieldType::Bool
        | FieldType::List { .. }
        | FieldType::Map { .. }
        | FieldType::ListRef { .. } => true,
        FieldType::String
        | FieldType::Text
        | FieldType::Date
        | FieldType::Datetime
        | FieldType::Time
        | FieldType::IpAddress
        | FieldType::Uuid
        | FieldType::Cidr
        | FieldType::Prefix
        | FieldType::Mac
        | FieldType::Slug
        | FieldType::Enum { .. }
        | FieldType::Ref { .. }
        | FieldType::Json => false,
    }
}

/// reject a declared `format:` that disagrees with the check the field's own type
/// already applies. core holds the value to both while a backend provisions the
/// declared one alone (`validation_regex_for_schema`), so alembic and the backend
/// it provisioned mean different things by the field; otherwise it is accepted at
/// load and only fails per-object, naming the shape the type asked for.
fn validate_schema_format_agreement(schema: &Schema, report: &mut ValidationReport) {
    for_each_schema_field(schema, |type_name, field, field_schema| {
        let Some(format) = field_schema.format.as_ref() else {
            return;
        };
        // off `type_check` rather than a table beside it, so a type whose check
        // changes cannot fall out of the rule. json-checked types carry no shape.
        let ValueCheck::Text(check) = type_check(&field_schema.r#type) else {
            return;
        };
        if check.is(format) {
            return;
        }
        report.errors.push(ValidationError::ConflictingFormat {
            type_name: type_name.to_string(),
            field: field.to_string(),
            field_type: field_type_label(&field_schema.r#type),
            format: format_label(format),
            type_check: check.label(),
        });
    });
}

/// reject an `enum` field declared with an empty `values` list. an empty enum is
/// unsatisfiable, so every value fails per-object with a confusing
/// `expected: enum()` message (and never at all for a type with no objects). this
/// catches the mistake at the schema level, attributed to the declaring type and
/// field.
///
/// `list`/`map` item types can nest an enum, and per-object validation recurses
/// into them keeping the same field label, so this walk recurses the same way.
fn validate_schema_enums(schema: &Schema, report: &mut ValidationReport) {
    for_each_schema_field(schema, |type_name, field, field_schema| {
        validate_field_enum(type_name, field, &field_schema.r#type, report);
    });
}

/// recursively check for an empty-values enum within a field type, descending
/// into `list` and `map` item types so enums nested inside them are caught too.
fn validate_field_enum(
    type_name: &str,
    field: &str,
    field_type: &FieldType,
    report: &mut ValidationReport,
) {
    match field_type {
        FieldType::Enum { values } => {
            if values.is_empty() {
                report.errors.push(ValidationError::EmptyEnum {
                    type_name: type_name.to_string(),
                    field: field.to_string(),
                });
            }
        }
        FieldType::List { item } => {
            validate_field_enum(type_name, field, item, report);
        }
        FieldType::Map { value } => {
            validate_field_enum(type_name, field, value, report);
        }
        FieldType::String
        | FieldType::Text
        | FieldType::Int
        | FieldType::Float
        | FieldType::Bool
        | FieldType::Uuid
        | FieldType::Date
        | FieldType::Datetime
        | FieldType::Time
        | FieldType::Json
        | FieldType::IpAddress
        | FieldType::Cidr
        | FieldType::Prefix
        | FieldType::Mac
        | FieldType::Slug
        | FieldType::Ref { .. }
        | FieldType::ListRef { .. } => {}
    }
}

/// reject a key field whose declared type is composite (`list`, `list_ref`, or
/// `map`). a key component feeds uid derivation and must render to a scalar
/// string, so `render_key`/`ensure_scalar` reject a composite value at map time
/// (docs/map.md). catching the type at schema load keeps the invalid state
/// unrepresentable before any object is authored.
///
/// only the top-level key field type matters: a key value is scalar or it is
/// not, so nesting is irrelevant (unlike `ref`-target and `enum` checks, which
/// descend into `list`/`map`). scalar-producing types stay allowed, including
/// `ref` (a ref key renders to a scalar uid string) and every scalar built-in.
fn validate_schema_key_scalar(schema: &Schema, report: &mut ValidationReport) {
    for (type_name, type_schema) in &schema.types {
        for (field, field_schema) in &type_schema.key {
            if is_composite_type(&field_schema.r#type) {
                report.errors.push(ValidationError::NonScalarKeyField {
                    type_name: type_name.to_string(),
                    field: format!("key.{field}"),
                    field_type: field_type_label(&field_schema.r#type),
                });
            }
        }
    }
}

/// true when a field's type is composite (`list`, `list_ref`, or `map`) and so
/// has no scalar string form. `ref` is scalar (a uid string); every other
/// built-in is scalar or coerces to one.
fn is_composite_type(field_type: &FieldType) -> bool {
    match field_type {
        FieldType::List { .. } | FieldType::ListRef { .. } | FieldType::Map { .. } => true,
        FieldType::String
        | FieldType::Text
        | FieldType::Int
        | FieldType::Float
        | FieldType::Bool
        | FieldType::Uuid
        | FieldType::Date
        | FieldType::Datetime
        | FieldType::Time
        | FieldType::Json
        | FieldType::IpAddress
        | FieldType::Cidr
        | FieldType::Prefix
        | FieldType::Mac
        | FieldType::Slug
        | FieldType::Enum { .. }
        | FieldType::Ref { .. } => false,
    }
}

/// reject a key field declared `nullable: true`. a key component must render
/// to a scalar to have an identity form (see `validate_schema_key_scalar`),
/// and a null value has none, so `render_key`/`ensure_scalar` reject a null
/// key at map time. rejecting the declaration at schema load keeps the invalid
/// state unrepresentable before any object is authored.
fn validate_schema_key_nullable(schema: &Schema, report: &mut ValidationReport) {
    for (type_name, type_schema) in &schema.types {
        for (field, field_schema) in &type_schema.key {
            if field_schema.nullable {
                report.errors.push(ValidationError::NullableKeyField {
                    type_name: type_name.to_string(),
                    field: format!("key.{field}"),
                });
            }
        }
    }
}

/// reject a field declared in both `key:` and `fields:` with a different
/// schema. every consumer of such a type has to pick a winner, and each picks
/// its own — the django generator reads the key entry, adapter normalization
/// reads the fields entry — so the disagreement is a schema bug, not a
/// precedence question. an identical duplicate declares one schema twice and
/// stays legal.
fn validate_schema_key_field_agreement(schema: &Schema, report: &mut ValidationReport) {
    for (type_name, type_schema) in &schema.types {
        for (field, key_schema) in &type_schema.key {
            if let Some(field_schema) = type_schema.fields.get(field) {
                if key_schema != field_schema {
                    report.errors.push(ValidationError::KeyFieldDisagreement {
                        type_name: type_name.to_string(),
                        field: field.to_string(),
                    });
                }
            }
        }
    }
}

/// a scope entry names a declared type and constrains its key fields with
/// values those fields could hold (`docs/inventory.md`). a ref-typed key field
/// is constrained by uid: the target may be an object the inventory does not
/// manage, so nothing here requires the uid to resolve.
fn validate_scope(
    scope: &crate::ir::Scope,
    schema: &Schema,
    pattern_cache: &BTreeMap<String, Regex>,
    report: &mut ValidationReport,
) {
    for (type_name, entry) in &scope.0 {
        let Some(type_schema) = schema.types.get(type_name) else {
            report.errors.push(ValidationError::ScopeUnknownType {
                type_name: type_name.clone(),
            });
            continue;
        };
        for (field, values) in entry {
            let Some(field_schema) = type_schema.key.get(field) else {
                report.errors.push(ValidationError::ScopeNonKeyField {
                    type_name: type_name.clone(),
                    field: field.clone(),
                });
                continue;
            };
            let values: &[Value] = match values {
                crate::ir::ScopeValues::One(value) => std::slice::from_ref(value),
                crate::ir::ScopeValues::Many(values) => values,
            };
            for value in values {
                validate_scope_value(type_name, field, field_schema, value, pattern_cache, report);
            }
        }
    }
}

/// check one scope value against the key field it constrains, reporting
/// through `ScopeInvalidValue` so the message names the scope rather than an
/// object. refs check only uid shape (see [`validate_scope`]). a constraint
/// value must be a scalar: an array at the constraint position always reads as
/// a list of allowed values, so a composite value there could only be the
/// ambiguous spelling of a constraint the syntax cannot express — a `json` key
/// holding composites is scoped whole-type or not at all.
fn validate_scope_value(
    type_name: &str,
    field: &str,
    field_schema: &crate::ir::FieldSchema,
    value: &Value,
    pattern_cache: &BTreeMap<String, Regex>,
    report: &mut ValidationReport,
) {
    if value.is_array() || value.is_object() {
        report.errors.push(ValidationError::ScopeInvalidValue {
            type_name: type_name.to_string(),
            field: field.to_string(),
            expected: "a scalar value".to_string(),
            actual: value_type_label(value),
        });
        return;
    }
    if let FieldType::Ref { .. } = field_schema.r#type {
        if parse_uid(value).is_none() {
            report.errors.push(ValidationError::ScopeInvalidValue {
                type_name: type_name.to_string(),
                field: field.to_string(),
                expected: "uuid".to_string(),
                actual: value_type_label(value),
            });
        }
        return;
    }
    let mut scratch = ValidationReport::default();
    validate_field_value(
        &TypeName::new(type_name),
        field,
        field_schema,
        value,
        &BTreeMap::new(),
        pattern_cache,
        &mut scratch,
    );
    for error in scratch.errors {
        if let ValidationError::InvalidValue {
            expected, actual, ..
        } = error
        {
            report.errors.push(ValidationError::ScopeInvalidValue {
                type_name: type_name.to_string(),
                field: field.to_string(),
                expected,
                actual,
            });
        }
    }
}

fn validate_schema_types(schema: &Schema, objects: &[Object], report: &mut ValidationReport) {
    for object in objects {
        if !schema.types.contains_key(object.type_name.as_str()) {
            report
                .errors
                .push(ValidationError::UnknownType(object.type_name.to_string()));
        }
    }
}

fn validate_object(
    object: &Object,
    schema: &Schema,
    uid_to_type: &BTreeMap<Uid, TypeName>,
    pattern_cache: &BTreeMap<String, Regex>,
    report: &mut ValidationReport,
) {
    let Some(type_schema) = schema.types.get(object.type_name.as_str()) else {
        return;
    };

    validate_key_fields(object, type_schema, uid_to_type, pattern_cache, report);
    validate_attr_fields(object, type_schema, uid_to_type, pattern_cache, report);
}

fn validate_key_fields(
    object: &Object,
    type_schema: &crate::ir::TypeSchema,
    uid_to_type: &BTreeMap<Uid, TypeName>,
    pattern_cache: &BTreeMap<String, Regex>,
    report: &mut ValidationReport,
) {
    for (field, field_schema) in &type_schema.key {
        let Some(value) = object.key.get(field) else {
            report.errors.push(ValidationError::MissingKeyField {
                type_name: object.type_name.to_string(),
                field: field.to_string(),
            });
            continue;
        };
        validate_field_value(
            &object.type_name,
            &format!("key.{field}"),
            field_schema,
            value,
            uid_to_type,
            pattern_cache,
            report,
        );
    }

    for field in object.key.keys() {
        if !type_schema.key.contains_key(field) {
            report.errors.push(ValidationError::ExtraKeyField {
                type_name: object.type_name.to_string(),
                field: field.to_string(),
            });
        }
    }
}

fn validate_attr_fields(
    object: &Object,
    type_schema: &crate::ir::TypeSchema,
    uid_to_type: &BTreeMap<Uid, TypeName>,
    pattern_cache: &BTreeMap<String, Regex>,
    report: &mut ValidationReport,
) {
    for (field, field_schema) in &type_schema.fields {
        let Some(value) = object.attrs.get(field) else {
            if field_schema.required {
                report.errors.push(ValidationError::MissingAttrField {
                    type_name: object.type_name.to_string(),
                    field: field.to_string(),
                });
            }
            continue;
        };
        validate_field_value(
            &object.type_name,
            field,
            field_schema,
            value,
            uid_to_type,
            pattern_cache,
            report,
        );
    }

    for field in object.attrs.keys() {
        if !type_schema.fields.contains_key(field) {
            report.errors.push(ValidationError::ExtraAttrField {
                type_name: object.type_name.to_string(),
                field: field.to_string(),
            });
        }
    }
}

/// the synthetic `FieldSchema` a `list`/`map` element is validated against: its item
/// type, required and non-nullable. shared by the `List` and `Map` arms.
fn element_schema(item: &FieldType) -> crate::ir::FieldSchema {
    crate::ir::FieldSchema {
        r#type: item.clone(),
        required: true,
        nullable: false,
        description: None,
        format: None,
        pattern: None,
    }
}

fn validate_field_value(
    type_name: &TypeName,
    field: &str,
    field_schema: &crate::ir::FieldSchema,
    value: &Value,
    uid_to_type: &BTreeMap<Uid, TypeName>,
    pattern_cache: &BTreeMap<String, Regex>,
    report: &mut ValidationReport,
) {
    if value.is_null() {
        if field_schema.nullable {
            return;
        }
        report.errors.push(ValidationError::InvalidValue {
            field: format!("{type_name}.{field}"),
            expected: field_type_label(&field_schema.r#type),
            actual: "null".to_string(),
        });
        return;
    }

    match &field_schema.r#type {
        FieldType::Ref { target } => {
            validate_ref(type_name, field, target, value, uid_to_type, report);
        }
        FieldType::ListRef { target } => {
            if let Some(entries) = value.as_array() {
                for entry in entries {
                    validate_ref(type_name, field, target, entry, uid_to_type, report);
                }
            } else {
                report.errors.push(ValidationError::InvalidValue {
                    field: format!("{type_name}.{field}"),
                    expected: "list_ref".to_string(),
                    actual: value_type_label(value),
                });
            }
        }
        FieldType::List { item } => {
            if let Some(entries) = value.as_array() {
                let schema = element_schema(item);
                for entry in entries {
                    validate_field_value(
                        type_name,
                        field,
                        &schema,
                        entry,
                        uid_to_type,
                        pattern_cache,
                        report,
                    );
                }
            } else {
                report.errors.push(ValidationError::InvalidValue {
                    field: format!("{type_name}.{field}"),
                    expected: "list".to_string(),
                    actual: value_type_label(value),
                });
            }
        }
        FieldType::Map { value: inner } => {
            if let Some(entries) = value.as_object() {
                let schema = element_schema(inner);
                for entry in entries.values() {
                    validate_field_value(
                        type_name,
                        field,
                        &schema,
                        entry,
                        uid_to_type,
                        pattern_cache,
                        report,
                    );
                }
            } else {
                report.errors.push(ValidationError::InvalidValue {
                    field: format!("{type_name}.{field}"),
                    expected: "map".to_string(),
                    actual: value_type_label(value),
                });
            }
        }
        FieldType::Enum { values } => {
            if let Some(raw) = value.as_str() {
                if !values.contains(&raw.to_string()) {
                    report.errors.push(ValidationError::InvalidValue {
                        field: format!("{type_name}.{field}"),
                        expected: format!("enum({})", values.join("|")),
                        actual: raw.to_string(),
                    });
                }
            } else {
                report.errors.push(ValidationError::InvalidValue {
                    field: format!("{type_name}.{field}"),
                    expected: "enum".to_string(),
                    actual: value_type_label(value),
                });
            }
        }
        _ => {
            if !value_matches_type(value, &field_schema.r#type) {
                report.errors.push(ValidationError::InvalidValue {
                    field: format!("{type_name}.{field}"),
                    expected: field_type_label(&field_schema.r#type),
                    actual: mismatch_actual(value, &field_schema.r#type),
                });
            }
        }
    }

    validate_string_constraints(type_name, field, field_schema, value, pattern_cache, report);
}

fn parse_uid(value: &Value) -> Option<Uid> {
    let raw = value.as_str()?;
    Uid::parse_str(raw).ok()
}

fn validate_ref(
    type_name: &TypeName,
    field: &str,
    target: &str,
    value: &Value,
    uid_to_type: &BTreeMap<Uid, TypeName>,
    report: &mut ValidationReport,
) {
    let Some(uid) = parse_uid(value) else {
        // `parse_uid` fails on both halves of the check, so it takes the same
        // split as the field types: a uuid check on a string is a format check.
        report.errors.push(ValidationError::InvalidValue {
            field: format!("{type_name}.{field}"),
            expected: "uuid".to_string(),
            actual: mismatch_actual(value, &FieldType::Uuid),
        });
        return;
    };
    let Some(actual) = uid_to_type.get(&uid) else {
        report.errors.push(ValidationError::MissingReference {
            field: format!("{type_name}.{field}"),
            target: uid,
        });
        return;
    };
    if actual.as_str() != target {
        report.errors.push(ValidationError::ReferenceTypeMismatch {
            field: format!("{type_name}.{field}"),
            target: uid,
            expected: target.to_string(),
            actual: actual.to_string(),
        });
    }
}

fn validate_string_constraints(
    type_name: &TypeName,
    field: &str,
    field_schema: &crate::ir::FieldSchema,
    value: &Value,
    pattern_cache: &BTreeMap<String, Regex>,
    report: &mut ValidationReport,
) {
    if field_schema.format.is_none() && field_schema.pattern.is_none() {
        return;
    }

    let Some(raw) = value.as_str() else {
        report.errors.push(ValidationError::InvalidValue {
            field: format!("{type_name}.{field}"),
            expected: "string".to_string(),
            actual: value_type_label(value),
        });
        return;
    };

    if let Some(format) = &field_schema.format {
        if !matches_format(format, raw) {
            report.errors.push(ValidationError::InvalidValue {
                field: format!("{type_name}.{field}"),
                expected: format_label(format),
                actual: raw.to_string(),
            });
        }
    }

    if let Some(pattern) = &field_schema.pattern {
        // malformed patterns are absent from the cache (already reported); skip.
        if let Some(regex) = pattern_cache.get(pattern) {
            if !regex.is_match(raw) {
                report.errors.push(ValidationError::InvalidValue {
                    field: format!("{type_name}.{field}"),
                    expected: format!("pattern({pattern})"),
                    actual: raw.to_string(),
                });
            }
        }
    }
}

fn slug_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(format_regex(&FieldFormat::Slug)).unwrap())
}

fn mac_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(format_regex(&FieldFormat::Mac)).unwrap())
}

// rfc 3339 spells every numeric field as `DIGIT`, which is ascii 0-9. `\d` in the
// `regex` crate is unicode by default, so it would also match `٥` and every other
// `\p{Nd}`; the classes are written out like `slug_regex` and `mac_regex` already
// do. this is not only a spec point: every group but the fractional seconds is
// re-read by `parse::<u32>()`, which refuses a non-ascii digit, so the fraction
// was the one place a `\d` reached the verdict on its own.
fn date_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^([0-9]{4})-([0-9]{2})-([0-9]{2})$").unwrap())
}

fn time_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^([0-9]{2}):([0-9]{2}):([0-9]{2})(?:\.[0-9]+)?$").unwrap())
}

fn datetime_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        // a lowercase zone is rejected like a leap second is: django takes any
        // separator character, but only an uppercase `Z` (measured, see the
        // module comment on `is_rfc3339_date`).
        Regex::new(
            r"^([0-9]{4}-[0-9]{2}-[0-9]{2})[Tt]([0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?)(?:Z|([+-][0-9]{2}:[0-9]{2}))?$",
        )
        .unwrap()
    })
}

/// rfc 3339 `full-date`, calendar included: a shape alone accepts `2026-02-30`.
///
/// rfc 3339 is the whole rule, and the ir stays vendor-neutral by fixing one
/// canonical shape rather than tracking any backend's parser. that makes the ir
/// deliberately **tighter** than the backends in places, which is the point: the
/// django adapter writes a fixture that `manage.py loaddata` parses, and django
/// 6.0 also takes `2026-8-1`, `22:00`, a space separator and `+0200`. none of
/// those are rfc 3339 and none are accepted here.
///
/// three values rfc 3339 itself permits are refused, and there the reason is the
/// backend rather than the spec: they pass `validate` and then fail at apply.
/// measured against django 6.0.8, `<Date|Time|DateTime>Field::to_python`, which
/// is the call `loaddata` makes:
///
/// | value | django |
/// | --- | --- |
/// | `0000-01-01` | refused |
/// | `23:59:60` | refused |
/// | `2026-08-01T22:00:00z` | refused (only the separator may be lowercase) |
///
/// `crates/alembic-cli/tests/django_e2e.rs` drives the accepted shapes through a
/// real `loaddata`, so the half of this that says "django takes it" is a test and
/// not a claim.
fn is_rfc3339_date(raw: &str) -> bool {
    let Some(caps) = date_regex().captures(raw) else {
        return false;
    };
    let (Ok(year), Ok(month), Ok(day)) = (
        caps[1].parse::<u32>(),
        caps[2].parse::<u32>(),
        caps[3].parse::<u32>(),
    ) else {
        return false;
    };
    // year 0 is rejected like a leap second is, and for the same reason: django's
    // date columns cannot hold it. `[0-9]{4}` already caps the top end at 9999,
    // which django takes.
    // a month outside 1..=12 has no days, so the day check rejects it too.
    year != 0 && (1..=days_in_month(year, month)).contains(&day)
}

/// rfc 3339 `partial-time`, with optional fractional seconds.
fn is_rfc3339_time(raw: &str) -> bool {
    let Some(caps) = time_regex().captures(raw) else {
        return false;
    };
    // a leap second (`:60`) is rejected: django's `TimeField` and `DateTimeField`
    // cannot hold one (see `is_rfc3339_date`).
    digits_within(&caps[1], 23) && digits_within(&caps[2], 59) && digits_within(&caps[3], 59)
}

/// rfc 3339 `date-time`, except that the offset is optional (see `type_check`).
fn is_rfc3339_datetime(raw: &str) -> bool {
    let Some(caps) = datetime_regex().captures(raw) else {
        return false;
    };
    is_rfc3339_date(&caps[1])
        && is_rfc3339_time(&caps[2])
        && caps
            .get(3)
            .is_none_or(|offset| is_rfc3339_offset(offset.as_str()))
}

/// the shape (`+HH:MM`) is fixed by the regex; only the ranges are left open.
fn is_rfc3339_offset(raw: &str) -> bool {
    let Some((hour, minute)) = raw[1..].split_once(':') else {
        return false;
    };
    digits_within(hour, 23) && digits_within(minute, 59)
}

fn digits_within(raw: &str, max: u32) -> bool {
    matches!(raw.parse::<u32>(), Ok(value) if value <= max)
}

fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        // the full gregorian rule: 2100 is not a leap year, 2000 is.
        2 if year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400)) => {
            29
        }
        2 => 28,
        _ => 0,
    }
}

fn matches_format(format: &FieldFormat, raw: &str) -> bool {
    match format_check(format) {
        FormatCheck::Slug => slug_regex().is_match(raw),
        FormatCheck::IpAddress => raw.parse::<IpAddr>().is_ok(),
        FormatCheck::IpNet => raw.parse::<IpNet>().is_ok(),
        FormatCheck::Mac => mac_regex().is_match(raw),
        FormatCheck::Uuid => Uid::parse_str(raw).is_ok(),
    }
}

/// the check a format resolves to. `matches_format` dispatches on this rather
/// than on the format, so two formats sharing a variant are one predicate under
/// two names, and `TextCheck::is` compares checks without a second table to keep
/// in step.
#[derive(PartialEq, Eq)]
enum FormatCheck {
    Slug,
    IpAddress,
    IpNet,
    Mac,
    Uuid,
}

fn format_check(format: &FieldFormat) -> FormatCheck {
    match format {
        FieldFormat::Slug => FormatCheck::Slug,
        FieldFormat::IpAddress => FormatCheck::IpAddress,
        FieldFormat::Cidr | FieldFormat::Prefix => FormatCheck::IpNet,
        FieldFormat::Mac => FormatCheck::Mac,
        FieldFormat::Uuid => FormatCheck::Uuid,
    }
}

/// the regex a backend with no native type for `format` should install to
/// enforce it. the contract: every entry is the widest regex that still accepts
/// everything `matches_format` accepts. looser is safe (core rejected a bad
/// value long before the backend saw it); stricter is a bug, because the
/// backend would then reject a value alembic's own validator passed, and the
/// operator would only learn at apply time. a test below walks a corpus to hold
/// the two sides to that.
pub fn format_regex(format: &FieldFormat) -> &'static str {
    match format {
        FieldFormat::Slug => r"^[a-z0-9]+(?:[a-z0-9_-]*[a-z0-9])?$",
        // `IpAddr` parses ipv4-mapped ipv6 (`::ffff:192.168.0.1`), which mixes
        // colons and dots, so the ipv6 arm takes both.
        FieldFormat::IpAddress => r"^([0-9]{1,3}\.){3}[0-9]{1,3}$|^[0-9a-fA-F:.]+$",
        FieldFormat::Cidr | FieldFormat::Prefix => r"^[0-9a-fA-F:\./]+$",
        FieldFormat::Mac => r"^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$",
        // `Uuid::parse_str` takes the simple, braced and urn spellings as well
        // as the hyphenated one, so the canonical 8-4-4-4-12 form alone would
        // be stricter than the check.
        FieldFormat::Uuid => {
            r"^(?:urn:uuid:)?\{?[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}\}?$"
        }
    }
}

/// the format a field type carries in its own right, for the types `type_check`
/// validates through `matches_format`. `None` for the rest, `ip_address`
/// included: core checks that one as a plain string, so any regex would be
/// stricter than the check.
pub fn format_for_field_type(field_type: &FieldType) -> Option<FieldFormat> {
    match field_type {
        FieldType::Uuid => Some(FieldFormat::Uuid),
        FieldType::Cidr => Some(FieldFormat::Cidr),
        FieldType::Prefix => Some(FieldFormat::Prefix),
        FieldType::Mac => Some(FieldFormat::Mac),
        FieldType::Slug => Some(FieldFormat::Slug),
        FieldType::String
        | FieldType::Text
        | FieldType::Int
        | FieldType::Float
        | FieldType::Bool
        | FieldType::Date
        | FieldType::Datetime
        | FieldType::Time
        | FieldType::Json
        | FieldType::IpAddress
        | FieldType::Enum { .. }
        | FieldType::List { .. }
        | FieldType::Map { .. }
        | FieldType::Ref { .. }
        | FieldType::ListRef { .. } => None,
    }
}

fn format_label(format: &FieldFormat) -> String {
    match format {
        FieldFormat::Slug => "format(slug)".to_string(),
        FieldFormat::IpAddress => "format(ip_address)".to_string(),
        FieldFormat::Cidr => "format(cidr)".to_string(),
        FieldFormat::Prefix => "format(prefix)".to_string(),
        FieldFormat::Mac => "format(mac)".to_string(),
        FieldFormat::Uuid => "format(uuid)".to_string(),
    }
}

/// the textual shape a field type holds its value to. `ValueCheck::Text` carries
/// this rather than a bare predicate so `validate_schema_format_agreement` can read
/// what a type checks off that one table instead of keeping its own.
enum TextCheck {
    Format(FieldFormat),
    Rfc3339Date,
    Rfc3339Datetime,
    Rfc3339Time,
}

impl TextCheck {
    fn matches(&self, raw: &str) -> bool {
        match self {
            TextCheck::Format(format) => matches_format(format, raw),
            TextCheck::Rfc3339Date => is_rfc3339_date(raw),
            TextCheck::Rfc3339Datetime => is_rfc3339_datetime(raw),
            TextCheck::Rfc3339Time => is_rfc3339_time(raw),
        }
    }

    /// whether a declared format is this same check. no `FieldFormat` is the rfc
    /// 3339 check, so every format disagrees with one of those types.
    fn is(&self, format: &FieldFormat) -> bool {
        match self {
            TextCheck::Format(own) => format_check(own) == format_check(format),
            TextCheck::Rfc3339Date | TextCheck::Rfc3339Datetime | TextCheck::Rfc3339Time => false,
        }
    }

    fn label(&self) -> String {
        match self {
            TextCheck::Format(format) => format_label(format),
            TextCheck::Rfc3339Date | TextCheck::Rfc3339Datetime | TextCheck::Rfc3339Time => {
                "rfc 3339".to_string()
            }
        }
    }
}

/// how a field type checks its value: as a textual check on a string, or as a
/// json-type test. the one list `value_matches_type` and `mismatch_actual` read.
enum ValueCheck {
    Text(TextCheck),
    Json(fn(&Value) -> bool),
}

fn type_check(field_type: &FieldType) -> ValueCheck {
    match field_type {
        // format-typed fields with an unambiguous textual format must hold a
        // string that matches it, mirroring how the `format:` constraint validates.
        FieldType::Uuid => ValueCheck::Text(TextCheck::Format(FieldFormat::Uuid)),
        FieldType::Cidr => ValueCheck::Text(TextCheck::Format(FieldFormat::Cidr)),
        FieldType::Prefix => ValueCheck::Text(TextCheck::Format(FieldFormat::Prefix)),
        FieldType::Mac => ValueCheck::Text(TextCheck::Format(FieldFormat::Mac)),
        FieldType::Slug => ValueCheck::Text(TextCheck::Format(FieldFormat::Slug)),
        // rfc 3339. the offset on `datetime` is optional: the ir is vendor-neutral and
        // django and netbox both take a naive one, so requiring it would reject what they accept.
        FieldType::Date => ValueCheck::Text(TextCheck::Rfc3339Date),
        FieldType::Datetime => ValueCheck::Text(TextCheck::Rfc3339Datetime),
        FieldType::Time => ValueCheck::Text(TextCheck::Rfc3339Time),
        FieldType::String
        | FieldType::Text
        // `ip_address` stays a plain string check: the canonical IPAM examples
        // carry NetBox-style masked addresses (`10.0.0.10/24`) that the strict
        // `IpAddr` format rejects, so whether it should accept a mask is a
        // convention decision left to the maintainer rather than guessed here.
        | FieldType::IpAddress
        | FieldType::Enum { .. } => ValueCheck::Json(Value::is_string),
        FieldType::Int => ValueCheck::Json(|value| value.is_i64() || value.is_u64()),
        FieldType::Float => {
            ValueCheck::Json(|value| value.as_f64().is_some() || value.is_i64() || value.is_u64())
        }
        FieldType::Bool => ValueCheck::Json(Value::is_boolean),
        FieldType::List { .. } => ValueCheck::Json(Value::is_array),
        FieldType::Map { .. } => ValueCheck::Json(Value::is_object),
        FieldType::Json | FieldType::Ref { .. } | FieldType::ListRef { .. } => {
            ValueCheck::Json(|_| true)
        }
    }
}

fn value_matches_type(value: &Value, field_type: &FieldType) -> bool {
    match type_check(field_type) {
        ValueCheck::Text(check) => value.as_str().is_some_and(|raw| check.matches(raw)),
        ValueCheck::Json(check) => check(value),
    }
}

fn field_type_label(field_type: &FieldType) -> String {
    match field_type {
        FieldType::String => "string".to_string(),
        FieldType::Text => "text".to_string(),
        FieldType::Int => "int".to_string(),
        FieldType::Float => "float".to_string(),
        FieldType::Bool => "bool".to_string(),
        FieldType::Uuid => "uuid".to_string(),
        FieldType::Date => "date".to_string(),
        FieldType::Datetime => "datetime".to_string(),
        FieldType::Time => "time".to_string(),
        FieldType::Json => "json".to_string(),
        FieldType::IpAddress => "ip_address".to_string(),
        FieldType::Cidr => "cidr".to_string(),
        FieldType::Prefix => "prefix".to_string(),
        FieldType::Mac => "mac".to_string(),
        FieldType::Slug => "slug".to_string(),
        FieldType::Enum { .. } => "enum".to_string(),
        FieldType::List { .. } => "list".to_string(),
        FieldType::Map { .. } => "map".to_string(),
        FieldType::Ref { target } => format!("ref({target})"),
        FieldType::ListRef { target } => format!("list_ref({target})"),
    }
}

fn value_type_label(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(_) => "bool".to_string(),
        Value::Number(_) => "number".to_string(),
        Value::String(_) => "string".to_string(),
        Value::Array(_) => "array".to_string(),
        Value::Object(_) => "object".to_string(),
    }
}

/// which half of a failed check to name: a string failing a textual check failed
/// the format, so it echoes the value; anything else names its json type.
fn mismatch_actual(value: &Value, field_type: &FieldType) -> String {
    match (value.as_str(), type_check(field_type)) {
        (Some(raw), ValueCheck::Text(_)) => raw.to_string(),
        _ => value_type_label(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{
        FieldFormat, FieldSchema, FieldType, JsonMap, Key, Object, Schema, TypeName, TypeSchema,
    };
    use serde_json::json;
    use std::collections::BTreeMap;
    use uuid::Uuid;

    fn uid(value: u128) -> Uid {
        Uuid::from_u128(value)
    }

    #[test]
    fn detects_duplicate_keys() {
        let mut key = BTreeMap::new();
        key.insert("slug".to_string(), serde_json::json!("fra1"));
        let key = Key::from(key);
        let type_schema = TypeSchema {
            key: BTreeMap::from([(
                "slug".to_string(),
                FieldSchema {
                    r#type: FieldType::Slug,
                    required: true,
                    nullable: false,
                    description: None,
                    format: None,
                    pattern: None,
                },
            )]),
            fields: BTreeMap::new(),
        };
        let objects = vec![
            Object::new(
                uid(1),
                TypeName::new("site"),
                key.clone(),
                JsonMap::default(),
            )
            .unwrap(),
            Object::new(uid(2), TypeName::new("site"), key, JsonMap::default()).unwrap(),
        ];
        let report = validate_inventory(&Inventory {
            scope: None,
            schema: Schema {
                types: BTreeMap::from([("site".to_string(), type_schema)]),
            },
            objects,
        });
        assert!(report
            .errors
            .iter()
            .any(|err| matches!(err, ValidationError::DuplicateKey(_))));
    }

    #[test]
    fn detects_missing_key() {
        let objects = vec![Object {
            uid: uid(30),
            type_name: TypeName::new("site"),
            key: Key::default(),
            attrs: JsonMap::default(),
            source: None,
        }];
        let report = validate_inventory(&Inventory {
            scope: None,
            schema: Schema {
                types: BTreeMap::from([(
                    "site".to_string(),
                    TypeSchema {
                        key: BTreeMap::new(),
                        fields: BTreeMap::new(),
                    },
                )]),
            },
            objects,
        });
        assert!(report
            .errors
            .iter()
            .any(|err| matches!(err, ValidationError::MissingKey)));
    }

    #[test]
    fn detects_missing_kind() {
        let mut key = BTreeMap::new();
        key.insert("slug".to_string(), serde_json::json!("fra1"));
        let objects = vec![Object {
            uid: uid(31),
            type_name: TypeName::new(""),
            key: Key::from(key),
            attrs: JsonMap::default(),
            source: None,
        }];
        let report = validate_inventory(&Inventory {
            scope: None,
            schema: Schema {
                types: BTreeMap::new(),
            },
            objects,
        });
        assert!(report
            .errors
            .iter()
            .any(|err| matches!(err, ValidationError::MissingType)));
    }

    #[test]
    fn detects_unknown_type() {
        let mut key = BTreeMap::new();
        key.insert("slug".to_string(), serde_json::json!("leaf01"));
        let objects = vec![Object::new(
            uid(40),
            TypeName::new("device"),
            Key::from(key),
            JsonMap::default(),
        )
        .unwrap()];
        let schema = Schema {
            types: BTreeMap::new(),
        };
        let report = validate_inventory(&Inventory {
            scope: None,
            schema,
            objects,
        });
        assert!(report
            .errors
            .iter()
            .any(|err| matches!(err, ValidationError::UnknownType(_))));
    }

    fn slug_key_schema() -> TypeSchema {
        TypeSchema {
            key: BTreeMap::from([(
                "slug".to_string(),
                FieldSchema {
                    r#type: FieldType::Slug,
                    required: true,
                    nullable: false,
                    description: None,
                    format: None,
                    pattern: None,
                },
            )]),
            fields: BTreeMap::new(),
        }
    }

    #[test]
    fn detects_duplicate_uid() {
        // same uid, distinct keys -> DuplicateUid fires but DuplicateKey does not
        let mk = |slug: &str| {
            let mut k = BTreeMap::new();
            k.insert("slug".to_string(), json!(slug));
            Object::new(
                uid(1),
                TypeName::new("site"),
                Key::from(k),
                JsonMap::default(),
            )
            .unwrap()
        };
        let report = validate_inventory(&Inventory {
            scope: None,
            schema: Schema {
                types: BTreeMap::from([("site".to_string(), slug_key_schema())]),
            },
            objects: vec![mk("fra1"), mk("ber1")],
        });
        assert!(report
            .errors
            .iter()
            .any(|err| matches!(err, ValidationError::DuplicateUid(_))));
    }

    #[test]
    fn detects_missing_key_field() {
        // non-empty key (so MissingKey does not fire) that lacks the declared `slug`
        let mut k = BTreeMap::new();
        k.insert("other".to_string(), json!("x"));
        let objects = vec![Object {
            uid: uid(50),
            type_name: TypeName::new("site"),
            key: Key::from(k),
            attrs: JsonMap::default(),
            source: None,
        }];
        let report = validate_inventory(&Inventory {
            scope: None,
            schema: Schema {
                types: BTreeMap::from([("site".to_string(), slug_key_schema())]),
            },
            objects,
        });
        assert!(report
            .errors
            .iter()
            .any(|err| matches!(err, ValidationError::MissingKeyField { .. })));
    }

    #[test]
    fn detects_extra_key_field() {
        // schema declares no key fields; object carries one -> ExtraKeyField
        let mut k = BTreeMap::new();
        k.insert("slug".to_string(), json!("fra1"));
        let objects = vec![Object {
            uid: uid(51),
            type_name: TypeName::new("site"),
            key: Key::from(k),
            attrs: JsonMap::default(),
            source: None,
        }];
        let report = validate_inventory(&Inventory {
            scope: None,
            schema: Schema {
                types: BTreeMap::from([(
                    "site".to_string(),
                    TypeSchema {
                        key: BTreeMap::new(),
                        fields: BTreeMap::new(),
                    },
                )]),
            },
            objects,
        });
        assert!(report
            .errors
            .iter()
            .any(|err| matches!(err, ValidationError::ExtraKeyField { .. })));
    }

    #[test]
    fn detects_missing_attr_field() {
        // required attr `name` declared, absent from attrs -> MissingAttrField
        let type_schema = TypeSchema {
            key: slug_key_schema().key,
            fields: BTreeMap::from([(
                "name".to_string(),
                FieldSchema {
                    r#type: FieldType::String,
                    required: true,
                    nullable: false,
                    description: None,
                    format: None,
                    pattern: None,
                },
            )]),
        };
        let mut k = BTreeMap::new();
        k.insert("slug".to_string(), json!("fra1"));
        let objects = vec![Object::new(
            uid(52),
            TypeName::new("site"),
            Key::from(k),
            JsonMap::default(),
        )
        .unwrap()];
        let report = validate_inventory(&Inventory {
            scope: None,
            schema: Schema {
                types: BTreeMap::from([("site".to_string(), type_schema)]),
            },
            objects,
        });
        assert!(report
            .errors
            .iter()
            .any(|err| matches!(err, ValidationError::MissingAttrField { .. })));
    }

    #[test]
    fn detects_extra_attr_field() {
        // attr `color` not declared in schema.fields -> ExtraAttrField
        let mut k = BTreeMap::new();
        k.insert("slug".to_string(), json!("fra1"));
        let mut attrs = BTreeMap::new();
        attrs.insert("color".to_string(), json!("blue"));
        let objects = vec![Object::new(
            uid(53),
            TypeName::new("site"),
            Key::from(k),
            JsonMap::from(attrs),
        )
        .unwrap()];
        let report = validate_inventory(&Inventory {
            scope: None,
            schema: Schema {
                types: BTreeMap::from([("site".to_string(), slug_key_schema())]),
            },
            objects,
        });
        assert!(report
            .errors
            .iter()
            .any(|err| matches!(err, ValidationError::ExtraAttrField { .. })));
    }

    #[test]
    fn detects_missing_references_with_schema() {
        let mut key_fields = BTreeMap::new();
        key_fields.insert(
            "slug".to_string(),
            FieldSchema {
                r#type: FieldType::Slug,
                required: true,
                nullable: false,
                description: None,
                format: None,
                pattern: None,
            },
        );
        let mut fields = BTreeMap::new();
        fields.insert(
            "owner".to_string(),
            FieldSchema {
                r#type: FieldType::Ref {
                    target: "person".to_string(),
                },
                required: false,
                nullable: false,
                description: None,
                format: None,
                pattern: None,
            },
        );
        let mut types = BTreeMap::new();
        types.insert(
            "device".to_string(),
            TypeSchema {
                key: key_fields,
                fields,
            },
        );
        let schema = Schema { types };

        let mut attrs = BTreeMap::new();
        attrs.insert(
            "owner".to_string(),
            serde_json::json!(Uuid::from_u128(99).to_string()),
        );
        let mut key = BTreeMap::new();
        key.insert("slug".to_string(), serde_json::json!("leaf01"));
        let objects = vec![Object::new(
            uid(41),
            TypeName::new("device"),
            Key::from(key),
            attrs.into(),
        )
        .unwrap()];
        let report = validate_inventory(&Inventory {
            scope: None,
            schema,
            objects,
        });
        assert!(report
            .errors
            .iter()
            .any(|err| matches!(err, ValidationError::MissingReference { .. })));
    }

    /// build a non-required field of the given type, for schema-shape tests.
    fn schema_field(r#type: FieldType) -> FieldSchema {
        FieldSchema {
            r#type,
            required: false,
            nullable: false,
            description: None,
            format: None,
            pattern: None,
        }
    }

    fn nullable_field(r#type: FieldType) -> FieldSchema {
        FieldSchema {
            nullable: true,
            ..schema_field(r#type)
        }
    }

    /// run schema-only validation (no objects) and return the report.
    fn validate_schema(types: BTreeMap<String, TypeSchema>) -> ValidationReport {
        validate_inventory(&Inventory {
            scope: None,
            schema: Schema { types },
            objects: vec![],
        })
    }

    /// a `dcim.site` keyed on a `slug` field, with the given scope, no objects.
    fn validate_scoped(scope: crate::ir::Scope) -> ValidationReport {
        let site = TypeSchema {
            key: BTreeMap::from([("slug".to_string(), schema_field(FieldType::Slug))]),
            fields: BTreeMap::new(),
        };
        validate_inventory(&Inventory {
            schema: Schema {
                types: BTreeMap::from([("dcim.site".to_string(), site)]),
            },
            scope: Some(scope),
            objects: vec![],
        })
    }

    fn scope_of(type_name: &str, entry: &[(&str, serde_json::Value)]) -> crate::ir::Scope {
        crate::ir::Scope(BTreeMap::from([(
            type_name.to_string(),
            entry
                .iter()
                .map(|(field, value)| {
                    (
                        field.to_string(),
                        crate::ir::ScopeValues::One(value.clone()),
                    )
                })
                .collect(),
        )]))
    }

    #[test]
    fn accepts_a_scope_on_a_declared_key_field() {
        let report = validate_scoped(scope_of(
            "dcim.site",
            &[("slug", serde_json::json!("fra1"))],
        ));
        assert!(report.errors.is_empty(), "{:?}", report.errors);
    }

    #[test]
    fn accepts_an_empty_scope_entry() {
        // an empty entry asserts completeness over the whole type.
        let report = validate_scoped(scope_of("dcim.site", &[]));
        assert!(report.errors.is_empty(), "{:?}", report.errors);
    }

    #[test]
    fn rejects_a_scope_on_an_undeclared_type() {
        let report = validate_scoped(scope_of("dcim.device", &[]));
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::ScopeUnknownType { type_name } if type_name == "dcim.device"
        )));
    }

    #[test]
    fn rejects_a_scope_on_a_non_key_field() {
        let report = validate_scoped(scope_of(
            "dcim.site",
            &[("name", serde_json::json!("FRA1"))],
        ));
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::ScopeNonKeyField { type_name, field }
                if type_name == "dcim.site" && field == "name"
        )));
    }

    #[test]
    fn rejects_a_scope_value_the_key_field_could_not_hold() {
        let report = validate_scoped(scope_of(
            "dcim.site",
            &[("slug", serde_json::json!("FRA 1"))],
        ));
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::ScopeInvalidValue { type_name, field, .. }
                if type_name == "dcim.site" && field == "slug"
        )));
    }

    #[test]
    fn rejects_a_composite_scope_value() {
        // a `json` key can hold an array, but an array at the constraint
        // position is always a list of allowed values — so a composite value
        // inside one is rejected rather than silently matching the scalars it
        // contains and widening delete authority.
        let window = TypeSchema {
            key: BTreeMap::from([("identity".to_string(), schema_field(FieldType::Json))]),
            fields: BTreeMap::new(),
        };
        let schema = Schema {
            types: BTreeMap::from([("ops.window".to_string(), window)]),
        };
        let scope: crate::ir::Scope =
            serde_json::from_value(serde_json::json!({ "ops.window": { "identity": [[1, 2]] } }))
                .unwrap();
        let report = validate_inventory(&Inventory {
            schema: schema.clone(),
            scope: Some(scope),
            objects: vec![],
        });
        assert!(
            report.errors.iter().any(|e| matches!(
                e,
                ValidationError::ScopeInvalidValue { field, expected, .. }
                    if field == "identity" && expected == "a scalar value"
            )),
            "{:?}",
            report.errors
        );
        // an object-valued constraint is the same ambiguity through `One`.
        let scope: crate::ir::Scope =
            serde_json::from_value(serde_json::json!({ "ops.window": { "identity": {"a": 1} } }))
                .unwrap();
        let report = validate_inventory(&Inventory {
            schema: schema.clone(),
            scope: Some(scope),
            objects: vec![],
        });
        assert!(report
            .errors
            .iter()
            .any(|e| matches!(e, ValidationError::ScopeInvalidValue { .. })));
        // scalar json values stay legal.
        let scope: crate::ir::Scope =
            serde_json::from_value(serde_json::json!({ "ops.window": { "identity": [1, 2] } }))
                .unwrap();
        let report = validate_inventory(&Inventory {
            schema,
            scope: Some(scope),
            objects: vec![],
        });
        assert!(report.errors.is_empty(), "{:?}", report.errors);
    }

    #[test]
    fn a_ref_scope_value_needs_only_uid_shape() {
        // the target may be an object the inventory does not manage, so the uid
        // is not required to resolve; a non-uid still fails.
        let device = TypeSchema {
            key: BTreeMap::from([(
                "site".to_string(),
                schema_field(FieldType::Ref {
                    target: "dcim.site".to_string(),
                }),
            )]),
            fields: BTreeMap::new(),
        };
        let site = TypeSchema {
            key: BTreeMap::from([("slug".to_string(), schema_field(FieldType::Slug))]),
            fields: BTreeMap::new(),
        };
        let schema = Schema {
            types: BTreeMap::from([
                ("dcim.device".to_string(), device),
                ("dcim.site".to_string(), site),
            ]),
        };
        let ok = validate_inventory(&Inventory {
            schema: schema.clone(),
            scope: Some(scope_of(
                "dcim.device",
                &[("site", serde_json::json!(uid(7).to_string()))],
            )),
            objects: vec![],
        });
        assert!(ok.errors.is_empty(), "{:?}", ok.errors);
        let bad = validate_inventory(&Inventory {
            schema,
            scope: Some(scope_of(
                "dcim.device",
                &[("site", serde_json::json!("not-a-uid"))],
            )),
            objects: vec![],
        });
        assert!(bad.errors.iter().any(|e| matches!(
            e,
            ValidationError::ScopeInvalidValue { field, .. } if field == "site"
        )));
    }

    #[test]
    fn detects_unknown_ref_target_in_attr_field() {
        let device = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::from([(
                "owner".to_string(),
                schema_field(FieldType::Ref {
                    target: "person".to_string(),
                }),
            )]),
        };
        let report = validate_schema(BTreeMap::from([("device".to_string(), device)]));
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::UnknownRefTarget { type_name, field, target }
                if type_name == "device" && field == "owner" && target == "person"
        )));
    }

    #[test]
    fn detects_unknown_ref_target_in_key_field() {
        let device = TypeSchema {
            key: BTreeMap::from([(
                "site".to_string(),
                schema_field(FieldType::Ref {
                    target: "place".to_string(),
                }),
            )]),
            fields: BTreeMap::new(),
        };
        let report = validate_schema(BTreeMap::from([("device".to_string(), device)]));
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::UnknownRefTarget { field, target, .. }
                if field == "key.site" && target == "place"
        )));
    }

    #[test]
    fn detects_unknown_ref_target_nested_in_list_and_map() {
        let group = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::from([
                (
                    "members".to_string(),
                    schema_field(FieldType::List {
                        item: Box::new(FieldType::Ref {
                            target: "ghost".to_string(),
                        }),
                    }),
                ),
                (
                    "roles".to_string(),
                    schema_field(FieldType::Map {
                        value: Box::new(FieldType::ListRef {
                            target: "phantom".to_string(),
                        }),
                    }),
                ),
            ]),
        };
        let report = validate_schema(BTreeMap::from([("group".to_string(), group)]));
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::UnknownRefTarget { target, .. } if target == "ghost"
        )));
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::UnknownRefTarget { target, .. } if target == "phantom"
        )));
    }

    #[test]
    fn valid_ref_targets_pass_schema_validation() {
        let device = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::from([
                (
                    "owner".to_string(),
                    schema_field(FieldType::Ref {
                        target: "person".to_string(),
                    }),
                ),
                (
                    "watchers".to_string(),
                    schema_field(FieldType::List {
                        item: Box::new(FieldType::ListRef {
                            target: "person".to_string(),
                        }),
                    }),
                ),
            ]),
        };
        let person = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::new(),
        };
        let report = validate_schema(BTreeMap::from([
            ("device".to_string(), device),
            ("person".to_string(), person),
        ]));
        assert!(!report
            .errors
            .iter()
            .any(|e| matches!(e, ValidationError::UnknownRefTarget { .. })));
    }

    #[test]
    fn detects_composite_key_field_type() {
        // schema-load rejects all three composite key types (see
        // validate_schema_key_scalar)
        let group = TypeSchema {
            key: BTreeMap::from([(
                "members".to_string(),
                schema_field(FieldType::List {
                    item: Box::new(FieldType::String),
                }),
            )]),
            fields: BTreeMap::new(),
        };
        let dict = TypeSchema {
            key: BTreeMap::from([(
                "labels".to_string(),
                schema_field(FieldType::Map {
                    value: Box::new(FieldType::String),
                }),
            )]),
            fields: BTreeMap::new(),
        };
        let device = TypeSchema {
            key: BTreeMap::from([(
                "peers".to_string(),
                schema_field(FieldType::ListRef {
                    target: "device".to_string(),
                }),
            )]),
            fields: BTreeMap::new(),
        };
        let report = validate_schema(BTreeMap::from([
            ("group".to_string(), group),
            ("dict".to_string(), dict),
            ("device".to_string(), device),
        ]));
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::NonScalarKeyField { type_name, field, field_type }
                if type_name == "group" && field == "key.members" && field_type == "list"
        )));
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::NonScalarKeyField { type_name, field, field_type }
                if type_name == "dict" && field == "key.labels" && field_type == "map"
        )));
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::NonScalarKeyField { type_name, field, field_type }
                if type_name == "device"
                    && field == "key.peers"
                    && field_type == "list_ref(device)"
        )));
    }

    #[test]
    fn accepts_ref_key_field_type() {
        // a `ref` key renders to a scalar uid string and stays legal
        let device = TypeSchema {
            key: BTreeMap::from([("name".to_string(), schema_field(FieldType::String))]),
            fields: BTreeMap::new(),
        };
        let interface = TypeSchema {
            key: BTreeMap::from([
                (
                    "device".to_string(),
                    schema_field(FieldType::Ref {
                        target: "device".to_string(),
                    }),
                ),
                ("name".to_string(), schema_field(FieldType::String)),
            ]),
            fields: BTreeMap::new(),
        };
        let report = validate_schema(BTreeMap::from([
            ("device".to_string(), device),
            ("interface".to_string(), interface),
        ]));
        assert!(!report
            .errors
            .iter()
            .any(|e| matches!(e, ValidationError::NonScalarKeyField { .. })));
    }

    #[test]
    fn accepts_scalar_key_field_types() {
        // every scalar-producing built-in stays legal as a key field type.
        let widget = TypeSchema {
            key: BTreeMap::from([
                ("s".to_string(), schema_field(FieldType::String)),
                ("i".to_string(), schema_field(FieldType::Int)),
                ("g".to_string(), schema_field(FieldType::Slug)),
                ("u".to_string(), schema_field(FieldType::Uuid)),
                (
                    "e".to_string(),
                    schema_field(FieldType::Enum {
                        values: vec!["a".to_string()],
                    }),
                ),
            ]),
            fields: BTreeMap::new(),
        };
        let report = validate_schema(BTreeMap::from([("widget".to_string(), widget)]));
        assert!(!report
            .errors
            .iter()
            .any(|e| matches!(e, ValidationError::NonScalarKeyField { .. })));
    }

    #[test]
    fn scalar_key_field_with_composite_value_is_rejected() {
        // the type-level check is not the only guard: a scalar-typed key whose
        // object supplies a composite value is already rejected by per-object
        // value-type validation, so a composite key value never reaches uid
        // derivation either. the schema is scalar-keyed, so no type-level error
        // fires here -- only the per-object value error.
        let device = TypeSchema {
            key: BTreeMap::from([("name".to_string(), schema_field(FieldType::String))]),
            fields: BTreeMap::new(),
        };
        let mut key = BTreeMap::new();
        key.insert("name".to_string(), serde_json::json!(["a", "b"]));
        let object = Object::new(
            uid(1),
            TypeName::new("device"),
            Key::from(key),
            JsonMap::default(),
        )
        .unwrap();
        let report = validate_inventory(&Inventory {
            scope: None,
            schema: Schema {
                types: BTreeMap::from([("device".to_string(), device)]),
            },
            objects: vec![object],
        });
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::InvalidValue { field, expected, actual }
                if field == "device.key.name" && expected == "string" && actual == "array"
        )));
        assert!(!report
            .errors
            .iter()
            .any(|e| matches!(e, ValidationError::NonScalarKeyField { .. })));
    }

    #[test]
    fn detects_nullable_key_field() {
        // schema-load rejects a nullable key field (see
        // validate_schema_key_nullable)
        let device = TypeSchema {
            key: BTreeMap::from([("slug".to_string(), nullable_field(FieldType::String))]),
            fields: BTreeMap::new(),
        };
        let report = validate_schema(BTreeMap::from([("device".to_string(), device)]));
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::NullableKeyField { type_name, field }
                if type_name == "device" && field == "key.slug"
        )));
    }

    #[test]
    fn detects_key_field_redeclared_with_a_different_schema() {
        // a field in both `key:` and `fields:` must carry one schema (see
        // validate_schema_key_field_agreement)
        let device = TypeSchema {
            key: BTreeMap::from([("name".to_string(), schema_field(FieldType::Slug))]),
            fields: BTreeMap::from([("name".to_string(), schema_field(FieldType::String))]),
        };
        let report = validate_schema(BTreeMap::from([("device".to_string(), device)]));
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::KeyFieldDisagreement { type_name, field }
                if type_name == "device" && field == "name"
        )));
    }

    #[test]
    fn accepts_a_key_field_redeclared_identically() {
        // an identical duplicate declares one schema twice; only a disagreement
        // is a bug.
        let device = TypeSchema {
            key: BTreeMap::from([("slug".to_string(), schema_field(FieldType::Slug))]),
            fields: BTreeMap::from([("slug".to_string(), schema_field(FieldType::Slug))]),
        };
        let report = validate_schema(BTreeMap::from([("device".to_string(), device)]));
        assert!(!report
            .errors
            .iter()
            .any(|e| matches!(e, ValidationError::KeyFieldDisagreement { .. })));
    }

    #[test]
    fn accepts_nullable_non_key_field() {
        // `nullable: true` stays legal on a regular (non-key) field: only a key
        // component must render to a scalar identity, so a nullable attr is fine.
        let device = TypeSchema {
            key: BTreeMap::from([("name".to_string(), schema_field(FieldType::String))]),
            fields: BTreeMap::from([("label".to_string(), nullable_field(FieldType::String))]),
        };
        let report = validate_schema(BTreeMap::from([("device".to_string(), device)]));
        assert!(!report
            .errors
            .iter()
            .any(|e| matches!(e, ValidationError::NullableKeyField { .. })));
    }

    #[test]
    fn accepts_non_nullable_key_fields() {
        // non-nullable `ref` and scalar keys must not fire a nullable-key error
        let device = TypeSchema {
            key: BTreeMap::from([("name".to_string(), schema_field(FieldType::String))]),
            fields: BTreeMap::new(),
        };
        let interface = TypeSchema {
            key: BTreeMap::from([
                (
                    "device".to_string(),
                    schema_field(FieldType::Ref {
                        target: "device".to_string(),
                    }),
                ),
                ("name".to_string(), schema_field(FieldType::String)),
                ("index".to_string(), schema_field(FieldType::Int)),
            ]),
            fields: BTreeMap::new(),
        };
        let report = validate_schema(BTreeMap::from([
            ("device".to_string(), device),
            ("interface".to_string(), interface),
        ]));
        assert!(!report
            .errors
            .iter()
            .any(|e| matches!(e, ValidationError::NullableKeyField { .. })));
    }

    #[test]
    fn detects_invalid_pattern_in_attr_field() {
        let device = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::from([("name".to_string(), pattern_field("[unclosed"))]),
        };
        let report = validate_schema(BTreeMap::from([("device".to_string(), device)]));
        let count = report
            .errors
            .iter()
            .filter(|e| {
                matches!(
                    e,
                    ValidationError::InvalidSchemaPattern { type_name, field, .. }
                        if type_name == "device" && field == "name"
                )
            })
            .count();
        assert_eq!(count, 1);
    }

    #[test]
    fn detects_invalid_pattern_in_key_field() {
        let device = TypeSchema {
            key: BTreeMap::from([("slug".to_string(), pattern_field("(unbalanced"))]),
            fields: BTreeMap::new(),
        };
        let report = validate_schema(BTreeMap::from([("device".to_string(), device)]));
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::InvalidSchemaPattern { type_name, field, .. }
                if type_name == "device" && field == "key.slug"
        )));
    }

    #[test]
    fn detects_invalid_pattern_for_type_with_no_objects() {
        // a bad pattern on a type with no objects is never reached by
        // per-object validation, but schema-load validation catches it.
        let ghost = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::from([("name".to_string(), pattern_field("[bad"))]),
        };
        let report = validate_inventory(&Inventory {
            scope: None,
            schema: Schema {
                types: BTreeMap::from([("ghost".to_string(), ghost)]),
            },
            objects: vec![],
        });
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::InvalidSchemaPattern { type_name, field, .. }
                if type_name == "ghost" && field == "name"
        )));
    }

    #[test]
    fn accumulates_invalid_patterns_across_fields_and_types() {
        let device = TypeSchema {
            key: BTreeMap::from([("slug".to_string(), pattern_field("(bad"))]),
            fields: BTreeMap::from([("name".to_string(), pattern_field("[bad"))]),
        };
        let site = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::from([("code".to_string(), pattern_field("*bad"))]),
        };
        let report = validate_schema(BTreeMap::from([
            ("device".to_string(), device),
            ("site".to_string(), site),
        ]));
        let count = report
            .errors
            .iter()
            .filter(|e| matches!(e, ValidationError::InvalidSchemaPattern { .. }))
            .count();
        assert_eq!(count, 3);
    }

    #[test]
    fn valid_and_absent_patterns_pass_schema_validation() {
        let device = TypeSchema {
            key: BTreeMap::from([("slug".to_string(), pattern_field("^[a-z0-9-]+$"))]),
            fields: BTreeMap::from([
                ("name".to_string(), pattern_field("^[A-Za-z ]+$")),
                ("count".to_string(), schema_field(FieldType::Int)),
            ]),
        };
        let report = validate_schema(BTreeMap::from([("device".to_string(), device)]));
        assert!(!report
            .errors
            .iter()
            .any(|e| matches!(e, ValidationError::InvalidSchemaPattern { .. })));
    }

    #[test]
    fn detects_format_on_int_field() {
        let mut count = schema_field(FieldType::Int);
        count.format = Some(FieldFormat::Slug);
        let device = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::from([("count".to_string(), count)]),
        };
        let report = validate_schema(BTreeMap::from([("device".to_string(), device)]));
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::ConstraintOnNonStringField { type_name, field, constraint, field_type }
                if type_name == "device"
                    && field == "count"
                    && constraint == "format"
                    && field_type == "int"
        )));
    }

    #[test]
    fn detects_pattern_on_list_field() {
        let mut tags = schema_field(FieldType::List {
            item: Box::new(FieldType::String),
        });
        tags.pattern = Some("^x$".to_string());
        let device = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::from([("tags".to_string(), tags)]),
        };
        let report = validate_schema(BTreeMap::from([("device".to_string(), device)]));
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::ConstraintOnNonStringField { type_name, field, constraint, field_type }
                if type_name == "device"
                    && field == "tags"
                    && constraint == "pattern"
                    && field_type == "list"
        )));
    }

    #[test]
    fn format_and_pattern_on_non_string_field_yield_two_errors() {
        let mut flag = schema_field(FieldType::Bool);
        flag.format = Some(FieldFormat::Slug);
        flag.pattern = Some("^x$".to_string());
        let device = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::from([("flag".to_string(), flag)]),
        };
        let report = validate_schema(BTreeMap::from([("device".to_string(), device)]));
        let count = report
            .errors
            .iter()
            .filter(|e| matches!(e, ValidationError::ConstraintOnNonStringField { .. }))
            .count();
        assert_eq!(count, 2);
    }

    #[test]
    fn detects_constraint_on_non_string_field_for_type_with_no_objects() {
        // see detects_invalid_pattern_for_type_with_no_objects; here for a
        // constraint on a never-string field.
        let mut count = schema_field(FieldType::Int);
        count.pattern = Some("^[0-9]+$".to_string());
        let ghost = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::from([("count".to_string(), count)]),
        };
        let report = validate_inventory(&Inventory {
            scope: None,
            schema: Schema {
                types: BTreeMap::from([("ghost".to_string(), ghost)]),
            },
            objects: vec![],
        });
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::ConstraintOnNonStringField { type_name, field, .. }
                if type_name == "ghost" && field == "count"
        )));
    }

    #[test]
    fn string_valued_fields_accept_format_and_pattern() {
        // string, slug, enum, and ref can all hold a string, so a format/pattern
        // on them is not a malformed schema.
        let mut name = schema_field(FieldType::String);
        name.pattern = Some("^[a-z]+$".to_string());
        let mut handle = schema_field(FieldType::Slug);
        handle.format = Some(FieldFormat::Slug);
        let mut role = schema_field(FieldType::Enum {
            values: vec!["leaf".to_string()],
        });
        role.pattern = Some("^[a-z]+$".to_string());
        let mut owner = schema_field(FieldType::Ref {
            target: "person".to_string(),
        });
        owner.format = Some(FieldFormat::Uuid);
        let device = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::from([
                ("name".to_string(), name),
                ("handle".to_string(), handle),
                ("role".to_string(), role),
                ("owner".to_string(), owner),
            ]),
        };
        let person = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::new(),
        };
        let report = validate_schema(BTreeMap::from([
            ("device".to_string(), device),
            ("person".to_string(), person),
        ]));
        assert!(!report
            .errors
            .iter()
            .any(|e| matches!(e, ValidationError::ConstraintOnNonStringField { .. })));
    }

    /// build a one-type schema whose single attr field carries `format`.
    fn formatted_field_schema(
        field_type: FieldType,
        format: FieldFormat,
    ) -> BTreeMap<String, TypeSchema> {
        let mut value = schema_field(field_type);
        value.format = Some(format);
        BTreeMap::from([(
            "device".to_string(),
            TypeSchema {
                key: BTreeMap::new(),
                fields: BTreeMap::from([("value".to_string(), value)]),
            },
        )])
    }

    fn conflicting_formats(report: &ValidationReport) -> Vec<&ValidationError> {
        report
            .errors
            .iter()
            .filter(|e| matches!(e, ValidationError::ConflictingFormat { .. }))
            .collect()
    }

    #[test]
    fn detects_format_disagreeing_with_its_field_type() {
        let report = validate_schema(formatted_field_schema(FieldType::Mac, FieldFormat::Uuid));
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::ConflictingFormat {
                type_name,
                field,
                field_type,
                format,
                type_check,
            } if type_name == "device"
                && field == "value"
                && field_type == "mac"
                && format == "format(uuid)"
                && type_check == "format(mac)"
        )));
    }

    #[test]
    fn detects_format_disagreeing_with_its_field_type_in_a_key_field() {
        let mut slug = schema_field(FieldType::Slug);
        slug.format = Some(FieldFormat::Uuid);
        let device = TypeSchema {
            key: BTreeMap::from([("slug".to_string(), slug)]),
            fields: BTreeMap::new(),
        };
        let report = validate_schema(BTreeMap::from([("device".to_string(), device)]));
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::ConflictingFormat { type_name, field, .. }
                if type_name == "device" && field == "key.slug"
        )));
    }

    #[test]
    fn a_satisfiable_disagreement_is_still_refused() {
        // the rule is the disagreement, not unsatisfiability: `aa-bb-cc-dd-ee-ff`
        // is both a mac and a slug, so the field holds values and alembic simply
        // means something the backend it provisioned does not.
        assert!(matches_format(&FieldFormat::Mac, "aa-bb-cc-dd-ee-ff"));
        assert!(matches_format(&FieldFormat::Slug, "aa-bb-cc-dd-ee-ff"));
        let report = validate_schema(formatted_field_schema(FieldType::Mac, FieldFormat::Slug));
        assert_eq!(conflicting_formats(&report).len(), 1, "{:?}", report.errors);
    }

    #[test]
    fn a_format_restating_its_field_type_is_accepted() {
        for (field_type, format) in [
            (FieldType::Uuid, FieldFormat::Uuid),
            (FieldType::Cidr, FieldFormat::Cidr),
            (FieldType::Prefix, FieldFormat::Prefix),
            (FieldType::Mac, FieldFormat::Mac),
            (FieldType::Slug, FieldFormat::Slug),
        ] {
            let report = validate_schema(formatted_field_schema(field_type.clone(), format));
            assert!(
                conflicting_formats(&report).is_empty(),
                "{field_type:?} conflicts with its own format: {:?}",
                report.errors
            );
        }
    }

    #[test]
    fn cidr_and_prefix_do_not_conflict_with_each_other() {
        for (field_type, format) in [
            (FieldType::Cidr, FieldFormat::Prefix),
            (FieldType::Prefix, FieldFormat::Cidr),
        ] {
            let report =
                validate_schema(formatted_field_schema(field_type.clone(), format.clone()));
            assert!(
                conflicting_formats(&report).is_empty(),
                "{field_type:?} conflicts with {format:?}: {:?}",
                report.errors
            );
        }
    }

    #[test]
    fn a_format_on_a_type_carrying_none_is_accepted() {
        for field_type in [
            FieldType::String,
            FieldType::Text,
            FieldType::IpAddress,
            FieldType::Json,
            FieldType::Ref {
                target: "device".to_string(),
            },
        ] {
            let report = validate_schema(formatted_field_schema(
                field_type.clone(),
                FieldFormat::Uuid,
            ));
            assert!(
                conflicting_formats(&report).is_empty(),
                "{field_type:?} conflicts with a declared format: {:?}",
                report.errors
            );
        }
    }

    #[test]
    fn a_pattern_on_a_format_typed_field_is_untouched() {
        // a pattern narrows the type rather than contradicting it, and core
        // stacking the two is the settled behaviour.
        let mut value = schema_field(FieldType::Mac);
        value.pattern = Some("^00:".to_string());
        let device = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::from([("value".to_string(), value)]),
        };
        let report = validate_schema(BTreeMap::from([("device".to_string(), device)]));
        assert!(report.errors.is_empty(), "{:?}", report.errors);
    }

    #[test]
    fn detects_a_format_on_an_rfc_3339_typed_field() {
        for (field_type, label, format) in [
            (FieldType::Time, "time", FieldFormat::Uuid),
            (FieldType::Date, "date", FieldFormat::Slug),
            (FieldType::Datetime, "datetime", FieldFormat::Mac),
        ] {
            let report = validate_schema(formatted_field_schema(field_type, format.clone()));
            let conflicts = conflicting_formats(&report);
            assert_eq!(conflicts.len(), 1, "{label}: {:?}", report.errors);
            assert!(
                matches!(
                    conflicts[0],
                    ValidationError::ConflictingFormat { type_name, field, field_type, format: declared, .. }
                        if type_name == "device"
                            && field == "value"
                            && field_type == label
                            && *declared == format_label(&format)
                ),
                "{label}: {:?}",
                conflicts[0]
            );
            // an rfc 3339 check has no `format(...)` spelling, so it is named for
            // what it is rather than forced into one.
            assert_eq!(
                conflicts[0].to_string(),
                format!(
                    "conflicting format on device.value: declared {}, but a {label} type already checks rfc 3339 (docs/ir.md)",
                    format_label(&format)
                )
            );
        }
    }

    #[test]
    fn every_format_disagrees_with_an_rfc_3339_type() {
        // none of the six formats is the rfc 3339 check, so unlike `cidr`/`prefix`
        // there is no agreeing pair to except.
        for field_type in [FieldType::Date, FieldType::Datetime, FieldType::Time] {
            for format in [
                FieldFormat::Slug,
                FieldFormat::IpAddress,
                FieldFormat::Cidr,
                FieldFormat::Prefix,
                FieldFormat::Mac,
                FieldFormat::Uuid,
            ] {
                let report =
                    validate_schema(formatted_field_schema(field_type.clone(), format.clone()));
                assert_eq!(
                    conflicting_formats(&report).len(),
                    1,
                    "{field_type:?} accepts {format:?}: {:?}",
                    report.errors
                );
            }
        }
    }

    #[test]
    fn detects_a_format_on_an_rfc_3339_typed_key_field() {
        let mut day = schema_field(FieldType::Date);
        day.format = Some(FieldFormat::Uuid);
        let device = TypeSchema {
            key: BTreeMap::from([("day".to_string(), day)]),
            fields: BTreeMap::new(),
        };
        let report = validate_schema(BTreeMap::from([("device".to_string(), device)]));
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::ConflictingFormat { type_name, field, .. }
                if type_name == "device" && field == "key.day"
        )));
    }

    #[test]
    fn detects_empty_enum_in_attr_field() {
        let device = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::from([(
                "role".to_string(),
                schema_field(FieldType::Enum { values: vec![] }),
            )]),
        };
        let report = validate_schema(BTreeMap::from([("device".to_string(), device)]));
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::EmptyEnum { type_name, field }
                if type_name == "device" && field == "role"
        )));
    }

    #[test]
    fn detects_empty_enum_nested_in_list_field() {
        let device = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::from([(
                "roles".to_string(),
                schema_field(FieldType::List {
                    item: Box::new(FieldType::Enum { values: vec![] }),
                }),
            )]),
        };
        let report = validate_schema(BTreeMap::from([("device".to_string(), device)]));
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::EmptyEnum { type_name, field }
                if type_name == "device" && field == "roles"
        )));
    }

    #[test]
    fn non_empty_enum_passes_schema_validation() {
        let device = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::from([
                (
                    "role".to_string(),
                    schema_field(FieldType::Enum {
                        values: vec!["leaf".to_string(), "spine".to_string()],
                    }),
                ),
                (
                    "roles".to_string(),
                    schema_field(FieldType::List {
                        item: Box::new(FieldType::Enum {
                            values: vec!["a".to_string()],
                        }),
                    }),
                ),
            ]),
        };
        let report = validate_schema(BTreeMap::from([("device".to_string(), device)]));
        assert!(!report
            .errors
            .iter()
            .any(|e| matches!(e, ValidationError::EmptyEnum { .. })));
    }

    #[test]
    fn detects_empty_enum_for_type_with_no_objects() {
        // see detects_invalid_pattern_for_type_with_no_objects; here for an
        // empty enum. the key field also exercises the `key.{field}` label path.
        let ghost = TypeSchema {
            key: BTreeMap::from([(
                "role".to_string(),
                schema_field(FieldType::Enum { values: vec![] }),
            )]),
            fields: BTreeMap::new(),
        };
        let report = validate_inventory(&Inventory {
            scope: None,
            schema: Schema {
                types: BTreeMap::from([("ghost".to_string(), ghost)]),
            },
            objects: vec![],
        });
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::EmptyEnum { type_name, field }
                if type_name == "ghost" && field == "key.role"
        )));
    }

    #[test]
    fn accumulates_errors_for_multiple_invalid_fields() {
        let field = |r#type| FieldSchema {
            r#type,
            required: true,
            nullable: false,
            description: None,
            format: None,
            pattern: None,
        };
        let type_schema = TypeSchema {
            key: BTreeMap::from([("slug".to_string(), field(FieldType::Slug))]),
            fields: BTreeMap::from([
                ("count".to_string(), field(FieldType::Int)),
                ("enabled".to_string(), field(FieldType::Bool)),
            ]),
        };
        let mut key = BTreeMap::new();
        key.insert("slug".to_string(), json!("leaf01"));
        let mut attrs = BTreeMap::new();
        attrs.insert("count".to_string(), json!("not-an-int"));
        attrs.insert("enabled".to_string(), json!("not-a-bool"));
        let objects = vec![Object::new(
            uid(1),
            TypeName::new("device"),
            Key::from(key),
            attrs.into(),
        )
        .unwrap()];
        let report = validate_inventory(&Inventory {
            scope: None,
            schema: Schema {
                types: BTreeMap::from([("device".to_string(), type_schema)]),
            },
            objects,
        });
        let invalid = report
            .errors
            .iter()
            .filter(|e| matches!(e, ValidationError::InvalidValue { .. }))
            .count();
        assert_eq!(invalid, 2);
    }

    #[test]
    fn with_sources_attaches_location_for_dotted_type() {
        let mut key = BTreeMap::new();
        key.insert("slug".to_string(), serde_json::json!("fra1"));
        let object = Object::new(
            uid(50),
            TypeName::new("dcim.site"),
            Key::from(key),
            JsonMap::default(),
        )
        .unwrap()
        .with_source(SourceLocation::file_line("inventory.yaml", 42));

        let report = ValidationReport {
            errors: vec![ValidationError::InvalidValue {
                field: "dcim.site.key.slug".to_string(),
                expected: "slug".to_string(),
                actual: "FRA1".to_string(),
            }],
        };

        let located = report.with_sources(&[object]);
        assert_eq!(located.len(), 1);
        assert_eq!(
            located[0].source,
            Some(SourceLocation::file_line("inventory.yaml", 42))
        );
    }

    #[test]
    fn with_sources_attributes_ref_mismatch_to_referencing_object() {
        let mut dkey = BTreeMap::new();
        dkey.insert("name".to_string(), serde_json::json!("leaf1"));
        let device = Object::new(
            uid(1),
            TypeName::new("dcim.device"),
            Key::from(dkey),
            JsonMap::default(),
        )
        .unwrap()
        .with_source(SourceLocation::file_line("inventory.yaml", 20));

        let mut skey = BTreeMap::new();
        skey.insert("slug".to_string(), serde_json::json!("fra1"));
        let site = Object::new(
            uid(2),
            TypeName::new("dcim.site"),
            Key::from(skey),
            JsonMap::default(),
        )
        .unwrap()
        .with_source(SourceLocation::file_line("inventory.yaml", 5));

        let report = ValidationReport {
            errors: vec![ValidationError::ReferenceTypeMismatch {
                field: "dcim.device.owner".to_string(),
                target: uid(2),
                expected: "tenancy.tenant".to_string(),
                actual: "dcim.site".to_string(),
            }],
        };

        let located = report.with_sources(&[device, site]);
        assert_eq!(located.len(), 1);
        assert_eq!(
            located[0].source,
            Some(SourceLocation::file_line("inventory.yaml", 20))
        );
    }

    #[test]
    fn with_sources_attaches_location_for_empty_enum() {
        // `EmptyEnum` carries a `type_name`; with_sources must resolve it to
        // the declaring type's source line, like every other schema-load error.
        let device = TypeSchema {
            key: BTreeMap::from([("name".to_string(), schema_field(FieldType::String))]),
            fields: BTreeMap::from([(
                "role".to_string(),
                schema_field(FieldType::Enum { values: vec![] }),
            )]),
        };
        let mut key = BTreeMap::new();
        key.insert("name".to_string(), serde_json::json!("leaf1"));
        let object = Object::new(
            uid(1),
            TypeName::new("device"),
            Key::from(key),
            JsonMap::default(),
        )
        .unwrap()
        .with_source(SourceLocation::file_line("inventory.yaml", 7));

        let inventory = Inventory {
            scope: None,
            schema: Schema {
                types: BTreeMap::from([("device".to_string(), device)]),
            },
            objects: vec![object],
        };
        let located = validate_inventory(&inventory).with_sources(&inventory.objects);

        let empty_enum = located
            .iter()
            .find(|l| matches!(l.error, ValidationError::EmptyEnum { .. }))
            .expect("empty-enum error present");
        assert_eq!(
            empty_enum.source,
            Some(SourceLocation::file_line("inventory.yaml", 7))
        );
    }

    #[test]
    fn with_sources_attaches_location_for_conflicting_format() {
        let mut asset = schema_field(FieldType::Mac);
        asset.format = Some(FieldFormat::Uuid);
        let device = TypeSchema {
            key: BTreeMap::from([("name".to_string(), schema_field(FieldType::String))]),
            fields: BTreeMap::from([("asset".to_string(), asset)]),
        };
        let mut key = BTreeMap::new();
        key.insert("name".to_string(), serde_json::json!("leaf1"));
        let object = Object::new(
            uid(1),
            TypeName::new("device"),
            Key::from(key),
            JsonMap(BTreeMap::from([(
                "asset".to_string(),
                serde_json::json!("aa:bb:cc:dd:ee:ff"),
            )])),
        )
        .unwrap()
        .with_source(SourceLocation::file_line("inventory.yaml", 7));

        let inventory = Inventory {
            scope: None,
            schema: Schema {
                types: BTreeMap::from([("device".to_string(), device)]),
            },
            objects: vec![object],
        };
        let located = validate_inventory(&inventory).with_sources(&inventory.objects);

        let conflict = located
            .iter()
            .find(|l| matches!(l.error, ValidationError::ConflictingFormat { .. }))
            .expect("conflicting-format error present");
        assert_eq!(
            conflict.source,
            Some(SourceLocation::file_line("inventory.yaml", 7))
        );
    }

    #[test]
    fn with_sources_attaches_location_for_unknown_ref_target() {
        // `UnknownRefTarget` resolves too: the arm matches per variant, not
        // just `EmptyEnum`.
        let device = TypeSchema {
            key: BTreeMap::from([("name".to_string(), schema_field(FieldType::String))]),
            fields: BTreeMap::from([(
                "site".to_string(),
                schema_field(FieldType::Ref {
                    target: "dcim.site".to_string(),
                }),
            )]),
        };
        let mut key = BTreeMap::new();
        key.insert("name".to_string(), serde_json::json!("leaf1"));
        let object = Object::new(
            uid(1),
            TypeName::new("device"),
            Key::from(key),
            JsonMap::default(),
        )
        .unwrap()
        .with_source(SourceLocation::file_line("inventory.yaml", 12));

        let inventory = Inventory {
            scope: None,
            schema: Schema {
                types: BTreeMap::from([("device".to_string(), device)]),
            },
            objects: vec![object],
        };
        let located = validate_inventory(&inventory).with_sources(&inventory.objects);

        let unknown_ref = located
            .iter()
            .find(|l| matches!(l.error, ValidationError::UnknownRefTarget { .. }))
            .expect("unknown-ref-target error present");
        assert_eq!(
            unknown_ref.source,
            Some(SourceLocation::file_line("inventory.yaml", 12))
        );
    }

    #[test]
    fn with_sources_attaches_location_for_non_scalar_key() {
        // `NonScalarKeyField` carries a `type_name`; with_sources must resolve
        // it to the declaring type's source line.
        let device = TypeSchema {
            key: BTreeMap::from([(
                "members".to_string(),
                schema_field(FieldType::List {
                    item: Box::new(FieldType::String),
                }),
            )]),
            fields: BTreeMap::new(),
        };
        let mut key = BTreeMap::new();
        key.insert("members".to_string(), serde_json::json!(["leaf1"]));
        let object = Object::new(
            uid(1),
            TypeName::new("device"),
            Key::from(key),
            JsonMap::default(),
        )
        .unwrap()
        .with_source(SourceLocation::file_line("inventory.yaml", 5));

        let inventory = Inventory {
            scope: None,
            schema: Schema {
                types: BTreeMap::from([("device".to_string(), device)]),
            },
            objects: vec![object],
        };
        let located = validate_inventory(&inventory).with_sources(&inventory.objects);

        let non_scalar = located
            .iter()
            .find(|l| matches!(l.error, ValidationError::NonScalarKeyField { .. }))
            .expect("non-scalar-key error present");
        assert_eq!(
            non_scalar.source,
            Some(SourceLocation::file_line("inventory.yaml", 5))
        );
    }

    #[test]
    fn with_sources_attaches_location_for_nullable_key() {
        // `NullableKeyField` carries a `type_name`; with_sources must resolve
        // it to the declaring type's source line.
        let device = TypeSchema {
            key: BTreeMap::from([("slug".to_string(), nullable_field(FieldType::String))]),
            fields: BTreeMap::new(),
        };
        let mut key = BTreeMap::new();
        key.insert("slug".to_string(), serde_json::json!("leaf1"));
        let object = Object::new(
            uid(1),
            TypeName::new("device"),
            Key::from(key),
            JsonMap::default(),
        )
        .unwrap()
        .with_source(SourceLocation::file_line("inventory.yaml", 7));

        let inventory = Inventory {
            scope: None,
            schema: Schema {
                types: BTreeMap::from([("device".to_string(), device)]),
            },
            objects: vec![object],
        };
        let located = validate_inventory(&inventory).with_sources(&inventory.objects);

        let nullable_key = located
            .iter()
            .find(|l| matches!(l.error, ValidationError::NullableKeyField { .. }))
            .expect("nullable-key error present");
        assert_eq!(
            nullable_key.source,
            Some(SourceLocation::file_line("inventory.yaml", 7))
        );
    }

    #[test]
    fn test_field_value_validation() {
        let uid_to_type = BTreeMap::from([(uid(1), TypeName::new("target"))]);
        let mut report = ValidationReport::default();

        // test type mismatch
        let schema = FieldSchema {
            r#type: FieldType::Int,
            required: true,
            nullable: false,
            description: None,
            format: None,
            pattern: None,
        };
        validate_field_value(
            &TypeName::new("test"),
            "field",
            &schema,
            &json!("not-int"),
            &uid_to_type,
            &BTreeMap::new(),
            &mut report,
        );
        assert!(report
            .errors
            .iter()
            .any(|e| matches!(e, ValidationError::InvalidValue { .. })));

        // test `enum`
        let schema = FieldSchema {
            r#type: FieldType::Enum {
                values: vec!["a".to_string(), "b".to_string()],
            },
            required: true,
            nullable: false,
            description: None,
            format: None,
            pattern: None,
        };
        report.errors.clear();
        validate_field_value(
            &TypeName::new("test"),
            "field",
            &schema,
            &json!("c"),
            &uid_to_type,
            &BTreeMap::new(),
            &mut report,
        );
        assert!(report
            .errors
            .iter()
            .any(|e| matches!(e, ValidationError::InvalidValue { .. })));

        // test reference type mismatch
        let schema = FieldSchema {
            r#type: FieldType::Ref {
                target: "wrong".to_string(),
            },
            required: true,
            nullable: false,
            description: None,
            format: None,
            pattern: None,
        };
        report.errors.clear();
        validate_field_value(
            &TypeName::new("test"),
            "field",
            &schema,
            &json!(uid(1).to_string()),
            &uid_to_type,
            &BTreeMap::new(),
            &mut report,
        );
        assert!(report
            .errors
            .iter()
            .any(|e| matches!(e, ValidationError::ReferenceTypeMismatch { .. })));

        // test `list_ref`
        let schema = FieldSchema {
            r#type: FieldType::ListRef {
                target: "target".to_string(),
            },
            required: true,
            nullable: false,
            description: None,
            format: None,
            pattern: None,
        };
        report.errors.clear();
        validate_field_value(
            &TypeName::new("test"),
            "field",
            &schema,
            &json!([uid(1).to_string()]),
            &uid_to_type,
            &BTreeMap::new(),
            &mut report,
        );
        assert!(report.errors.is_empty());

        // test `map`
        let schema = FieldSchema {
            r#type: FieldType::Map {
                value: Box::new(FieldType::Int),
            },
            required: true,
            nullable: false,
            description: None,
            format: None,
            pattern: None,
        };
        report.errors.clear();
        validate_field_value(
            &TypeName::new("test"),
            "field",
            &schema,
            &json!({"a": 1, "b": "not-int"}),
            &uid_to_type,
            &BTreeMap::new(),
            &mut report,
        );
        assert!(report
            .errors
            .iter()
            .any(|e| matches!(e, ValidationError::InvalidValue { .. })));

        // test `uuid`
        let schema = FieldSchema {
            r#type: FieldType::Uuid,
            required: true,
            nullable: false,
            description: None,
            format: None,
            pattern: None,
        };
        report.errors.clear();
        validate_field_value(
            &TypeName::new("test"),
            "field",
            &schema,
            &json!("not-a-uuid"),
            &uid_to_type,
            &BTreeMap::new(),
            &mut report,
        );
        assert!(report
            .errors
            .iter()
            .any(|e| matches!(e, ValidationError::InvalidValue { .. })));

        // test a list of refs
        let schema = FieldSchema {
            r#type: FieldType::List {
                item: Box::new(FieldType::Ref {
                    target: "target".to_string(),
                }),
            },
            required: true,
            nullable: false,
            description: None,
            format: None,
            pattern: None,
        };
        report.errors.clear();
        validate_field_value(
            &TypeName::new("test"),
            "field",
            &schema,
            &json!([uid(1).to_string()]),
            &uid_to_type,
            &BTreeMap::new(),
            &mut report,
        );
        assert!(report.errors.is_empty());
    }

    #[test]
    fn validate_field_value_null_tristate() {
        let nullable = FieldSchema {
            r#type: FieldType::String,
            required: false,
            nullable: true,
            description: None,
            format: None,
            pattern: None,
        };
        assert!(check(&nullable, &json!(null)).errors.is_empty());

        let non_nullable = FieldSchema {
            r#type: FieldType::String,
            required: true,
            nullable: false,
            description: None,
            format: None,
            pattern: None,
        };
        assert!(check(&non_nullable, &json!(null))
            .errors
            .iter()
            .any(|e| matches!(
                e,
                ValidationError::InvalidValue { actual, .. } if actual == "null"
            )));
    }

    // ----- string constraint (format / pattern) tests -----

    /// build a string-typed field carrying a `format` constraint.
    fn fmt_field(format: FieldFormat) -> FieldSchema {
        FieldSchema {
            r#type: FieldType::String,
            required: true,
            nullable: false,
            description: None,
            format: Some(format),
            pattern: None,
        }
    }

    /// build a string-typed field carrying a `pattern` constraint.
    fn pattern_field(pattern: &str) -> FieldSchema {
        FieldSchema {
            r#type: FieldType::String,
            required: true,
            nullable: false,
            description: None,
            format: None,
            pattern: Some(pattern.to_string()),
        }
    }

    /// build a field whose `FieldType` carries an implicit format and no
    /// separate `format:` constraint, so validation comes solely from the type.
    fn typed_field(field_type: FieldType) -> FieldSchema {
        FieldSchema {
            r#type: field_type,
            required: true,
            nullable: false,
            description: None,
            format: None,
            pattern: None,
        }
    }

    /// run `validate_field_value` against a value and return the report.
    fn check(schema: &FieldSchema, value: &serde_json::Value) -> ValidationReport {
        let uid_to_type: BTreeMap<Uid, TypeName> = BTreeMap::new();
        let mut report = ValidationReport::default();
        let mut pattern_cache = BTreeMap::new();
        if let Some(pattern) = &schema.pattern {
            match Regex::new(pattern) {
                Ok(regex) => {
                    pattern_cache.insert(pattern.clone(), regex);
                }
                Err(err) => report.errors.push(ValidationError::InvalidSchemaPattern {
                    type_name: "test".to_string(),
                    field: "field".to_string(),
                    pattern: pattern.clone(),
                    error: err.to_string(),
                }),
            }
        }
        validate_field_value(
            &TypeName::new("test"),
            "field",
            schema,
            value,
            &uid_to_type,
            &pattern_cache,
            &mut report,
        );
        report
    }

    fn has_invalid_value(report: &ValidationReport) -> bool {
        report
            .errors
            .iter()
            .any(|e| matches!(e, ValidationError::InvalidValue { .. }))
    }

    /// the `expected`/`actual` pair of the first `InvalidValue` in the report.
    fn invalid_value_pair(report: &ValidationReport) -> (String, String) {
        report
            .errors
            .iter()
            .find_map(|e| match e {
                ValidationError::InvalidValue {
                    expected, actual, ..
                } => Some((expected.clone(), actual.clone())),
                _ => None,
            })
            .expect("expected an InvalidValue")
    }

    fn accepts(field_type: FieldType, value: &str) -> bool {
        check(&typed_field(field_type), &json!(value))
            .errors
            .is_empty()
    }

    fn rejects(field_type: FieldType, value: &str) -> bool {
        has_invalid_value(&check(&typed_field(field_type), &json!(value)))
    }

    #[test]
    fn format_slug_accepts_valid_and_rejects_invalid() {
        assert!(check(&fmt_field(FieldFormat::Slug), &json!("leaf-01"))
            .errors
            .is_empty());
        // trailing hyphen is not allowed by the slug regex.
        assert!(has_invalid_value(&check(
            &fmt_field(FieldFormat::Slug),
            &json!("leaf01-")
        )));
        // uppercase is not allowed either.
        assert!(has_invalid_value(&check(
            &fmt_field(FieldFormat::Slug),
            &json!("Leaf01")
        )));
    }

    #[test]
    fn format_ip_address_accepts_valid_and_rejects_invalid() {
        assert!(
            check(&fmt_field(FieldFormat::IpAddress), &json!("10.0.0.1"))
                .errors
                .is_empty()
        );
        assert!(has_invalid_value(&check(
            &fmt_field(FieldFormat::IpAddress),
            &json!("not-an-ip")
        )));
    }

    #[test]
    fn format_cidr_and_prefix_accept_valid_and_reject_invalid() {
        assert!(check(&fmt_field(FieldFormat::Cidr), &json!("10.0.0.0/24"))
            .errors
            .is_empty());
        assert!(
            check(&fmt_field(FieldFormat::Prefix), &json!("10.0.0.0/24"))
                .errors
                .is_empty()
        );
        assert!(has_invalid_value(&check(
            &fmt_field(FieldFormat::Cidr),
            &json!("not-a-cidr")
        )));
    }

    #[test]
    fn format_mac_accepts_valid_and_rejects_invalid() {
        assert!(
            check(&fmt_field(FieldFormat::Mac), &json!("aa:bb:cc:dd:ee:ff"))
                .errors
                .is_empty()
        );
        // too short to be a full mac address.
        assert!(has_invalid_value(&check(
            &fmt_field(FieldFormat::Mac),
            &json!("aa:bb")
        )));
    }

    #[test]
    fn format_uuid_accepts_valid_and_rejects_invalid() {
        assert!(
            check(&fmt_field(FieldFormat::Uuid), &json!(uid(1).to_string()))
                .errors
                .is_empty()
        );
        assert!(has_invalid_value(&check(
            &fmt_field(FieldFormat::Uuid),
            &json!("not-a-uuid")
        )));
    }

    // every value a `format_regex` entry has to keep accepting, mixed across
    // formats on purpose: a regex only has to be wide enough for the values its
    // own `matches_format` arm passes, so the corpus is shared and the
    // invariant filters it.
    const FORMAT_CORPUS: &[&str] = &[
        // slug
        "a",
        "9",
        "web-01",
        "web_01",
        "a-b_c-9",
        "0000",
        "Web-01",
        "-lead",
        "trail-",
        // ipv4 and ipv6, including the ipv4-mapped spelling that mixes both
        "10.0.0.10",
        "0.0.0.0",
        "255.255.255.255",
        "::1",
        "::",
        "2001:db8::1",
        "2001:0db8:0000:0000:0000:0000:0000:0001",
        "fe80::a00:27ff:fe4e:66a1",
        "::ffff:192.168.0.1",
        "::ffff:0:0",
        "10.0.0.10/24",
        // prefixes at both ends of the mask range
        "10.0.0.0/8",
        "0.0.0.0/0",
        "192.168.1.1/32",
        "2001:db8::/32",
        "::/0",
        "2001:db8::1/128",
        // mac, both separators and both cases
        "aa:bb:cc:dd:ee:ff",
        "AA:BB:CC:DD:EE:FF",
        "aa-bb-cc-dd-ee-ff",
        "00:00:00:00:00:00",
        "aA:bB:cC:dD:eE:fF",
        "aa:bb:cc:dd:ee",
        // every uuid spelling `Uuid::parse_str` takes
        "67e55044-10b1-426f-9247-bb680e5fe0c8",
        "67E55044-10B1-426F-9247-BB680E5FE0C8",
        "00000000-0000-0000-0000-000000000000",
        "67e5504410b1426f9247bb680e5fe0c8",
        "67E5504410B1426F9247BB680E5FE0C8",
        "{67e55044-10b1-426f-9247-bb680e5fe0c8}",
        "urn:uuid:67e55044-10b1-426f-9247-bb680e5fe0c8",
        // neither a format nor a regex should take these
        "",
        " ",
        "not-a-uuid",
        "hello world",
        "10.0.0.256",
    ];

    // the contract on `format_regex`: looser than `matches_format` is safe,
    // stricter installs a backend constraint that rejects what core accepts.
    #[test]
    fn format_regex_accepts_everything_matches_format_does() {
        for format in [
            FieldFormat::Slug,
            FieldFormat::IpAddress,
            FieldFormat::Cidr,
            FieldFormat::Prefix,
            FieldFormat::Mac,
            FieldFormat::Uuid,
        ] {
            let re = Regex::new(format_regex(&format)).unwrap();
            for value in FORMAT_CORPUS {
                if matches_format(&format, value) {
                    assert!(
                        re.is_match(value),
                        "{format:?} accepts {value:?} but its regex rejects it"
                    );
                }
            }
        }
    }

    // the other side of `format_check`: two formats sharing a variant are one
    // predicate by construction, so what needs holding is that two formats given
    // *different* variants really do differ. otherwise a duplicated arm would
    // have `validate_schema_format_agreement` report a conflict between two
    // spellings of the same check.

    #[test]
    fn distinct_format_checks_disagree_on_some_value() {
        let formats = [
            FieldFormat::Slug,
            FieldFormat::IpAddress,
            FieldFormat::Cidr,
            FieldFormat::Prefix,
            FieldFormat::Mac,
            FieldFormat::Uuid,
        ];
        for left in &formats {
            for right in &formats {
                if format_check(left) == format_check(right) {
                    continue;
                }
                assert!(
                    FORMAT_CORPUS
                        .iter()
                        .any(|value| matches_format(left, value) != matches_format(right, value)),
                    "{left:?} and {right:?} are separate checks but accept the same corpus"
                );
            }
        }
    }

    // the type-implied table may only name types core itself checks through
    // `matches_format`; a type checked some other way has no format to carry.
    #[test]
    fn type_implied_format_agrees_with_the_type_check() {
        for (field_type, expected) in [
            (FieldType::Uuid, Some(FieldFormat::Uuid)),
            (FieldType::Cidr, Some(FieldFormat::Cidr)),
            (FieldType::Prefix, Some(FieldFormat::Prefix)),
            (FieldType::Mac, Some(FieldFormat::Mac)),
            (FieldType::Slug, Some(FieldFormat::Slug)),
            (FieldType::IpAddress, None),
            (FieldType::Time, None),
            (FieldType::String, None),
            (FieldType::Text, None),
            (FieldType::Int, None),
            (FieldType::Json, None),
            (FieldType::Enum { values: vec![] }, None),
        ] {
            assert_eq!(format_for_field_type(&field_type), expected);
            if let Some(format) = expected {
                for value in FORMAT_CORPUS {
                    assert_eq!(
                        check(&typed_field(field_type.clone()), &json!(value))
                            .errors
                            .is_empty(),
                        matches_format(&format, value),
                        "type {field_type:?} and format {format:?} disagree on {value:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn type_uuid_enforces_format() {
        assert!(
            check(&typed_field(FieldType::Uuid), &json!(uid(1).to_string()))
                .errors
                .is_empty()
        );
        assert!(has_invalid_value(&check(
            &typed_field(FieldType::Uuid),
            &json!("not-a-uuid")
        )));
    }

    #[test]
    fn type_ip_address_accepts_any_string_including_masked() {
        // `ip_address` is a plain string check (see type_check)
        assert!(
            check(&typed_field(FieldType::IpAddress), &json!("10.0.0.10/24"))
                .errors
                .is_empty()
        );
        assert!(
            check(&typed_field(FieldType::IpAddress), &json!("10.0.0.1"))
                .errors
                .is_empty()
        );
    }

    #[test]
    fn type_cidr_and_prefix_enforce_format() {
        assert!(check(&typed_field(FieldType::Cidr), &json!("10.0.0.0/24"))
            .errors
            .is_empty());
        assert!(
            check(&typed_field(FieldType::Prefix), &json!("10.0.0.0/24"))
                .errors
                .is_empty()
        );
        assert!(has_invalid_value(&check(
            &typed_field(FieldType::Cidr),
            &json!("not-a-cidr")
        )));
        assert!(has_invalid_value(&check(
            &typed_field(FieldType::Prefix),
            &json!("10.0.0.1")
        )));
    }

    #[test]
    fn type_mac_enforces_format() {
        assert!(
            check(&typed_field(FieldType::Mac), &json!("aa:bb:cc:dd:ee:ff"))
                .errors
                .is_empty()
        );
        assert!(has_invalid_value(&check(
            &typed_field(FieldType::Mac),
            &json!("aa:bb")
        )));
    }

    #[test]
    fn type_slug_enforces_format() {
        assert!(check(&typed_field(FieldType::Slug), &json!("leaf-01"))
            .errors
            .is_empty());
        // uppercase is rejected by the slug regex.
        assert!(has_invalid_value(&check(
            &typed_field(FieldType::Slug),
            &json!("Leaf01")
        )));
    }

    #[test]
    fn format_typed_mismatch_names_the_value() {
        // a string reaching the error path failed the format, not the json type,
        // so the message names it the way a `format:` constraint does.
        let cases = [
            (FieldType::Uuid, "not-a-uuid", "uuid"),
            (FieldType::Cidr, "not-a-cidr", "cidr"),
            (FieldType::Prefix, "10.0.0.1", "prefix"),
            (FieldType::Mac, "not-a-mac", "mac"),
            (FieldType::Slug, "Leaf01", "slug"),
            (FieldType::Date, "2026-02-30", "date"),
            (FieldType::Datetime, "2026-08-01T22:00:00z", "datetime"),
            (FieldType::Time, "23:59:60", "time"),
        ];
        for (field_type, raw, expected) in cases {
            let report = check(&typed_field(field_type.clone()), &json!(raw));
            assert_eq!(
                invalid_value_pair(&report),
                (expected.to_string(), raw.to_string()),
                "{field_type:?}"
            );
        }
    }

    #[test]
    fn wrong_json_type_names_the_type() {
        // the naive "echo every string" fix breaks this one: an int field holding
        // "7" failed on the json type, and `got 7` would hide that.
        assert_eq!(
            invalid_value_pair(&check(&typed_field(FieldType::Int), &json!("7"))),
            ("int".to_string(), "string".to_string())
        );
        // and a format-typed field can still fail on the json type.
        assert_eq!(
            invalid_value_pair(&check(&typed_field(FieldType::Mac), &json!(42))),
            ("mac".to_string(), "number".to_string())
        );
    }

    #[test]
    fn ref_mismatch_names_the_malformed_uuid() {
        let target = "device".to_string();
        let field = typed_field(FieldType::Ref {
            target: target.clone(),
        });
        assert_eq!(
            invalid_value_pair(&check(&field, &json!("not-a-uuid"))),
            ("uuid".to_string(), "not-a-uuid".to_string())
        );
        assert_eq!(
            invalid_value_pair(&check(&field, &json!(42))),
            ("uuid".to_string(), "number".to_string())
        );
        // list_ref reaches the same check per element.
        assert_eq!(
            invalid_value_pair(&check(
                &typed_field(FieldType::ListRef { target }),
                &json!(["not-a-uuid"])
            )),
            ("uuid".to_string(), "not-a-uuid".to_string())
        );
    }

    #[test]
    fn type_float_rejects_a_numeric_string() {
        // a backend that stores a float as text reads it back quoted, and import
        // hard-validates what it writes, so such a backend field is lossy.
        assert!(rejects(FieldType::Float, "1.5"));
        assert!(check(&typed_field(FieldType::Float), &json!(1.5))
            .errors
            .is_empty());
    }

    #[test]
    fn type_date_enforces_rfc3339() {
        assert!(accepts(FieldType::Date, "2026-08-01"));
        assert!(rejects(FieldType::Date, "not a timestamp"));
        assert!(rejects(FieldType::Date, "2026-8-1"));
        assert!(rejects(FieldType::Date, "2026-08-01T22:00:00Z"));
        // ascii digits only, as rfc 3339 spells `DIGIT`.
        assert!(rejects(FieldType::Date, "٢٠٢٦-٠٨-٠١"));
        // a non-string is rejected as it was before the format check.
        assert!(has_invalid_value(&check(
            &typed_field(FieldType::Date),
            &json!(20260801)
        )));
    }

    #[test]
    fn type_date_checks_the_calendar() {
        // the shape alone accepts every one of these.
        assert!(rejects(FieldType::Date, "2026-13-01"));
        assert!(rejects(FieldType::Date, "2026-00-01"));
        assert!(rejects(FieldType::Date, "2026-02-30"));
        assert!(rejects(FieldType::Date, "2026-04-31"));
        assert!(rejects(FieldType::Date, "2026-01-00"));
        // leap years by the full gregorian rule.
        assert!(rejects(FieldType::Date, "2026-02-29"));
        assert!(accepts(FieldType::Date, "2024-02-29"));
        assert!(rejects(FieldType::Date, "2100-02-29"));
        assert!(accepts(FieldType::Date, "2000-02-29"));
        // year 0 is rejected (see `is_rfc3339_date`), and the gregorian rule
        // would otherwise call it a leap year.
        assert!(rejects(FieldType::Date, "0000-01-01"));
        assert!(rejects(FieldType::Date, "0000-02-29"));
        // the bounds themselves validate; this is not a narrower range check.
        assert!(accepts(FieldType::Date, "0001-01-01"));
        assert!(accepts(FieldType::Date, "9999-12-31"));
    }

    #[test]
    fn type_time_enforces_rfc3339() {
        assert!(accepts(FieldType::Time, "22:00:00"));
        assert!(accepts(FieldType::Time, "22:00:00.123456"));
        assert!(rejects(FieldType::Time, "25:00:00"));
        assert!(rejects(FieldType::Time, "12:60:00"));
        // a leap second is rejected (see `is_rfc3339_time`).
        assert!(rejects(FieldType::Time, "23:59:60"));
        assert!(rejects(FieldType::Time, "22:00"));
        assert!(rejects(FieldType::Time, "not a timestamp"));
        // rfc 3339's `DIGIT` is ascii. the hour/minute/second groups are re-read
        // by `parse::<u32>()`, which refuses these anyway, but the fractional
        // seconds are only ever shape-checked, so a unicode-aware `\d` there
        // decided the verdict on its own.
        assert!(rejects(FieldType::Time, "22:00:00.٥"));
        assert!(rejects(FieldType::Time, "٢٢:00:00"));
        assert!(has_invalid_value(&check(
            &typed_field(FieldType::Time),
            &json!(2200)
        )));
    }

    #[test]
    fn type_datetime_enforces_rfc3339() {
        // the value `examples/walkthroughs/custom-model.yaml` carries.
        assert!(accepts(FieldType::Datetime, "2026-08-01T22:00:00Z"));
        // rfc 3339 permits both in lowercase; python takes the separator, not the zone.
        assert!(accepts(FieldType::Datetime, "2026-08-01t22:00:00Z"));
        assert!(rejects(FieldType::Datetime, "2026-08-01T22:00:00z"));
        assert!(rejects(FieldType::Datetime, "2026-08-01t22:00:00z"));
        assert!(accepts(FieldType::Datetime, "2026-08-01T22:00:00+02:00"));
        assert!(accepts(FieldType::Datetime, "2026-08-01T22:00:00-05:30"));
        // the offset is optional here, deliberately (see `type_check`).
        assert!(accepts(FieldType::Datetime, "2026-08-01T22:00:00"));
        assert!(rejects(FieldType::Datetime, "not a timestamp"));
        assert!(rejects(FieldType::Datetime, "2026-08-01 22:00:00"));
        assert!(rejects(FieldType::Datetime, "2026-08-01"));
        // both halves are checked as they are on their own types.
        assert!(rejects(FieldType::Datetime, "2026-02-30T22:00:00Z"));
        assert!(rejects(FieldType::Datetime, "0000-01-01T22:00:00Z"));
        assert!(accepts(FieldType::Datetime, "0001-01-01T22:00:00Z"));
        assert!(rejects(FieldType::Datetime, "2026-08-01T25:00:00Z"));
        assert!(rejects(FieldType::Datetime, "2026-08-01T22:00:00+24:00"));
        assert!(rejects(FieldType::Datetime, "2026-08-01T22:00:00+02:60"));
        // ascii digits only, in the fraction as everywhere else. this one is a
        // no-regression pin rather than a red-green case: the time half is
        // re-checked by `is_rfc3339_time`, so `datetime_regex`'s own digit
        // classes could rot without it going red. they are written out anyway,
        // so the three regexes do not disagree about what a digit is.
        assert!(rejects(FieldType::Datetime, "2026-08-01T22:00:00.٥Z"));
        assert!(has_invalid_value(&check(
            &typed_field(FieldType::Datetime),
            &json!(true)
        )));
    }

    #[test]
    fn type_datetime_accepts_what_import_reads_back() {
        // netbox and nautobot return fractional seconds and an explicit offset,
        // and `import` writes them into ir that is validated on the next load.
        assert!(accepts(FieldType::Datetime, "2026-08-04T20:11:22.123456Z"));
        assert!(accepts(
            FieldType::Datetime,
            "2026-08-04T20:11:22.123456+00:00"
        ));
        assert!(accepts(FieldType::Datetime, "2026-08-04T20:11:22+00:00"));
    }

    #[test]
    fn pattern_matches_and_mismatches() {
        assert!(check(&pattern_field(r"^[a-z]+$"), &json!("abc"))
            .errors
            .is_empty());
        assert!(has_invalid_value(&check(
            &pattern_field(r"^[a-z]+$"),
            &json!("ABC")
        )));
    }

    #[test]
    fn invalid_pattern_reports_schema_error_not_per_value() {
        // malformed pattern -> one InvalidSchemaPattern, never a per-value InvalidValue.
        let report = check(&pattern_field("["), &json!("anything"));
        assert!(report
            .errors
            .iter()
            .any(|e| matches!(e, ValidationError::InvalidSchemaPattern { .. })));
        assert!(!report.errors.iter().any(|e| matches!(
            e,
            ValidationError::InvalidValue { actual, .. } if actual.contains("invalid pattern")
        )));
    }

    #[test]
    fn malformed_pattern_reported_once_across_many_objects() {
        // reported once at the schema level, not once per object.
        let device = TypeSchema {
            key: BTreeMap::from([("name".to_string(), schema_field(FieldType::String))]),
            fields: BTreeMap::from([("code".to_string(), pattern_field("["))]),
        };
        let objects: Vec<Object> = (0..3u128)
            .map(|i| {
                let key = BTreeMap::from([("name".to_string(), json!(format!("leaf{i}")))]);
                let attrs = BTreeMap::from([("code".to_string(), json!(format!("c{i}")))]);
                Object::new(
                    uid(i + 1),
                    TypeName::new("device"),
                    Key::from(key),
                    JsonMap::from(attrs),
                )
                .unwrap()
            })
            .collect();
        let report = validate_inventory(&Inventory {
            scope: None,
            schema: Schema {
                types: BTreeMap::from([("device".to_string(), device)]),
            },
            objects,
        });
        assert_eq!(
            report
                .errors
                .iter()
                .filter(|e| matches!(e, ValidationError::InvalidSchemaPattern { .. }))
                .count(),
            1
        );
        assert_eq!(
            report
                .errors
                .iter()
                .filter(|e| matches!(
                    e,
                    ValidationError::InvalidValue { actual, .. } if actual.contains("invalid pattern")
                ))
                .count(),
            0
        );
    }

    #[test]
    fn format_or_pattern_requires_string_value() {
        // a json base type accepts the number through the type check, so the
        // only error comes from the string-constraint `as_str` else-branch.
        let mut schema = fmt_field(FieldFormat::Slug);
        schema.r#type = FieldType::Json;
        let report = check(&schema, &json!(42));
        assert_eq!(report.errors.len(), 1);
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::InvalidValue { expected, .. } if expected == "string"
        )));

        let mut schema = pattern_field(r"^\d+$");
        schema.r#type = FieldType::Json;
        let report = check(&schema, &json!(42));
        assert_eq!(report.errors.len(), 1);
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::InvalidValue { expected, .. } if expected == "string"
        )));
    }

    /// the `kind` every variant serializes as. exhaustive on purpose (see uid()):
    /// a new variant is a compile error here, so it cannot be added without its
    /// wire name being decided. that arm is all the compiler forces, though: the
    /// table below is what compares the name against the serialization.
    fn wire_kind(error: &ValidationError) -> &'static str {
        match error {
            ValidationError::DuplicateUid(_) => "duplicate_uid",
            ValidationError::DuplicateKey(_) => "duplicate_key",
            ValidationError::MissingType => "missing_type",
            ValidationError::MissingKey => "missing_key",
            ValidationError::MissingKeyField { .. } => "missing_key_field",
            ValidationError::ExtraKeyField { .. } => "extra_key_field",
            ValidationError::MissingAttrField { .. } => "missing_attr_field",
            ValidationError::ExtraAttrField { .. } => "extra_attr_field",
            ValidationError::InvalidValue { .. } => "invalid_value",
            ValidationError::UnknownType(_) => "unknown_type",
            ValidationError::MissingReference { .. } => "missing_reference",
            ValidationError::ReferenceTypeMismatch { .. } => "reference_type_mismatch",
            ValidationError::UnknownRefTarget { .. } => "unknown_ref_target",
            ValidationError::InvalidSchemaPattern { .. } => "invalid_schema_pattern",
            ValidationError::ConstraintOnNonStringField { .. } => "constraint_on_non_string_field",
            ValidationError::ConflictingFormat { .. } => "conflicting_format",
            ValidationError::EmptyEnum { .. } => "empty_enum",
            ValidationError::NonScalarKeyField { .. } => "non_scalar_key_field",
            ValidationError::NullableKeyField { .. } => "nullable_key_field",
            ValidationError::KeyFieldDisagreement { .. } => "key_field_disagreement",
            ValidationError::ScopeUnknownType { .. } => "scope_unknown_type",
            ValidationError::ScopeNonKeyField { .. } => "scope_non_key_field",
            ValidationError::ScopeInvalidValue { .. } => "scope_invalid_value",
        }
    }

    #[test]
    fn every_error_variant_serializes_its_pinned_kind() {
        // `kind` is the consumer contract (docs/cli.md), so renaming a variant is
        // a breaking change to the wire format rather than a refactor. the table
        // is hand-maintained: a twentieth variant gets its wire_kind arm from the
        // compiler, but is neither serialized nor compared until it is added here.
        let all: [ValidationError; 23] = [
            ValidationError::DuplicateUid(uid(1)),
            ValidationError::DuplicateKey("dcim.site::fra1".into()),
            ValidationError::MissingType,
            ValidationError::MissingKey,
            ValidationError::MissingKeyField {
                type_name: "dcim.site".into(),
                field: "site".into(),
            },
            ValidationError::ExtraKeyField {
                type_name: "dcim.site".into(),
                field: "bogus".into(),
            },
            ValidationError::MissingAttrField {
                type_name: "dcim.site".into(),
                field: "name".into(),
            },
            ValidationError::ExtraAttrField {
                type_name: "dcim.site".into(),
                field: "bogus".into(),
            },
            ValidationError::InvalidValue {
                field: "dcim.site.name".into(),
                expected: "string".into(),
                actual: "42".into(),
            },
            ValidationError::UnknownType("dcim.nope".into()),
            ValidationError::MissingReference {
                field: "dcim.device.site".into(),
                target: uid(2),
            },
            ValidationError::ReferenceTypeMismatch {
                field: "dcim.device.site".into(),
                target: uid(2),
                expected: "dcim.site".into(),
                actual: "dcim.device".into(),
            },
            ValidationError::UnknownRefTarget {
                type_name: "dcim.device".into(),
                field: "site".into(),
                target: "dcim.nope".into(),
            },
            ValidationError::InvalidSchemaPattern {
                type_name: "dcim.site".into(),
                field: "site".into(),
                pattern: "[".into(),
                error: "unclosed character class".into(),
            },
            ValidationError::ConstraintOnNonStringField {
                type_name: "dcim.site".into(),
                field: "count".into(),
                constraint: "pattern".into(),
                field_type: "int".into(),
            },
            ValidationError::ConflictingFormat {
                type_name: "dcim.site".into(),
                field: "asset".into(),
                field_type: "mac".into(),
                format: "format(uuid)".into(),
                type_check: "format(mac)".into(),
            },
            ValidationError::EmptyEnum {
                type_name: "dcim.site".into(),
                field: "status".into(),
            },
            ValidationError::NonScalarKeyField {
                type_name: "dcim.site".into(),
                field: "members".into(),
                field_type: "list".into(),
            },
            ValidationError::NullableKeyField {
                type_name: "dcim.site".into(),
                field: "site".into(),
            },
            ValidationError::KeyFieldDisagreement {
                type_name: "dcim.site".into(),
                field: "name".into(),
            },
            ValidationError::ScopeUnknownType {
                type_name: "dcim.site".into(),
            },
            ValidationError::ScopeNonKeyField {
                type_name: "dcim.site".into(),
                field: "name".into(),
            },
            ValidationError::ScopeInvalidValue {
                type_name: "dcim.site".into(),
                field: "slug".into(),
                expected: "slug".into(),
                actual: "FRA 1".into(),
            },
        ];

        for error in &all {
            let value = serde_json::to_value(error).unwrap();
            assert_eq!(value["kind"], json!(wire_kind(error)), "{error:?}");
        }
        let kinds: BTreeSet<&str> = all.iter().map(wire_kind).collect();
        assert_eq!(kinds.len(), all.len(), "one value per variant");
    }

    #[test]
    fn newtype_errors_carry_the_bare_inner_value_as_detail() {
        // what adjacent tagging buys: internally tagged, a newtype variant over a
        // scalar cannot serialize at all.
        assert_eq!(
            serde_json::to_value(ValidationError::DuplicateUid(uid(1))).unwrap(),
            json!({ "kind": "duplicate_uid", "detail": uid(1).to_string() })
        );
        assert_eq!(
            serde_json::to_value(ValidationError::DuplicateKey("dcim.site::fra1".into())).unwrap(),
            json!({ "kind": "duplicate_key", "detail": "dcim.site::fra1" })
        );
        assert_eq!(
            serde_json::to_value(ValidationError::UnknownType("dcim.nope".into())).unwrap(),
            json!({ "kind": "unknown_type", "detail": "dcim.nope" })
        );
    }
}
