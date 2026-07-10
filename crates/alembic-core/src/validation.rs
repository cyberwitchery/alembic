//! validation utilities for the ir.

use crate::ir::{
    key_string, FieldFormat, FieldType, Inventory, Object, Schema, SourceLocation, TypeName, Uid,
};
use ipnet::IpNet;
use regex::Regex;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::IpAddr;
use std::sync::OnceLock;
use thiserror::Error;

/// validation errors emitted during graph validation.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
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
    #[error("empty enum for {type_name}.{field}: an enum with no values is unsatisfiable")]
    EmptyEnum { type_name: String, field: String },
    #[error(
        "non-scalar key field {type_name}.{field}: a {field_type} key has no scalar identity form (docs/map.md)"
    )]
    NonScalarKeyField {
        type_name: String,
        field: String,
        field_type: String,
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
            | ValidationError::EmptyEnum { .. }
            | ValidationError::NonScalarKeyField { .. } => None,
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
            // exhaustive on purpose (no `_`): a new variant that carries a key must
            // be classified here rather than silently returning None.
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
            | ValidationError::EmptyEnum { .. }
            | ValidationError::NonScalarKeyField { .. } => None,
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
            | ValidationError::EmptyEnum { type_name, .. }
            | ValidationError::NonScalarKeyField { type_name, .. } => Some(type_name.clone()),
            ValidationError::InvalidValue { field, .. } => {
                field.split('.').next().map(|s| s.to_string())
            }
            ValidationError::MissingReference { field, .. }
            | ValidationError::ReferenceTypeMismatch { field, .. } => {
                field.split('.').next().map(|s| s.to_string())
            }
            ValidationError::DuplicateKey(key) => key.split("::").next().map(|s| s.to_string()),
            // exhaustive on purpose (no `_`): a new variant that carries a type name
            // must be classified here rather than silently returning None, which is
            // what dropped these four validators' source locations to begin with.
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
            // exhaustive on purpose (no `_`): a new variant that carries a dotted
            // field path must be classified here rather than silently returning None.
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
            | ValidationError::EmptyEnum { .. }
            | ValidationError::NonScalarKeyField { .. } => None,
        }
    }
}

/// a validation error with optional source location.
#[derive(Debug, Clone)]
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
        // build lookup maps
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
                            // for DuplicateKey errors, try to find source
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
    validate_schema_patterns(&inventory.schema, &mut report);
    validate_schema_constraint_types(&inventory.schema, &mut report);
    validate_schema_enums(&inventory.schema, &mut report);
    validate_schema_key_scalar(&inventory.schema, &mut report);
    validate_schema_types(&inventory.schema, &inventory.objects, &mut report);
    for object in &inventory.objects {
        validate_object(object, &inventory.schema, &uid_to_type, &mut report);
    }

    report
}

/// validate that every `ref`/`list_ref` target declared in the schema names a
/// declared type.
///
/// targets are free-form strings, so a typo (`tenant` for `tenancy.tenant`)
/// would otherwise pass schema validation and only surface later as misleading
/// per-object reference errors. this catches the mistake at the schema level,
/// attributed to the declaring type and field.
fn validate_schema_ref_targets(schema: &Schema, report: &mut ValidationReport) {
    for (type_name, type_schema) in &schema.types {
        for (field, field_schema) in &type_schema.key {
            validate_field_ref_targets(
                schema,
                type_name,
                &format!("key.{field}"),
                &field_schema.r#type,
                report,
            );
        }
        for (field, field_schema) in &type_schema.fields {
            validate_field_ref_targets(schema, type_name, field, &field_schema.r#type, report);
        }
    }
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

/// validate that every `pattern:` regex declared in the schema compiles.
///
/// a typo'd pattern would otherwise only surface when some object happens to
/// use the field (and never at all for a type that has no objects yet), as a
/// confusing per-object error. this catches the mistake at the schema level,
/// attributed to the declaring type and field.
///
/// `pattern` lives only on the top-level `FieldSchema` of each key/attr field;
/// it is never nested inside `list`/`map` item types, so a flat iteration over
/// key and attr fields is complete.
fn validate_schema_patterns(schema: &Schema, report: &mut ValidationReport) {
    for (type_name, type_schema) in &schema.types {
        for (field, field_schema) in &type_schema.key {
            validate_field_pattern(type_name, &format!("key.{field}"), field_schema, report);
        }
        for (field, field_schema) in &type_schema.fields {
            validate_field_pattern(type_name, field, field_schema, report);
        }
    }
}

/// compile a single field's `pattern:`, recording an error if it is malformed.
fn validate_field_pattern(
    type_name: &str,
    field: &str,
    field_schema: &crate::ir::FieldSchema,
    report: &mut ValidationReport,
) {
    let Some(pattern) = &field_schema.pattern else {
        return;
    };
    if let Err(err) = Regex::new(pattern) {
        report.errors.push(ValidationError::InvalidSchemaPattern {
            type_name: type_name.to_string(),
            field: field.to_string(),
            pattern: pattern.to_string(),
            error: err.to_string(),
        });
    }
}

/// reject a top-level `format:`/`pattern:` on a field whose type can never hold
/// a string; otherwise it is silently accepted at load and only fails per-object
/// as a misleading `expected string` error (never at all for an empty type).
///
/// `format`/`pattern` live only on the top-level `FieldSchema`, so a flat walk
/// over key and attr fields is complete.
fn validate_schema_constraint_types(schema: &Schema, report: &mut ValidationReport) {
    for (type_name, type_schema) in &schema.types {
        for (field, field_schema) in &type_schema.key {
            validate_field_constraint_type(
                type_name,
                &format!("key.{field}"),
                field_schema,
                report,
            );
        }
        for (field, field_schema) in &type_schema.fields {
            validate_field_constraint_type(type_name, field, field_schema, report);
        }
    }
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

/// reject an `enum` field declared with an empty `values` list. an empty enum is
/// unsatisfiable, so every value fails per-object with a confusing
/// `expected: enum()` message (and never at all for a type with no objects). this
/// catches the mistake at the schema level, attributed to the declaring type and
/// field.
///
/// `list`/`map` item types can nest an enum, and per-object validation recurses
/// into them keeping the same field label, so this walk recurses the same way.
fn validate_schema_enums(schema: &Schema, report: &mut ValidationReport) {
    for (type_name, type_schema) in &schema.types {
        for (field, field_schema) in &type_schema.key {
            validate_field_enum(
                type_name,
                &format!("key.{field}"),
                &field_schema.r#type,
                report,
            );
        }
        for (field, field_schema) in &type_schema.fields {
            validate_field_enum(type_name, field, &field_schema.r#type, report);
        }
    }
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
/// (docs/map.md). validation enforced this per-value but not per-type, so a
/// schema declaring a composite key field passed `alembic validate` clean yet
/// was un-representable by the pipeline. catching the type at schema load keeps
/// the invalid state unrepresentable before any object is authored.
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
    report: &mut ValidationReport,
) {
    let Some(type_schema) = schema.types.get(object.type_name.as_str()) else {
        return;
    };

    validate_key_fields(object, type_schema, uid_to_type, report);
    validate_attr_fields(object, type_schema, uid_to_type, report);
}

fn validate_key_fields(
    object: &Object,
    type_schema: &crate::ir::TypeSchema,
    uid_to_type: &BTreeMap<Uid, TypeName>,
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

fn validate_field_value(
    type_name: &TypeName,
    field: &str,
    field_schema: &crate::ir::FieldSchema,
    value: &Value,
    uid_to_type: &BTreeMap<Uid, TypeName>,
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
                for entry in entries {
                    let schema = crate::ir::FieldSchema {
                        r#type: (**item).clone(),
                        required: true,
                        nullable: false,
                        description: None,
                        format: None,
                        pattern: None,
                    };
                    validate_field_value(type_name, field, &schema, entry, uid_to_type, report);
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
                for entry in entries.values() {
                    let schema = crate::ir::FieldSchema {
                        r#type: (**inner).clone(),
                        required: true,
                        nullable: false,
                        description: None,
                        format: None,
                        pattern: None,
                    };
                    validate_field_value(type_name, field, &schema, entry, uid_to_type, report);
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
                    actual: value_type_label(value),
                });
            }
        }
    }

    validate_string_constraints(type_name, field, field_schema, value, report);
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
        report.errors.push(ValidationError::InvalidValue {
            field: format!("{type_name}.{field}"),
            expected: "uuid".to_string(),
            actual: value_type_label(value),
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
        match Regex::new(pattern) {
            Ok(regex) => {
                if !regex.is_match(raw) {
                    report.errors.push(ValidationError::InvalidValue {
                        field: format!("{type_name}.{field}"),
                        expected: format!("pattern({pattern})"),
                        actual: raw.to_string(),
                    });
                }
            }
            Err(err) => {
                report.errors.push(ValidationError::InvalidValue {
                    field: format!("{type_name}.{field}"),
                    expected: format!("pattern({pattern})"),
                    actual: format!("invalid pattern: {err}"),
                });
            }
        }
    }
}

fn slug_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[a-z0-9]+(?:[a-z0-9_-]*[a-z0-9])?$").unwrap())
}

fn mac_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$").unwrap())
}

fn matches_format(format: &FieldFormat, raw: &str) -> bool {
    match format {
        FieldFormat::Slug => slug_regex().is_match(raw),
        FieldFormat::IpAddress => raw.parse::<IpAddr>().is_ok(),
        FieldFormat::Cidr | FieldFormat::Prefix => raw.parse::<IpNet>().is_ok(),
        FieldFormat::Mac => mac_regex().is_match(raw),
        FieldFormat::Uuid => Uid::parse_str(raw).is_ok(),
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

fn value_matches_type(value: &Value, field_type: &FieldType) -> bool {
    match field_type {
        FieldType::String
        | FieldType::Text
        | FieldType::Date
        | FieldType::Datetime
        | FieldType::Time
        // `ip_address` stays a plain string check: the canonical IPAM examples
        // carry NetBox-style masked addresses (`10.0.0.10/24`) that the strict
        // `IpAddr` format rejects, so whether it should accept a mask is a
        // convention decision left to the maintainer rather than guessed here.
        | FieldType::IpAddress => value.is_string(),
        // format-typed fields with an unambiguous textual format must hold a
        // string that matches it, mirroring how the `format:` constraint validates.
        FieldType::Uuid => value_matches_format(value, &FieldFormat::Uuid),
        FieldType::Cidr => value_matches_format(value, &FieldFormat::Cidr),
        FieldType::Prefix => value_matches_format(value, &FieldFormat::Prefix),
        FieldType::Mac => value_matches_format(value, &FieldFormat::Mac),
        FieldType::Slug => value_matches_format(value, &FieldFormat::Slug),
        FieldType::Int => value.is_i64() || value.is_u64(),
        FieldType::Float => value.as_f64().is_some() || value.is_i64() || value.is_u64(),
        FieldType::Bool => value.is_boolean(),
        FieldType::Json => true,
        FieldType::Enum { .. } => value.is_string(),
        FieldType::List { .. } => value.is_array(),
        FieldType::Map { .. } => value.is_object(),
        FieldType::Ref { .. } | FieldType::ListRef { .. } => true,
    }
}

/// a value satisfies a format-typed field when it is a string matching that format.
fn value_matches_format(value: &Value, format: &FieldFormat) -> bool {
    value
        .as_str()
        .map(|raw| matches_format(format, raw))
        .unwrap_or(false)
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
        let report = validate_inventory(&Inventory { schema, objects });
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
        let report = validate_inventory(&Inventory { schema, objects });
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

    /// run schema-only validation (no objects) and return the report.
    fn validate_schema(types: BTreeMap<String, TypeSchema>) -> ValidationReport {
        validate_inventory(&Inventory {
            schema: Schema { types },
            objects: vec![],
        })
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
        // a key component must render to a scalar to have an identity form
        // (docs/map.md); a composite key type never can. schema-load validation
        // catches all three composite types even with no objects present (the
        // `validate_schema` helper passes none), before render would reject the
        // value at map time.
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
        // the critical carve-out: a `ref` key value renders to a scalar uid
        // string, so a `ref` key is legal. infrahub keys an interface by
        // (device: ref, name: string); both fields must validate clean.
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
        // object supplies a composite VALUE is already rejected by per-object
        // value-type validation, so a composite key VALUE never reaches uid
        // derivation either. the schema is scalar-keyed, so no type-level error
        // fires here — only the per-object value error.
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
        // the headline win: a bad pattern on a type with NO objects is never
        // reached by per-object validation, but schema-load validation catches it.
        let ghost = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::from([("name".to_string(), pattern_field("[bad"))]),
        };
        let report = validate_inventory(&Inventory {
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
        // the headline win: a constraint on a never-string field of a type with
        // NO objects is never reached by per-object validation, but schema-load
        // validation catches it.
        let mut count = schema_field(FieldType::Int);
        count.pattern = Some("^[0-9]+$".to_string());
        let ghost = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::from([("count".to_string(), count)]),
        };
        let report = validate_inventory(&Inventory {
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
        // the headline win: an empty enum on a type with NO objects is never
        // reached by per-object validation, but schema-load validation catches
        // it. the key field also exercises the `key.{field}` label path.
        let ghost = TypeSchema {
            key: BTreeMap::from([(
                "role".to_string(),
                schema_field(FieldType::Enum { values: vec![] }),
            )]),
            fields: BTreeMap::new(),
        };
        let report = validate_inventory(&Inventory {
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
        // the four newest schema validators carry a `type_name`; with_sources must
        // resolve it to the declaring type's source line, like every older error.
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
    fn with_sources_attaches_location_for_unknown_ref_target() {
        // a second of the four newest validators, proving the arm resolves per
        // variant and not just for empty-enum.
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
        // the newest schema validator also carries a `type_name`; with_sources
        // must resolve it to the declaring type's source line, like every older
        // schema-load error.
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
    fn test_field_value_validation() {
        let uid_to_type = BTreeMap::from([(uid(1), TypeName::new("target"))]);
        let mut report = ValidationReport::default();

        // test Type Mismatch
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
            &mut report,
        );
        assert!(report
            .errors
            .iter()
            .any(|e| matches!(e, ValidationError::InvalidValue { .. })));

        // test Enum
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
            &mut report,
        );
        assert!(report
            .errors
            .iter()
            .any(|e| matches!(e, ValidationError::InvalidValue { .. })));

        // test Reference Type Mismatch
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
            &mut report,
        );
        assert!(report
            .errors
            .iter()
            .any(|e| matches!(e, ValidationError::ReferenceTypeMismatch { .. })));

        // test ListRef
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
            &mut report,
        );
        assert!(report.errors.is_empty());

        // test Map
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
            &mut report,
        );
        assert!(report
            .errors
            .iter()
            .any(|e| matches!(e, ValidationError::InvalidValue { .. })));

        // test Uuid
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
            &mut report,
        );
        assert!(report
            .errors
            .iter()
            .any(|e| matches!(e, ValidationError::InvalidValue { .. })));

        // test List of Refs
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
        validate_field_value(
            &TypeName::new("test"),
            "field",
            schema,
            value,
            &uid_to_type,
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
        // `ip_address` is intentionally not format-validated yet: NetBox-style
        // masked addresses (as in examples/e2e.yaml) must keep passing until the
        // mask convention is decided. both bare and masked strings are accepted.
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
    fn invalid_pattern_reports_error_without_panicking() {
        // an unparsable regex must surface a clean InvalidValue, not panic.
        let report = check(&pattern_field("["), &json!("anything"));
        assert!(report.errors.iter().any(|e| matches!(
            e,
            ValidationError::InvalidValue { actual, .. } if actual.contains("invalid pattern")
        )));
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
}
