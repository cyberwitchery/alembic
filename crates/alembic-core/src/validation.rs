//! validation utilities for the ir.

use crate::ir::{key_string, FieldType, Inventory, Object, Schema, TypeName, Uid};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
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
}

impl ValidationError {
    /// return the uid associated with this error, if any.
    pub fn uid(&self) -> Option<Uid> {
        match self {
            ValidationError::DuplicateUid(uid) => Some(*uid),
            ValidationError::MissingReference { target, .. } => Some(*target),
            ValidationError::ReferenceTypeMismatch { target, .. } => Some(*target),
            _ => None,
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
            _ => None,
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

    validate_schema_types(&inventory.schema, &inventory.objects, &mut report);
    for object in &inventory.objects {
        validate_object(object, &inventory.schema, &uid_to_type, &mut report);
    }

    report
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

fn value_matches_type(value: &Value, field_type: &FieldType) -> bool {
    match field_type {
        FieldType::String
        | FieldType::Text
        | FieldType::Date
        | FieldType::Datetime
        | FieldType::Time
        | FieldType::IpAddress
        | FieldType::Cidr
        | FieldType::Prefix
        | FieldType::Mac
        | FieldType::Slug => value.is_string(),
        FieldType::Uuid => value
            .as_str()
            .map(|raw| Uid::parse_str(raw).is_ok())
            .unwrap_or(false),
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
    use crate::ir::{FieldSchema, FieldType, JsonMap, Key, Object, Schema, TypeName, TypeSchema};
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

    #[test]
    fn test_field_value_validation() {
        let uid_to_type = BTreeMap::from([(uid(1), TypeName::new("target"))]);
        let mut report = ValidationReport::default();

        // Test Type Mismatch
        let schema = FieldSchema {
            r#type: FieldType::Int,
            required: true,
            nullable: false,
            description: None,
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

        // Test Enum
        let schema = FieldSchema {
            r#type: FieldType::Enum {
                values: vec!["a".to_string(), "b".to_string()],
            },
            required: true,
            nullable: false,
            description: None,
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

        // Test Reference Type Mismatch
        let schema = FieldSchema {
            r#type: FieldType::Ref {
                target: "wrong".to_string(),
            },
            required: true,
            nullable: false,
            description: None,
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

        // Test ListRef
        let schema = FieldSchema {
            r#type: FieldType::ListRef {
                target: "target".to_string(),
            },
            required: true,
            nullable: false,
            description: None,
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

        // Test Map
        let schema = FieldSchema {
            r#type: FieldType::Map {
                value: Box::new(FieldType::Int),
            },
            required: true,
            nullable: false,
            description: None,
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

        // Test Uuid
        let schema = FieldSchema {
            r#type: FieldType::Uuid,
            required: true,
            nullable: false,
            description: None,
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

        // Test List of Refs
        let schema = FieldSchema {
            r#type: FieldType::List {
                item: Box::new(FieldType::Ref {
                    target: "target".to_string(),
                }),
            },
            required: true,
            nullable: false,
            description: None,
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
}
