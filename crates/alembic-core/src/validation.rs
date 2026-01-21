//! validation utilities for the ir.

use crate::ir::{FieldType, Inventory, Object, Schema, TypeName, Uid};
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
    #[error("missing field: {0}")]
    MissingField(&'static str),
    #[error("unknown type: {0}")]
    UnknownType(String),
    #[error("missing reference {field} -> {target}")]
    MissingReference { field: String, target: Uid },
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
}

/// validate uniqueness and reference integrity for the given inventory.
pub fn validate_inventory(inventory: &Inventory) -> ValidationReport {
    let mut report = ValidationReport::default();
    let mut seen_uids = BTreeSet::new();
    let mut seen_keys = BTreeSet::new();
    let mut uid_to_type = BTreeMap::new();

    for object in &inventory.objects {
        if object.key.trim().is_empty() {
            report.errors.push(ValidationError::MissingField("key"));
        }
        if object.type_name.is_empty() {
            report.errors.push(ValidationError::MissingField("type"));
        }
        if !seen_uids.insert(object.uid) {
            report
                .errors
                .push(ValidationError::DuplicateUid(object.uid));
        }
        let key = format!("{}::{}", object.type_name, object.key);
        if !seen_keys.insert(key.clone()) {
            report.errors.push(ValidationError::DuplicateKey(key));
        }
        uid_to_type.insert(object.uid, object.type_name.clone());
    }

    if let Some(schema) = &inventory.schema {
        validate_schema_types(schema, &inventory.objects, &mut report);
        for object in &inventory.objects {
            validate_object_refs(object, schema, &uid_to_type, &mut report);
        }
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

fn validate_object_refs(
    object: &Object,
    schema: &Schema,
    uid_to_type: &BTreeMap<Uid, TypeName>,
    report: &mut ValidationReport,
) {
    let Some(type_schema) = schema.types.get(object.type_name.as_str()) else {
        return;
    };

    for (field, field_schema) in &type_schema.fields {
        if field_schema.required {
            let Some(value) = object.attrs.get(field) else {
                report.errors.push(ValidationError::MissingField("attrs"));
                continue;
            };
            if value.is_null() && !field_schema.nullable {
                report.errors.push(ValidationError::MissingField("attrs"));
                continue;
            }
        }
        let Some(value) = object.attrs.get(field) else {
            continue;
        };
        validate_field_ref(
            &object.type_name,
            field,
            &field_schema.r#type,
            value,
            uid_to_type,
            report,
        );
    }
}

fn validate_field_ref(
    type_name: &TypeName,
    field: &str,
    field_type: &FieldType,
    value: &Value,
    uid_to_type: &BTreeMap<Uid, TypeName>,
    report: &mut ValidationReport,
) {
    match field_type {
        FieldType::Ref { .. } => {
            if let Some(uid) = parse_uid(value) {
                if !uid_to_type.contains_key(&uid) {
                    report.errors.push(ValidationError::MissingReference {
                        field: format!("{}.{}", type_name, field),
                        target: uid,
                    });
                }
            }
        }
        FieldType::ListRef { .. } => {
            if let Some(values) = value.as_array() {
                for entry in values {
                    if let Some(uid) = parse_uid(entry) {
                        if !uid_to_type.contains_key(&uid) {
                            report.errors.push(ValidationError::MissingReference {
                                field: format!("{}.{}", type_name, field),
                                target: uid,
                            });
                        }
                    }
                }
            }
        }
        FieldType::List { item } => {
            if let Some(values) = value.as_array() {
                for entry in values {
                    validate_field_ref(type_name, field, item, entry, uid_to_type, report);
                }
            }
        }
        FieldType::Map { value: inner } => {
            if let Some(entries) = value.as_object() {
                for entry in entries.values() {
                    validate_field_ref(type_name, field, inner, entry, uid_to_type, report);
                }
            }
        }
        _ => {}
    }
}

fn parse_uid(value: &Value) -> Option<Uid> {
    let raw = value.as_str()?;
    Uid::parse_str(raw).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{FieldSchema, FieldType, JsonMap, Object, Schema, TypeName, TypeSchema};
    use std::collections::BTreeMap;
    use uuid::Uuid;

    fn uid(value: u128) -> Uid {
        Uuid::from_u128(value)
    }

    #[test]
    fn detects_duplicate_keys() {
        let objects = vec![
            Object::new(
                uid(1),
                TypeName::new("site"),
                "fra1".to_string(),
                JsonMap::default(),
            )
            .unwrap(),
            Object::new(
                uid(2),
                TypeName::new("site"),
                "fra1".to_string(),
                JsonMap::default(),
            )
            .unwrap(),
        ];
        let report = validate_inventory(&Inventory {
            schema: None,
            objects,
        });
        assert!(report
            .errors
            .iter()
            .any(|err| matches!(err, ValidationError::DuplicateKey(_))));
    }

    #[test]
    fn detects_missing_key() {
        let objects = vec![Object::new(
            uid(30),
            TypeName::new("site"),
            "".to_string(),
            JsonMap::default(),
        )
        .unwrap()];
        let report = validate_inventory(&Inventory {
            schema: None,
            objects,
        });
        assert!(report
            .errors
            .iter()
            .any(|err| matches!(err, ValidationError::MissingField("key"))));
    }

    #[test]
    fn detects_missing_kind() {
        let objects = vec![Object {
            uid: uid(31),
            type_name: TypeName::new(""),
            key: "site=fra1".to_string(),
            attrs: JsonMap::default(),
        }];
        let report = validate_inventory(&Inventory {
            schema: None,
            objects,
        });
        assert!(report
            .errors
            .iter()
            .any(|err| matches!(err, ValidationError::MissingField("type"))));
    }

    #[test]
    fn detects_unknown_type() {
        let objects = vec![Object::new(
            uid(40),
            TypeName::new("device"),
            "leaf01".to_string(),
            JsonMap::default(),
        )
        .unwrap()];
        let schema = Schema {
            types: BTreeMap::new(),
        };
        let report = validate_inventory(&Inventory {
            schema: Some(schema),
            objects,
        });
        assert!(report
            .errors
            .iter()
            .any(|err| matches!(err, ValidationError::UnknownType(_))));
    }

    #[test]
    fn detects_missing_references_with_schema() {
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
        types.insert("device".to_string(), TypeSchema { fields });
        let schema = Schema { types };

        let mut attrs = BTreeMap::new();
        attrs.insert(
            "owner".to_string(),
            serde_json::json!(Uuid::from_u128(99).to_string()),
        );
        let objects = vec![Object::new(
            uid(41),
            TypeName::new("device"),
            "leaf01".to_string(),
            attrs.into(),
        )
        .unwrap()];
        let report = validate_inventory(&Inventory {
            schema: Some(schema),
            objects,
        });
        assert!(report
            .errors
            .iter()
            .any(|err| matches!(err, ValidationError::MissingReference { .. })));
    }
}
