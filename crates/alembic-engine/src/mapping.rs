//! shared mapping helpers used by multiple adapters.

use alembic_core::{format_for_field_type, format_regex, FieldSchema, FieldType};
use anyhow::{anyhow, Result};
use serde_json::Value;
use std::collections::BTreeSet;

/// convert a human-readable label into a URL-safe slug.
///
/// lowercases all characters, replaces runs of non-alphanumeric characters
/// with a single dash, and strips leading/trailing dashes.
pub fn slugify(input: &str) -> String {
    let mut out = String::new();
    let mut last_dash = false;
    for ch in input.chars() {
        let lower = ch.to_ascii_lowercase();
        if lower.is_ascii_alphanumeric() {
            out.push(lower);
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }
    while out.ends_with('-') {
        out.pop();
    }
    while out.starts_with('-') {
        out.remove(0);
    }
    out
}

/// map an Alembic field type to the backend custom-field type string. shared by
/// netbox and nautobot, so it stays on the types both accept as plain fields;
/// richer per-backend mappings (e.g. netbox object/longtext) live in the adapter.
pub fn custom_field_type_for_schema(field: &FieldSchema) -> String {
    match field.r#type {
        FieldType::Int => "integer".to_string(),
        // nautobot has no `decimal`, and its `text` reads back as a json string
        // the ir's float check rejects; netbox upgrades this to `decimal`.
        FieldType::Float => "json".to_string(),
        FieldType::Bool => "boolean".to_string(),
        FieldType::Date => "date".to_string(),
        FieldType::Datetime => "datetime".to_string(),
        FieldType::Json | FieldType::List { .. } | FieldType::Map { .. } => "json".to_string(),
        // string-like scalars become plain text. ref/listref can't reach here
        // (adapters skip them before custom-field creation); listed for exhaustiveness.
        FieldType::String
        | FieldType::Text
        | FieldType::Uuid
        | FieldType::Time
        | FieldType::IpAddress
        | FieldType::Cidr
        | FieldType::Prefix
        | FieldType::Mac
        | FieldType::Slug
        | FieldType::Enum { .. }
        | FieldType::Ref { .. }
        | FieldType::ListRef { .. } => "text".to_string(),
    }
}

/// the regex a backend custom field should enforce as `validation_regex`, or
/// `None` when it would enforce nothing: the declared `pattern:`, else the
/// declared `format:`, else the format the field's type carries. takes the
/// already-mapped backend type string because a regex only constrains text, and
/// core allows a `pattern` on json/ref/date/datetime/time fields too.
pub fn validation_regex_for_schema<'a>(
    field: &'a FieldSchema,
    backend_type: &str,
) -> Option<&'a str> {
    if !matches!(backend_type, "text" | "longtext") {
        return None;
    }
    // an author-written constraint beats a derived one.
    if let Some(pattern) = field.pattern.as_deref() {
        return Some(pattern);
    }
    field
        .format
        .clone()
        .or_else(|| format_for_field_type(&field.r#type))
        .map(|format| format_regex(&format))
}

/// what a backend custom field currently holds for the properties a provision
/// converges. an unset one reads as the backends' own empty value, so a property
/// neither side sets compares equal and produces no patch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExistingCustomField {
    pub required: bool,
    pub description: String,
    pub validation_regex: String,
}

impl ExistingCustomField {
    /// the converged properties, each paired with what the backend holds for it.
    /// one list rather than a name list and a lookup, so a property cannot be
    /// compared against a value nothing supplies. these are exactly the
    /// properties both create payloads carry beyond identity and type, and all
    /// three sit on the vendors' patch bodies.
    fn converged(&self) -> [(&'static str, Value); 3] {
        [
            ("required", Value::Bool(self.required)),
            ("description", Value::String(self.description.clone())),
            (
                "validation_regex",
                Value::String(self.validation_regex.clone()),
            ),
        ]
    }
}

/// the names `converged` pairs values with.
fn converged_properties() -> impl Iterator<Item = &'static str> {
    ExistingCustomField::default()
        .converged()
        .into_iter()
        .map(|(property, _)| property)
}

/// fold one declaration's create payload into `desired`, the converged properties
/// every declaration landing on the same backend field has agreed on so far.
/// returns the property two of them disagree on.
///
/// one backend custom field carries a *list* of content types, so several declared
/// types can share it. a property only one declaration carries is taken as
/// declared: the other is silent about it, not opposed to it.
pub fn merge_converged_properties(
    desired: &mut serde_json::Map<String, Value>,
    create_payload: &Value,
) -> Option<&'static str> {
    let payload = create_payload.as_object()?;
    for property in converged_properties() {
        let Some(value) = payload.get(property) else {
            continue;
        };
        match desired.get(property) {
            Some(agreed) if agreed != value => return Some(property),
            _ => {
                desired.insert(property.to_string(), value.clone());
            }
        }
    }
    None
}

/// the patch that converges an existing custom field onto `desired`, or `None`
/// when it already agrees.
///
/// this is the engine's additive-only diff rule one level up, at the schema layer:
/// a converged property `desired` omits is one the schema does not declare, so the
/// backend keeps whatever it has rather than being blanked. `desired` is built from
/// the create payloads themselves so an update can never converge onto something a
/// create would not have written.
pub fn custom_field_update_payload(
    existing: &ExistingCustomField,
    desired: &Value,
) -> Option<Value> {
    let desired = desired.as_object()?;
    let mut patch = serde_json::Map::new();
    for (property, current) in existing.converged() {
        let Some(value) = desired.get(property) else {
            continue;
        };
        if current != *value {
            patch.insert(property.to_string(), value.clone());
        }
    }
    (!patch.is_empty()).then_some(Value::Object(patch))
}

/// extract tag names from a JSON value returned by a backend.
///
/// accepts arrays of strings or objects with `"name"` / `"slug"` fields,
/// and returns the collected tag names.
pub fn tags_from_value(value: &Value) -> Result<Vec<String>> {
    let items = match value {
        Value::Array(items) => items,
        Value::Null => return Ok(Vec::new()),
        _ => return Err(anyhow!("tags must be an array")),
    };
    let mut tags = Vec::new();
    for item in items {
        match item {
            Value::String(name) => tags.push(name.clone()),
            Value::Object(map) => {
                if let Some(Value::String(name)) = map.get("name") {
                    tags.push(name.clone());
                } else if let Some(Value::String(slug)) = map.get("slug") {
                    tags.push(slug.clone());
                } else {
                    return Err(anyhow!(
                        "tag object must have a string name or slug: {item}"
                    ));
                }
            }
            _ => return Err(anyhow!("tag item must be a string or object: {item}")),
        }
    }
    Ok(tags)
}

/// return whether the feature set contains any of the candidate feature flags.
pub fn supports_feature(features: &BTreeSet<String>, candidates: &[&str]) -> bool {
    candidates.iter().any(|name| features.contains(*name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alembic_core::FieldFormat;
    use serde_json::json;

    #[test]
    fn test_slugify() {
        assert_eq!(slugify("Hello World"), "hello-world");
        assert_eq!(slugify("EVPN Fabric!"), "evpn-fabric");
        assert_eq!(slugify("---test---"), "test");
    }

    /// the backend custom-field type strings netbox and nautobot both accept,
    /// from their generated clients' own enums. netbox spells its multi-valued
    /// selection `multiselect` and nautobot `multi-select`, so neither is here.
    const BOTH_BACKENDS_ACCEPT: &[&str] = &[
        "boolean", "date", "datetime", "integer", "json", "select", "text", "url",
    ];

    fn schema(r#type: FieldType) -> FieldSchema {
        FieldSchema {
            r#type,
            required: false,
            nullable: true,
            description: None,
            format: None,
            pattern: None,
        }
    }

    /// every variant of the ir's field type, so the sweep below is a property of
    /// the map rather than of the cases someone thought to list.
    fn every_field_type() -> Vec<FieldType> {
        vec![
            FieldType::String,
            FieldType::Text,
            FieldType::Int,
            FieldType::Float,
            FieldType::Bool,
            FieldType::Uuid,
            FieldType::Date,
            FieldType::Datetime,
            FieldType::Time,
            FieldType::Json,
            FieldType::IpAddress,
            FieldType::Cidr,
            FieldType::Prefix,
            FieldType::Mac,
            FieldType::Slug,
            FieldType::Enum {
                values: vec!["a".to_string()],
            },
            FieldType::List {
                item: Box::new(FieldType::String),
            },
            FieldType::Map {
                value: Box::new(FieldType::String),
            },
            FieldType::Ref {
                target: "site".to_string(),
            },
            FieldType::ListRef {
                target: "site".to_string(),
            },
        ]
    }

    #[test]
    fn test_shared_map_stays_within_the_backend_intersection() {
        for r#type in every_field_type() {
            let mapped = custom_field_type_for_schema(&schema(r#type.clone()));
            assert!(
                BOTH_BACKENDS_ACCEPT.contains(&mapped.as_str()),
                "{type:?} maps to `{mapped}`, which not both backends accept"
            );
        }
    }

    #[test]
    fn test_custom_field_type_for_schema() {
        assert_eq!(
            custom_field_type_for_schema(&schema(FieldType::String)),
            "text"
        );
        assert_eq!(
            custom_field_type_for_schema(&schema(FieldType::Int)),
            "integer"
        );
        assert_eq!(
            custom_field_type_for_schema(&schema(FieldType::Float)),
            "json"
        );
        assert_eq!(
            custom_field_type_for_schema(&schema(FieldType::Bool)),
            "boolean"
        );
        assert_eq!(
            custom_field_type_for_schema(&schema(FieldType::Json)),
            "json"
        );
        assert_eq!(
            custom_field_type_for_schema(&schema(FieldType::Date)),
            "date"
        );
        assert_eq!(
            custom_field_type_for_schema(&schema(FieldType::Datetime)),
            "datetime"
        );
        assert_eq!(
            custom_field_type_for_schema(&schema(FieldType::Text)),
            "text"
        );
        assert_eq!(
            custom_field_type_for_schema(&schema(FieldType::List {
                item: Box::new(FieldType::String)
            })),
            "json"
        );
        assert_eq!(
            custom_field_type_for_schema(&schema(FieldType::Map {
                value: Box::new(FieldType::String)
            })),
            "json"
        );
    }

    #[test]
    fn test_validation_regex_for_schema() {
        let with_pattern = FieldSchema {
            r#type: FieldType::String,
            required: false,
            nullable: true,
            description: None,
            format: None,
            pattern: Some("^[A-Z]{3}$".to_string()),
        };
        assert_eq!(
            validation_regex_for_schema(&with_pattern, "text"),
            Some("^[A-Z]{3}$")
        );
        // netbox's custom-object equivalent of `text`.
        assert_eq!(
            validation_regex_for_schema(&with_pattern, "longtext"),
            Some("^[A-Z]{3}$")
        );
        // a regex on a non-text backend field constrains nothing.
        for backend_type in ["json", "object", "date", "datetime", "integer"] {
            assert_eq!(
                validation_regex_for_schema(&with_pattern, backend_type),
                None
            );
        }
        let without_pattern = FieldSchema {
            pattern: None,
            ..with_pattern
        };
        assert_eq!(validation_regex_for_schema(&without_pattern, "text"), None);
    }

    #[test]
    fn test_validation_regex_resolves_pattern_then_format_then_type() {
        let base = FieldSchema {
            r#type: FieldType::String,
            required: false,
            nullable: true,
            description: None,
            format: None,
            pattern: None,
        };
        let declared = FieldSchema {
            format: Some(FieldFormat::Mac),
            ..base.clone()
        };
        assert_eq!(
            validation_regex_for_schema(&declared, "text"),
            Some(format_regex(&FieldFormat::Mac))
        );
        // an author-written pattern beats the derived one.
        let both = FieldSchema {
            pattern: Some("^[A-Z]{3}$".to_string()),
            ..declared.clone()
        };
        assert_eq!(
            validation_regex_for_schema(&both, "text"),
            Some("^[A-Z]{3}$")
        );
        // the type carries the format on its own.
        for (r#type, format) in [
            (FieldType::Mac, FieldFormat::Mac),
            (FieldType::Uuid, FieldFormat::Uuid),
            (FieldType::Cidr, FieldFormat::Cidr),
            (FieldType::Prefix, FieldFormat::Prefix),
            (FieldType::Slug, FieldFormat::Slug),
        ] {
            let typed = FieldSchema {
                r#type,
                ..base.clone()
            };
            assert_eq!(
                validation_regex_for_schema(&typed, "text"),
                Some(format_regex(&format))
            );
        }
        // a declared format on a non-text backend field still constrains nothing.
        assert_eq!(validation_regex_for_schema(&declared, "select"), None);
    }

    #[test]
    fn test_supports_feature() {
        let mut features = BTreeSet::new();
        features.insert("tags".to_string());
        assert!(supports_feature(&features, &["tags"]));
        assert!(!supports_feature(&features, &["custom-fields"]));
    }

    #[test]
    fn test_tags_from_value_array_of_strings() {
        let val = json!(["tag1", "tag2"]);
        assert_eq!(tags_from_value(&val).unwrap(), vec!["tag1", "tag2"]);
    }

    #[test]
    fn test_tags_from_value_array_of_objects() {
        let val = json!([{"name": "tag1"}, {"slug": "tag-2"}]);
        assert_eq!(tags_from_value(&val).unwrap(), vec!["tag1", "tag-2"]);
    }

    #[test]
    fn test_tags_from_value_null() {
        let val = json!(null);
        assert_eq!(tags_from_value(&val).unwrap(), Vec::<String>::new());
    }

    #[test]
    fn test_tags_from_value_invalid() {
        let val = json!("not an array");
        assert!(tags_from_value(&val).is_err());
    }

    #[test]
    fn test_tags_from_value_non_string_non_object_item() {
        assert!(tags_from_value(&json!([1, 2])).is_err());
        assert!(tags_from_value(&json!(["ok", 5])).is_err());
    }

    #[test]
    fn test_tags_from_value_object_without_name_or_slug() {
        assert!(tags_from_value(&json!([{"id": 5}])).is_err());
    }
}
