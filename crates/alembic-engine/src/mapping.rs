//! shared mapping helpers used by multiple adapters.

use alembic_core::{FieldSchema, FieldType};
use anyhow::{anyhow, Result};
use serde_json::Value;

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

/// map an Alembic field type to the backend custom-field type string.
pub fn custom_field_type_for_schema(field: &FieldSchema) -> String {
    match field.r#type {
        FieldType::Int => "integer".to_string(),
        FieldType::Float => "decimal".to_string(),
        FieldType::Bool => "boolean".to_string(),
        FieldType::Date => "date".to_string(),
        FieldType::Datetime => "datetime".to_string(),
        FieldType::Json | FieldType::List { .. } | FieldType::Map { .. } => "json".to_string(),
        _ => "text".to_string(),
    }
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
                }
            }
            _ => {}
        }
    }
    Ok(tags)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_slugify() {
        assert_eq!(slugify("Hello World"), "hello-world");
        assert_eq!(slugify("EVPN Fabric!"), "evpn-fabric");
        assert_eq!(slugify("---test---"), "test");
    }

    #[test]
    fn test_custom_field_type_for_schema() {
        let schema = |r#type| FieldSchema {
            r#type,
            required: false,
            nullable: true,
            description: None,
            format: None,
            pattern: None,
        };
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
            "decimal"
        );
        assert_eq!(
            custom_field_type_for_schema(&schema(FieldType::Bool)),
            "boolean"
        );
        assert_eq!(
            custom_field_type_for_schema(&schema(FieldType::Json)),
            "json"
        );
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
}
