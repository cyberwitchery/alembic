use alembic_engine::MissingCustomField;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn slugify(input: &str) -> String {
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

pub(super) fn build_tag_inputs(tags: &[String]) -> Vec<Value> {
    tags.iter()
        .map(|tag| {
            serde_json::json!({
                "name": tag,
                "slug": slugify(tag),
            })
        })
        .collect()
}

#[derive(Default)]
pub(super) struct CustomFieldProposal {
    pub(super) object_types: BTreeSet<String>,
    pub(super) field_type: String,
}

pub(super) fn group_custom_fields(
    missing: &[MissingCustomField],
) -> BTreeMap<String, CustomFieldProposal> {
    let mut grouped: BTreeMap<String, CustomFieldProposal> = BTreeMap::new();
    for entry in missing {
        let proposal = grouped.entry(entry.field.clone()).or_default();
        proposal.object_types.insert(entry.type_name.clone());
        let entry_type = custom_field_type(&entry.sample);
        proposal.field_type = merge_field_type(&proposal.field_type, entry_type);
    }
    grouped
}

pub(super) fn custom_field_type(value: &Value) -> String {
    match value {
        Value::String(_) => "text".to_string(),
        Value::Bool(_) => "boolean".to_string(),
        Value::Number(number) => {
            if number.is_i64() || number.is_u64() {
                "integer".to_string()
            } else {
                "decimal".to_string()
            }
        }
        Value::Array(_) | Value::Object(_) => "json".to_string(),
        Value::Null => "text".to_string(),
    }
}

pub(super) fn merge_field_type(current: &str, incoming: String) -> String {
    if current.is_empty() {
        return incoming;
    }
    if current == "json" || incoming == "json" {
        return "json".to_string();
    }
    if current == "text" || incoming == "text" {
        return "text".to_string();
    }
    if current == "decimal" || incoming == "decimal" {
        return "decimal".to_string();
    }
    if current == "boolean" || incoming == "boolean" {
        return "boolean".to_string();
    }
    "integer".to_string()
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
    fn test_build_tag_inputs() {
        let tags = vec!["Alembic Test".to_string()];
        let inputs = build_tag_inputs(&tags);
        assert_eq!(inputs.len(), 1);
        assert_eq!(
            inputs[0],
            json!({"name": "Alembic Test", "slug": "alembic-test"})
        );
    }

    #[test]
    fn test_custom_field_type() {
        assert_eq!(custom_field_type(&json!("string")), "text");
        assert_eq!(custom_field_type(&json!(123)), "integer");
        assert_eq!(custom_field_type(&json!(1.23)), "decimal");
        assert_eq!(custom_field_type(&json!(true)), "boolean");
        assert_eq!(custom_field_type(&json!([1, 2])), "json");
    }

    #[test]
    fn test_merge_field_type() {
        assert_eq!(merge_field_type("", "text".to_string()), "text");
        assert_eq!(merge_field_type("integer", "json".to_string()), "json");
        assert_eq!(merge_field_type("integer", "text".to_string()), "text");
        assert_eq!(
            merge_field_type("integer", "decimal".to_string()),
            "decimal"
        );
    }
}
