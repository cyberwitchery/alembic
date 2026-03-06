use alembic_core::{FieldSchema, FieldType};

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
    out
}

pub(super) fn build_tag_inputs(tags: &[String]) -> Vec<netbox::models::NestedTag> {
    tags.iter()
        .map(|tag| netbox::models::NestedTag::new(tag.clone(), slugify(tag)))
        .collect()
}

pub(super) fn custom_field_type_for_schema(field: &FieldSchema) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slugify() {
        assert_eq!(slugify("Hello World"), "hello-world");
        assert_eq!(slugify("EVPN Fabric!"), "evpn-fabric");
    }

    #[test]
    fn test_build_tag_inputs() {
        let tags = vec!["Alembic Test".to_string()];
        let inputs = build_tag_inputs(&tags);
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].name, "Alembic Test");
        assert_eq!(inputs[0].slug, "alembic-test");
    }

    #[test]
    fn test_custom_field_type_for_schema() {
        use alembic_core::FieldType;
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
}
