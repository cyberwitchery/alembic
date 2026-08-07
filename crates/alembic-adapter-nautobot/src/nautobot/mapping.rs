pub(super) use alembic_engine::mapping::{
    custom_field_type_for_schema, slugify, supports_feature, tags_from_value,
    validation_regex_for_schema,
};
use serde_json::Value;

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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
}
