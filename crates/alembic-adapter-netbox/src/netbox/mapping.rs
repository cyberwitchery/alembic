pub(super) use alembic_engine::mapping::{
    custom_field_type_for_schema, custom_field_update_payload, merge_converged_properties, slugify,
    supports_feature, tags_from_value, validation_regex_for_schema, ExistingCustomField,
};

pub(super) fn build_tag_inputs(tags: &[String]) -> Vec<netbox::models::NestedTag> {
    tags.iter()
        .map(|tag| netbox::models::NestedTag::new(tag.clone(), slugify(tag)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_tag_inputs() {
        let tags = vec!["Alembic Test".to_string()];
        let inputs = build_tag_inputs(&tags);
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].name, "Alembic Test");
        assert_eq!(inputs[0].slug, "alembic-test");
    }
}
