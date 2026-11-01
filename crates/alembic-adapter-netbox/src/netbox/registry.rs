use alembic_core::TypeName;
use alembic_engine::normalize_endpoint;
use anyhow::{anyhow, Result};
use netbox::models::ObjectType;
use std::collections::{BTreeMap, BTreeSet};
#[derive(Debug, Clone)]
pub(super) struct ObjectTypeInfo {
    pub(super) type_name: TypeName,
    pub(super) endpoint: String,
    pub(super) features: BTreeSet<String>,
    pub(super) app_label: String,
    pub(super) model: String,
}

#[derive(Debug, Clone, Default)]
pub(super) struct ObjectTypeRegistry {
    by_type: BTreeMap<String, ObjectTypeInfo>,
    by_endpoint: BTreeMap<String, String>,
}

impl ObjectTypeRegistry {
    pub(super) fn from_object_types(types: Vec<ObjectType>) -> Result<Self> {
        let mut registry = ObjectTypeRegistry::default();
        for object_type in types {
            let Some(endpoint) = object_type
                .rest_api_endpoint
                .as_deref()
                .and_then(|e| normalize_endpoint(e, |s| s.chars().all(|c| c.is_ascii_digit())))
            else {
                continue;
            };
            let type_name = format!("{}.{}", object_type.app_label, object_type.model);
            // netbox names features with underscores (`custom_fields`); the
            // adapters check the hyphenated spelling nautobot's registry uses.
            let features: BTreeSet<String> = object_type
                .features
                .unwrap_or_default()
                .into_iter()
                .map(|feature| feature.replace('_', "-"))
                .collect();
            let info = ObjectTypeInfo {
                type_name: TypeName::new(type_name.clone()),
                endpoint: endpoint.clone(),
                features,
                app_label: object_type.app_label.clone(),
                model: object_type.model.clone(),
            };
            registry.by_endpoint.insert(endpoint, type_name.clone());
            registry.by_type.insert(type_name, info);
        }

        if registry.by_type.is_empty() {
            return Err(anyhow!(
                "netbox returned no object types with rest_api_endpoint"
            ));
        }

        Ok(registry)
    }

    pub(super) fn info_for(&self, type_name: &TypeName) -> Option<ObjectTypeInfo> {
        self.by_type.get(type_name.as_str()).cloned()
    }

    pub(super) fn contains_type(&self, type_name: &TypeName) -> bool {
        self.by_type.contains_key(type_name.as_str())
    }

    pub(super) fn insert_custom_object_type(
        &mut self,
        type_name: TypeName,
        endpoint: String,
        features: BTreeSet<String>,
        app_label: String,
        model: String,
    ) {
        let info = ObjectTypeInfo {
            type_name: type_name.clone(),
            endpoint: endpoint.clone(),
            features,
            app_label,
            model,
        };
        self.by_endpoint
            .insert(endpoint, type_name.as_str().to_string());
        self.by_type.insert(type_name.as_str().to_string(), info);
    }

    pub(super) fn type_names(&self) -> Vec<TypeName> {
        self.by_type
            .values()
            .map(|info| info.type_name.clone())
            .collect()
    }

    pub(super) fn type_name_for_endpoint(&self, endpoint: &str) -> Option<&str> {
        let normalized = normalize_endpoint(endpoint, |s| s.chars().all(|c| c.is_ascii_digit()))?;
        self.by_endpoint.get(&normalized).map(|name| name.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object_type(app_label: &str, model: &str, endpoint: &str) -> ObjectType {
        ObjectType {
            app_label: app_label.to_string(),
            model: model.to_string(),
            rest_api_endpoint: Some(endpoint.to_string()),
            ..Default::default()
        }
    }

    /// the name and the endpoint are what netbox reports; nothing is derived.
    #[test]
    fn a_type_is_its_app_label_and_model_at_its_reported_endpoint() {
        let registry = ObjectTypeRegistry::from_object_types(vec![
            object_type("ipam", "ipaddress", "/api/ipam/ip-addresses/"),
            object_type(
                "virtualization",
                "vminterface",
                "/api/virtualization/interfaces/",
            ),
            object_type(
                "netbox_custom_objects",
                "customobjecttype",
                "/api/plugins/custom-objects/custom-object-types/",
            ),
        ])
        .unwrap();

        for (name, endpoint) in [
            ("ipam.ipaddress", "ipam/ip-addresses/"),
            ("virtualization.vminterface", "virtualization/interfaces/"),
            (
                "netbox_custom_objects.customobjecttype",
                "plugins/custom-objects/custom-object-types/",
            ),
        ] {
            let info = registry.info_for(&TypeName::new(name)).unwrap();
            assert_eq!(info.endpoint, endpoint);
            assert_eq!(
                registry.type_name_for_endpoint(&format!(
                    "https://netbox.example.com/api/{endpoint}7/"
                )),
                Some(name)
            );
        }
    }

    /// a name netbox did not report has no endpoint to guess.
    #[test]
    fn an_unreported_type_has_no_info() {
        let registry = ObjectTypeRegistry::from_object_types(vec![object_type(
            "ipam",
            "ipaddress",
            "/api/ipam/ip-addresses/",
        )])
        .unwrap();

        assert!(registry
            .info_for(&TypeName::new("ipam.ip_address"))
            .is_none());
    }
}
