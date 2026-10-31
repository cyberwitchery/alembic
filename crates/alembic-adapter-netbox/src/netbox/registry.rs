use alembic_core::TypeName;
use alembic_engine::{normalize_endpoint, pluralize};
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
            let endpoint_type = type_name(&object_type.app_label, &object_type.model, &endpoint);
            let model_type = format!("{}.{}", object_type.app_label, object_type.model);
            // netbox names features with underscores (`custom_fields`); the
            // adapters check the hyphenated spelling nautobot's registry uses.
            let features: BTreeSet<String> = object_type
                .features
                .unwrap_or_default()
                .into_iter()
                .map(|feature| feature.replace('_', "-"))
                .collect();
            let info = ObjectTypeInfo {
                type_name: TypeName::new(endpoint_type.clone()),
                endpoint: endpoint.clone(),
                features,
                app_label: object_type.app_label.clone(),
                model: object_type.model.clone(),
            };
            registry.by_endpoint.insert(endpoint, endpoint_type.clone());
            registry.by_type.insert(endpoint_type, info.clone());
            registry.by_type.insert(model_type, info);
        }

        if registry.by_type.is_empty() {
            return Err(anyhow!(
                "netbox returned no object types with rest_api_endpoint"
            ));
        }

        Ok(registry)
    }

    pub(super) fn info_for(&self, type_name: &TypeName) -> Option<ObjectTypeInfo> {
        if let Some(info) = self.by_type.get(type_name.as_str()) {
            return Some(info.clone());
        }
        let (app_label, model) = split_type_name(type_name.as_str())?;
        let endpoint = endpoint_from_type_name(type_name.as_str())?;
        Some(ObjectTypeInfo {
            type_name: type_name.clone(),
            endpoint,
            features: BTreeSet::new(),
            app_label,
            model,
        })
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

/// the user-facing name of a netbox object type: its model, with `_` where the
/// route spells a word boundary in the stretch the two share
/// (`ipaddress` + `ip-addresses/` is `ip_address`). both come from netbox, so no
/// inflection decides an identity.
fn type_name(app_label: &str, model: &str, endpoint: &str) -> String {
    let route = endpoint
        .trim_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or_default();
    let mut route = route.chars().peekable();
    let mut name = String::with_capacity(model.len() + 2);
    for (at, c) in model.char_indices() {
        while route.next_if_eq(&'-').is_some() {
            name.push('_');
        }
        if route.next_if_eq(&c).is_none() {
            name.push_str(&model[at..]);
            break;
        }
        name.push(c);
    }
    format!("{app_label}.{name}")
}

fn split_type_name(value: &str) -> Option<(String, String)> {
    let (app, model) = value.split_once('.')?;
    Some((app.to_string(), model.to_string()))
}

fn endpoint_from_type_name(type_name: &str) -> Option<String> {
    let (app, model) = type_name.split_once('.')?;
    let resource = pluralize(model).replace('_', "-");
    Some(format!("{app}/{resource}/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_type_name_is_the_model_split_where_the_route_splits() {
        for (app_label, model, endpoint, expected) in [
            ("dcim", "device", "dcim/devices/", "dcim.device"),
            ("dcim", "devicebay", "dcim/device-bays/", "dcim.device_bay"),
            ("ipam", "ipaddress", "ipam/ip-addresses/", "ipam.ip_address"),
            ("ipam", "prefix", "ipam/prefixes/", "ipam.prefix"),
            (
                "extras",
                "journalentry",
                "extras/journal-entries/",
                "extras.journal_entry",
            ),
            (
                "dcim",
                "virtualchassis",
                "dcim/virtual-chassis/",
                "dcim.virtual_chassis",
            ),
        ] {
            assert_eq!(type_name(app_label, model, endpoint), expected);
        }
    }

    /// a route named for something other than the model contributes nothing.
    #[test]
    fn a_route_named_apart_from_its_model_leaves_the_model() {
        assert_eq!(
            type_name(
                "virtualization",
                "vminterface",
                "virtualization/interfaces/"
            ),
            "virtualization.vminterface"
        );
        assert_eq!(
            type_name("users", "objectpermission", "users/permissions/"),
            "users.objectpermission"
        );
        assert_eq!(
            type_name("extras", "taggeditem", "extras/tagged-objects/"),
            "extras.tagged_item"
        );
    }

    /// a plugin's routes sit under `plugins/<plugin>/`, so the app is the
    /// app label, never the route's first segment.
    #[test]
    fn a_plugin_type_is_named_by_its_app_label() {
        let registry = ObjectTypeRegistry::from_object_types(vec![
            ObjectType {
                app_label: "netbox_custom_objects".to_string(),
                model: "customobjecttype".to_string(),
                rest_api_endpoint: Some(
                    "/api/plugins/custom-objects/custom-object-types/".to_string(),
                ),
                ..Default::default()
            },
            ObjectType {
                app_label: "netbox_custom_objects".to_string(),
                model: "customobjecttypefield".to_string(),
                rest_api_endpoint: Some(
                    "/api/plugins/custom-objects/custom-object-type-fields/".to_string(),
                ),
                ..Default::default()
            },
        ])
        .unwrap();

        assert_eq!(
            registry.type_name_for_endpoint("/api/plugins/custom-objects/custom-object-types/1/"),
            Some("netbox_custom_objects.custom_object_type")
        );
        assert_eq!(
            registry
                .type_name_for_endpoint("/api/plugins/custom-objects/custom-object-type-fields/"),
            Some("netbox_custom_objects.custom_object_type_field")
        );
    }

    #[test]
    fn endpoint_from_type_name_handles_pluralization() {
        assert_eq!(
            endpoint_from_type_name("ipam.ip_address"),
            Some("ipam/ip-addresses/".to_string())
        );
        assert_eq!(
            endpoint_from_type_name("circuits.circuit_termination"),
            Some("circuits/circuit-terminations/".to_string())
        );
        assert_eq!(
            endpoint_from_type_name("dcim.device"),
            Some("dcim/devices/".to_string())
        );
        assert_eq!(
            endpoint_from_type_name("ipam.prefix"),
            Some("ipam/prefixes/".to_string())
        );
        assert_eq!(
            endpoint_from_type_name("dcim.device_bay"),
            Some("dcim/device-bays/".to_string())
        );
    }
}
