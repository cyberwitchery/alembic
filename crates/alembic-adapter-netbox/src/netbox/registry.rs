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
            let endpoint_type = type_name_from_endpoint(&endpoint)
                .unwrap_or_else(|| format!("{}.{}", object_type.app_label, object_type.model));
            let model_type = format!("{}.{}", object_type.app_label, object_type.model);
            let features: BTreeSet<String> = object_type
                .features
                .unwrap_or_default()
                .into_iter()
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

fn type_name_from_endpoint(endpoint: &str) -> Option<String> {
    let trimmed = endpoint.trim().trim_matches('/');
    let mut parts = trimmed.split('/');
    let app = parts.next()?;
    let resource = parts.next()?;
    let singular = singularize(resource);
    let normalized = singular.replace('-', "_");
    Some(format!("{app}.{normalized}"))
}

fn split_type_name(value: &str) -> Option<(String, String)> {
    let (app, model) = value.split_once('.')?;
    Some((app.to_string(), model.to_string()))
}

fn singularize(value: &str) -> String {
    if let Some(stripped) = value.strip_suffix("resses") {
        return format!("{stripped}ress");
    }
    if let Some(stripped) = value.strip_suffix("ies") {
        return format!("{stripped}y");
    }
    // inverse of pluralize's x/z/ch/sh -> +es rule (e.g. prefix -> prefixes)
    if ["xes", "zes", "ches", "shes"]
        .iter()
        .any(|s| value.ends_with(s))
    {
        return value[..value.len() - 2].to_string();
    }
    if let Some(stripped) = value.strip_suffix("ses") {
        return stripped.to_string();
    }
    if let Some(stripped) = value.strip_suffix('s') {
        return stripped.to_string();
    }
    value.to_string()
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
    fn type_name_from_endpoint_handles_pluralization() {
        assert_eq!(
            type_name_from_endpoint("ipam/ip-addresses/"),
            Some("ipam.ip_address".to_string())
        );
        assert_eq!(
            type_name_from_endpoint("circuits/circuit-terminations/"),
            Some("circuits.circuit_termination".to_string())
        );
        assert_eq!(
            type_name_from_endpoint("dcim/devices/"),
            Some("dcim.device".to_string())
        );
        assert_eq!(
            type_name_from_endpoint("ipam/prefixes/"),
            Some("ipam.prefix".to_string())
        );
        assert_eq!(
            type_name_from_endpoint("dcim/device-bays/"),
            Some("dcim.device_bay".to_string())
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

    #[test]
    fn singularize_inverts_pluralize() {
        for stem in [
            "device",
            "prefix",
            "device_bay",
            "ip_address",
            "circuit_termination",
            "interface",
        ] {
            assert_eq!(singularize(&pluralize(stem)), stem);
        }
    }
}
