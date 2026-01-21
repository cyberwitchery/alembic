use alembic_core::TypeName;
use anyhow::{anyhow, Result};
use netbox::models::ObjectType;
use std::collections::{BTreeMap, BTreeSet};
#[derive(Debug, Clone)]
pub(super) struct ObjectTypeInfo {
    pub(super) type_name: TypeName,
    pub(super) endpoint: String,
    pub(super) features: BTreeSet<String>,
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
                .and_then(normalize_endpoint)
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
        let endpoint = endpoint_from_type_name(type_name.as_str())?;
        Some(ObjectTypeInfo {
            type_name: type_name.clone(),
            endpoint,
            features: BTreeSet::new(),
        })
    }

    pub(super) fn type_names(&self) -> Vec<TypeName> {
        self.by_type
            .values()
            .map(|info| info.type_name.clone())
            .collect()
    }

    pub(super) fn type_name_for_endpoint(&self, endpoint: &str) -> Option<&str> {
        let normalized = normalize_endpoint(endpoint)?;
        self.by_endpoint.get(&normalized).map(|name| name.as_str())
    }
}

fn normalize_endpoint(endpoint: &str) -> Option<String> {
    let trimmed = endpoint.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut path = trimmed;
    if let Some(idx) = trimmed.find("/api/") {
        path = &trimmed[idx + 5..];
    }
    let path = path.trim_start_matches('/');
    let path = path.strip_prefix("api/").unwrap_or(path);
    let trimmed = path.trim_end_matches('/');
    let mut segments: Vec<&str> = trimmed.split('/').collect();
    if let Some(last) = segments.last().copied() {
        if !last.is_empty() && last.chars().all(|ch| ch.is_ascii_digit()) {
            segments.pop();
        }
    }
    if segments.is_empty() {
        return None;
    }
    let mut normalized = segments.join("/");
    normalized.push('/');
    Some(normalized)
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

fn singularize(value: &str) -> String {
    if let Some(stripped) = value.strip_suffix("resses") {
        return format!("{stripped}ress");
    }
    if let Some(stripped) = value.strip_suffix("ies") {
        return format!("{stripped}y");
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

fn pluralize(value: &str) -> String {
    if value.ends_with("address") {
        return format!("{value}es");
    }
    if let Some(stripped) = value.strip_suffix('y') {
        return format!("{stripped}ies");
    }
    if value.ends_with('s')
        || value.ends_with('x')
        || value.ends_with('z')
        || value.ends_with("ch")
        || value.ends_with("sh")
    {
        return format!("{value}es");
    }
    format!("{value}s")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_endpoint_handles_urls_and_paths() {
        let url = "https://netbox.example.com/api/dcim/sites/";
        assert_eq!(normalize_endpoint(url), Some("dcim/sites/".to_string()));
        assert_eq!(
            normalize_endpoint("/api/ipam/prefixes/"),
            Some("ipam/prefixes/".to_string())
        );
        assert_eq!(
            normalize_endpoint("dcim/devices"),
            Some("dcim/devices/".to_string())
        );
    }

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
    }
}
