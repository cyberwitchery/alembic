use alembic_core::TypeName;
use anyhow::{anyhow, Result};
use nautobot::models::ContentType;
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
    pub(super) fn from_content_types(types: Vec<ContentType>) -> Result<Self> {
        let mut registry = ObjectTypeRegistry::default();
        for content_type in types {
            let app_label = content_type.app_label;
            let model = content_type.model;

            let endpoint_type = format!("{}.{}", app_label, model);
            let endpoint_name = if let Some(display) = content_type.display {
                if let Some((_, name)) = display.split_once('|') {
                    name.trim().replace(' ', "-")
                } else {
                    model.clone()
                }
            } else {
                model.clone()
            };
            let endpoint = format!("{}/{}/", app_label, pluralize(&endpoint_name));

            let features: BTreeSet<String> = ["custom-fields", "tags", "local-context"]
                .iter()
                .map(|s| s.to_string())
                .collect();

            let info = ObjectTypeInfo {
                type_name: TypeName::new(endpoint_type.clone()),
                endpoint: endpoint.clone(),
                features,
            };

            registry.by_endpoint.insert(endpoint, endpoint_type.clone());
            registry.by_type.insert(endpoint_type, info);
        }

        if registry.by_type.is_empty() {
            return Err(anyhow!("nautobot returned no content types"));
        }

        Ok(registry)
    }

    pub(super) fn info_for(&self, type_name: &TypeName) -> Option<ObjectTypeInfo> {
        self.by_type.get(type_name.as_str()).cloned()
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
        if !last.is_empty()
            && (last.chars().all(|ch| ch.is_ascii_digit()) || uuid::Uuid::parse_str(last).is_ok())
        {
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
    use nautobot::models::ContentType;

    #[test]
    fn registry_from_content_types() {
        let types = vec![
            ContentType {
                app_label: "dcim".to_string(),
                model: "device".to_string(),
                display: Some("dcim | device".to_string()),
                ..Default::default()
            },
            ContentType {
                app_label: "dcim".to_string(),
                model: "locationtype".to_string(),
                display: Some("dcim | location type".to_string()),
                ..Default::default()
            },
        ];

        let registry = ObjectTypeRegistry::from_content_types(types).unwrap();
        let device = registry.info_for(&TypeName::new("dcim.device")).unwrap();
        assert_eq!(device.endpoint, "dcim/devices/");

        let loc_type = registry
            .info_for(&TypeName::new("dcim.locationtype"))
            .unwrap();
        assert_eq!(loc_type.endpoint, "dcim/location-types/");
    }

    #[test]
    fn endpoint_normalization() {
        assert_eq!(
            normalize_endpoint("http://localhost/api/dcim/devices/"),
            Some("dcim/devices/".to_string())
        );
        assert_eq!(
            normalize_endpoint("/api/dcim/devices/6d74797d-de61-46b6-95e4-27c5eadb8fc6/"),
            Some("dcim/devices/".to_string())
        );
        assert_eq!(
            normalize_endpoint("dcim/devices"),
            Some("dcim/devices/".to_string())
        );
    }

    #[test]
    fn pluralization_rules() {
        assert_eq!(pluralize("device"), "devices");
        assert_eq!(pluralize("location-type"), "location-types");
        assert_eq!(pluralize("address"), "addresses");
        assert_eq!(pluralize("facility"), "facilities");
    }
}
