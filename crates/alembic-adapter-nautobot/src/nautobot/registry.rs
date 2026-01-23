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
            let endpoint = format!("{}/{}/", app_label, pluralize(&model).replace('_', "-"));

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
