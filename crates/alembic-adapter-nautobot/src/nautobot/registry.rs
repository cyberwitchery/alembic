use alembic_core::TypeName;
use alembic_engine::normalize_endpoint;
use anyhow::{anyhow, Result};
use nautobot::ObjectTypes;
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
    /// every object type nautobot serves over rest, named `app_label.model` at
    /// the route nautobot.rs paired it with. a type no route serves is left out.
    pub(super) fn from_object_types(types: &ObjectTypes) -> Result<Self> {
        let mut registry = ObjectTypeRegistry::default();
        for object_type in types.iter() {
            let Some(route) = object_type.route() else {
                continue;
            };
            let features: BTreeSet<String> = ["custom-fields", "tags", "local-context"]
                .iter()
                .map(|s| s.to_string())
                .collect();
            let info = ObjectTypeInfo {
                type_name: TypeName::new(object_type.name()),
                endpoint: route.to_string(),
                features,
            };
            registry
                .by_endpoint
                .insert(route.to_string(), object_type.name().to_string());
            registry
                .by_type
                .insert(object_type.name().to_string(), info);
        }

        if registry.by_type.is_empty() {
            return Err(anyhow!("nautobot returned no object types with a route"));
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
        let normalized = normalize_endpoint(endpoint, |s| {
            s.chars().all(|c| c.is_ascii_digit()) || uuid::Uuid::parse_str(s).is_ok()
        })?;
        self.by_endpoint.get(&normalized).map(|name| name.as_str())
    }
}
