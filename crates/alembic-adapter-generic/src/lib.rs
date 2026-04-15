//! generic rest adapter for alembic.

use alembic_core::{JsonMap, Key, Schema, TypeName, Uid};
use alembic_engine::{
    apply_non_delete_with_retries, build_key_from_schema, resolved_from_state, state_mappings,
    Adapter, AdapterApplyError, AppliedOp, ApplyReport, BackendId, ObservedObject, ObservedState,
    Op, RetryApplyDriver, StateMappings,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// configuration for the generic rest adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenericConfig {
    /// base url for the api.
    pub base_url: String,
    /// authentication headers.
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// type-to-endpoint mappings.
    pub types: BTreeMap<String, EndpointConfig>,
}

/// endpoint configuration for a specific type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointConfig {
    /// path for listing and creating objects.
    pub path: String,
    /// json path to the results array in the list response (default: root).
    pub results_path: Option<String>,
    /// json path to the object id (default: "id").
    #[serde(default = "default_id_path")]
    pub id_path: String,
    /// strategy for deletions.
    #[serde(default)]
    pub delete_strategy: DeleteStrategy,
    /// method for updates (default: PATCH).
    #[serde(default = "default_update_method")]
    pub update_method: String,
}

fn default_id_path() -> String {
    "id".to_string()
}

fn default_update_method() -> String {
    "PATCH".to_string()
}

/// strategy for deleting objects.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeleteStrategy {
    /// deletes are not supported for this type.
    #[default]
    None,
    /// delete via DELETE method to path + id.
    Standard,
}

pub struct GenericAdapter {
    config: GenericConfig,
    client: reqwest::Client,
}

impl GenericAdapter {
    pub fn new(config: GenericConfig) -> Result<Self> {
        let mut headers = reqwest::header::HeaderMap::new();
        for (k, v) in &config.headers {
            let name = reqwest::header::HeaderName::from_bytes(k.as_bytes())?;
            let value = reqwest::header::HeaderValue::from_str(v)?;
            headers.insert(name, value);
        }

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;

        for (type_name, endpoint) in &config.types {
            match endpoint.update_method.as_str() {
                "PATCH" | "PUT" => {}
                other => {
                    return Err(anyhow!(
                        "invalid update_method {:?} for type {} (expected PATCH or PUT)",
                        other,
                        type_name
                    ));
                }
            }
        }

        Ok(Self { config, client })
    }

    async fn apply_create(
        &self,
        uid: Uid,
        type_name: &TypeName,
        desired: &alembic_core::Object,
        schema: &Schema,
        resolved: &mut BTreeMap<Uid, BackendId>,
    ) -> Result<AppliedOp> {
        let endpoint = self
            .config
            .types
            .get(type_name.as_str())
            .ok_or_else(|| anyhow!("no config for {}", type_name))?;
        let type_schema = schema
            .types
            .get(type_name.as_str())
            .ok_or_else(|| anyhow!("missing schema for {}", type_name))?;

        let url = format!(
            "{}/{}",
            self.config.base_url.trim_end_matches('/'),
            endpoint.path.trim_start_matches('/')
        );
        let body = resolve_attrs(&desired.attrs, type_schema, resolved)?;

        let resp = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await?
            .error_for_status()?;
        let body: serde_json::Value = resp.json().await?;

        let id_val = resolve_path(&body, &endpoint.id_path)?;
        let backend_id = match id_val {
            serde_json::Value::Number(n) => {
                BackendId::Int(n.as_u64().ok_or_else(|| anyhow!("invalid integer id"))?)
            }
            serde_json::Value::String(s) => BackendId::String(s),
            _ => return Err(anyhow!("id must be number or string")),
        };
        resolved.insert(uid, backend_id.clone());

        Ok(AppliedOp {
            uid,
            type_name: type_name.clone(),
            backend_id: Some(backend_id),
        })
    }

    async fn apply_update(
        &self,
        uid: Uid,
        type_name: &TypeName,
        desired: &alembic_core::Object,
        backend_id: Option<&BackendId>,
        schema: &Schema,
        resolved: &BTreeMap<Uid, BackendId>,
    ) -> Result<AppliedOp> {
        let endpoint = self
            .config
            .types
            .get(type_name.as_str())
            .ok_or_else(|| anyhow!("no config for {}", type_name))?;
        let type_schema = schema
            .types
            .get(type_name.as_str())
            .ok_or_else(|| anyhow!("missing schema for {}", type_name))?;

        let id = backend_id.ok_or_else(|| anyhow!("update requires backend id"))?;
        let url = self.backend_id_to_url(endpoint, id);
        let body = resolve_attrs(&desired.attrs, type_schema, resolved)?;

        let req = match endpoint.update_method.as_str() {
            "PUT" => self.client.put(&url),
            _ => self.client.patch(&url),
        };

        req.json(&body).send().await?.error_for_status()?;

        Ok(AppliedOp {
            uid,
            type_name: type_name.clone(),
            backend_id: Some(id.clone()),
        })
    }

    async fn apply_delete(&self, type_name: &TypeName, id: &BackendId) -> Result<()> {
        let endpoint = self
            .config
            .types
            .get(type_name.as_str())
            .ok_or_else(|| anyhow!("no config for {}", type_name))?;

        match endpoint.delete_strategy {
            DeleteStrategy::Standard => {
                let url = self.backend_id_to_url(endpoint, id);
                self.client.delete(&url).send().await?.error_for_status()?;
            }
            DeleteStrategy::None => {
                return Err(anyhow!(
                    "delete not supported for type {} (delete_strategy: none)",
                    type_name
                ));
            }
        }
        Ok(())
    }

    fn backend_id_to_url(&self, endpoint: &EndpointConfig, id: &BackendId) -> String {
        let id_str = match id {
            BackendId::Int(n) => n.to_string(),
            BackendId::String(s) => s.clone(),
        };
        format!(
            "{}/{}/{}",
            self.config.base_url.trim_end_matches('/'),
            endpoint.path.trim_matches('/'),
            id_str
        )
    }
}

#[async_trait]
impl Adapter for GenericAdapter {
    async fn read(
        &self,
        schema: &Schema,
        types: &[TypeName],
        state_store: &alembic_engine::StateStore,
    ) -> Result<ObservedState> {
        let mut state = ObservedState::default();
        let mappings = state_mappings(state_store);
        let requested: BTreeSet<TypeName> = if types.is_empty() {
            self.config
                .types
                .keys()
                .map(|s| TypeName::new(s.clone()))
                .collect()
        } else {
            types.iter().cloned().collect()
        };

        let mut tasks = Vec::new();
        for type_name in requested {
            let endpoint = self
                .config
                .types
                .get(type_name.as_str())
                .ok_or_else(|| anyhow!("no generic config for type {}", type_name))?
                .clone();
            let type_schema = schema
                .types
                .get(type_name.as_str())
                .ok_or_else(|| anyhow!("missing schema for {}", type_name))?
                .clone();

            let client = self.client.clone();
            let base_url = self.config.base_url.clone();
            let mappings = mappings.clone();

            tasks.push(tokio::spawn(async move {
                let url = format!(
                    "{}/{}",
                    base_url.trim_end_matches('/'),
                    endpoint.path.trim_start_matches('/')
                );
                let resp = client.get(&url).send().await?.error_for_status()?;
                let body: serde_json::Value = resp.json().await?;

                let results = if let Some(path) = &endpoint.results_path {
                    let val = resolve_path(&body, path)?;
                    val.as_array()
                        .ok_or_else(|| {
                            anyhow!("expected array at path {} for {}", path, type_name)
                        })?
                        .clone()
                } else if let Some(arr) = body.as_array() {
                    arr.clone()
                } else {
                    return Err(anyhow!("expected array in list response for {}", type_name));
                };

                let mut observed = Vec::new();
                for item in results {
                    let id_val = resolve_path(&item, &endpoint.id_path)?;
                    let backend_id = match id_val {
                        serde_json::Value::Number(n) => {
                            BackendId::Int(n.as_u64().ok_or_else(|| anyhow!("invalid integer id"))?)
                        }
                        serde_json::Value::String(s) => BackendId::String(s),
                        _ => return Err(anyhow!("id must be number or string")),
                    };

                    let attrs = match item {
                        serde_json::Value::Object(map) => {
                            map.into_iter().collect::<BTreeMap<_, _>>().into()
                        }
                        _ => return Err(anyhow!("expected object in results")),
                    };

                    let attrs = normalize_attrs_refs(&attrs, &type_schema, &mappings);
                    let key = build_key_from_schema(&type_schema, &attrs)?;

                    observed.push(ObservedObject {
                        type_name: type_name.clone(),
                        key,
                        attrs,
                        backend_id: Some(backend_id),
                    });
                }
                Ok::<Vec<ObservedObject>, anyhow::Error>(observed)
            }));
        }

        let results = futures::future::join_all(tasks).await;
        for result in results {
            let objects = result??;
            for object in objects {
                state.insert(object);
            }
        }

        Ok(state)
    }

    async fn write(
        &self,
        schema: &Schema,
        ops: &[Op],
        state: &alembic_engine::StateStore,
    ) -> Result<ApplyReport> {
        let mut applied = Vec::new();
        let mut resolved = resolved_from_state(state);

        let mut creates_updates = Vec::new();
        let mut deletes = Vec::new();
        for op in ops {
            match op {
                Op::Delete { .. } => deletes.push(op.clone()),
                _ => creates_updates.push(op.clone()),
            }
        }

        struct ApplyDriver<'a> {
            adapter: &'a GenericAdapter,
            resolved: &'a mut BTreeMap<Uid, BackendId>,
            schema: &'a Schema,
        }

        #[async_trait]
        impl RetryApplyDriver for ApplyDriver<'_> {
            async fn apply_non_delete(&mut self, op: &Op) -> Result<AppliedOp> {
                match op {
                    Op::Create {
                        uid,
                        type_name,
                        desired,
                    } => {
                        self.adapter
                            .apply_create(*uid, type_name, desired, self.schema, self.resolved)
                            .await
                    }
                    Op::Update {
                        uid,
                        type_name,
                        desired,
                        backend_id,
                        ..
                    } => {
                        self.adapter
                            .apply_update(
                                *uid,
                                type_name,
                                desired,
                                backend_id.as_ref(),
                                self.schema,
                                self.resolved,
                            )
                            .await
                    }
                    Op::Delete { .. } => unreachable!("delete ops filtered before retry"),
                }
            }

            fn is_retryable(&self, err: &anyhow::Error) -> bool {
                is_missing_ref_error(err)
            }
        }

        let mut driver = ApplyDriver {
            adapter: self,
            resolved: &mut resolved,
            schema,
        };
        let retry_result = apply_non_delete_with_retries(&creates_updates, &mut driver).await?;
        if !retry_result.pending.is_empty() {
            let missing = describe_missing_refs(&retry_result.pending, &resolved);
            return Err(anyhow!("unresolved references: {missing}"));
        }

        for applied_op in retry_result.applied {
            if let Some(backend_id) = &applied_op.backend_id {
                resolved.insert(applied_op.uid, backend_id.clone());
            }
            applied.push(applied_op);
        }

        for op in deletes {
            if let Op::Delete {
                uid,
                type_name,
                backend_id,
                ..
            } = op
            {
                let id = backend_id.ok_or_else(|| anyhow!("delete requires backend id"))?;
                self.apply_delete(&type_name, &id).await?;
                applied.push(AppliedOp {
                    uid,
                    type_name,
                    backend_id: None,
                });
            }
        }

        Ok(ApplyReport {
            applied,
            ..Default::default()
        })
    }
}

fn resolve_path(value: &serde_json::Value, path: &str) -> Result<serde_json::Value> {
    let mut current = value;
    for segment in path.split('.') {
        if segment.is_empty() {
            continue;
        }
        current = current
            .get(segment)
            .ok_or_else(|| anyhow!("path segment not found: {}", segment))?;
    }
    Ok(current.clone())
}

fn normalize_attrs_refs(
    attrs: &JsonMap,
    type_schema: &alembic_core::TypeSchema,
    mappings: &StateMappings,
) -> JsonMap {
    let mut normalized = attrs.clone();
    for (field, schema) in &type_schema.fields {
        match &schema.r#type {
            alembic_core::FieldType::Ref { target } => {
                if let Some(value) = attrs.get(field) {
                    normalized.insert(
                        field.clone(),
                        normalize_ref_value(value.clone(), target, mappings),
                    );
                }
            }
            alembic_core::FieldType::ListRef { target } => {
                if let Some(value) = attrs.get(field) {
                    let updated = if let serde_json::Value::Array(items) = value {
                        let mapped = items
                            .iter()
                            .cloned()
                            .map(|item| normalize_ref_value(item, target, mappings))
                            .collect::<Vec<_>>();
                        serde_json::Value::Array(mapped)
                    } else {
                        value.clone()
                    };
                    normalized.insert(field.clone(), updated);
                }
            }
            _ => {}
        }
    }
    normalized
}

fn normalize_ref_value(
    value: serde_json::Value,
    target: &str,
    mappings: &StateMappings,
) -> serde_json::Value {
    if value.is_null() {
        return value;
    }
    let backend_id = match backend_id_from_value(&value) {
        Some(id) => id,
        None => return value,
    };
    mappings
        .uid_for(target, &backend_id)
        .map(|uid| serde_json::Value::String(uid.to_string()))
        .unwrap_or(value)
}

fn backend_id_from_value(value: &serde_json::Value) -> Option<BackendId> {
    match value {
        serde_json::Value::Number(n) => n.as_u64().map(BackendId::Int).or_else(|| {
            n.as_i64()
                .and_then(|v| u64::try_from(v).ok())
                .map(BackendId::Int)
        }),
        serde_json::Value::String(s) => Some(BackendId::String(s.clone())),
        serde_json::Value::Object(map) => map.get("id").and_then(backend_id_from_value),
        _ => None,
    }
}

fn resolve_attrs(
    attrs: &JsonMap,
    type_schema: &alembic_core::TypeSchema,
    resolved: &BTreeMap<Uid, BackendId>,
) -> Result<serde_json::Value> {
    let mut map = serde_json::Map::new();
    for (key, value) in attrs.iter() {
        let field_schema = type_schema
            .fields
            .get(key)
            .ok_or_else(|| anyhow!("missing schema for field {key}"))?;
        map.insert(
            key.clone(),
            resolve_value_for_type(&field_schema.r#type, value.clone(), resolved)?,
        );
    }
    Ok(serde_json::Value::Object(map))
}

fn resolve_value_for_type(
    field_type: &alembic_core::FieldType,
    value: serde_json::Value,
    resolved: &BTreeMap<Uid, BackendId>,
) -> Result<serde_json::Value> {
    match field_type {
        alembic_core::FieldType::Ref { .. } => resolve_ref_value(value, resolved),
        alembic_core::FieldType::ListRef { .. } => {
            let serde_json::Value::Array(items) = value else {
                return Err(anyhow!("expected array for list_ref"));
            };
            let mut out = Vec::new();
            for item in items {
                out.push(resolve_ref_value(item, resolved)?);
            }
            Ok(serde_json::Value::Array(out))
        }
        _ => Ok(value),
    }
}

fn resolve_ref_value(
    value: serde_json::Value,
    resolved: &BTreeMap<Uid, BackendId>,
) -> Result<serde_json::Value> {
    let serde_json::Value::String(raw) = value else {
        return Err(anyhow!("ref must be uuid string"));
    };
    let uid = Uid::parse_str(&raw).map_err(|_| anyhow!("invalid uuid: {}", raw))?;
    let id = resolved
        .get(&uid)
        .ok_or(AdapterApplyError::MissingRef { uid })?;
    Ok(match id {
        BackendId::Int(n) => serde_json::Value::Number((*n).into()),
        BackendId::String(s) => serde_json::Value::String(s.clone()),
    })
}

fn is_missing_ref_error(err: &anyhow::Error) -> bool {
    err.downcast_ref::<AdapterApplyError>()
        .is_some_and(|e| matches!(e, AdapterApplyError::MissingRef { .. }))
}

fn describe_missing_refs(ops: &[Op], resolved: &BTreeMap<Uid, BackendId>) -> String {
    let mut missing = BTreeSet::new();
    for op in ops {
        if let Op::Create { desired, .. } | Op::Update { desired, .. } = op {
            for value in desired.attrs.values() {
                collect_missing_refs(value, resolved, &mut missing);
            }
        }
    }
    missing
        .into_iter()
        .map(|uid| uid.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn collect_missing_refs(
    value: &serde_json::Value,
    resolved: &BTreeMap<Uid, BackendId>,
    missing: &mut BTreeSet<Uid>,
) {
    match value {
        serde_json::Value::String(raw) => {
            if let Ok(uid) = Uid::parse_str(raw) {
                if !resolved.contains_key(&uid) {
                    missing.insert(uid);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_missing_refs(item, resolved, missing);
            }
        }
        serde_json::Value::Object(map) => {
            for value in map.values() {
                collect_missing_refs(value, resolved, missing);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests;
