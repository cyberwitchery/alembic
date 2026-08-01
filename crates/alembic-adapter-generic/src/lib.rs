//! generic rest adapter for alembic.

use alembic_core::{JsonMap, Key, Schema, TypeName, TypeSchema, Uid};
use alembic_engine::{
    apply_non_delete_journaled, build_key_from_schema, describe_missing_refs, is_missing_ref_error,
    normalize_attrs_refs, resolved_ids_identity, Adapter, AppliedOp, ApplyReport, BackendId,
    Emitter, ObservedObject, ObservedState, Observer, Op, ProvisionReport, RetryApplyDriver,
    StateMappings,
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
        mappings: &StateMappings,
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

        let resp = self.client.post(&url).json(&body).send().await?;
        let resp = match resp.error_for_status() {
            Ok(resp) => resp,
            Err(err) if err.status() == Some(reqwest::StatusCode::CONFLICT) => {
                // a prior (possibly interrupted) run may already have created this
                // object; reuse the existing one when present.
                let key = build_key_from_schema(type_schema, &desired.attrs)?;
                if let Some(existing) = self
                    .lookup_backend_id(type_name, endpoint, type_schema, mappings, &key)
                    .await?
                {
                    tracing::warn!(
                        type_name = %type_name,
                        "create already exists; using existing object"
                    );
                    resolved.insert(uid, existing.clone());
                    return Ok(AppliedOp {
                        uid,
                        type_name: type_name.clone(),
                        backend_id: Some(existing),
                    });
                }
                return Err(err.into());
            }
            Err(err) => return Err(err.into()),
        };
        let body: serde_json::Value = resp.json().await?;

        let id_val = resolve_path(&body, &endpoint.id_path)?;
        let backend_id = parse_backend_id(id_val)?;
        resolved.insert(uid, backend_id.clone());

        Ok(AppliedOp {
            uid,
            type_name: type_name.clone(),
            backend_id: Some(backend_id),
        })
    }

    /// list the endpoint and return the backend id of the object whose key matches,
    /// or `None` when no such object exists. used to recover from a create conflict.
    async fn lookup_backend_id(
        &self,
        type_name: &TypeName,
        endpoint: &EndpointConfig,
        type_schema: &TypeSchema,
        mappings: &StateMappings,
        key: &Key,
    ) -> Result<Option<BackendId>> {
        let results =
            list_endpoint_results(&self.client, &self.config.base_url, endpoint, type_name).await?;

        for item in results {
            let attrs: JsonMap = match &item {
                serde_json::Value::Object(map) => {
                    map.clone().into_iter().collect::<BTreeMap<_, _>>().into()
                }
                _ => return Err(anyhow!("expected object in results")),
            };
            let attrs = normalize_attrs_refs(&attrs, type_schema, mappings);
            if build_key_from_schema(type_schema, &attrs)? == *key {
                let id_val = resolve_path(&item, &endpoint.id_path)?;
                let backend_id = parse_backend_id(id_val)?;
                return Ok(Some(backend_id));
            }
        }
        Ok(None)
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
                let resp = self.client.delete(&url).send().await?;
                match resp.error_for_status() {
                    Ok(_) => {}
                    // already gone: a prior run (or another actor) removed it.
                    Err(err) if err.status() == Some(reqwest::StatusCode::NOT_FOUND) => {
                        tracing::warn!(type_name = %type_name, "delete target already gone");
                    }
                    Err(err) => return Err(err.into()),
                }
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
impl Observer for GenericAdapter {
    async fn read(
        &self,
        schema: &Schema,
        types: &[TypeName],
        state_store: &alembic_engine::StateStore,
    ) -> Result<ObservedState> {
        let mut state = ObservedState::default();
        let mappings = StateMappings::from_state(state_store);
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
                let results =
                    list_endpoint_results(&client, &base_url, &endpoint, &type_name).await?;

                let mut observed = Vec::new();
                for item in results {
                    let id_val = resolve_path(&item, &endpoint.id_path)?;
                    let backend_id = parse_backend_id(id_val)?;

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
                state.insert(object)?;
            }
        }

        Ok(state)
    }
}

#[async_trait]
impl Emitter for GenericAdapter {
    async fn write(
        &self,
        schema: &Schema,
        ops: &[Op],
        state: &alembic_engine::StateStore,
    ) -> Result<ApplyReport> {
        let mut applied = Vec::new();
        let mut resolved = resolved_ids_identity(state);
        for op in ops {
            if let Op::Create { uid, .. } = op {
                resolved.remove(uid);
            }
        }
        let mappings = StateMappings::from_state(state);

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
            mappings: &'a StateMappings,
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
                            .apply_create(
                                *uid,
                                type_name,
                                desired,
                                self.schema,
                                self.mappings,
                                self.resolved,
                            )
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

            fn resume(&mut self, resumed: &[AppliedOp]) {
                for op in resumed {
                    if let Some(backend_id) = &op.backend_id {
                        self.resolved.insert(op.uid, backend_id.clone());
                    }
                }
            }
        }

        let mut driver = ApplyDriver {
            adapter: self,
            resolved: &mut resolved,
            schema,
            mappings: &mappings,
        };
        let (retry_result, previously_applied_count) =
            apply_non_delete_journaled(state, "generic", &creates_updates, &mut driver).await?;
        if !retry_result.pending.is_empty() {
            let missing = describe_missing_refs(&retry_result.pending, &resolved);
            return Err(anyhow!("unresolved references: {missing}"));
        }

        let resumed = retry_result.resumed;
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
            resumed,
            previously_applied_count,
            ..Default::default()
        })
    }
}

#[async_trait]
impl Adapter for GenericAdapter {
    // the generic rest adapter never provisions schema: it assumes the backend
    // schema already exists, so ensure_schema is the no-op default (an empty
    // report). preview must mirror that honestly as "nothing to provision"
    // rather than the default None, which the cli renders as "preview
    // unavailable for this backend" -- a capability limit generic does not have.
    async fn preview_schema(&self, _schema: &Schema) -> Result<Option<ProvisionReport>> {
        Ok(Some(ProvisionReport::default()))
    }
}

/// list an endpoint and return its results array (from `results_path` when set,
/// else the response root). shared by `read` and `lookup_backend_id` so the
/// 409-recovery path fetches and extracts identically to observation.
async fn list_endpoint_results(
    client: &reqwest::Client,
    base_url: &str,
    endpoint: &EndpointConfig,
    type_name: &TypeName,
) -> Result<Vec<serde_json::Value>> {
    let url = format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        endpoint.path.trim_start_matches('/')
    );
    let resp = client.get(&url).send().await?.error_for_status()?;
    let body: serde_json::Value = resp.json().await?;

    if let Some(path) = &endpoint.results_path {
        Ok(resolve_path(&body, path)?
            .as_array()
            .ok_or_else(|| anyhow!("expected array at path {} for {}", path, type_name))?
            .clone())
    } else if let Some(arr) = body.as_array() {
        Ok(arr.clone())
    } else {
        Err(anyhow!("expected array in list response for {}", type_name))
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

/// decode a backend id from the id-path value the api returned.
fn parse_backend_id(id_val: serde_json::Value) -> Result<BackendId> {
    match id_val {
        serde_json::Value::Number(n) => Ok(BackendId::Int(
            n.as_u64().ok_or_else(|| anyhow!("invalid integer id"))?,
        )),
        serde_json::Value::String(s) => Ok(BackendId::String(s)),
        _ => Err(anyhow!("id must be number or string")),
    }
}

/// builds the generic api request body from an object's attrs, encoding a
/// resolved ref as the backend id (number or string) the generic api expects.
///
/// delegates to the shared engine `build_request_body`, which passes a null
/// value straight through (clearing a nullable field) and recurses into refs
/// nested inside `List` and `Map` fields, matching the netbox and nautobot
/// adapters.
fn resolve_attrs(
    attrs: &JsonMap,
    type_schema: &alembic_core::TypeSchema,
    resolved: &BTreeMap<Uid, BackendId>,
) -> Result<serde_json::Value> {
    alembic_engine::build_request_body(type_schema, attrs, resolved, |id| match id {
        BackendId::Int(n) => serde_json::Value::Number((*n).into()),
        BackendId::String(s) => serde_json::Value::String(s.clone()),
    })
}

#[cfg(test)]
mod tests;
