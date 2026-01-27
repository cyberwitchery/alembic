//! generic rest adapter for alembic.

use alembic_core::{JsonMap, Key, Schema, TypeName, Uid};
use alembic_engine::{
    Adapter, AppliedOp, ApplyReport, BackendId, ObservedObject, ObservedState, Op, ProjectionData,
    StateStore,
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
    state: std::sync::Mutex<StateStore>,
}

impl GenericAdapter {
    pub fn new(config: GenericConfig, state: StateStore) -> Result<Self> {
        let mut headers = reqwest::header::HeaderMap::new();
        for (k, v) in &config.headers {
            let name = reqwest::header::HeaderName::from_bytes(k.as_bytes())?;
            let value = reqwest::header::HeaderValue::from_str(v)?;
            headers.insert(name, value);
        }

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;

        Ok(Self {
            config,
            client,
            state: std::sync::Mutex::new(state),
        })
    }

    fn state_guard(&self) -> Result<std::sync::MutexGuard<'_, StateStore>> {
        self.state
            .lock()
            .map_err(|_| anyhow!("state lock poisoned"))
    }

    async fn apply_create(
        &self,
        uid: Uid,
        type_name: &TypeName,
        desired: &alembic_engine::ProjectedObject,
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
        let body = resolve_attrs(&desired.base.attrs, type_schema, resolved)?;

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
        desired: &alembic_engine::ProjectedObject,
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
        let body = resolve_attrs(&desired.base.attrs, type_schema, resolved)?;

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

        if let DeleteStrategy::Standard = endpoint.delete_strategy {
            let url = self.backend_id_to_url(endpoint, id);
            self.client.delete(&url).send().await?.error_for_status()?;
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
    async fn observe(&self, schema: &Schema, types: &[TypeName]) -> Result<ObservedState> {
        let mut state = ObservedState::default();
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

                    let key = build_key_from_schema(&type_schema, &attrs)?;

                    observed.push(ObservedObject {
                        type_name: type_name.clone(),
                        key,
                        attrs,
                        projection: ProjectionData::default(),
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

    async fn apply(&self, schema: &Schema, ops: &[Op]) -> Result<ApplyReport> {
        let mut applied = Vec::new();
        let mut resolved = {
            let state_guard = self.state_guard()?;
            resolved_from_state(&state_guard)
        };

        let mut creates_updates = Vec::new();
        let mut deletes = Vec::new();
        for op in ops {
            match op {
                Op::Delete { .. } => deletes.push(op.clone()),
                _ => creates_updates.push(op.clone()),
            }
        }

        let mut pending = creates_updates;
        let mut progress = true;
        while !pending.is_empty() && progress {
            progress = false;
            let current = std::mem::take(&mut pending);
            let current_len = current.len();
            let mut next = Vec::new();

            for op in current {
                let result = match &op {
                    Op::Create {
                        uid,
                        type_name,
                        desired,
                    } => {
                        self.apply_create(*uid, type_name, desired, schema, &mut resolved)
                            .await
                    }
                    Op::Update {
                        uid,
                        type_name,
                        desired,
                        backend_id,
                        ..
                    } => {
                        self.apply_update(
                            *uid,
                            type_name,
                            desired,
                            backend_id.as_ref(),
                            schema,
                            &resolved,
                        )
                        .await
                    }
                    Op::Delete { .. } => continue,
                };

                match result {
                    Ok(applied_op) => {
                        if let Some(backend_id) = &applied_op.backend_id {
                            resolved.insert(applied_op.uid, backend_id.clone());
                        }
                        applied.push(applied_op);
                        progress = true;
                    }
                    Err(err) if is_missing_ref_error(&err) => next.push(op),
                    Err(err) => return Err(err),
                }
            }

            if next.len() == current_len {
                return Err(anyhow!("unresolved references in generic plan"));
            }
            pending = next;
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

        Ok(ApplyReport { applied })
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

fn build_key_from_schema(type_schema: &alembic_core::TypeSchema, attrs: &JsonMap) -> Result<Key> {
    let mut map = BTreeMap::new();
    for field in type_schema.key.keys() {
        let Some(value) = attrs.get(field) else {
            return Err(anyhow!("missing key field {field}"));
        };
        map.insert(field.clone(), value.clone());
    }
    Ok(Key::from(map))
}

fn resolved_from_state(state: &StateStore) -> BTreeMap<Uid, BackendId> {
    let mut resolved = BTreeMap::new();
    for mapping in state.all_mappings().values() {
        for (uid, backend_id) in mapping {
            resolved.insert(*uid, backend_id.clone());
        }
    }
    resolved
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
        .ok_or_else(|| anyhow!("missing referenced uid {}", uid))?;
    Ok(match id {
        BackendId::Int(n) => serde_json::Value::Number((*n).into()),
        BackendId::String(s) => serde_json::Value::String(s.clone()),
    })
}

fn is_missing_ref_error(err: &anyhow::Error) -> bool {
    err.to_string().contains("missing referenced uid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use alembic_core::{FieldSchema, FieldType, TypeSchema};
    use alembic_engine::StateData;
    use httpmock::prelude::*;
    use httpmock::Method::PATCH;

    fn new_state_store() -> StateStore {
        StateStore::new(None, StateData::default())
    }

    fn test_config(base_url: &str) -> GenericConfig {
        let mut types = BTreeMap::new();
        types.insert(
            "device".to_string(),
            EndpointConfig {
                path: "/api/devices".to_string(),
                results_path: Some("results".to_string()),
                id_path: "id".to_string(),
                delete_strategy: DeleteStrategy::Standard,
                update_method: "PATCH".to_string(),
            },
        );
        types.insert(
            "site".to_string(),
            EndpointConfig {
                path: "/api/sites".to_string(),
                results_path: None,
                id_path: "id".to_string(),
                delete_strategy: DeleteStrategy::None,
                update_method: "PUT".to_string(),
            },
        );
        GenericConfig {
            base_url: base_url.to_string(),
            headers: BTreeMap::new(),
            types,
        }
    }

    fn test_schema() -> Schema {
        let mut types = BTreeMap::new();

        let mut device_fields = BTreeMap::new();
        device_fields.insert(
            "name".to_string(),
            FieldSchema {
                r#type: FieldType::String,
                required: true,
                nullable: false,
                description: None,
            },
        );
        device_fields.insert(
            "site".to_string(),
            FieldSchema {
                r#type: FieldType::Ref {
                    target: "site".to_string(),
                },
                required: true,
                nullable: false,
                description: None,
            },
        );

        let mut device_key = BTreeMap::new();
        device_key.insert(
            "name".to_string(),
            FieldSchema {
                r#type: FieldType::String,
                required: true,
                nullable: false,
                description: None,
            },
        );
        types.insert(
            "device".to_string(),
            TypeSchema {
                key: device_key,
                fields: device_fields,
            },
        );

        let mut site_fields = BTreeMap::new();
        site_fields.insert(
            "name".to_string(),
            FieldSchema {
                r#type: FieldType::String,
                required: true,
                nullable: false,
                description: None,
            },
        );

        let mut site_key = BTreeMap::new();
        site_key.insert(
            "name".to_string(),
            FieldSchema {
                r#type: FieldType::String,
                required: true,
                nullable: false,
                description: None,
            },
        );
        types.insert(
            "site".to_string(),
            TypeSchema {
                key: site_key,
                fields: site_fields,
            },
        );

        Schema { types }
    }

    fn empty_schema() -> Schema {
        Schema {
            types: BTreeMap::new(),
        }
    }

    // Tests for resolve_path
    #[test]
    fn test_resolve_path_simple() {
        let value = serde_json::json!({"id": 42, "name": "test"});
        let result = resolve_path(&value, "id").unwrap();
        assert_eq!(result, serde_json::json!(42));
    }

    #[test]
    fn test_resolve_path_nested() {
        let value = serde_json::json!({"data": {"results": [1, 2, 3]}});
        let result = resolve_path(&value, "data.results").unwrap();
        assert_eq!(result, serde_json::json!([1, 2, 3]));
    }

    #[test]
    fn test_resolve_path_empty() {
        let value = serde_json::json!({"id": 42});
        let result = resolve_path(&value, "").unwrap();
        assert_eq!(result, serde_json::json!({"id": 42}));
    }

    #[test]
    fn test_resolve_path_not_found() {
        let value = serde_json::json!({"id": 42});
        let err = resolve_path(&value, "missing").unwrap_err();
        assert!(err.to_string().contains("path segment not found"));
    }

    // Tests for build_key_from_schema
    #[test]
    fn test_build_key_from_schema_success() {
        let schema = test_schema();
        let type_schema = schema.types.get("device").unwrap();
        let attrs: JsonMap = serde_json::json!({"name": "dev1", "site": "site1"})
            .as_object()
            .unwrap()
            .clone()
            .into_iter()
            .collect::<BTreeMap<_, _>>()
            .into();
        let key = build_key_from_schema(type_schema, &attrs).unwrap();
        assert_eq!(key.get("name"), Some(&serde_json::json!("dev1")));
    }

    #[test]
    fn test_build_key_from_schema_missing_field() {
        let schema = test_schema();
        let type_schema = schema.types.get("device").unwrap();
        let attrs: JsonMap = serde_json::json!({"other": "value"})
            .as_object()
            .unwrap()
            .clone()
            .into_iter()
            .collect::<BTreeMap<_, _>>()
            .into();
        let err = build_key_from_schema(type_schema, &attrs).unwrap_err();
        assert!(err.to_string().contains("missing key field"));
    }

    // Tests for resolved_from_state
    #[test]
    fn test_resolved_from_state_empty() {
        let state = new_state_store();
        let resolved = resolved_from_state(&state);
        assert!(resolved.is_empty());
    }

    #[test]
    fn test_resolved_from_state_with_mappings() {
        let mut state = new_state_store();
        let uid = Uid::new_v4();
        state.set_backend_id(TypeName::new("device".to_string()), uid, BackendId::Int(42));
        let resolved = resolved_from_state(&state);
        assert_eq!(resolved.get(&uid), Some(&BackendId::Int(42)));
    }

    // Tests for resolve_value_for_type
    #[test]
    fn test_resolve_value_for_type_string() {
        let resolved = BTreeMap::new();
        let result =
            resolve_value_for_type(&FieldType::String, serde_json::json!("test"), &resolved)
                .unwrap();
        assert_eq!(result, serde_json::json!("test"));
    }

    #[test]
    fn test_resolve_value_for_type_int() {
        let resolved = BTreeMap::new();
        let result =
            resolve_value_for_type(&FieldType::Int, serde_json::json!(42), &resolved).unwrap();
        assert_eq!(result, serde_json::json!(42));
    }

    #[test]
    fn test_resolve_value_for_type_ref() {
        let mut resolved = BTreeMap::new();
        let uid = Uid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        resolved.insert(uid, BackendId::Int(123));

        let result = resolve_value_for_type(
            &FieldType::Ref {
                target: "site".to_string(),
            },
            serde_json::json!("550e8400-e29b-41d4-a716-446655440000"),
            &resolved,
        )
        .unwrap();
        assert_eq!(result, serde_json::json!(123));
    }

    #[test]
    fn test_resolve_value_for_type_list_ref() {
        let mut resolved = BTreeMap::new();
        let uid1 = Uid::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap();
        let uid2 = Uid::parse_str("550e8400-e29b-41d4-a716-446655440002").unwrap();
        resolved.insert(uid1, BackendId::Int(1));
        resolved.insert(uid2, BackendId::String("abc".to_string()));

        let result = resolve_value_for_type(
            &FieldType::ListRef {
                target: "tag".to_string(),
            },
            serde_json::json!([
                "550e8400-e29b-41d4-a716-446655440001",
                "550e8400-e29b-41d4-a716-446655440002"
            ]),
            &resolved,
        )
        .unwrap();
        assert_eq!(result, serde_json::json!([1, "abc"]));
    }

    #[test]
    fn test_resolve_value_for_type_list_ref_not_array() {
        let resolved = BTreeMap::new();
        let err = resolve_value_for_type(
            &FieldType::ListRef {
                target: "tag".to_string(),
            },
            serde_json::json!("not_an_array"),
            &resolved,
        )
        .unwrap_err();
        assert!(err.to_string().contains("expected array for list_ref"));
    }

    // Tests for resolve_ref_value
    #[test]
    fn test_resolve_ref_value_int_backend_id() {
        let mut resolved = BTreeMap::new();
        let uid = Uid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        resolved.insert(uid, BackendId::Int(42));

        let result = resolve_ref_value(
            serde_json::json!("550e8400-e29b-41d4-a716-446655440000"),
            &resolved,
        )
        .unwrap();
        assert_eq!(result, serde_json::json!(42));
    }

    #[test]
    fn test_resolve_ref_value_string_backend_id() {
        let mut resolved = BTreeMap::new();
        let uid = Uid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        resolved.insert(uid, BackendId::String("abc-123".to_string()));

        let result = resolve_ref_value(
            serde_json::json!("550e8400-e29b-41d4-a716-446655440000"),
            &resolved,
        )
        .unwrap();
        assert_eq!(result, serde_json::json!("abc-123"));
    }

    #[test]
    fn test_resolve_ref_value_not_string() {
        let resolved = BTreeMap::new();
        let err = resolve_ref_value(serde_json::json!(42), &resolved).unwrap_err();
        assert!(err.to_string().contains("ref must be uuid string"));
    }

    #[test]
    fn test_resolve_ref_value_invalid_uuid() {
        let resolved = BTreeMap::new();
        let err = resolve_ref_value(serde_json::json!("not-a-uuid"), &resolved).unwrap_err();
        assert!(err.to_string().contains("invalid uuid"));
    }

    #[test]
    fn test_resolve_ref_value_missing_uid() {
        let resolved = BTreeMap::new();
        let err = resolve_ref_value(
            serde_json::json!("550e8400-e29b-41d4-a716-446655440000"),
            &resolved,
        )
        .unwrap_err();
        assert!(err.to_string().contains("missing referenced uid"));
    }

    // Tests for is_missing_ref_error
    #[test]
    fn test_is_missing_ref_error_true() {
        let err = anyhow!("missing referenced uid 550e8400-e29b-41d4-a716-446655440000");
        assert!(is_missing_ref_error(&err));
    }

    #[test]
    fn test_is_missing_ref_error_false() {
        let err = anyhow!("some other error");
        assert!(!is_missing_ref_error(&err));
    }

    // Tests for resolve_attrs
    #[test]
    fn test_resolve_attrs_success() {
        let schema = test_schema();
        let type_schema = schema.types.get("site").unwrap();
        let attrs: JsonMap = serde_json::json!({"name": "site1"})
            .as_object()
            .unwrap()
            .clone()
            .into_iter()
            .collect::<BTreeMap<_, _>>()
            .into();
        let resolved = BTreeMap::new();
        let result = resolve_attrs(&attrs, type_schema, &resolved).unwrap();
        assert_eq!(result, serde_json::json!({"name": "site1"}));
    }

    #[test]
    fn test_resolve_attrs_missing_schema() {
        let schema = test_schema();
        let type_schema = schema.types.get("site").unwrap();
        let attrs: JsonMap = serde_json::json!({"unknown_field": "value"})
            .as_object()
            .unwrap()
            .clone()
            .into_iter()
            .collect::<BTreeMap<_, _>>()
            .into();
        let resolved = BTreeMap::new();
        let err = resolve_attrs(&attrs, type_schema, &resolved).unwrap_err();
        assert!(err.to_string().contains("missing schema for field"));
    }

    // Tests for default functions
    #[test]
    fn test_default_id_path() {
        assert_eq!(default_id_path(), "id");
    }

    #[test]
    fn test_default_update_method() {
        assert_eq!(default_update_method(), "PATCH");
    }

    // Tests for DeleteStrategy
    #[test]
    fn test_delete_strategy_default() {
        let strategy = DeleteStrategy::default();
        assert!(matches!(strategy, DeleteStrategy::None));
    }

    #[test]
    fn test_delete_strategy_serde() {
        let standard: DeleteStrategy = serde_json::from_str("\"standard\"").unwrap();
        assert!(matches!(standard, DeleteStrategy::Standard));

        let none: DeleteStrategy = serde_json::from_str("\"none\"").unwrap();
        assert!(matches!(none, DeleteStrategy::None));
    }

    // Tests for GenericConfig serialization
    #[test]
    fn test_generic_config_serde() {
        let config = test_config("http://example.com");
        let json = serde_json::to_string(&config).unwrap();
        let parsed: GenericConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.base_url, "http://example.com");
        assert!(parsed.types.contains_key("device"));
    }

    // Tests for GenericAdapter::new
    #[test]
    fn test_generic_adapter_new_success() {
        let config = test_config("http://example.com");
        let state = new_state_store();
        let adapter = GenericAdapter::new(config, state);
        assert!(adapter.is_ok());
    }

    #[test]
    fn test_generic_adapter_new_with_headers() {
        let mut config = test_config("http://example.com");
        config
            .headers
            .insert("Authorization".to_string(), "Bearer token".to_string());
        config
            .headers
            .insert("Content-Type".to_string(), "application/json".to_string());
        let state = new_state_store();
        let adapter = GenericAdapter::new(config, state);
        assert!(adapter.is_ok());
    }

    #[test]
    fn test_generic_adapter_new_invalid_header_name() {
        let mut config = test_config("http://example.com");
        config
            .headers
            .insert("invalid\nheader".to_string(), "value".to_string());
        let state = new_state_store();
        let adapter = GenericAdapter::new(config, state);
        assert!(adapter.is_err());
    }

    // Tests for backend_id_to_url
    #[test]
    fn test_backend_id_to_url_int() {
        let config = test_config("http://example.com/");
        let adapter = GenericAdapter::new(config, new_state_store()).unwrap();
        let endpoint = adapter.config.types.get("device").unwrap();
        let url = adapter.backend_id_to_url(endpoint, &BackendId::Int(42));
        assert_eq!(url, "http://example.com/api/devices/42");
    }

    #[test]
    fn test_backend_id_to_url_string() {
        let config = test_config("http://example.com");
        let adapter = GenericAdapter::new(config, new_state_store()).unwrap();
        let endpoint = adapter.config.types.get("device").unwrap();
        let url = adapter.backend_id_to_url(endpoint, &BackendId::String("abc-123".to_string()));
        assert_eq!(url, "http://example.com/api/devices/abc-123");
    }

    // Tests for observe with mocked server
    #[tokio::test]
    async fn test_observe_with_results_path() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET).path("/api/devices");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({
                    "results": [
                        {"id": 1, "name": "device1"},
                        {"id": 2, "name": "device2"}
                    ]
                }));
        });

        let config = test_config(&server.base_url());
        let adapter = GenericAdapter::new(config, new_state_store()).unwrap();
        let schema = test_schema();

        let state = adapter
            .observe(&schema, &[TypeName::new("device".to_string())])
            .await
            .unwrap();

        mock.assert();
        assert_eq!(state.by_key.len(), 2);
    }

    #[tokio::test]
    async fn test_observe_without_results_path() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET).path("/api/sites");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!([
                    {"id": 1, "name": "site1"},
                    {"id": 2, "name": "site2"}
                ]));
        });

        let config = test_config(&server.base_url());
        let adapter = GenericAdapter::new(config, new_state_store()).unwrap();
        let schema = test_schema();

        let state = adapter
            .observe(&schema, &[TypeName::new("site".to_string())])
            .await
            .unwrap();

        mock.assert();
        assert_eq!(state.by_key.len(), 2);
    }

    #[tokio::test]
    async fn test_observe_all_types() {
        let server = MockServer::start();
        let device_mock = server.mock(|when, then| {
            when.method(GET).path("/api/devices");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({"results": [{"id": 1, "name": "device1"}]}));
        });
        let site_mock = server.mock(|when, then| {
            when.method(GET).path("/api/sites");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!([{"id": 1, "name": "site1"}]));
        });

        let config = test_config(&server.base_url());
        let adapter = GenericAdapter::new(config, new_state_store()).unwrap();
        let schema = test_schema();

        let state = adapter.observe(&schema, &[]).await.unwrap();

        device_mock.assert();
        site_mock.assert();
        assert_eq!(state.by_key.len(), 2);
    }

    #[tokio::test]
    async fn test_observe_string_id() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path("/api/sites");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!([{"id": "uuid-123", "name": "site1"}]));
        });

        let config = test_config(&server.base_url());
        let adapter = GenericAdapter::new(config, new_state_store()).unwrap();
        let schema = test_schema();

        let state = adapter
            .observe(&schema, &[TypeName::new("site".to_string())])
            .await
            .unwrap();

        assert_eq!(state.by_key.len(), 1);
        let obj = state.by_key.values().next().unwrap();
        assert_eq!(
            obj.backend_id,
            Some(BackendId::String("uuid-123".to_string()))
        );
    }

    #[tokio::test]
    async fn test_observe_unknown_type() {
        let config = test_config("http://example.com");
        let adapter = GenericAdapter::new(config, new_state_store()).unwrap();
        let schema = test_schema();

        let err = adapter
            .observe(&schema, &[TypeName::new("unknown".to_string())])
            .await
            .unwrap_err();

        assert!(err.to_string().contains("no generic config for type"));
    }

    #[tokio::test]
    async fn test_observe_missing_schema() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path("/api/devices");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({"results": []}));
        });

        let config = test_config(&server.base_url());
        let adapter = GenericAdapter::new(config, new_state_store()).unwrap();
        let empty_schema = empty_schema();

        let err = adapter
            .observe(&empty_schema, &[TypeName::new("device".to_string())])
            .await
            .unwrap_err();

        assert!(err.to_string().contains("missing schema for"));
    }

    // Tests for apply with mocked server
    #[tokio::test]
    async fn test_apply_create() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(POST).path("/api/sites");
            then.status(201)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({"id": 42, "name": "new-site"}));
        });

        let config = test_config(&server.base_url());
        let adapter = GenericAdapter::new(config, new_state_store()).unwrap();
        let schema = test_schema();

        let uid = Uid::new_v4();
        let mut key = BTreeMap::new();
        key.insert("name".to_string(), serde_json::json!("new-site"));
        let mut attrs = BTreeMap::new();
        attrs.insert("name".to_string(), serde_json::json!("new-site"));

        let ops = vec![Op::Create {
            uid,
            type_name: TypeName::new("site".to_string()),
            desired: alembic_engine::ProjectedObject {
                base: alembic_core::Object {
                    uid,
                    type_name: TypeName::new("site".to_string()),
                    key: Key::from(key),
                    attrs: attrs.into(),
                    source: None,
                },
                projection: ProjectionData::default(),
                projection_inputs: BTreeSet::new(),
            },
        }];

        let report = adapter.apply(&schema, &ops).await.unwrap();
        mock.assert();
        assert_eq!(report.applied.len(), 1);
        assert_eq!(report.applied[0].backend_id, Some(BackendId::Int(42)));
    }

    #[tokio::test]
    async fn test_apply_update_patch() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(PATCH).path("/api/devices/42");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({"id": 42, "name": "updated"}));
        });

        let mut state = new_state_store();
        let uid = Uid::new_v4();
        state.set_backend_id(TypeName::new("device".to_string()), uid, BackendId::Int(42));

        // Add a site reference that will be resolved
        let site_uid = Uid::new_v4();
        state.set_backend_id(
            TypeName::new("site".to_string()),
            site_uid,
            BackendId::Int(1),
        );

        let config = test_config(&server.base_url());
        let adapter = GenericAdapter::new(config, state).unwrap();
        let schema = test_schema();

        let mut key = BTreeMap::new();
        key.insert("name".to_string(), serde_json::json!("updated"));
        let mut attrs = BTreeMap::new();
        attrs.insert("name".to_string(), serde_json::json!("updated"));
        attrs.insert("site".to_string(), serde_json::json!(site_uid.to_string()));

        let ops = vec![Op::Update {
            uid,
            type_name: TypeName::new("device".to_string()),
            desired: alembic_engine::ProjectedObject {
                base: alembic_core::Object {
                    uid,
                    type_name: TypeName::new("device".to_string()),
                    key: Key::from(key),
                    attrs: attrs.into(),
                    source: None,
                },
                projection: ProjectionData::default(),
                projection_inputs: BTreeSet::new(),
            },
            backend_id: Some(BackendId::Int(42)),
            changes: vec![],
        }];

        let report = adapter.apply(&schema, &ops).await.unwrap();
        mock.assert();
        assert_eq!(report.applied.len(), 1);
    }

    #[tokio::test]
    async fn test_apply_update_put() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(PUT).path("/api/sites/42");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({"id": 42, "name": "updated"}));
        });

        let mut state = new_state_store();
        let uid = Uid::new_v4();
        state.set_backend_id(TypeName::new("site".to_string()), uid, BackendId::Int(42));

        let config = test_config(&server.base_url());
        let adapter = GenericAdapter::new(config, state).unwrap();
        let schema = test_schema();

        let mut key = BTreeMap::new();
        key.insert("name".to_string(), serde_json::json!("updated"));
        let mut attrs = BTreeMap::new();
        attrs.insert("name".to_string(), serde_json::json!("updated"));

        let ops = vec![Op::Update {
            uid,
            type_name: TypeName::new("site".to_string()),
            desired: alembic_engine::ProjectedObject {
                base: alembic_core::Object {
                    uid,
                    type_name: TypeName::new("site".to_string()),
                    key: Key::from(key),
                    attrs: attrs.into(),
                    source: None,
                },
                projection: ProjectionData::default(),
                projection_inputs: BTreeSet::new(),
            },
            backend_id: Some(BackendId::Int(42)),
            changes: vec![],
        }];

        let report = adapter.apply(&schema, &ops).await.unwrap();
        mock.assert();
        assert_eq!(report.applied.len(), 1);
    }

    #[tokio::test]
    async fn test_apply_delete_standard() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(DELETE).path("/api/devices/42");
            then.status(204);
        });

        let config = test_config(&server.base_url());
        let adapter = GenericAdapter::new(config, new_state_store()).unwrap();
        let schema = test_schema();

        let uid = Uid::new_v4();
        let mut key = BTreeMap::new();
        key.insert("name".to_string(), serde_json::json!("to-delete"));

        let ops = vec![Op::Delete {
            uid,
            type_name: TypeName::new("device".to_string()),
            key: Key::from(key),
            backend_id: Some(BackendId::Int(42)),
        }];

        let report = adapter.apply(&schema, &ops).await.unwrap();
        mock.assert();
        assert_eq!(report.applied.len(), 1);
        assert_eq!(report.applied[0].backend_id, None);
    }

    #[tokio::test]
    async fn test_apply_delete_none_strategy() {
        // site has DeleteStrategy::None, so no DELETE call should be made
        let server = MockServer::start();

        let config = test_config(&server.base_url());
        let adapter = GenericAdapter::new(config, new_state_store()).unwrap();
        let schema = test_schema();

        let uid = Uid::new_v4();
        let mut key = BTreeMap::new();
        key.insert("name".to_string(), serde_json::json!("to-delete"));

        let ops = vec![Op::Delete {
            uid,
            type_name: TypeName::new("site".to_string()),
            key: Key::from(key),
            backend_id: Some(BackendId::Int(42)),
        }];

        let report = adapter.apply(&schema, &ops).await.unwrap();
        // No HTTP calls should be made
        assert_eq!(report.applied.len(), 1);
    }

    #[tokio::test]
    async fn test_apply_delete_missing_backend_id() {
        let config = test_config("http://example.com");
        let adapter = GenericAdapter::new(config, new_state_store()).unwrap();
        let schema = test_schema();

        let uid = Uid::new_v4();
        let mut key = BTreeMap::new();
        key.insert("name".to_string(), serde_json::json!("to-delete"));

        let ops = vec![Op::Delete {
            uid,
            type_name: TypeName::new("device".to_string()),
            key: Key::from(key),
            backend_id: None,
        }];

        let err = adapter.apply(&schema, &ops).await.unwrap_err();
        assert!(err.to_string().contains("delete requires backend id"));
    }

    #[tokio::test]
    async fn test_apply_update_missing_backend_id() {
        let config = test_config("http://example.com");
        let adapter = GenericAdapter::new(config, new_state_store()).unwrap();
        let schema = test_schema();

        let uid = Uid::new_v4();
        let mut key = BTreeMap::new();
        key.insert("name".to_string(), serde_json::json!("test"));
        let mut attrs = BTreeMap::new();
        attrs.insert("name".to_string(), serde_json::json!("test"));

        let ops = vec![Op::Update {
            uid,
            type_name: TypeName::new("site".to_string()),
            desired: alembic_engine::ProjectedObject {
                base: alembic_core::Object {
                    uid,
                    type_name: TypeName::new("site".to_string()),
                    key: Key::from(key),
                    attrs: attrs.into(),
                    source: None,
                },
                projection: ProjectionData::default(),
                projection_inputs: BTreeSet::new(),
            },
            backend_id: None,
            changes: vec![],
        }];

        let err = adapter.apply(&schema, &ops).await.unwrap_err();
        assert!(err.to_string().contains("update requires backend id"));
    }

    #[tokio::test]
    async fn test_apply_create_string_id() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(POST).path("/api/sites");
            then.status(201)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({"id": "uuid-abc-123", "name": "new-site"}));
        });

        let config = test_config(&server.base_url());
        let adapter = GenericAdapter::new(config, new_state_store()).unwrap();
        let schema = test_schema();

        let uid = Uid::new_v4();
        let mut key = BTreeMap::new();
        key.insert("name".to_string(), serde_json::json!("new-site"));
        let mut attrs = BTreeMap::new();
        attrs.insert("name".to_string(), serde_json::json!("new-site"));

        let ops = vec![Op::Create {
            uid,
            type_name: TypeName::new("site".to_string()),
            desired: alembic_engine::ProjectedObject {
                base: alembic_core::Object {
                    uid,
                    type_name: TypeName::new("site".to_string()),
                    key: Key::from(key),
                    attrs: attrs.into(),
                    source: None,
                },
                projection: ProjectionData::default(),
                projection_inputs: BTreeSet::new(),
            },
        }];

        let report = adapter.apply(&schema, &ops).await.unwrap();
        mock.assert();
        assert_eq!(
            report.applied[0].backend_id,
            Some(BackendId::String("uuid-abc-123".to_string()))
        );
    }

    #[tokio::test]
    async fn test_apply_unknown_type() {
        let config = test_config("http://example.com");
        let adapter = GenericAdapter::new(config, new_state_store()).unwrap();
        let schema = test_schema();

        let uid = Uid::new_v4();
        let mut key = BTreeMap::new();
        key.insert("name".to_string(), serde_json::json!("test"));
        let mut attrs = BTreeMap::new();
        attrs.insert("name".to_string(), serde_json::json!("test"));

        let ops = vec![Op::Create {
            uid,
            type_name: TypeName::new("unknown".to_string()),
            desired: alembic_engine::ProjectedObject {
                base: alembic_core::Object {
                    uid,
                    type_name: TypeName::new("unknown".to_string()),
                    key: Key::from(key),
                    attrs: attrs.into(),
                    source: None,
                },
                projection: ProjectionData::default(),
                projection_inputs: BTreeSet::new(),
            },
        }];

        let err = adapter.apply(&schema, &ops).await.unwrap_err();
        assert!(err.to_string().contains("no config for unknown"));
    }

    #[tokio::test]
    async fn test_observe_invalid_id_type() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path("/api/sites");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!([{"id": {"nested": "object"}, "name": "site1"}]));
        });

        let config = test_config(&server.base_url());
        let adapter = GenericAdapter::new(config, new_state_store()).unwrap();
        let schema = test_schema();

        let err = adapter
            .observe(&schema, &[TypeName::new("site".to_string())])
            .await
            .unwrap_err();

        assert!(err.to_string().contains("id must be number or string"));
    }

    #[tokio::test]
    async fn test_observe_non_object_in_results() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path("/api/sites");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!(["string_item", "another"]));
        });

        let config = test_config(&server.base_url());
        let adapter = GenericAdapter::new(config, new_state_store()).unwrap();
        let schema = test_schema();

        let err = adapter
            .observe(&schema, &[TypeName::new("site".to_string())])
            .await
            .unwrap_err();

        // The error could be about missing id path since strings don't have "id"
        let err_str = err.to_string();
        assert!(
            err_str.contains("expected object in results")
                || err_str.contains("path segment not found"),
            "unexpected error: {}",
            err_str
        );
    }

    #[tokio::test]
    async fn test_observe_non_array_response() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path("/api/sites");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(serde_json::json!({"not": "an_array"}));
        });

        let config = test_config(&server.base_url());
        let adapter = GenericAdapter::new(config, new_state_store()).unwrap();
        let schema = test_schema();

        let err = adapter
            .observe(&schema, &[TypeName::new("site".to_string())])
            .await
            .unwrap_err();

        assert!(err.to_string().contains("expected array in list response"));
    }
}
