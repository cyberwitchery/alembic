use super::mapping::build_tag_inputs;
use super::registry::ObjectTypeRegistry;
use super::state::{resolved_from_state, state_mappings};
use super::NautobotAdapter;
use alembic_core::{
    key_string, uid_v5, FieldType, JsonMap, Key, Schema, TypeName, TypeSchema, Uid,
};
use alembic_engine::{
    Adapter, AppliedOp, ApplyReport, BackendId, ObservedObject, ObservedState, Op, ProjectionData,
    StateStore,
};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use nautobot::{QueryBuilder, Resource};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[async_trait]
impl Adapter for NautobotAdapter {
    async fn observe(&self, schema: &Schema, types: &[TypeName]) -> Result<ObservedState> {
        let registry: ObjectTypeRegistry = self.client.fetch_object_types().await?;
        let mappings = {
            let state_guard = self.state_guard()?;
            state_mappings(&state_guard)
        };

        let requested: BTreeSet<TypeName> = if types.is_empty() {
            registry.type_names().into_iter().collect()
        } else {
            types.iter().cloned().collect()
        };

        let mut tasks = Vec::new();
        for type_name in requested {
            let info = registry
                .info_for(&type_name)
                .ok_or_else(|| anyhow!("unsupported type {}", type_name))?
                .clone();
            let type_schema = schema
                .types
                .get(type_name.as_str())
                .ok_or_else(|| anyhow!("missing schema for {}", type_name))?
                .clone();
            let client = Arc::clone(&self.client);
            let registry = registry.clone();
            let mappings = mappings.clone();
            let schema = schema.clone();

            tasks.push(tokio::spawn(async move {
                let resource: Resource<Value> = client.resource(info.endpoint.clone());
                let objects = client.list_all(&resource, None).await?;
                let mut observed = Vec::new();
                for object in objects {
                    let (backend_id, mut attrs) = extract_attrs(object)?;
                    let projection = extract_projection(&mut attrs);
                    normalize_attrs(&mut attrs, &type_schema, &schema, &registry, &mappings);
                    let key = build_key_from_schema(&type_schema, &attrs)
                        .with_context(|| format!("build key for {}", type_name))?;
                    observed.push(ObservedObject {
                        type_name: type_name.clone(),
                        key,
                        attrs,
                        projection,
                        backend_id: Some(BackendId::String(backend_id)),
                    });
                }
                Ok::<Vec<ObservedObject>, anyhow::Error>(observed)
            }));
        }

        let mut state = ObservedState::default();
        let results = futures::future::join_all(tasks).await;
        for result in results {
            let objects = result??;
            for object in objects {
                state.insert(object);
            }
        }

        state.capabilities = self.client.fetch_capabilities().await?;
        Ok(state)
    }

    async fn apply(&self, schema: &Schema, ops: &[Op]) -> Result<ApplyReport> {
        let registry: ObjectTypeRegistry = self.client.fetch_object_types().await?;
        let mut applied = Vec::new();
        let mut resolved = {
            let state_guard = self.state_guard()?;
            resolved_from_state(&state_guard)
        };

        for op in ops {
            if let Op::Create { uid, .. } = op {
                resolved.remove(uid);
            }
        }

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
                    Op::Create { .. } => self
                        .apply_create(&op, &mut resolved, &registry, schema)
                        .await
                        .map(|backend_id| AppliedOp {
                            uid: op.uid(),
                            type_name: op.type_name().clone(),
                            backend_id: Some(BackendId::String(backend_id)),
                        }),
                    Op::Update { .. } => self
                        .apply_update(&op, &resolved, &registry, schema)
                        .await
                        .map(|backend_id| AppliedOp {
                            uid: op.uid(),
                            type_name: op.type_name().clone(),
                            backend_id: Some(BackendId::String(backend_id)),
                        }),
                    Op::Delete { .. } => continue,
                };

                match result {
                    Ok(applied_op) => {
                        if let Some(BackendId::String(backend_id)) = &applied_op.backend_id {
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
                let missing = describe_missing_refs(&next, &resolved);
                return Err(anyhow!("unresolved references: {missing}"));
            }

            pending = next;
        }

        for op in deletes {
            if let Op::Delete {
                uid,
                type_name,
                key,
                backend_id,
            } = op
            {
                let id = if let Some(BackendId::String(id)) = backend_id {
                    id.clone()
                } else if let Some(id) = resolved.get(&uid) {
                    id.clone()
                } else {
                    let info = registry
                        .info_for(&type_name)
                        .ok_or_else(|| anyhow!("unsupported type {}", type_name))?;
                    let type_schema = schema
                        .types
                        .get(type_name.as_str())
                        .ok_or_else(|| anyhow!("missing schema for {}", type_name))?;
                    self.lookup_backend_id(&type_name, &info, type_schema, &key, &resolved)
                        .await
                        .with_context(|| {
                            format!("resolve backend id for delete: {}", key_string(&key))
                        })?
                };
                let info = registry
                    .info_for(&type_name)
                    .ok_or_else(|| anyhow!("unsupported type {}", type_name))?;
                let resource: Resource<Value> = self.client.resource(info.endpoint.clone());
                match resource.delete(&id).await {
                    Ok(_) => {}
                    Err(err) if is_404_error(&err) => {
                        eprintln!("warning: {} already deleted", type_name);
                    }
                    Err(err) => return Err(err.into()),
                }
                applied.push(AppliedOp {
                    uid,
                    type_name: type_name.clone(),
                    backend_id: None,
                });
            }
        }

        Ok(ApplyReport { applied })
    }

    async fn create_custom_fields(
        &self,
        missing: &[alembic_engine::MissingCustomField],
    ) -> Result<()> {
        self.create_custom_fields(missing).await
    }

    async fn create_tags(&self, tags: &[String]) -> Result<()> {
        self.create_tags(tags).await
    }

    fn update_state(&self, state: &StateStore) {
        match self.state_guard() {
            Ok(mut guard) => {
                *guard = state.clone();
            }
            Err(err) => {
                eprintln!("warning: {err}");
            }
        }
    }
}

impl NautobotAdapter {
    async fn apply_create(
        &self,
        op: &Op,
        resolved: &mut BTreeMap<Uid, String>,
        registry: &ObjectTypeRegistry,
        schema: &Schema,
    ) -> Result<String> {
        let (uid, type_name, desired) = match op {
            Op::Create {
                uid,
                type_name,
                desired,
            } => (*uid, type_name, desired),
            _ => return Err(anyhow!("expected create operation")),
        };
        let info = registry
            .info_for(type_name)
            .ok_or_else(|| anyhow!("unsupported type {}", type_name))?;
        let type_schema = schema
            .types
            .get(type_name.as_str())
            .ok_or_else(|| anyhow!("missing schema for {}", type_name))?;
        let resource: Resource<Value> = self.client.resource(info.endpoint.clone());
        let body = build_request_body(type_schema, &desired.base.attrs, resolved)?;
        let response: Value = resource.create(&body).await?;
        let backend_id = response
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("create {} returned no id", type_name))?
            .to_string();
        resolved.insert(uid, backend_id.clone());
        self.apply_projection_patch(type_name, &info, &backend_id, &desired.projection)
            .await?;
        Ok(backend_id)
    }

    async fn apply_update(
        &self,
        op: &Op,
        resolved: &BTreeMap<Uid, String>,
        registry: &ObjectTypeRegistry,
        schema: &Schema,
    ) -> Result<String> {
        let (uid, type_name, desired, backend_id) = match op {
            Op::Update {
                uid,
                type_name,
                desired,
                backend_id,
                ..
            } => {
                let id = match backend_id {
                    Some(BackendId::String(id)) => Some(id.clone()),
                    Some(_) => return Err(anyhow!("nautobot requires string backend id")),
                    None => None,
                };
                (*uid, type_name, desired, id)
            }
            _ => return Err(anyhow!("expected update operation")),
        };
        let info = registry
            .info_for(type_name)
            .ok_or_else(|| anyhow!("unsupported type {}", type_name))?;
        let type_schema = schema
            .types
            .get(type_name.as_str())
            .ok_or_else(|| anyhow!("missing schema for {}", type_name))?;
        let id = if let Some(id) = backend_id {
            id
        } else if let Some(id) = resolved.get(&uid).cloned() {
            id
        } else {
            self.lookup_backend_id(type_name, &info, type_schema, &desired.base.key, resolved)
                .await
                .with_context(|| format!("resolve backend id for {}", type_name))?
        };
        let resource: Resource<Value> = self.client.resource(info.endpoint.clone());
        let body = build_request_body(type_schema, &desired.base.attrs, resolved)?;
        let _response = resource.patch(&id, &body).await?;
        self.apply_projection_patch(type_name, &info, &id, &desired.projection)
            .await?;
        Ok(id)
    }

    async fn lookup_backend_id(
        &self,
        type_name: &TypeName,
        info: &super::registry::ObjectTypeInfo,
        type_schema: &TypeSchema,
        key: &Key,
        resolved: &BTreeMap<Uid, String>,
    ) -> Result<String> {
        let query = query_from_key(type_schema, key, resolved)?;
        let resource: Resource<Value> = self.client.resource(info.endpoint.clone());
        let page = resource.list(Some(query)).await?;
        let item = page
            .results
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("{} not found for key {}", type_name, key_string(key)))?;
        item.get("id")
            .and_then(Value::as_str)
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("{} lookup missing id", type_name))
    }

    async fn apply_projection_patch(
        &self,
        type_name: &TypeName,
        info: &super::registry::ObjectTypeInfo,
        backend_id: &str,
        projection: &ProjectionData,
    ) -> Result<()> {
        if projection.custom_fields.is_none()
            && projection.tags.is_none()
            && projection.local_context.is_none()
        {
            return Ok(());
        }

        let mut body = Map::new();

        if let Some(custom_fields) = &projection.custom_fields {
            if !supports_feature(&info.features, &["custom-fields", "custom_fields"]) {
                return Err(anyhow!("{} does not support custom_fields", type_name));
            }
            body.insert(
                "_custom_field_data".to_string(),
                Value::Object(custom_fields.clone().into_iter().collect()),
            );
        }

        if let Some(tags) = &projection.tags {
            if !supports_feature(&info.features, &["tags"]) {
                return Err(anyhow!("{} does not support tags", type_name));
            }
            let inputs = build_tag_inputs(tags);
            body.insert("tags".to_string(), serde_json::to_value(inputs)?);
        }

        if let Some(local_context) = &projection.local_context {
            if !supports_feature(&info.features, &["config-context", "local-context"]) {
                return Err(anyhow!("{} does not support local_context", type_name));
            }
            body.insert("local_context_data".to_string(), local_context.clone());
        }

        if body.is_empty() {
            return Ok(());
        }

        let resource: Resource<Value> = self.client.resource(info.endpoint.clone());
        let _ = resource.patch(backend_id, &Value::Object(body)).await?;
        Ok(())
    }
}

fn extract_attrs(value: Value) -> Result<(String, JsonMap)> {
    let Value::Object(mut map) = value else {
        return Err(anyhow!("expected object payload"));
    };
    let backend_id = map
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing id in payload"))?
        .to_string();
    map.remove("id");
    map.remove("url");
    map.remove("display");
    let attrs = map.into_iter().collect::<BTreeMap<_, _>>().into();
    Ok((backend_id, attrs))
}

fn extract_projection(attrs: &mut JsonMap) -> ProjectionData {
    let custom_fields = attrs
        .remove("_custom_field_data")
        .and_then(|value| match value {
            Value::Object(map) => Some(map.into_iter().collect()),
            _ => None,
        });
    // Fallback or cleanup if custom_fields is also present (NetBox compatibility)
    if custom_fields.is_none() {
        let _ = attrs.remove("custom_fields");
    }

    let tags = attrs.remove("tags").and_then(parse_tags);
    let local_context = attrs.remove("local_context_data");

    ProjectionData {
        custom_fields,
        tags,
        local_context,
    }
}

fn parse_tags(value: Value) -> Option<Vec<String>> {
    match value {
        Value::Array(items) => {
            let mut tags = Vec::new();
            for item in items {
                match item {
                    Value::String(name) => tags.push(name),
                    Value::Object(map) => {
                        if let Some(Value::String(name)) = map.get("name") {
                            tags.push(name.clone());
                        }
                    }
                    _ => {}
                }
            }
            tags.sort();
            Some(tags)
        }
        _ => None,
    }
}

fn normalize_attrs(
    attrs: &mut JsonMap,
    type_schema: &TypeSchema,
    schema: &Schema,
    registry: &ObjectTypeRegistry,
    mappings: &super::state::StateMappings,
) {
    let keys: Vec<String> = attrs.keys().cloned().collect();
    for key in keys {
        if let Some(value) = attrs.get(&key).cloned() {
            // Look up the field's target type from the schema
            let target_hint = type_schema
                .fields
                .get(&key)
                .map(|fs| &fs.r#type)
                .and_then(|ft| match ft {
                    FieldType::Ref { target } => Some(target.as_str()),
                    FieldType::ListRef { target } => Some(target.as_str()),
                    _ => None,
                });
            let normalized = normalize_value(value, target_hint, schema, registry, mappings);
            attrs.insert(key, normalized);
        }
    }
    if attrs.contains_key("type") && !attrs.contains_key("if_type") {
        if let Some(value) = attrs.remove("type") {
            attrs.insert("if_type".to_string(), value);
        }
    }
    if let (Some(Value::String(kind)), Some(id_value)) = (
        attrs.remove("assigned_object_type"),
        attrs.remove("assigned_object_id"),
    ) {
        if kind == "dcim.interface" {
            // Nautobot: assigned_object_id is UUID string
            if let Some(str_val) = as_string(&id_value) {
                if let Some(uid) = mappings.uid_for("dcim.interface", &str_val) {
                    attrs.insert(
                        "assigned_interface".to_string(),
                        Value::String(uid.to_string()),
                    );
                }
            }
        }
    }
    if let (Some(Value::String(scope)), Some(id_value)) =
        (attrs.remove("scope_type"), attrs.remove("scope_id"))
    {
        if scope == "dcim.site" {
            if let Some(str_val) = as_string(&id_value) {
                if let Some(uid) = mappings.uid_for("dcim.site", &str_val) {
                    attrs.insert("site".to_string(), Value::String(uid.to_string()));
                }
            }
        }
    }
}

fn normalize_value(
    value: Value,
    target_hint: Option<&str>,
    schema: &Schema,
    registry: &ObjectTypeRegistry,
    mappings: &super::state::StateMappings,
) -> Value {
    match value {
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(|item| normalize_value(item, target_hint, schema, registry, mappings))
                .collect(),
        ),
        Value::Object(map) => {
            if let Some(id) = map.get("id").and_then(Value::as_str) {
                // First, try existing approach: lookup via URL + mappings
                if let Some(uid) = uid_for_nested_object(&map, registry, mappings) {
                    return Value::String(uid.to_string());
                }
                // If we know the target type from schema, try to generate UID from key fields
                if let Some(target) = target_hint {
                    if let Some(uid) = uid_from_key_fields(&map, target, schema, registry, mappings)
                    {
                        return Value::String(uid.to_string());
                    }
                }
                // If it looks like a resource summary but isn't managed by us,
                // fall back to the ID string to match desired state UUIDs.
                if map.contains_key("url") || map.contains_key("object_type") {
                    return Value::String(id.to_string());
                }
            }
            if let Some(value) = map.get("value").and_then(Value::as_str) {
                let label_only = map.keys().all(|key| key == "value" || key == "label");
                if label_only {
                    return Value::String(value.to_string());
                }
            }
            // Recurse into nested objects without a target hint
            let mut normalized = Map::new();
            for (key, value) in map {
                normalized.insert(
                    key,
                    normalize_value(value, None, schema, registry, mappings),
                );
            }
            Value::Object(normalized)
        }
        other => other,
    }
}

fn as_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

fn uid_for_nested_object(
    map: &Map<String, Value>,
    registry: &ObjectTypeRegistry,
    mappings: &super::state::StateMappings,
) -> Option<Uid> {
    let id = map.get("id")?.as_str()?;
    let endpoint = map
        .get("url")
        .and_then(Value::as_str)
        .and_then(|url| registry.type_name_for_endpoint(url))?;
    mappings.uid_for(endpoint, id)
}

/// Generate a UID from key fields when we know the target type but the object isn't in mappings.
/// This handles the case where nested objects don't have URLs but we know the target type from schema.
fn uid_from_key_fields(
    map: &Map<String, Value>,
    target: &str,
    schema: &Schema,
    registry: &ObjectTypeRegistry,
    mappings: &super::state::StateMappings,
) -> Option<Uid> {
    // First, try to determine type from URL if available and use mappings
    if let Some(type_from_url) = map
        .get("url")
        .and_then(Value::as_str)
        .and_then(|url| registry.type_name_for_endpoint(url))
    {
        if let Some(id) = map.get("id").and_then(Value::as_str) {
            if let Some(uid) = mappings.uid_for(type_from_url, id) {
                return Some(uid);
            }
        }
    }

    // Get the target type's schema to find its key fields
    let target_schema = schema.types.get(target)?;

    // Build a key from available fields
    let mut key_map = BTreeMap::new();
    for key_field in target_schema.key.keys() {
        let value = map.get(key_field)?;
        key_map.insert(key_field.clone(), value.clone());
    }

    // Generate deterministic UID from type name and key
    let key = Key::from(key_map);
    Some(uid_v5(target, &key_string(&key)))
}

fn build_key_from_schema(type_schema: &TypeSchema, attrs: &JsonMap) -> Result<Key> {
    let mut map = BTreeMap::new();
    for field in type_schema.key.keys() {
        let Some(value) = attrs.get(field) else {
            return Err(anyhow!("missing key field {field}"));
        };
        map.insert(field.clone(), value.clone());
    }
    Ok(Key::from(map))
}

fn build_request_body(
    type_schema: &TypeSchema,
    attrs: &JsonMap,
    resolved: &BTreeMap<Uid, String>,
) -> Result<Value> {
    let mut map = Map::new();
    for (key, value) in attrs.iter() {
        let field_schema = type_schema
            .fields
            .get(key)
            .ok_or_else(|| anyhow!("missing schema for field {key}"))?;
        if value.is_null() {
            map.insert(key.clone(), Value::Null);
            continue;
        }
        map.insert(
            key.clone(),
            resolve_value_for_type(&field_schema.r#type, value.clone(), resolved)?,
        );
    }
    Ok(Value::Object(map))
}

fn resolve_value_for_type(
    field_type: &alembic_core::FieldType,
    value: Value,
    resolved: &BTreeMap<Uid, String>,
) -> Result<Value> {
    match field_type {
        alembic_core::FieldType::Ref { .. } => resolve_ref_value(value, resolved),
        alembic_core::FieldType::ListRef { .. } => resolve_list_ref_value(value, resolved),
        alembic_core::FieldType::List { item } => resolve_list_value(item, value, resolved),
        alembic_core::FieldType::Map { value: inner } => resolve_map_value(inner, value, resolved),
        _ => Ok(value),
    }
}

fn resolve_ref_value(value: Value, resolved: &BTreeMap<Uid, String>) -> Result<Value> {
    let Value::String(raw) = value else {
        return Err(anyhow!("ref value must be a uuid string"));
    };
    let uid = Uid::parse_str(&raw).map_err(|_| anyhow!("ref value is not a uuid: {raw}"))?;
    let id = resolved
        .get(&uid)
        .cloned()
        .ok_or_else(|| anyhow!("missing referenced uid {uid}"))?;
    Ok(Value::String(id))
}

fn resolve_list_ref_value(value: Value, resolved: &BTreeMap<Uid, String>) -> Result<Value> {
    let Value::Array(items) = value else {
        return Err(anyhow!("list_ref value must be an array"));
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        out.push(resolve_ref_value(item, resolved)?);
    }
    Ok(Value::Array(out))
}

fn resolve_list_value(
    item_type: &alembic_core::FieldType,
    value: Value,
    resolved: &BTreeMap<Uid, String>,
) -> Result<Value> {
    let Value::Array(items) = value else {
        return Err(anyhow!("list value must be an array"));
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        out.push(resolve_value_for_type(item_type, item, resolved)?);
    }
    Ok(Value::Array(out))
}

fn resolve_map_value(
    value_type: &alembic_core::FieldType,
    value: Value,
    resolved: &BTreeMap<Uid, String>,
) -> Result<Value> {
    let Value::Object(map) = value else {
        return Err(anyhow!("map value must be an object"));
    };
    let mut out = Map::new();
    for (key, value) in map {
        out.insert(key, resolve_value_for_type(value_type, value, resolved)?);
    }
    Ok(Value::Object(out))
}

fn query_from_key(
    type_schema: &TypeSchema,
    key: &Key,
    resolved: &BTreeMap<Uid, String>,
) -> Result<QueryBuilder> {
    let mut query = QueryBuilder::new();
    for (field, value) in key.iter() {
        let field_schema = type_schema
            .key
            .get(field)
            .ok_or_else(|| anyhow!("missing schema for key field {field}"))?;
        query = add_query_filters(query, field, &field_schema.r#type, value, resolved)?;
    }
    Ok(query)
}

fn add_query_filters(
    mut query: QueryBuilder,
    field: &str,
    field_type: &alembic_core::FieldType,
    value: &Value,
    resolved: &BTreeMap<Uid, String>,
) -> Result<QueryBuilder> {
    match field_type {
        alembic_core::FieldType::Ref { .. } => {
            let id = resolve_query_ref(value, resolved)?;
            Ok(query.filter(field, id))
        }
        alembic_core::FieldType::ListRef { .. } => {
            let Value::Array(items) = value else {
                return Err(anyhow!("key field {field} must be an array"));
            };
            for item in items {
                let id = resolve_query_ref(item, resolved)?;
                query = query.filter(field, id);
            }
            Ok(query)
        }
        _ => {
            let scalar = value_to_query_value(value)?;
            Ok(query.filter(field, scalar))
        }
    }
}

fn resolve_query_ref(value: &Value, resolved: &BTreeMap<Uid, String>) -> Result<String> {
    let Value::String(raw) = value else {
        return Err(anyhow!("ref value must be a uuid string"));
    };
    let uid = Uid::parse_str(raw).map_err(|_| anyhow!("ref value is not a uuid: {raw}"))?;
    let id = resolved
        .get(&uid)
        .cloned()
        .ok_or_else(|| anyhow!("missing referenced uid {uid}"))?;
    Ok(id)
}

fn value_to_query_value(value: &Value) -> Result<String> {
    match value {
        Value::String(raw) => Ok(raw.clone()),
        Value::Number(num) => Ok(num.to_string()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Null => Err(anyhow!("key value is null")),
        Value::Array(_) | Value::Object(_) => Err(anyhow!("key value must be scalar")),
    }
}

fn supports_feature(features: &BTreeSet<String>, candidates: &[&str]) -> bool {
    candidates.iter().any(|name| features.contains(*name))
}

fn is_missing_ref_error(err: &anyhow::Error) -> bool {
    let msg = err.to_string();
    msg.contains("missing referenced uid") || msg.contains("Related object not found")
}

fn is_404_error(err: &nautobot::Error) -> bool {
    err.to_string().contains("status 404")
}

fn describe_missing_refs(ops: &[Op], resolved: &BTreeMap<Uid, String>) -> String {
    let mut missing = BTreeSet::new();
    for op in ops {
        if let Op::Create { desired, .. } | Op::Update { desired, .. } = op {
            for value in desired.base.attrs.values() {
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
    value: &Value,
    resolved: &BTreeMap<Uid, String>,
    missing: &mut BTreeSet<Uid>,
) {
    match value {
        Value::String(raw) => {
            if let Ok(uid) = Uid::parse_str(raw) {
                if !resolved.contains_key(&uid) {
                    missing.insert(uid);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_missing_refs(item, resolved, missing);
            }
        }
        Value::Object(map) => {
            for value in map.values() {
                collect_missing_refs(value, resolved, missing);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alembic_core::FieldSchema;
    use serde_json::json;

    #[test]
    fn test_normalize_value_nautobot() {
        let registry = ObjectTypeRegistry::default();
        let mappings = super::super::state::StateMappings::default();
        let schema = Schema {
            types: BTreeMap::new(),
        };

        // Test summary object to UUID string normalization
        let summary = json!({
            "id": "6f7f1c2c-2b9a-4f5b-a187-2d757fe48abd",
            "url": "http://localhost/api/extras/statuses/6f7f1c2c-2b9a-4f5b-a187-2d757fe48abd/",
            "display": "Active"
        });
        let normalized = normalize_value(summary, None, &schema, &registry, &mappings);
        assert_eq!(normalized, json!("6f7f1c2c-2b9a-4f5b-a187-2d757fe48abd"));

        // Test simple value map normalization
        let choice = json!({
            "value": "active",
            "label": "Active"
        });
        let normalized = normalize_value(choice, None, &schema, &registry, &mappings);
        assert_eq!(normalized, json!("active"));
    }

    #[test]
    fn test_normalize_attrs_nautobot() {
        let registry = ObjectTypeRegistry::default();
        let mappings = super::super::state::StateMappings::default();
        let type_schema = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::new(),
        };
        let schema = Schema {
            types: BTreeMap::new(),
        };
        let mut attrs = JsonMap::default();
        attrs.insert("type".to_string(), json!("1000base-t"));

        normalize_attrs(&mut attrs, &type_schema, &schema, &registry, &mappings);
        assert_eq!(attrs.get("if_type").unwrap(), &json!("1000base-t"));
        assert!(!attrs.contains_key("type"));
    }

    #[test]
    fn test_uid_from_key_fields() {
        let registry = ObjectTypeRegistry::default();
        let mappings = super::super::state::StateMappings::default();

        // Build a schema with a type that has "name" as the key field
        let mut schema = Schema {
            types: BTreeMap::new(),
        };
        let mut type_schema = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::new(),
        };
        type_schema.key.insert(
            "name".to_string(),
            FieldSchema {
                r#type: FieldType::String,
                required: true,
                nullable: false,
                description: None,
                format: None,
                pattern: None,
            },
        );
        schema.types.insert("dcim.device".to_string(), type_schema);

        // Nested object without URL but with key field
        let nested = serde_json::Map::from_iter([
            ("id".to_string(), json!("some-uuid")),
            ("name".to_string(), json!("router-01")),
        ]);

        let uid = uid_from_key_fields(&nested, "dcim.device", &schema, &registry, &mappings);
        assert!(uid.is_some());

        // The UID should be deterministic: same inputs = same output
        let uid2 = uid_from_key_fields(&nested, "dcim.device", &schema, &registry, &mappings);
        assert_eq!(uid, uid2);

        // Different key value should produce different UID
        let nested2 = serde_json::Map::from_iter([
            ("id".to_string(), json!("other-uuid")),
            ("name".to_string(), json!("router-02")),
        ]);
        let uid3 = uid_from_key_fields(&nested2, "dcim.device", &schema, &registry, &mappings);
        assert!(uid3.is_some());
        assert_ne!(uid, uid3);
    }

    #[test]
    fn test_extract_projection_nautobot() {
        let mut attrs = JsonMap::default();
        attrs.insert("_custom_field_data".to_string(), json!({"fabric": "fra1"}));
        attrs.insert("tags".to_string(), json!(["tag1"]));

        let projection = extract_projection(&mut attrs);
        assert_eq!(
            projection.custom_fields.unwrap().get("fabric").unwrap(),
            &json!("fra1")
        );
        assert_eq!(projection.tags.unwrap(), vec!["tag1"]);
        assert!(!attrs.contains_key("_custom_field_data"));
        assert!(!attrs.contains_key("tags"));
    }

    #[test]
    fn test_build_key_from_schema() {
        let mut types = BTreeMap::new();
        types.insert(
            "name".to_string(),
            FieldSchema {
                r#type: alembic_core::FieldType::String,
                required: true,
                nullable: false,
                description: None,
                format: None,
                pattern: None,
            },
        );
        let type_schema = TypeSchema {
            key: types,
            fields: BTreeMap::new(),
        };
        let mut attrs = JsonMap::default();
        attrs.insert("name".to_string(), json!("leaf01"));

        let key = build_key_from_schema(&type_schema, &attrs).unwrap();
        assert_eq!(key.get("name").unwrap(), &json!("leaf01"));
    }

    #[test]
    fn test_build_request_body() {
        let mut fields = BTreeMap::new();
        fields.insert(
            "site".to_string(),
            FieldSchema {
                r#type: alembic_core::FieldType::Ref {
                    target: "dcim.site".to_string(),
                },
                required: true,
                nullable: false,
                description: None,
                format: None,
                pattern: None,
            },
        );
        let type_schema = TypeSchema {
            key: BTreeMap::new(),
            fields,
        };
        let mut attrs = JsonMap::default();
        let site_uid = Uid::from_u128(1);
        attrs.insert("site".to_string(), json!(site_uid.to_string()));

        let mut resolved = BTreeMap::new();
        resolved.insert(site_uid, "site-uuid".to_string());

        let body = build_request_body(&type_schema, &attrs, &resolved).unwrap();
        assert_eq!(body.get("site").unwrap(), &json!("site-uuid"));
    }

    #[test]
    fn test_resolve_value_for_type() {
        let resolved = BTreeMap::from([(Uid::from_u128(1), "uuid-1".to_string())]);

        // Ref
        let val = resolve_value_for_type(
            &alembic_core::FieldType::Ref {
                target: "t".to_string(),
            },
            json!(Uid::from_u128(1).to_string()),
            &resolved,
        )
        .unwrap();
        assert_eq!(val, json!("uuid-1"));

        // ListRef
        let val = resolve_value_for_type(
            &alembic_core::FieldType::ListRef {
                target: "t".to_string(),
            },
            json!([Uid::from_u128(1).to_string()]),
            &resolved,
        )
        .unwrap();
        assert_eq!(val, json!(["uuid-1"]));

        // List
        let val = resolve_value_for_type(
            &alembic_core::FieldType::List {
                item: Box::new(alembic_core::FieldType::String),
            },
            json!(["a"]),
            &resolved,
        )
        .unwrap();
        assert_eq!(val, json!(["a"]));
    }

    #[test]
    fn test_supports_feature() {
        let mut features = BTreeSet::new();
        features.insert("tags".to_string());
        assert!(supports_feature(&features, &["tags"]));
        assert!(!supports_feature(&features, &["custom-fields"]));
    }

    #[test]
    fn test_is_missing_ref_error() {
        assert!(is_missing_ref_error(&anyhow!("missing referenced uid 123")));
        assert!(is_missing_ref_error(&anyhow!(
            "Related object not found using the provided attributes"
        )));
        assert!(!is_missing_ref_error(&anyhow!("other error")));
    }

    #[test]
    fn test_query_from_key() {
        let mut key_fields = BTreeMap::new();
        key_fields.insert(
            "name".to_string(),
            FieldSchema {
                r#type: alembic_core::FieldType::String,
                required: true,
                nullable: false,
                description: None,
                format: None,
                pattern: None,
            },
        );
        key_fields.insert(
            "site".to_string(),
            FieldSchema {
                r#type: alembic_core::FieldType::Ref {
                    target: "dcim.site".to_string(),
                },
                required: true,
                nullable: false,
                description: None,
                format: None,
                pattern: None,
            },
        );
        let type_schema = TypeSchema {
            key: key_fields,
            fields: BTreeMap::new(),
        };

        let site_uid = Uid::from_u128(1);
        let mut key_map = BTreeMap::new();
        key_map.insert("name".to_string(), json!("leaf01"));
        key_map.insert("site".to_string(), json!(site_uid.to_string()));
        let key = Key::from(key_map);

        let mut resolved = BTreeMap::new();
        resolved.insert(site_uid, "site-uuid".to_string());

        let query = query_from_key(&type_schema, &key, &resolved).unwrap();
        let json = serde_json::to_value(&query).unwrap();
        let pairs = json.as_array().unwrap();
        assert_eq!(pairs.len(), 2);
        assert!(pairs.iter().any(|p| p == &json!(["name", "leaf01"])));
        assert!(pairs.iter().any(|p| p == &json!(["site", "site-uuid"])));
    }

    #[test]
    fn test_normalize_value_complex() {
        let registry = ObjectTypeRegistry::default();
        let mappings = super::super::state::StateMappings::default();
        let schema = Schema {
            types: BTreeMap::new(),
        };

        // Test array of summary objects
        let input = json!([
            {"id": "uuid-1", "url": "/api/t/1/", "display": "D1"},
            {"id": "uuid-2", "url": "/api/t/2/", "display": "D2"}
        ]);
        let normalized = normalize_value(input, None, &schema, &registry, &mappings);
        assert_eq!(normalized, json!(["uuid-1", "uuid-2"]));
    }
}
