use super::mapping::build_tag_inputs;
use super::registry::ObjectTypeRegistry;
use super::state::{resolved_from_state, state_mappings};
use super::NetBoxAdapter;
use alembic_core::{key_string, JsonMap, Key, Schema, TypeName, TypeSchema, Uid};
use alembic_engine::{
    Adapter, AppliedOp, ApplyReport, BackendId, ObservedObject, ObservedState, Op, ProjectionData,
    StateStore,
};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use netbox::{BulkDelete, QueryBuilder, Resource};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

#[async_trait]
impl Adapter for NetBoxAdapter {
    async fn observe(&self, schema: &Schema, types: &[TypeName]) -> Result<ObservedState> {
        let registry: ObjectTypeRegistry = self.client.fetch_object_types().await?;
        let mut state = ObservedState::default();
        let mappings = {
            let state_guard = self.state_guard()?;
            state_mappings(&state_guard)
        };

        let requested: BTreeSet<TypeName> = if types.is_empty() {
            registry.type_names().into_iter().collect()
        } else {
            types.iter().cloned().collect()
        };

        for type_name in requested {
            let info = registry
                .info_for(&type_name)
                .ok_or_else(|| anyhow!("unsupported type {}", type_name))?;
            let type_schema = schema
                .types
                .get(type_name.as_str())
                .ok_or_else(|| anyhow!("missing schema for {}", type_name))?;
            let resource: Resource<Value> = self.client.resource(info.endpoint.clone());
            let objects = self.client.list_all(&resource, None).await?;
            for object in objects {
                let (backend_id, mut attrs) = extract_attrs(object)?;
                let projection = extract_projection(&mut attrs);
                normalize_attrs(&mut attrs, &registry, &mappings);
                let key = build_key_from_schema(type_schema, &attrs)
                    .with_context(|| format!("build key for {}", type_name))?;
                state.insert(ObservedObject {
                    type_name: type_name.clone(),
                    key,
                    attrs,
                    projection,
                    backend_id: Some(BackendId::Int(backend_id)),
                });
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
                            backend_id: Some(BackendId::Int(backend_id)),
                        }),
                    Op::Update { .. } => self
                        .apply_update(&op, &resolved, &registry, schema)
                        .await
                        .map(|backend_id| AppliedOp {
                            uid: op.uid(),
                            type_name: op.type_name().clone(),
                            backend_id: Some(BackendId::Int(backend_id)),
                        }),
                    Op::Delete { .. } => continue,
                };

                match result {
                    Ok(applied_op) => {
                        if let Some(BackendId::Int(backend_id)) = applied_op.backend_id {
                            resolved.insert(applied_op.uid, backend_id);
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
                let id = if let Some(BackendId::Int(id)) = backend_id {
                    id
                } else if let Some(id) = resolved.get(&uid).copied() {
                    id
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
                let batch = [BulkDelete::new(id)];
                match resource.bulk_delete(&batch).await {
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

impl NetBoxAdapter {
    async fn apply_create(
        &self,
        op: &Op,
        resolved: &mut BTreeMap<Uid, u64>,
        registry: &ObjectTypeRegistry,
        schema: &Schema,
    ) -> Result<u64> {
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
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("create {} returned no id", type_name))?;
        resolved.insert(uid, backend_id);
        self.apply_projection_patch(type_name, &info, backend_id, &desired.projection)
            .await?;
        Ok(backend_id)
    }

    async fn apply_update(
        &self,
        op: &Op,
        resolved: &BTreeMap<Uid, u64>,
        registry: &ObjectTypeRegistry,
        schema: &Schema,
    ) -> Result<u64> {
        let (uid, type_name, desired, backend_id) = match op {
            Op::Update {
                uid,
                type_name,
                desired,
                backend_id,
                ..
            } => {
                let id = match backend_id {
                    Some(BackendId::Int(id)) => Some(*id),
                    Some(_) => return Err(anyhow!("netbox requires integer backend id")),
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
        } else if let Some(id) = resolved.get(&uid).copied() {
            id
        } else {
            self.lookup_backend_id(type_name, &info, type_schema, &desired.base.key, resolved)
                .await
                .with_context(|| format!("resolve backend id for {}", type_name))?
        };
        let resource: Resource<Value> = self.client.resource(info.endpoint.clone());
        let body = build_request_body(type_schema, &desired.base.attrs, resolved)?;
        let _response = resource.patch(id, &body).await?;
        self.apply_projection_patch(type_name, &info, id, &desired.projection)
            .await?;
        Ok(id)
    }

    async fn lookup_backend_id(
        &self,
        type_name: &TypeName,
        info: &super::registry::ObjectTypeInfo,
        type_schema: &TypeSchema,
        key: &Key,
        resolved: &BTreeMap<Uid, u64>,
    ) -> Result<u64> {
        let query = query_from_key(type_schema, key, resolved)?;
        let resource: Resource<Value> = self.client.resource(info.endpoint.clone());
        let page = resource.list(Some(query)).await?;
        let item = page
            .results
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("{} not found for key {}", type_name, key_string(key)))?;
        item.get("id")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("{} lookup missing id", type_name))
    }

    async fn apply_projection_patch(
        &self,
        type_name: &TypeName,
        info: &super::registry::ObjectTypeInfo,
        backend_id: u64,
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
                "custom_fields".to_string(),
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

fn extract_attrs(value: Value) -> Result<(u64, JsonMap)> {
    let Value::Object(mut map) = value else {
        return Err(anyhow!("expected object payload"));
    };
    let backend_id = map
        .get("id")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("missing id in payload"))?;
    map.remove("id");
    map.remove("url");
    map.remove("display");
    let attrs = map.into_iter().collect::<BTreeMap<_, _>>().into();
    Ok((backend_id, attrs))
}

fn extract_projection(attrs: &mut JsonMap) -> ProjectionData {
    let custom_fields = attrs.remove("custom_fields").and_then(|value| match value {
        Value::Object(map) => Some(map.into_iter().collect()),
        _ => None,
    });
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
    registry: &ObjectTypeRegistry,
    mappings: &super::state::StateMappings,
) {
    let keys: Vec<String> = attrs.keys().cloned().collect();
    for key in keys {
        if let Some(value) = attrs.get(&key).cloned() {
            let normalized = normalize_value(value, registry, mappings);
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
            if let Some(id) = as_u64(&id_value) {
                if let Some(uid) = mappings.uid_for("dcim.interface", id) {
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
            if let Some(id) = as_u64(&id_value) {
                if let Some(uid) = mappings.uid_for("dcim.site", id) {
                    attrs.insert("site".to_string(), Value::String(uid.to_string()));
                }
            }
        }
    }
}

fn normalize_value(
    value: Value,
    registry: &ObjectTypeRegistry,
    mappings: &super::state::StateMappings,
) -> Value {
    match value {
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(|item| normalize_value(item, registry, mappings))
                .collect(),
        ),
        Value::Object(map) => {
            if let Some(id) = map.get("id").and_then(as_u64) {
                if let Some(uid) = uid_for_nested_object(&map, registry, mappings) {
                    return Value::String(uid.to_string());
                }
                // If it looks like a resource summary but isn't managed by us,
                // fall back to the ID integer to match desired state integers.
                if map.contains_key("url") || map.contains_key("display") {
                    return Value::Number(id.into());
                }
            }
            if let Some(value) = map.get("value").and_then(Value::as_str) {
                let label_only = map.keys().all(|key| key == "value" || key == "label");
                if label_only {
                    return Value::String(value.to_string());
                }
            }
            let mut normalized = Map::new();
            for (key, value) in map {
                normalized.insert(key, normalize_value(value, registry, mappings));
            }
            Value::Object(normalized)
        }
        other => other,
    }
}

fn as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(num) => num.as_u64(),
        Value::String(raw) => raw.parse().ok(),
        _ => None,
    }
}

fn uid_for_nested_object(
    map: &Map<String, Value>,
    registry: &ObjectTypeRegistry,
    mappings: &super::state::StateMappings,
) -> Option<Uid> {
    let id = map.get("id")?.as_u64()?;
    let endpoint = map
        .get("url")
        .and_then(Value::as_str)
        .and_then(|url| registry.type_name_for_endpoint(url))?;
    mappings.uid_for(endpoint, id)
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
    resolved: &BTreeMap<Uid, u64>,
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
    resolved: &BTreeMap<Uid, u64>,
) -> Result<Value> {
    match field_type {
        alembic_core::FieldType::Ref { .. } => resolve_ref_value(value, resolved),
        alembic_core::FieldType::ListRef { .. } => resolve_list_ref_value(value, resolved),
        alembic_core::FieldType::List { item } => resolve_list_value(item, value, resolved),
        alembic_core::FieldType::Map { value: inner } => resolve_map_value(inner, value, resolved),
        _ => Ok(value),
    }
}

fn resolve_ref_value(value: Value, resolved: &BTreeMap<Uid, u64>) -> Result<Value> {
    let Value::String(raw) = value else {
        return Err(anyhow!("ref value must be a uuid string"));
    };
    let uid = Uid::parse_str(&raw).map_err(|_| anyhow!("ref value is not a uuid: {raw}"))?;
    let id = resolved
        .get(&uid)
        .copied()
        .ok_or_else(|| anyhow!("missing referenced uid {uid}"))?;
    Ok(Value::Number(id.into()))
}

fn resolve_list_ref_value(value: Value, resolved: &BTreeMap<Uid, u64>) -> Result<Value> {
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
    resolved: &BTreeMap<Uid, u64>,
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
    resolved: &BTreeMap<Uid, u64>,
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
    resolved: &BTreeMap<Uid, u64>,
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
    resolved: &BTreeMap<Uid, u64>,
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

fn resolve_query_ref(value: &Value, resolved: &BTreeMap<Uid, u64>) -> Result<String> {
    let Value::String(raw) = value else {
        return Err(anyhow!("ref value must be a uuid string"));
    };
    let uid = Uid::parse_str(raw).map_err(|_| anyhow!("ref value is not a uuid: {raw}"))?;
    let id = resolved
        .get(&uid)
        .copied()
        .ok_or_else(|| anyhow!("missing referenced uid {uid}"))?;
    Ok(id.to_string())
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
    err.to_string().contains("missing referenced uid")
}

fn is_404_error(err: &netbox::Error) -> bool {
    err.to_string().contains("status 404")
}

fn describe_missing_refs(ops: &[Op], resolved: &BTreeMap<Uid, u64>) -> String {
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

fn collect_missing_refs(value: &Value, resolved: &BTreeMap<Uid, u64>, missing: &mut BTreeSet<Uid>) {
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
