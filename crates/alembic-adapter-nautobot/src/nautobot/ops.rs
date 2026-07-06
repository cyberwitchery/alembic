use super::mapping::{build_tag_inputs, custom_field_type_for_schema, slugify, tags_from_value};
use super::registry::ObjectTypeRegistry;
use super::state::{resolved_from_state, state_mappings};
use super::NautobotAdapter;
use alembic_core::{
    key_string, uid_v5, FieldSchema, FieldType, JsonMap, Key, Schema, TypeName, TypeSchema, Uid,
};
use alembic_engine::{
    apply_non_delete_journaled, build_key_from_schema, describe_missing_refs, is_missing_ref_error,
    query_filters_from_key, Adapter, AppliedOp, ApplyReport, BackendId, Emitter, ObservedObject,
    ObservedState, Observer, Op, ProvisionReport, RetryApplyDriver,
};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use nautobot::{QueryBuilder, Resource};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[async_trait]
impl Observer for NautobotAdapter {
    async fn read(
        &self,
        schema: &Schema,
        types: &[TypeName],
        state_store: &alembic_engine::StateStore,
    ) -> Result<ObservedState> {
        let registry: ObjectTypeRegistry = self.client.fetch_object_types().await?;
        let mappings = state_mappings(state_store);

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
                    normalize_attrs(&mut attrs, &type_schema, &schema, &registry, &mappings);
                    let key = build_key_from_schema(&type_schema, &attrs)
                        .with_context(|| format!("build key for {}", type_name))?;
                    observed.push(ObservedObject {
                        type_name: type_name.clone(),
                        key,
                        attrs,
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
                state.insert(object)?;
            }
        }

        Ok(state)
    }
}

#[async_trait]
impl Emitter for NautobotAdapter {
    async fn write(
        &self,
        schema: &Schema,
        ops: &[Op],
        state: &alembic_engine::StateStore,
    ) -> Result<ApplyReport> {
        let registry: ObjectTypeRegistry = self.client.fetch_object_types().await?;
        let custom_fields_by_type = self.client.fetch_custom_fields().await?;
        let mut applied = Vec::new();
        let mut resolved = resolved_from_state(state);

        for op in ops {
            if let Op::Create { uid, .. } = op {
                resolved.remove(uid);
            }
        }

        let tag_names = collect_tag_names(ops, &registry)?;
        if !tag_names.is_empty() {
            let mut existing = self.client.fetch_tags().await?;
            let missing: Vec<String> = tag_names.difference(&existing).cloned().collect();
            if !missing.is_empty() {
                self.create_tags(&missing).await?;
                for tag in missing {
                    existing.insert(tag);
                }
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

        struct ApplyDriver<'a> {
            adapter: &'a NautobotAdapter,
            resolved: &'a mut BTreeMap<Uid, String>,
            registry: &'a ObjectTypeRegistry,
            schema: &'a Schema,
            custom_fields_by_type: &'a BTreeMap<String, BTreeSet<String>>,
        }

        #[async_trait]
        impl RetryApplyDriver for ApplyDriver<'_> {
            async fn apply_non_delete(&mut self, op: &Op) -> Result<AppliedOp> {
                match op {
                    Op::Create { .. } => self
                        .adapter
                        .apply_create(
                            op,
                            self.resolved,
                            self.registry,
                            self.schema,
                            self.custom_fields_by_type,
                        )
                        .await
                        .map(|backend_id| AppliedOp {
                            uid: op.uid(),
                            type_name: op.type_name().clone(),
                            backend_id: Some(BackendId::String(backend_id)),
                        }),
                    Op::Update { .. } => self
                        .adapter
                        .apply_update(
                            op,
                            self.resolved,
                            self.registry,
                            self.schema,
                            self.custom_fields_by_type,
                        )
                        .await
                        .map(|backend_id| AppliedOp {
                            uid: op.uid(),
                            type_name: op.type_name().clone(),
                            backend_id: Some(BackendId::String(backend_id)),
                        }),
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
            registry: &registry,
            schema,
            custom_fields_by_type: &custom_fields_by_type,
        };
        let (retry_result, previously_applied_count) =
            apply_non_delete_journaled(state, "nautobot", &creates_updates, &mut driver).await?;

        if !retry_result.pending.is_empty() {
            let missing = describe_missing_refs(&retry_result.pending, &resolved);
            return Err(anyhow!("unresolved references: {missing}"));
        }

        for applied_op in retry_result.applied {
            if let Some(BackendId::String(backend_id)) = &applied_op.backend_id {
                resolved.insert(applied_op.uid, backend_id.clone());
            }
            applied.push(applied_op);
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
                        tracing::warn!(type_name = %type_name, "object already deleted");
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

        Ok(ApplyReport {
            applied,
            previously_applied_count,
            ..Default::default()
        })
    }
}

#[async_trait]
impl Adapter for NautobotAdapter {
    async fn ensure_schema(&self, schema: &Schema) -> Result<ProvisionReport> {
        let mut created_fields = Vec::new();
        for (type_name, field_name, field_schema) in self.missing_custom_fields(schema).await? {
            if self
                .create_custom_field(&type_name, field_name, field_schema)
                .await?
            {
                created_fields.push(format!("{type_name}.{field_name}"));
            }
        }
        Ok(ProvisionReport {
            created_fields,
            ..Default::default()
        })
    }

    async fn preview_schema(&self, schema: &Schema) -> Result<Option<ProvisionReport>> {
        let created_fields = self
            .missing_custom_fields(schema)
            .await?
            .into_iter()
            .map(|(type_name, field_name, _)| format!("{type_name}.{field_name}"))
            .collect();
        Ok(Some(ProvisionReport {
            created_fields,
            ..Default::default()
        }))
    }
}

impl NautobotAdapter {
    /// read the live schema and compute which declared custom fields the backend
    /// lacks. read-only: shared by `ensure_schema` (which then creates them) and
    /// `preview_schema` (which only reports them), so the decision never drifts
    /// between preview and apply.
    async fn missing_custom_fields<'a>(
        &self,
        schema: &'a Schema,
    ) -> Result<Vec<(TypeName, &'a String, &'a FieldSchema)>> {
        let registry: ObjectTypeRegistry = self.client.fetch_object_types().await?;
        let custom_fields_by_type = self.client.fetch_custom_fields().await?;
        let mut missing = Vec::new();

        for (type_name, type_schema) in &schema.types {
            let type_name = TypeName::new(type_name);
            let info = registry
                .info_for(&type_name)
                .ok_or_else(|| anyhow!("unsupported type {}", type_name))?;
            if !supports_feature(&info.features, &["custom-fields"]) {
                continue;
            }

            let native_fields = native_fields_for_type(self, &info, type_schema).await?;
            let existing = custom_fields_by_type
                .get(type_name.as_str())
                .cloned()
                .unwrap_or_default();

            for (field_name, field_schema) in &type_schema.fields {
                if matches!(
                    field_schema.r#type,
                    FieldType::Ref { .. } | FieldType::ListRef { .. }
                ) {
                    continue;
                }
                if native_fields.contains(field_name) || existing.contains(field_name) {
                    continue;
                }
                missing.push((type_name.clone(), field_name, field_schema));
            }
        }

        Ok(missing)
    }

    async fn apply_create(
        &self,
        op: &Op,
        resolved: &mut BTreeMap<Uid, String>,
        registry: &ObjectTypeRegistry,
        schema: &Schema,
        custom_fields_by_type: &BTreeMap<String, BTreeSet<String>>,
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
        let custom_fields = custom_fields_by_type
            .get(info.type_name.as_str())
            .cloned()
            .unwrap_or_default();
        let body = build_request_body(
            type_name,
            type_schema,
            &desired.attrs,
            resolved,
            &custom_fields,
            &info.features,
        )?;
        let response: Value = match resource.create(&body).await {
            Ok(response) => response,
            Err(err) if is_conflict_error(&err) => {
                if let Ok(existing) = self
                    .lookup_backend_id(type_name, &info, type_schema, &desired.key, resolved)
                    .await
                {
                    tracing::warn!(
                        type_name = %type_name,
                        key = %key_string(&desired.key),
                        "create already exists; using existing object"
                    );
                    resolved.insert(uid, existing.clone());
                    return Ok(existing);
                }
                return Err(err.into());
            }
            Err(err) => return Err(err.into()),
        };
        let backend_id = response
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("create {} returned no id", type_name))?
            .to_string();
        resolved.insert(uid, backend_id.clone());
        Ok(backend_id)
    }

    async fn apply_update(
        &self,
        op: &Op,
        resolved: &BTreeMap<Uid, String>,
        registry: &ObjectTypeRegistry,
        schema: &Schema,
        custom_fields_by_type: &BTreeMap<String, BTreeSet<String>>,
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
            self.lookup_backend_id(type_name, &info, type_schema, &desired.key, resolved)
                .await
                .with_context(|| format!("resolve backend id for {}", type_name))?
        };
        let resource: Resource<Value> = self.client.resource(info.endpoint.clone());
        let custom_fields = custom_fields_by_type
            .get(info.type_name.as_str())
            .cloned()
            .unwrap_or_default();
        let body = build_request_body(
            type_name,
            type_schema,
            &desired.attrs,
            resolved,
            &custom_fields,
            &info.features,
        )?;
        let _response = resource.patch(&id, &body).await?;
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

    async fn create_tags(&self, tags: &[String]) -> Result<()> {
        let resource = self.client.extras().tags();
        for tag in tags {
            let payload = serde_json::json!({
                "name": tag,
                "slug": slugify(tag),
            });
            if let Err(err) = resource.create(&payload).await {
                let existing = self.client.fetch_tags().await?;
                if existing.contains(tag) {
                    tracing::warn!(tag = %tag, "tag already exists");
                    continue;
                }
                return Err(err.into());
            }
        }
        Ok(())
    }

    async fn create_custom_field(
        &self,
        type_name: &TypeName,
        field_name: &str,
        field_schema: &FieldSchema,
    ) -> Result<bool> {
        let field_type = custom_field_type_for_schema(field_schema);
        let mut payload = Map::new();
        // nautobot 2.x identifies a custom field by `key`, not `name`: the writable
        // serializer has no `name`, so sending it lets nautobot derive `key` from
        // `label` (slugified), and a non-slug field name then never matches the
        // `field.key` the read/detect/write paths key on. set `key` = field name.
        payload.insert("key".to_string(), Value::String(field_name.to_string()));
        payload.insert("label".to_string(), Value::String(field_name.to_string()));
        payload.insert("type".to_string(), Value::String(field_type));
        payload.insert(
            "content_types".to_string(),
            Value::Array(vec![Value::String(type_name.as_str().to_string())]),
        );
        if field_schema.required {
            payload.insert("required".to_string(), Value::Bool(true));
        }
        if let Some(description) = &field_schema.description {
            payload.insert(
                "description".to_string(),
                Value::String(description.clone()),
            );
        }
        let resource = self.client.extras().custom_fields();
        match resource.create(&Value::Object(payload)).await {
            Ok(_) => Ok(true),
            Err(err) => {
                let existing = self.client.fetch_custom_fields().await?;
                if existing
                    .get(type_name.as_str())
                    .is_some_and(|fields| fields.contains(field_name))
                {
                    tracing::warn!(
                        type_name = %type_name,
                        field = %field_name,
                        "custom field already exists"
                    );
                    Ok(false)
                } else {
                    Err(err.into())
                }
            }
        }
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
    let custom_fields = map.remove("_custom_field_data");
    let tags = map.remove("tags");
    map.remove("id");
    map.remove("url");
    map.remove("display");
    let mut attrs: JsonMap = map.into_iter().collect::<BTreeMap<_, _>>().into();
    if let Some(Value::Object(fields)) = custom_fields {
        for (key, value) in fields {
            attrs.entry(key).or_insert(value);
        }
    }
    if let Some(tags_value) = tags {
        let tags = tags_from_value(&tags_value)?;
        attrs.insert(
            "tags".to_string(),
            Value::Array(tags.into_iter().map(Value::String).collect()),
        );
    }
    Ok((backend_id, attrs))
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
                // lookup via URL + mappings
                if let Some(uid) = uid_for_nested_object(&map, registry, mappings) {
                    return Value::String(uid.to_string());
                }
                // if we know the target type from schema, try to generate UID from key fields
                if let Some(target) = target_hint {
                    if let Some(uid) = uid_from_key_fields(&map, target, schema, registry, mappings)
                    {
                        return Value::String(uid.to_string());
                    }
                }
                // if it looks like a resource summary but isn't managed by us,
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
            // recurse into nested objects without a target hint
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

/// generate a UID from key fields when we know the target type but the object isn't in mappings.
/// this handles the case where nested objects don't have URLs but we know the target type from schema.
fn uid_from_key_fields(
    map: &Map<String, Value>,
    target: &str,
    schema: &Schema,
    registry: &ObjectTypeRegistry,
    mappings: &super::state::StateMappings,
) -> Option<Uid> {
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

    let target_schema = schema.types.get(target)?;

    // a ref-typed key field (e.g. an interface keyed by `(device, name)`)
    // arrives as a nested brief, so resolve it to the referent's uid first,
    // mirroring how the referent itself is keyed.
    let mut key_map = BTreeMap::new();
    for (key_field, field_schema) in &target_schema.key {
        let value = map.get(key_field)?;
        let resolved = match &field_schema.r#type {
            FieldType::Ref { target: ref_target } | FieldType::ListRef { target: ref_target } => {
                match value {
                    Value::Object(brief) => {
                        uid_from_key_fields(brief, ref_target, schema, registry, mappings)
                            .map(|uid| Value::String(uid.to_string()))
                            .unwrap_or_else(|| value.clone())
                    }
                    _ => value.clone(),
                }
            }
            _ => value.clone(),
        };
        key_map.insert(key_field.clone(), resolved);
    }

    let key = Key::from(key_map);
    Some(uid_v5(target, &key_string(&key)))
}

fn build_request_body(
    type_name: &TypeName,
    type_schema: &TypeSchema,
    attrs: &JsonMap,
    resolved: &BTreeMap<Uid, String>,
    custom_fields: &BTreeSet<String>,
    features: &BTreeSet<String>,
) -> Result<Value> {
    let mut body = Map::new();
    let mut custom = Map::new();

    for (key, value) in attrs.iter() {
        let api_key = if type_name.as_str() == "dcim.interface" && key == "if_type" {
            "type"
        } else {
            key.as_str()
        };
        if key == "tags" {
            if !supports_feature(features, &["tags"]) {
                return Err(anyhow!("{} does not support tags", type_name));
            }
            let tags = tags_from_value(value)?;
            let tag_inputs = build_tag_inputs(&tags);
            body.insert(api_key.to_string(), Value::Array(tag_inputs));
            continue;
        }

        let field_schema = type_schema
            .fields
            .get(key)
            .ok_or_else(|| anyhow!("missing schema for field {key}"))?;
        let encoded = resolve_value_for_type(&field_schema.r#type, value.clone(), resolved)?;

        if custom_fields.contains(key) {
            if !supports_feature(features, &["custom-fields"]) {
                return Err(anyhow!("{} does not support custom fields", type_name));
            }
            custom.insert(key.clone(), encoded);
        } else {
            body.insert(api_key.to_string(), encoded);
        }
    }

    if !custom.is_empty() {
        body.insert("_custom_field_data".to_string(), Value::Object(custom));
    }

    Ok(Value::Object(body))
}

fn resolve_value_for_type(
    field_type: &alembic_core::FieldType,
    value: Value,
    resolved: &BTreeMap<Uid, String>,
) -> Result<Value> {
    alembic_engine::resolve_value_for_type(field_type, value, resolved, |id| {
        Value::String(id.clone())
    })
}

fn query_from_key(
    type_schema: &TypeSchema,
    key: &Key,
    resolved: &BTreeMap<Uid, String>,
) -> Result<QueryBuilder> {
    let mut query = QueryBuilder::new();
    for (field, value) in query_filters_from_key(type_schema, key, resolved)? {
        query = query.filter(field, value);
    }
    Ok(query)
}

fn collect_tag_names(ops: &[Op], registry: &ObjectTypeRegistry) -> Result<BTreeSet<String>> {
    let mut tags = BTreeSet::new();
    for op in ops {
        let (type_name, desired) = match op {
            Op::Create {
                type_name, desired, ..
            } => (type_name, desired),
            Op::Update {
                type_name, desired, ..
            } => (type_name, desired),
            Op::Delete { .. } => continue,
        };
        if let Some(tag_value) = desired.attrs.get("tags") {
            let info = registry
                .info_for(type_name)
                .ok_or_else(|| anyhow!("unsupported type {}", type_name))?;
            if !supports_feature(&info.features, &["tags"]) {
                return Err(anyhow!("{} does not support tags", type_name));
            }
            for tag in tags_from_value(tag_value)? {
                tags.insert(tag);
            }
        }
    }
    Ok(tags)
}

async fn native_fields_for_type(
    adapter: &NautobotAdapter,
    info: &super::registry::ObjectTypeInfo,
    type_schema: &TypeSchema,
) -> Result<BTreeSet<String>> {
    let mut native: BTreeSet<String> = type_schema.key.keys().cloned().collect();
    for field in [
        "name",
        "slug",
        "description",
        "status",
        "role",
        "type",
        "site",
        "tenant",
        "device",
        "tags",
        "_custom_field_data",
        "created",
        "last_updated",
    ] {
        native.insert(field.to_string());
    }

    let resource: Resource<Value> = adapter.client.resource(info.endpoint.clone());
    let page = resource
        .list(Some(QueryBuilder::default().limit(1)))
        .await?;
    if let Some(Value::Object(map)) = page.results.into_iter().next() {
        for key in map.keys() {
            native.insert(key.clone());
        }
    }
    if info.type_name.as_str() == "dcim.interface" {
        native.insert("if_type".to_string());
    }

    Ok(native)
}

fn supports_feature(features: &BTreeSet<String>, candidates: &[&str]) -> bool {
    candidates.iter().any(|name| features.contains(*name))
}

fn is_404_error(err: &nautobot::Error) -> bool {
    err.to_string().contains("status 404")
}

fn is_conflict_error(err: &nautobot::Error) -> bool {
    match err {
        nautobot::Error::ApiError {
            status,
            message,
            body,
        } => {
            if !matches!(status, 400 | 409) {
                return false;
            }
            let message = message.to_lowercase();
            let body = body.to_lowercase();
            message.contains("already exists")
                || message.contains("unique")
                || body.contains("already exists")
                || body.contains("unique")
        }
        _ => false,
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

        // test summary object to UUID string normalization
        let summary = json!({
            "id": "6f7f1c2c-2b9a-4f5b-a187-2d757fe48abd",
            "url": "http://localhost/api/extras/statuses/6f7f1c2c-2b9a-4f5b-a187-2d757fe48abd/",
            "display": "Active"
        });
        let normalized = normalize_value(summary, None, &schema, &registry, &mappings);
        assert_eq!(normalized, json!("6f7f1c2c-2b9a-4f5b-a187-2d757fe48abd"));

        // test simple value map normalization
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

        // build a schema with a type that has "name" as the key field
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

        // nested object without URL but with key field
        let nested = serde_json::Map::from_iter([
            ("id".to_string(), json!("some-uuid")),
            ("name".to_string(), json!("router-01")),
        ]);

        let uid = uid_from_key_fields(&nested, "dcim.device", &schema, &registry, &mappings);
        assert!(uid.is_some());

        // the UID should be deterministic: same inputs = same output
        let uid2 = uid_from_key_fields(&nested, "dcim.device", &schema, &registry, &mappings);
        assert_eq!(uid, uid2);

        // different key value should produce different UID
        let nested2 = serde_json::Map::from_iter([
            ("id".to_string(), json!("other-uuid")),
            ("name".to_string(), json!("router-02")),
        ]);
        let uid3 = uid_from_key_fields(&nested2, "dcim.device", &schema, &registry, &mappings);
        assert!(uid3.is_some());
        assert_ne!(uid, uid3);
    }

    #[test]
    fn test_uid_from_key_fields_resolves_ref_key() {
        let registry = ObjectTypeRegistry::default();
        let mappings = super::super::state::StateMappings::default();

        // dcim.device is keyed by `name`; dcim.interface is keyed by both
        // `device` (a ref to dcim.device) and `name`.
        let mut schema = Schema {
            types: BTreeMap::new(),
        };

        let mut device_schema = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::new(),
        };
        device_schema.key.insert(
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
        schema
            .types
            .insert("dcim.device".to_string(), device_schema);

        let mut interface_schema = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::new(),
        };
        interface_schema.key.insert(
            "device".to_string(),
            FieldSchema {
                r#type: FieldType::Ref {
                    target: "dcim.device".to_string(),
                },
                required: true,
                nullable: false,
                description: None,
                format: None,
                pattern: None,
            },
        );
        interface_schema.key.insert(
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
        schema
            .types
            .insert("dcim.interface".to_string(), interface_schema);

        // resolve the device's own uid from its key
        let device_map = serde_json::Map::from_iter([("name".to_string(), json!("router-01"))]);
        let device_uid =
            uid_from_key_fields(&device_map, "dcim.device", &schema, &registry, &mappings)
                .expect("device uid");

        // expected: interface uid with the device key already resolved to its uid string
        let expected_map = serde_json::Map::from_iter([
            ("device".to_string(), json!(device_uid.to_string())),
            ("name".to_string(), json!("eth1")),
        ]);
        let expected = uid_from_key_fields(
            &expected_map,
            "dcim.interface",
            &schema,
            &registry,
            &mappings,
        );

        // actual: interface uid with the device key as a nested brief
        let actual_map = serde_json::Map::from_iter([
            ("device".to_string(), json!({ "name": "router-01" })),
            ("name".to_string(), json!("eth1")),
        ]);
        let actual =
            uid_from_key_fields(&actual_map, "dcim.interface", &schema, &registry, &mappings);

        // the nested device brief got resolved to the device uid
        assert!(actual.is_some());
        assert_eq!(actual, expected);

        // eth1 under a different device must not collide with eth1 here
        let other_map = serde_json::Map::from_iter([
            ("device".to_string(), json!({ "name": "router-02" })),
            ("name".to_string(), json!("eth1")),
        ]);
        let other =
            uid_from_key_fields(&other_map, "dcim.interface", &schema, &registry, &mappings);
        assert_ne!(actual, other);
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

        let body = build_request_body(
            &TypeName::new("dcim.device"),
            &type_schema,
            &attrs,
            &resolved,
            &BTreeSet::new(),
            &BTreeSet::new(),
        )
        .unwrap();
        assert_eq!(body.get("site").unwrap(), &json!("site-uuid"));
    }

    #[test]
    fn test_build_request_body_interface_if_type() {
        let mut fields = BTreeMap::new();
        fields.insert(
            "if_type".to_string(),
            FieldSchema {
                r#type: alembic_core::FieldType::String,
                required: false,
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
        attrs.insert("if_type".to_string(), json!("1000base-t"));

        let body = build_request_body(
            &TypeName::new("dcim.interface"),
            &type_schema,
            &attrs,
            &BTreeMap::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
        )
        .unwrap();

        // the schema field `if_type` is written back under the nautobot api name `type`
        assert_eq!(body.get("type"), Some(&json!("1000base-t")));
        assert!(body.get("if_type").is_none());
    }

    #[test]
    fn test_resolve_value_for_type() {
        let resolved = BTreeMap::from([(Uid::from_u128(1), "uuid-1".to_string())]);

        // ref
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

        // list
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

        // test array of summary objects
        let input = json!([
            {"id": "uuid-1", "url": "/api/t/1/", "display": "D1"},
            {"id": "uuid-2", "url": "/api/t/2/", "display": "D2"}
        ]);
        let normalized = normalize_value(input, None, &schema, &registry, &mappings);
        assert_eq!(normalized, json!(["uuid-1", "uuid-2"]));
    }

    #[test]
    fn test_conflict_error_detects_unique_message() {
        let err = nautobot::Error::ApiError {
            status: 400,
            message: "name: This field must be unique.".to_string(),
            body: String::new(),
        };
        assert!(is_conflict_error(&err));
    }
}
