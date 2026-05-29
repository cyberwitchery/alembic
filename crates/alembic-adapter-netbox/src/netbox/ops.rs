use super::client::CustomObjectType;
use super::mapping::{build_tag_inputs, custom_field_type_for_schema, slugify, tags_from_value};
use super::registry::ObjectTypeRegistry;
use super::state::{resolved_from_state, state_mappings};
use super::NetBoxAdapter;
use alembic_core::{
    key_string, uid_v5, FieldSchema, FieldType, JsonMap, Key, Schema, TypeName, TypeSchema, Uid,
};
use alembic_engine::{
    apply_non_delete_with_retries, build_key_from_schema, query_filters_from_key, Adapter,
    AdapterApplyError, AppliedOp, ApplyReport, BackendId, ObservedObject, ObservedState, Op,
    ProvisionReport, RetryApplyDriver,
};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use netbox::{BulkDelete, QueryBuilder, Resource};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};

const CUSTOM_OBJECT_FEATURE: &str = "custom-object";
const CUSTOM_OBJECT_APP_LABEL: &str = "netbox_custom_objects";
const ALEMBIC_CUSTOM_OBJECT_PREFIX: &str = "alembic custom object for ";

#[async_trait]
impl Adapter for NetBoxAdapter {
    async fn read(
        &self,
        schema: &Schema,
        types: &[TypeName],
        state_store: &alembic_engine::StateStore,
    ) -> Result<ObservedState> {
        let registry: ObjectTypeRegistry = build_registry_for_schema(self, schema).await?;
        let mut state = ObservedState::default();
        let mappings = state_mappings(state_store);

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
            let objects = match self.client.list_all(&resource, None).await {
                Ok(objects) => objects,
                Err(err)
                    if is_404_anyhow(&err) && info.features.contains(CUSTOM_OBJECT_FEATURE) =>
                {
                    continue;
                }
                Err(err) => return Err(err),
            };
            for object in objects {
                let (backend_id, mut attrs) = extract_attrs(object)?;
                normalize_attrs(&mut attrs, type_schema, schema, &registry, &mappings);
                let key = build_key_from_schema(type_schema, &attrs)
                    .with_context(|| format!("build key for {}", type_name))?;
                state.insert(ObservedObject {
                    type_name: type_name.clone(),
                    key,
                    attrs,
                    backend_id: Some(BackendId::Int(backend_id)),
                });
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
        let registry: ObjectTypeRegistry = build_registry_for_schema(self, schema).await?;
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
            adapter: &'a NetBoxAdapter,
            resolved: &'a mut BTreeMap<Uid, u64>,
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
                            backend_id: Some(BackendId::Int(backend_id)),
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
                            backend_id: Some(BackendId::Int(backend_id)),
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
        let retry_result = apply_non_delete_with_retries(&creates_updates, &mut driver).await?;

        if !retry_result.pending.is_empty() {
            let missing = describe_missing_refs(&retry_result.pending, &resolved);
            return Err(anyhow!("unresolved references: {missing}"));
        }

        for applied_op in retry_result.applied {
            if let Some(BackendId::Int(backend_id)) = &applied_op.backend_id {
                resolved.insert(applied_op.uid, *backend_id);
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
            ..Default::default()
        })
    }

    async fn ensure_schema(&self, schema: &Schema) -> Result<ProvisionReport> {
        let mut registry: ObjectTypeRegistry = self.client.fetch_object_types().await?;
        let custom_fields_by_type = self.client.fetch_custom_fields().await?;
        let custom_object_types = self.client.fetch_custom_object_types().await?;
        let custom_object_fields = self.client.fetch_custom_object_type_fields().await?;
        let custom_objects_available = custom_object_types.is_some();

        let mut created_fields = Vec::new();
        let created_tags = Vec::new();
        let mut created_object_types = Vec::new();
        let mut created_object_fields = Vec::new();
        let mut deleted_object_types = Vec::new();
        let mut deleted_object_fields = Vec::new();

        let mut custom_types_by_name: BTreeMap<String, CustomObjectType> = BTreeMap::new();
        if let Some(types) = custom_object_types {
            for item in types {
                custom_types_by_name.insert(item.name.clone(), item);
            }
        }

        let mut custom_fields_by_type_id: BTreeMap<u64, BTreeMap<String, u64>> = BTreeMap::new();
        if let Some(fields) = custom_object_fields {
            for field in fields {
                custom_fields_by_type_id
                    .entry(field.custom_object_type)
                    .or_default()
                    .insert(field.name, field.id);
            }
        }

        let mut custom_schema_types: Vec<(TypeName, &TypeSchema)> = Vec::new();
        let mut custom_schema_type_names: BTreeSet<String> = BTreeSet::new();
        for (type_name, type_schema) in &schema.types {
            let type_name = TypeName::new(type_name);
            if registry.contains_type(&type_name) {
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
                    if self
                        .create_custom_field(&type_name, field_name, field_schema)
                        .await?
                    {
                        created_fields.push(format!("{}.{}", type_name, field_name));
                    }
                }
                continue;
            }

            custom_schema_type_names.insert(type_name.as_str().to_string());
            custom_schema_types.push((type_name, type_schema));
        }

        let mut desired_fields_by_type_id: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();

        if !custom_schema_types.is_empty() {
            if !custom_objects_available {
                let list = custom_schema_types
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(anyhow!(
                    "schema includes custom type(s) but netbox custom objects are not available: {list}"
                ));
            }

            let mut custom_type_ids: BTreeMap<String, u64> = BTreeMap::new();
            for (type_name, _) in &custom_schema_types {
                let custom_name = custom_object_type_name(type_name);
                let type_id = if let Some(existing) = custom_types_by_name.get(&custom_name) {
                    let (app_label, model) =
                        custom_object_type_parts(existing).unwrap_or_else(|| {
                            (CUSTOM_OBJECT_APP_LABEL.to_string(), custom_name.clone())
                        });
                    registry.insert_custom_object_type(
                        type_name.clone(),
                        custom_object_endpoint(&custom_name),
                        custom_object_features(),
                        app_label,
                        model,
                    );
                    existing.id
                } else {
                    let payload = Map::from_iter([
                        ("name".to_string(), Value::String(custom_name.clone())),
                        ("slug".to_string(), Value::String(custom_name.clone())),
                        (
                            "description".to_string(),
                            Value::String(format!(
                                "alembic custom object for {}",
                                type_name.as_str()
                            )),
                        ),
                        (
                            "verbose_name_plural".to_string(),
                            Value::String(custom_object_verbose_name_plural(type_name)),
                        ),
                    ]);
                    let resource: Resource<Value> = self
                        .client
                        .resource("plugins/custom-objects/custom-object-types/");
                    match resource.create(&Value::Object(payload)).await {
                        Ok(created) => {
                            let created_type = super::client::parse_custom_object_type(created)?;
                            let id = created_type.id;
                            let (app_label, model) = custom_object_type_parts(&created_type)
                                .unwrap_or_else(|| {
                                    (CUSTOM_OBJECT_APP_LABEL.to_string(), custom_name.clone())
                                });
                            registry.insert_custom_object_type(
                                type_name.clone(),
                                custom_object_endpoint(&custom_name),
                                custom_object_features(),
                                app_label,
                                model,
                            );
                            custom_types_by_name.insert(custom_name.clone(), created_type);
                            created_object_types.push(type_name.to_string());
                            id
                        }
                        Err(err) => {
                            if let Some(types) = self.client.fetch_custom_object_types().await? {
                                if let Some(existing) =
                                    types.into_iter().find(|item| item.name == custom_name)
                                {
                                    let (app_label, model) = custom_object_type_parts(&existing)
                                        .unwrap_or_else(|| {
                                            (
                                                CUSTOM_OBJECT_APP_LABEL.to_string(),
                                                custom_name.clone(),
                                            )
                                        });
                                    registry.insert_custom_object_type(
                                        type_name.clone(),
                                        custom_object_endpoint(&custom_name),
                                        custom_object_features(),
                                        app_label,
                                        model,
                                    );
                                    let id = existing.id;
                                    custom_types_by_name.insert(custom_name.clone(), existing);
                                    id
                                } else {
                                    return Err(err.into());
                                }
                            } else {
                                return Err(err.into());
                            }
                        }
                    }
                };
                custom_type_ids.insert(type_name.as_str().to_string(), type_id);
            }

            for (type_name, type_schema) in custom_schema_types {
                let Some(type_id) = custom_type_ids.get(type_name.as_str()).copied() else {
                    return Err(anyhow!("custom object type id missing for {}", type_name));
                };
                let existing_fields = custom_fields_by_type_id.entry(type_id).or_default();
                let mut provisioner = CustomObjectFieldProvisioner {
                    adapter: self,
                    registry: &registry,
                    custom_object_type_id: type_id,
                    existing_fields,
                    created_object_fields: &mut created_object_fields,
                    type_name: &type_name,
                };
                let mut desired_fields = BTreeSet::new();
                for (field_name, field_schema) in &type_schema.key {
                    if desired_fields.insert(field_name.clone()) {
                        provisioner.ensure(field_name, field_schema, true).await?;
                    }
                }
                for (field_name, field_schema) in &type_schema.fields {
                    if desired_fields.insert(field_name.clone()) {
                        provisioner.ensure(field_name, field_schema, false).await?;
                    }
                }
                desired_fields_by_type_id.insert(type_id, desired_fields);
            }
        }

        if custom_objects_available {
            let resource_fields: Resource<Value> = self
                .client
                .resource("plugins/custom-objects/custom-object-type-fields/");
            let resource_types: Resource<Value> = self
                .client
                .resource("plugins/custom-objects/custom-object-types/");

            for custom_type in custom_types_by_name.values() {
                let Some(type_name) = alembic_custom_object_name(custom_type) else {
                    continue;
                };
                let is_desired = custom_schema_type_names.contains(type_name.as_str());
                if is_desired {
                    let Some(existing_fields) = custom_fields_by_type_id.get(&custom_type.id)
                    else {
                        continue;
                    };
                    let desired_fields = desired_fields_by_type_id.get(&custom_type.id);
                    for (field_name, field_id) in existing_fields {
                        if is_reserved_custom_object_field(field_name) {
                            continue;
                        }
                        if desired_fields.is_some_and(|fields| fields.contains(field_name)) {
                            continue;
                        }
                        match resource_fields.delete(*field_id).await {
                            Ok(_) => {}
                            Err(err) if is_404_error(&err) => {
                                tracing::warn!(
                                    type_name = %type_name,
                                    field = %field_name,
                                    "custom object field already deleted"
                                );
                            }
                            Err(err) => return Err(err.into()),
                        }
                        deleted_object_fields.push(format!("{}.{}", type_name, field_name));
                    }
                } else {
                    if let Some(existing_fields) = custom_fields_by_type_id.get(&custom_type.id) {
                        for (field_name, field_id) in existing_fields {
                            if is_reserved_custom_object_field(field_name) {
                                continue;
                            }
                            match resource_fields.delete(*field_id).await {
                                Ok(_) => {}
                                Err(err) if is_404_error(&err) => {
                                    tracing::warn!(
                                        type_name = %type_name,
                                        field = %field_name,
                                        "custom object field already deleted"
                                    );
                                }
                                Err(err) => return Err(err.into()),
                            }
                            deleted_object_fields.push(format!("{}.{}", type_name, field_name));
                        }
                    }
                    match resource_types.delete(custom_type.id).await {
                        Ok(_) => {}
                        Err(err) if is_404_error(&err) => {
                            tracing::warn!(
                                type_name = %type_name,
                                "custom object type already deleted"
                            );
                        }
                        Err(err) => return Err(err.into()),
                    }
                    deleted_object_types.push(type_name);
                }
            }
        }

        Ok(ProvisionReport {
            created_fields,
            created_tags,
            created_object_types,
            created_object_fields,
            deprecated_object_types: Vec::new(),
            deprecated_object_fields: Vec::new(),
            deleted_object_types,
            deleted_object_fields,
        })
    }
}

impl NetBoxAdapter {
    async fn apply_create(
        &self,
        op: &Op,
        resolved: &mut BTreeMap<Uid, u64>,
        registry: &ObjectTypeRegistry,
        schema: &Schema,
        custom_fields_by_type: &BTreeMap<String, BTreeSet<String>>,
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
                    resolved.insert(uid, existing);
                    return Ok(existing);
                }
                return Err(err.into());
            }
            Err(err) => return Err(err.into()),
        };
        let backend_id = response
            .get("id")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("create {} returned no id", type_name))?;
        resolved.insert(uid, backend_id);
        Ok(backend_id)
    }

    async fn apply_update(
        &self,
        op: &Op,
        resolved: &BTreeMap<Uid, u64>,
        registry: &ObjectTypeRegistry,
        schema: &Schema,
        custom_fields_by_type: &BTreeMap<String, BTreeSet<String>>,
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
        let _response = resource.patch(id, &body).await?;
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
        payload.insert("name".to_string(), Value::String(field_name.to_string()));
        payload.insert("label".to_string(), Value::String(field_name.to_string()));
        payload.insert("type".to_string(), Value::String(field_type));
        payload.insert(
            "object_types".to_string(),
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

fn extract_attrs(value: Value) -> Result<(u64, JsonMap)> {
    let Value::Object(mut map) = value else {
        return Err(anyhow!("expected object payload"));
    };
    let backend_id = map
        .get("id")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("missing id in payload"))?;
    let custom_fields = map.remove("custom_fields");
    let tags = map.remove("tags");
    map.remove("id");
    map.remove("url");
    map.remove("display");
    map.remove("custom_object_type");
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
            if let Some(id) = map.get("id").and_then(as_u64) {
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
        if let Some(id) = map.get("id").and_then(as_u64) {
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

fn build_request_body(
    type_name: &TypeName,
    type_schema: &TypeSchema,
    attrs: &JsonMap,
    resolved: &BTreeMap<Uid, u64>,
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
            body.insert(api_key.to_string(), serde_json::to_value(tag_inputs)?);
            continue;
        }

        let field_schema = type_schema
            .fields
            .get(key)
            .ok_or_else(|| anyhow!("missing schema for field {key}"))?;
        let encoded = resolve_value_for_type(&field_schema.r#type, value.clone(), resolved)?;
        let wrapped = if should_wrap_as_generic_foreign_key(type_name, key) {
            wrap_as_generic_foreign_key(&field_schema.r#type, encoded)?
        } else {
            encoded
        };

        if custom_fields.contains(key) {
            if !supports_feature(features, &["custom-fields"]) {
                return Err(anyhow!("{} does not support custom fields", type_name));
            }
            custom.insert(key.clone(), wrapped);
        } else {
            body.insert(api_key.to_string(), wrapped);
        }
    }

    if !custom.is_empty() {
        body.insert("custom_fields".to_string(), Value::Object(custom));
    }

    Ok(Value::Object(body))
}

fn resolve_value_for_type(
    field_type: &alembic_core::FieldType,
    value: Value,
    resolved: &BTreeMap<Uid, u64>,
) -> Result<Value> {
    alembic_engine::resolve_value_for_type(field_type, value, resolved, |id| {
        Value::Number((*id).into())
    })
}

fn should_wrap_as_generic_foreign_key(type_name: &TypeName, key: &str) -> bool {
    match type_name.as_str() {
        "dcim.cable" => key == "a_terminations" || key == "b_terminations",
        _ => false,
    }
}

/// adds a generic foreign key wrapper around types that require it
fn wrap_as_generic_foreign_key(field_type: &FieldType, value: Value) -> Result<Value> {
    match field_type {
        FieldType::Ref { target } => {
            Ok(json!({"object_type": target.to_string(), "object_id": value}))
        }
        FieldType::ListRef { target } => {
            if let Some(array) = value.as_array() {
                array
                    .iter()
                    .map(|element| {
                        wrap_as_generic_foreign_key(
                            &FieldType::Ref {
                                target: target.clone(),
                            },
                            element.clone(),
                        )
                    })
                    .collect()
            } else {
                Err(anyhow!("value has type {:?} (expected array)", field_type))
            }
        }
        _ => Err(anyhow!("can't wrap unsupported type {:?}", field_type)),
    }
}

fn query_from_key(
    type_schema: &TypeSchema,
    key: &Key,
    resolved: &BTreeMap<Uid, u64>,
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

async fn build_registry_for_schema(
    adapter: &NetBoxAdapter,
    schema: &Schema,
) -> Result<ObjectTypeRegistry> {
    let mut registry = adapter.client.fetch_object_types().await?;
    let mut missing = Vec::new();
    for type_name in schema.types.keys() {
        let type_name = TypeName::new(type_name);
        if !registry.contains_type(&type_name) {
            missing.push(type_name);
        }
    }
    if missing.is_empty() {
        return Ok(registry);
    }

    let custom_object_types = adapter.client.fetch_custom_object_types().await?;
    if custom_object_types.is_none() {
        let list = missing
            .iter()
            .map(|t| t.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(anyhow!(
            "schema includes custom types but netbox custom objects are not available: {list}"
        ));
    }
    let custom_object_types = custom_object_types.unwrap_or_default();
    let mut custom_by_name: BTreeMap<String, CustomObjectType> = BTreeMap::new();
    for custom_type in custom_object_types {
        custom_by_name.insert(custom_type.name.clone(), custom_type);
    }

    for type_name in missing {
        let custom_name = custom_object_type_name(&type_name);
        let endpoint = custom_object_endpoint(&custom_name);
        if let Some(custom_type) = custom_by_name.get(&custom_name) {
            if let Some((app_label, model)) = custom_object_type_parts(custom_type) {
                registry.insert_custom_object_type(
                    type_name,
                    endpoint,
                    custom_object_features(),
                    app_label,
                    model,
                );
                continue;
            }
        }
        registry.insert_custom_object_type(
            type_name,
            endpoint,
            custom_object_features(),
            CUSTOM_OBJECT_APP_LABEL.to_string(),
            custom_name,
        );
    }

    Ok(registry)
}

struct CustomObjectFieldProvisioner<'a> {
    adapter: &'a NetBoxAdapter,
    registry: &'a ObjectTypeRegistry,
    custom_object_type_id: u64,
    existing_fields: &'a mut BTreeMap<String, u64>,
    created_object_fields: &'a mut Vec<String>,
    type_name: &'a TypeName,
}

impl<'a> CustomObjectFieldProvisioner<'a> {
    async fn ensure(
        &mut self,
        field_name: &str,
        field_schema: &FieldSchema,
        is_key: bool,
    ) -> Result<()> {
        if is_reserved_custom_object_field(field_name) {
            return Ok(());
        }
        if self.existing_fields.contains_key(field_name) {
            return Ok(());
        }
        validate_custom_object_field_name(field_name)?;
        let payload = custom_object_field_payload(
            self.registry,
            self.custom_object_type_id,
            field_name,
            field_schema,
            is_key,
        )?;
        let resource: Resource<Value> = self
            .adapter
            .client
            .resource("plugins/custom-objects/custom-object-type-fields/");
        match resource.create(&payload).await {
            Ok(created) => {
                if let Some(field_id) = custom_object_field_id(&created) {
                    self.existing_fields
                        .insert(field_name.to_string(), field_id);
                }
                self.created_object_fields
                    .push(format!("{}.{}", self.type_name, field_name));
            }
            Err(err) => {
                let Some(fields) = self
                    .adapter
                    .client
                    .fetch_custom_object_type_fields()
                    .await?
                else {
                    return Err(err.into());
                };
                if fields.iter().any(|field| {
                    field.custom_object_type == self.custom_object_type_id
                        && field.name == field_name
                }) {
                    tracing::warn!(
                        type_name = %self.type_name,
                        field = %field_name,
                        "custom object field already exists"
                    );
                    if let Some(existing) = fields.iter().find(|field| {
                        field.custom_object_type == self.custom_object_type_id
                            && field.name == field_name
                    }) {
                        self.existing_fields
                            .insert(field_name.to_string(), existing.id);
                    }
                } else {
                    return Err(err.into());
                }
            }
        }
        Ok(())
    }
}

async fn native_fields_for_type(
    adapter: &NetBoxAdapter,
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
        "custom_fields",
        "local_context_data",
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

fn custom_object_type_name(type_name: &TypeName) -> String {
    let mut out = String::new();
    let mut last_underscore = false;
    for ch in type_name.as_str().chars() {
        let lower = ch.to_ascii_lowercase();
        if lower.is_ascii_alphanumeric() {
            out.push(lower);
            last_underscore = false;
        } else if !last_underscore {
            out.push('_');
            last_underscore = true;
        }
    }
    while out.ends_with('_') {
        out.pop();
    }
    out
}

fn custom_object_features() -> BTreeSet<String> {
    [CUSTOM_OBJECT_FEATURE.to_string(), "tags".to_string()]
        .into_iter()
        .collect()
}

fn custom_object_type_parts(custom_type: &CustomObjectType) -> Option<(String, String)> {
    if let Some(parts) = custom_type.object_type_parts() {
        return Some(parts);
    }
    custom_type
        .table_model_name
        .as_deref()
        .map(|name| (CUSTOM_OBJECT_APP_LABEL.to_string(), name.to_lowercase()))
}

fn alembic_custom_object_name(custom_type: &CustomObjectType) -> Option<String> {
    custom_type
        .description
        .as_deref()
        .and_then(|desc| desc.strip_prefix(ALEMBIC_CUSTOM_OBJECT_PREFIX))
        .map(|name| name.to_string())
}

fn custom_object_endpoint(custom_name: &str) -> String {
    format!("plugins/custom-objects/{custom_name}/")
}

fn custom_object_verbose_name_plural(type_name: &TypeName) -> String {
    let base = type_name
        .as_str()
        .split('.')
        .next_back()
        .unwrap_or_else(|| type_name.as_str());
    let label = title_case(base);
    if label.ends_with('s') {
        label
    } else {
        format!("{label}s")
    }
}

fn custom_object_field_id(value: &Value) -> Option<u64> {
    match value {
        Value::Object(map) => map.get("id").and_then(as_u64),
        _ => None,
    }
}

fn title_case(value: &str) -> String {
    value
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|segment| !segment.is_empty())
        .map(|segment| {
            let mut chars = segment.chars();
            let Some(first) = chars.next() else {
                return String::new();
            };
            let mut out = String::new();
            out.push(first.to_ascii_uppercase());
            out.push_str(&chars.as_str().to_ascii_lowercase());
            out
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn is_reserved_custom_object_field(name: &str) -> bool {
    matches!(
        name,
        "id" | "url" | "display" | "custom_object_type" | "created" | "last_updated" | "tags"
    )
}

fn validate_custom_object_field_name(name: &str) -> Result<()> {
    if name.is_empty()
        || !name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(anyhow!(
            "invalid custom object field name '{}': only letters, digits, and underscores are allowed",
            name
        ));
    }
    Ok(())
}

fn custom_object_field_payload(
    registry: &ObjectTypeRegistry,
    custom_object_type_id: u64,
    field_name: &str,
    field_schema: &FieldSchema,
    is_key: bool,
) -> Result<Value> {
    let mut payload = Map::new();
    payload.insert(
        "custom_object_type".to_string(),
        Value::Number(custom_object_type_id.into()),
    );
    payload.insert("name".to_string(), Value::String(field_name.to_string()));
    payload.insert("label".to_string(), Value::String(title_case(field_name)));
    payload.insert(
        "type".to_string(),
        Value::String(custom_object_field_type(&field_schema.r#type).to_string()),
    );
    if is_key || field_schema.required {
        payload.insert("required".to_string(), Value::Bool(true));
    }
    if let Some(pattern) = &field_schema.pattern {
        payload.insert(
            "validation_regex".to_string(),
            Value::String(pattern.clone()),
        );
    }

    match &field_schema.r#type {
        FieldType::Ref { target } | FieldType::ListRef { target } => {
            let target_type = TypeName::new(target);
            if registry.contains_type(&target_type) {
                let info = registry
                    .info_for(&target_type)
                    .ok_or_else(|| anyhow!("invalid target type {}", target))?;
                payload.insert("app_label".to_string(), Value::String(info.app_label));
                payload.insert("model".to_string(), Value::String(info.model));
            } else {
                let custom_name = custom_object_type_name(&target_type);
                payload.insert(
                    "app_label".to_string(),
                    Value::String(CUSTOM_OBJECT_APP_LABEL.to_string()),
                );
                payload.insert("model".to_string(), Value::String(custom_name));
            }
        }
        _ => {}
    }

    Ok(Value::Object(payload))
}

fn custom_object_field_type(field_type: &FieldType) -> &'static str {
    match field_type {
        FieldType::Text => "longtext",
        FieldType::Int => "integer",
        FieldType::Float => "decimal",
        FieldType::Bool => "boolean",
        FieldType::Date => "date",
        FieldType::Datetime => "datetime",
        FieldType::Json | FieldType::List { .. } | FieldType::Map { .. } => "json",
        FieldType::Ref { .. } => "object",
        FieldType::ListRef { .. } => "multiobject",
        _ => "text",
    }
}

fn supports_feature(features: &BTreeSet<String>, candidates: &[&str]) -> bool {
    candidates.iter().any(|name| features.contains(*name))
}

fn is_missing_ref_error(err: &anyhow::Error) -> bool {
    err.downcast_ref::<AdapterApplyError>()
        .is_some_and(|e| matches!(e, AdapterApplyError::MissingRef { .. }))
}

fn is_404_error(err: &netbox::Error) -> bool {
    err.to_string().contains("status 404")
}

fn is_404_anyhow(err: &anyhow::Error) -> bool {
    err.downcast_ref::<netbox::Error>()
        .is_some_and(|e| matches!(e, netbox::Error::ApiError { status: 404, .. }))
}

fn is_conflict_error(err: &netbox::Error) -> bool {
    match err {
        netbox::Error::ApiError {
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

fn describe_missing_refs(ops: &[Op], resolved: &BTreeMap<Uid, u64>) -> String {
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

#[cfg(test)]
mod tests {
    use super::is_conflict_error;

    #[test]
    fn conflict_error_detects_unique_message() {
        let err = netbox::Error::ApiError {
            status: 400,
            message: "slug: This field must be unique.".to_string(),
            body: String::new(),
        };
        assert!(is_conflict_error(&err));
    }

    #[test]
    fn conflict_error_rejects_other_status() {
        let err = netbox::Error::ApiError {
            status: 404,
            message: "Not found".to_string(),
            body: String::new(),
        };
        assert!(!is_conflict_error(&err));
    }
}

#[cfg(test)]
mod test_normalization {
    use super::*;
    use alembic_core::FieldSchema;
    use serde_json::json;

    #[test]
    fn test_normalize_value_netbox() {
        let registry = ObjectTypeRegistry::default();
        let mappings = super::super::state::StateMappings::default();
        let schema = Schema {
            types: BTreeMap::new(),
        };

        // Test summary object to integer ID normalization
        let summary = json!({
            "id": 5,
            "url": "http://localhost/api/dcim/sites/5/",
            "display": "FRA1"
        });
        let normalized = normalize_value(summary, None, &schema, &registry, &mappings);
        assert_eq!(normalized, json!(5));

        // Test value/label normalization
        let status = json!({
            "value": "active",
            "label": "Active"
        });
        let normalized = normalize_value(status, None, &schema, &registry, &mappings);
        assert_eq!(normalized, json!("active"));
    }

    #[test]
    fn test_normalize_attrs_netbox() {
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
            ("id".to_string(), json!(123)),
            ("name".to_string(), json!("router-01")),
        ]);

        let uid = uid_from_key_fields(&nested, "dcim.device", &schema, &registry, &mappings);
        assert!(uid.is_some());

        // The UID should be deterministic: same inputs = same output
        let uid2 = uid_from_key_fields(&nested, "dcim.device", &schema, &registry, &mappings);
        assert_eq!(uid, uid2);

        // Different key value should produce different UID
        let nested2 = serde_json::Map::from_iter([
            ("id".to_string(), json!(456)),
            ("name".to_string(), json!("router-02")),
        ]);
        let uid3 = uid_from_key_fields(&nested2, "dcim.device", &schema, &registry, &mappings);
        assert!(uid3.is_some());
        assert_ne!(uid, uid3);
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
        resolved.insert(site_uid, 5);

        let body = build_request_body(
            &TypeName::new("dcim.device"),
            &type_schema,
            &attrs,
            &resolved,
            &BTreeSet::new(),
            &BTreeSet::new(),
        )
        .unwrap();
        assert_eq!(body.get("site").unwrap(), &json!(5));
    }

    #[test]
    fn test_resolve_value_for_type() {
        let resolved = BTreeMap::from([(Uid::from_u128(1), 5u64)]);

        // Ref
        let val = resolve_value_for_type(
            &alembic_core::FieldType::Ref {
                target: "t".to_string(),
            },
            json!(Uid::from_u128(1).to_string()),
            &resolved,
        )
        .unwrap();
        assert_eq!(val, json!(5));

        // ListRef
        let val = resolve_value_for_type(
            &alembic_core::FieldType::ListRef {
                target: "t".to_string(),
            },
            json!([Uid::from_u128(1).to_string()]),
            &resolved,
        )
        .unwrap();
        assert_eq!(val, json!([5]));
    }

    #[test]
    fn test_should_wrap_terminations_in_cables() {
        assert!(should_wrap_as_generic_foreign_key(
            &TypeName::new("dcim.cable"),
            "a_terminations"
        ));
        assert!(!should_wrap_as_generic_foreign_key(
            &TypeName::new("dcim.device"),
            "a_terminations"
        ));
        assert!(!should_wrap_as_generic_foreign_key(
            &TypeName::new("dcim.cable"),
            "label"
        ));
    }

    #[test]
    fn test_resolve_value_for_generic_fk_type() {
        let resolved = BTreeMap::from([(Uid::from_u128(1), 42u64), (Uid::from_u128(2), 1000u64)]);

        // Ref
        let field_type = FieldType::Ref {
            target: "dcim.interface".to_string(),
        };
        let val =
            resolve_value_for_type(&field_type, json!(Uid::from_u128(1).to_string()), &resolved)
                .unwrap();
        let wrapped = wrap_as_generic_foreign_key(&field_type, val).unwrap();

        assert_eq!(
            wrapped,
            json!({"object_type": "dcim.interface", "object_id": 42})
        );

        // ListRef
        let field_type = alembic_core::FieldType::ListRef {
            target: "dcim.interface".to_string(),
        };
        let val = resolve_value_for_type(
            &field_type,
            json!([Uid::from_u128(2).to_string(), Uid::from_u128(1).to_string()]),
            &resolved,
        )
        .unwrap();
        let wrapped = wrap_as_generic_foreign_key(&field_type, val).unwrap();

        assert_eq!(
            wrapped,
            json!([{"object_type": "dcim.interface", "object_id": 1000}, {"object_type": "dcim.interface", "object_id": 42}])
        );
    }

    #[test]
    fn test_supports_feature() {
        let mut features = BTreeSet::new();
        features.insert("tags".to_string());
        assert!(supports_feature(&features, &["tags"]));
        assert!(!supports_feature(&features, &["custom-fields"]));
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
        resolved.insert(site_uid, 5u64);

        let query = query_from_key(&type_schema, &key, &resolved).unwrap();
        let json = serde_json::to_value(&query).unwrap();
        let pairs = json.as_array().unwrap();
        assert_eq!(pairs.len(), 2);
        assert!(pairs.iter().any(|p| p == &json!(["name", "leaf01"])));
        assert!(pairs.iter().any(|p| p == &json!(["site", "5"])));
    }
}
