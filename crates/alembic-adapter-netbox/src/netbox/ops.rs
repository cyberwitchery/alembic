use super::client::{is_404_anyhow, CustomFieldDef, CustomObjectField, CustomObjectType};
use super::mapping::{
    build_tag_inputs, custom_field_type_for_schema, custom_field_update_payload,
    describe_custom_field_update, merge_shared_field_properties, slugify, supports_feature,
    tags_from_value, validation_regex_for_schema, ExistingCustomField,
};
use super::registry::ObjectTypeRegistry;
use super::state::{resolved_from_state, state_mappings};
use super::NetBoxAdapter;
use alembic_core::{
    key_string, FieldSchema, FieldType, JsonMap, Key, Schema, TypeName, TypeSchema, Uid,
};
use alembic_engine::{
    apply_non_delete_journaled, build_key_from_schema, collect_tag_names, describe_missing_refs,
    is_missing_ref_error, query_filters_from_key, resolve_nested_ref_uid,
    resolve_ref_keyed_identity, Adapter, AppliedOp, ApplyReport, BackendId, Emitter, ObservedState,
    Observer, Op, ProvisionReport, RawNode, RetryApplyDriver,
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
impl Observer for NetBoxAdapter {
    async fn read(
        &self,
        schema: &Schema,
        types: &[TypeName],
        state_store: &alembic_engine::StateStore,
        _scope: &alembic_engine::ReadScope,
    ) -> Result<ObservedState> {
        let registry: ObjectTypeRegistry = build_registry_for_schema(self, schema).await?;
        let mut mappings = state_mappings(state_store);

        let requested: BTreeSet<TypeName> = if types.is_empty() {
            // empty means every schema-declared type; skip backend types the schema omits.
            registry
                .type_names()
                .into_iter()
                .filter(|tn| schema.types.contains_key(tn.as_str()))
                .collect()
        } else {
            types.iter().cloned().collect()
        };

        let mut raw = Vec::new();
        for type_name in requested {
            let info = registry
                .info_for(&type_name)
                .ok_or_else(|| anyhow!("unsupported type {}", type_name))?;
            schema
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
                let (backend_id, attrs) = extract_attrs(object)?;
                raw.push(RawNode {
                    type_name: type_name.clone(),
                    backend_id: BackendId::Int(backend_id),
                    attrs,
                });
            }
        }

        let observed = resolve_ref_keyed_identity(
            &raw,
            schema,
            &mut mappings,
            |node, type_schema, mappings| {
                let mut attrs = node.attrs.clone();
                // decode generic FKs first: they carry a nested object brief that
                // `normalize_attrs` would otherwise mangle as an ordinary ref.
                decode_generic_fks(&mut attrs, &node.type_name, schema, &registry, mappings);
                normalize_attrs(&mut attrs, type_schema, schema, &registry, mappings);
                attrs
            },
            |node, type_schema, attrs| {
                build_key_from_schema(type_schema, attrs)
                    .with_context(|| format!("build key for {}", node.type_name))
            },
        )?;

        let mut state = ObservedState::default();
        for object in observed {
            state.insert(object)?;
        }

        Ok(state)
    }
}

#[async_trait]
impl Emitter for NetBoxAdapter {
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

        let mut created_tags = Vec::new();
        let tag_names = collect_tag_names(ops, |tn| registry.info_for(tn).map(|i| i.features))?;
        if !tag_names.is_empty() {
            let mut existing = self.client.fetch_tags().await?;
            let missing: Vec<String> = tag_names.difference(&existing).cloned().collect();
            if !missing.is_empty() {
                created_tags = self.create_tags(&missing).await?;
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

            fn resume(&mut self, resumed: &[AppliedOp]) {
                for op in resumed {
                    if let Some(BackendId::Int(id)) = &op.backend_id {
                        self.resolved.insert(op.uid, *id);
                    }
                }
            }
        }

        let mut driver = ApplyDriver {
            adapter: self,
            resolved: &mut resolved,
            registry: &registry,
            schema,
            custom_fields_by_type: &custom_fields_by_type,
        };
        let (retry_result, previously_applied_count, journal) =
            apply_non_delete_journaled(state, "netbox", &creates_updates, &mut driver).await?;

        if !retry_result.pending.is_empty() {
            let missing = describe_missing_refs(&retry_result.pending, &resolved);
            return Err(anyhow!("unresolved references: {missing}"));
        }

        let resumed = retry_result.resumed;
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
                        .and_then(|found| {
                            found.ok_or_else(|| {
                                anyhow!("{} not found for key {}", type_name, key_string(&key))
                            })
                        })
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
        journal.finish()?;

        Ok(ApplyReport {
            applied,
            resumed,
            previously_applied_count,
            provision: ProvisionReport {
                created_tags,
                ..Default::default()
            },
        })
    }

    async fn ensure_schema(&self, schema: &Schema) -> Result<ProvisionReport> {
        let mut registry: ObjectTypeRegistry = self.client.fetch_object_types().await?;
        let custom_fields_by_type = self.client.fetch_custom_field_defs().await?;
        let custom_object_types = self.client.fetch_custom_object_types().await?;
        let custom_object_fields = self.client.fetch_custom_object_type_fields().await?;

        let plan = self
            .plan_schema(
                &registry,
                &custom_fields_by_type,
                custom_object_types,
                custom_object_fields,
                schema,
            )
            .await?;

        let mut created_fields = Vec::new();
        let mut updated_fields = Vec::new();
        let mut created_object_types = Vec::new();
        let mut created_object_fields = Vec::new();
        let mut updated_object_fields = Vec::new();
        let mut deleted_object_types = Vec::new();
        let mut deleted_object_fields = Vec::new();

        // native custom fields on existing object types.
        for field in &plan.native_fields {
            let content_type = content_type_of(&registry, field.type_name.as_str());
            if self
                .create_custom_field(
                    &field.type_name,
                    &content_type,
                    field.field_name,
                    field.field_schema,
                )
                .await?
            {
                created_fields.push(format!("{}.{}", field.type_name, field.field_name));
            }
        }

        // existing custom fields: converge only the properties the schema declares.
        // untyped: the patch response is not read, so it need not deserialize
        // into the vendor's custom field model.
        let custom_fields: Resource<Value> = self.client.resource("extras/custom-fields/");
        for update in &plan.updated_fields {
            custom_fields.patch(update.field_id, &update.patch).await?;
            updated_fields.extend(update.declarations.iter().cloned());
        }

        // resolve or create every custom object type first, registering each so a
        // field payload can reference a sibling custom type by its resolved
        // identity.
        let mut type_ids = Vec::with_capacity(plan.object_types.len());
        for object_type in &plan.object_types {
            let type_id = match &object_type.existing {
                Some(existing) => {
                    let (app_label, model) =
                        custom_object_type_parts(existing).unwrap_or_else(|| {
                            (
                                CUSTOM_OBJECT_APP_LABEL.to_string(),
                                object_type.custom_name.clone(),
                            )
                        });
                    registry.insert_custom_object_type(
                        object_type.type_name.clone(),
                        custom_object_endpoint(&object_type.custom_name),
                        custom_object_features(),
                        app_label,
                        model,
                    );
                    existing.id
                }
                None => {
                    self.create_custom_object_type(
                        &mut registry,
                        &object_type.type_name,
                        &object_type.custom_name,
                        &mut created_object_types,
                    )
                    .await?
                }
            };
            type_ids.push(type_id);
        }

        // create the planned fields for each custom object type. ensure reports
        // only real creates: a create that turns out to already exist (returns
        // false / refetches) is not reported, though the plan lists it and preview
        // renders it -- the deliberate TOCTOU divergence.
        for (object_type, &type_id) in plan.object_types.iter().zip(&type_ids) {
            let mut existing_fields = object_type.existing_field_ids.clone();
            let mut provisioner = CustomObjectFieldProvisioner {
                adapter: self,
                registry: &registry,
                custom_object_type_id: type_id,
                existing_fields: &mut existing_fields,
                created_object_fields: &mut created_object_fields,
                type_name: &object_type.type_name,
            };
            for field in &object_type.fields {
                provisioner
                    .ensure(field.field_name, field.field_schema, field.is_key)
                    .await?;
            }
        }

        // existing custom object fields: the same convergence as the native ones,
        // patched by field id.
        let object_fields: Resource<Value> = self
            .client
            .resource("plugins/custom-objects/custom-object-type-fields/");
        for update in &plan.updated_object_fields {
            object_fields.patch(update.field_id, &update.patch).await?;
            updated_object_fields.extend(update.declarations.iter().cloned());
        }

        // deletes: alembic-owned custom object fields, then types, the schema no
        // longer declares. the plan carries their backend ids; a 404 on delete
        // is tolerated.
        if !plan.deleted_object_fields.is_empty() || !plan.deleted_object_types.is_empty() {
            let resource_fields: Resource<Value> = self
                .client
                .resource("plugins/custom-objects/custom-object-type-fields/");
            let resource_types: Resource<Value> = self
                .client
                .resource("plugins/custom-objects/custom-object-types/");
            for delete in &plan.deleted_object_fields {
                match resource_fields.delete(delete.field_id).await {
                    Ok(_) => {}
                    Err(err) if is_404_error(&err) => {
                        tracing::warn!(
                            type_name = %delete.type_name,
                            field = %delete.field_name,
                            "custom object field already deleted"
                        );
                    }
                    Err(err) => return Err(err.into()),
                }
                deleted_object_fields.push(format!("{}.{}", delete.type_name, delete.field_name));
            }
            for delete in &plan.deleted_object_types {
                match resource_types.delete(delete.type_id).await {
                    Ok(_) => {}
                    Err(err) if is_404_error(&err) => {
                        tracing::warn!(
                            type_name = %delete.type_name,
                            "custom object type already deleted"
                        );
                    }
                    Err(err) => return Err(err.into()),
                }
                deleted_object_types.push(delete.type_name.clone());
            }
        }

        Ok(ProvisionReport {
            created_fields,
            updated_fields,
            // tags are derived from the plan's ops, so only `write` creates them.
            created_tags: Vec::new(),
            created_object_types,
            created_object_fields,
            updated_object_fields,
            deprecated_object_types: Vec::new(),
            deprecated_object_fields: Vec::new(),
            deleted_object_types,
            deleted_object_fields,
        })
    }

    /// read-only counterpart to `ensure_schema`: renders the shared
    /// `ProvisionPlan` (see its doc) without writing.
    async fn preview_schema(&self, schema: &Schema) -> Result<Option<ProvisionReport>> {
        let registry: ObjectTypeRegistry = self.client.fetch_object_types().await?;
        let custom_fields_by_type = self.client.fetch_custom_field_defs().await?;
        let custom_object_types = self.client.fetch_custom_object_types().await?;
        let custom_object_fields = self.client.fetch_custom_object_type_fields().await?;

        let plan = self
            .plan_schema(
                &registry,
                &custom_fields_by_type,
                custom_object_types,
                custom_object_fields,
                schema,
            )
            .await?;

        let mut created_fields = Vec::new();
        for field in &plan.native_fields {
            created_fields.push(format!("{}.{}", field.type_name, field.field_name));
        }
        let updated_fields = plan
            .updated_fields
            .iter()
            .flat_map(|update| update.declarations.iter().cloned())
            .collect();

        let mut created_object_types = Vec::new();
        let mut created_object_fields = Vec::new();
        for object_type in &plan.object_types {
            if object_type.existing.is_none() {
                created_object_types.push(object_type.type_name.to_string());
            }
            for field in &object_type.fields {
                created_object_fields
                    .push(format!("{}.{}", object_type.type_name, field.field_name));
            }
        }

        let updated_object_fields = plan
            .updated_object_fields
            .iter()
            .flat_map(|update| update.declarations.iter().cloned())
            .collect();
        let deleted_object_fields = plan
            .deleted_object_fields
            .iter()
            .map(|delete| format!("{}.{}", delete.type_name, delete.field_name))
            .collect();
        let deleted_object_types = plan
            .deleted_object_types
            .iter()
            .map(|delete| delete.type_name.clone())
            .collect();

        Ok(Some(ProvisionReport {
            created_fields,
            updated_fields,
            created_tags: Vec::new(),
            created_object_types,
            created_object_fields,
            updated_object_fields,
            deprecated_object_types: Vec::new(),
            deprecated_object_fields: Vec::new(),
            deleted_object_types,
            deleted_object_fields,
        }))
    }
}

impl Adapter for NetBoxAdapter {}

/// decodes every generic foreign key on `type_name` from its NetBox read shape
/// into the alembic uid(s) it references. both wire forms expose a content type
/// and a backend id -- a nested `{ object_type, object_id }` (single or array) or
/// a split `<field>_type` / `<field>_id` pair -- which the recorded id->uid
/// mappings turn back into uids. a reference to an object alembic does not manage
/// (no mapping) is dropped rather than surfaced as an opaque id.
fn decode_generic_fks(
    attrs: &mut JsonMap,
    type_name: &TypeName,
    schema: &Schema,
    registry: &ObjectTypeRegistry,
    mappings: &super::state::StateMappings,
) {
    let content_type = content_type_of(registry, type_name.as_str());
    for (_, field, encoding) in netbox::GENERIC_FK_FIELDS
        .iter()
        .filter(|(model, _, _)| *model == content_type)
    {
        match encoding {
            netbox::GenericFkEncoding::Split => {
                let kind = attrs.remove(&format!("{field}_type"));
                let id = attrs.remove(&format!("{field}_id"));
                if let (Some(Value::String(kind)), Some(id)) = (kind, id) {
                    if let Some(uid) = resolve_generic_uid(&kind, &id, registry, mappings) {
                        attrs.insert((*field).to_string(), Value::String(uid));
                    }
                }
            }
            netbox::GenericFkEncoding::Nested => {
                if let Some(value) = attrs.get(*field).cloned() {
                    match decode_nested_generic_ref(&value, schema, registry, mappings) {
                        Some(uid) => {
                            attrs.insert((*field).to_string(), Value::String(uid));
                        }
                        None => {
                            attrs.remove(*field);
                        }
                    }
                }
            }
            netbox::GenericFkEncoding::NestedList => {
                if let Some(Value::Array(items)) = attrs.get(*field).cloned() {
                    let uids = items
                        .iter()
                        .filter_map(|item| {
                            decode_nested_generic_ref(item, schema, registry, mappings)
                        })
                        .map(Value::String)
                        .collect();
                    attrs.insert((*field).to_string(), Value::Array(uids));
                }
            }
        }
    }
}

/// resolves a `(content_type, backend_id)` pair to the alembic uid it maps to,
/// using the recorded id->uid mappings.
fn resolve_generic_uid(
    content_type: &str,
    id: &Value,
    registry: &ObjectTypeRegistry,
    mappings: &super::state::StateMappings,
) -> Option<String> {
    let id = as_u64(id)?;
    let type_name = registry
        .info_for(&TypeName::new(content_type))
        .map(|info| info.type_name.as_str().to_string())
        .unwrap_or_else(|| content_type.to_string());
    mappings.uid_for(&type_name, id).map(|uid| uid.to_string())
}

/// resolves a nested generic FK payload (`{ object_type, object_id, object }`)
/// to a uid. like a normal reference, it recomputes the uid from the embedded
/// `object` brief's key fields when present (so it round-trips without prior
/// state), falling back to the recorded id->uid mappings.
fn decode_nested_generic_ref(
    value: &Value,
    schema: &Schema,
    registry: &ObjectTypeRegistry,
    mappings: &super::state::StateMappings,
) -> Option<String> {
    let content_type = value.get("object_type")?.as_str()?;
    let type_name = registry
        .info_for(&TypeName::new(content_type))
        .map(|info| info.type_name.as_str().to_string())
        .unwrap_or_else(|| content_type.to_string());
    if let Some(Value::Object(brief)) = value.get("object") {
        if let Some(uid) = resolve_ref_uid(brief, Some(&type_name), schema, registry, mappings) {
            return Some(uid.to_string());
        }
    }
    resolve_generic_uid(content_type, value.get("object_id")?, registry, mappings)
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
            .get(&content_type_of(registry, type_name.as_str()))
            .cloned()
            .unwrap_or_default();
        let body = build_request_body(
            type_name,
            type_schema,
            &desired.attrs,
            resolved,
            &custom_fields,
            &info.features,
            registry,
        )?;
        let response: Value = match resource.create(&body).await {
            Ok(response) => response,
            Err(err) if is_conflict_error(&err) => {
                if let Some(existing) = self
                    .lookup_backend_id(type_name, &info, type_schema, &desired.key, resolved)
                    .await?
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
                .and_then(|found| {
                    found.ok_or_else(|| {
                        anyhow!(
                            "{} not found for key {}",
                            type_name,
                            key_string(&desired.key)
                        )
                    })
                })
                .with_context(|| format!("resolve backend id for {}", type_name))?
        };
        let resource: Resource<Value> = self.client.resource(info.endpoint.clone());
        let custom_fields = custom_fields_by_type
            .get(&content_type_of(registry, type_name.as_str()))
            .cloned()
            .unwrap_or_default();
        let body = build_request_body(
            type_name,
            type_schema,
            &desired.attrs,
            resolved,
            &custom_fields,
            &info.features,
            registry,
        )?;
        let _response = resource.patch(id, &body).await?;
        Ok(id)
    }

    /// list the endpoint and return the backend id of the object whose key matches,
    /// or `None` when no such object exists. used to recover from a create conflict.
    /// a key matching several backend objects is an error naming the count:
    /// alembic never picks among same-key objects.
    async fn lookup_backend_id(
        &self,
        type_name: &TypeName,
        info: &super::registry::ObjectTypeInfo,
        type_schema: &TypeSchema,
        key: &Key,
        resolved: &BTreeMap<Uid, u64>,
    ) -> Result<Option<u64>> {
        let query = query_from_key(type_schema, key, resolved)?;
        let resource: Resource<Value> = self.client.resource(info.endpoint.clone());
        let page = resource.list(Some(query)).await?;
        if page.count > 1 {
            return Err(anyhow!(
                "{} backend objects match the {} key {}; alembic cannot pick one, so \
                 resolve the collision or key the type the way the backend scopes uniqueness",
                page.count,
                type_name,
                alembic_core::key_string(key)
            ));
        }
        let Some(item) = page.results.into_iter().next() else {
            return Ok(None);
        };
        let id = item
            .get("id")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("{} lookup missing id", type_name))?;
        Ok(Some(id))
    }

    /// returns the tags this call created. a tag that lost the race and already
    /// existed is not one of them.
    async fn create_tags(&self, tags: &[String]) -> Result<Vec<String>> {
        let resource = self.client.extras().tags();
        let mut created = Vec::new();
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
            created.push(tag.clone());
        }
        Ok(created)
    }

    async fn create_custom_field(
        &self,
        type_name: &TypeName,
        content_type: &str,
        field_name: &str,
        field_schema: &FieldSchema,
    ) -> Result<bool> {
        let payload = custom_field_payload(content_type, field_name, field_schema);
        let resource = self.client.extras().custom_fields();
        match resource.create(&payload).await {
            Ok(_) => Ok(true),
            Err(err) => {
                let existing = self.client.fetch_custom_fields().await?;
                if existing
                    .get(content_type)
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

    /// computes the shared `ProvisionPlan` (see its doc) from the four backend
    /// reads and the schema. `native_fields_for_type` still reads a sample
    /// object per native type; no write happens here.
    async fn plan_schema<'a>(
        &self,
        registry: &ObjectTypeRegistry,
        custom_fields_by_type: &BTreeMap<String, BTreeMap<String, CustomFieldDef>>,
        custom_object_types: Option<Vec<CustomObjectType>>,
        custom_object_fields: Option<Vec<CustomObjectField>>,
        schema: &'a Schema,
    ) -> Result<ProvisionPlan<'a>> {
        let custom_objects_available = custom_object_types.is_some();

        let mut custom_types_by_name: BTreeMap<String, CustomObjectType> = BTreeMap::new();
        if let Some(types) = custom_object_types {
            for item in types {
                custom_types_by_name.insert(item.name.clone(), item);
            }
        }

        let mut custom_fields_by_type_id: BTreeMap<u64, BTreeMap<String, CustomObjectField>> =
            BTreeMap::new();
        if let Some(fields) = custom_object_fields {
            for field in fields {
                custom_fields_by_type_id
                    .entry(field.custom_object_type)
                    .or_default()
                    .insert(field.name.clone(), field);
            }
        }

        // partition declared types into native (custom fields on an existing type)
        // and custom object types, collecting the native field creates.
        let mut native_fields = Vec::new();
        // keyed by backend field id, not by (content type, field name): one netbox
        // field carries a list of object types, so two declared types can land on
        // the same id and must produce one patch between them.
        let mut shared_fields: BTreeMap<u64, SharedCustomField> = BTreeMap::new();
        let mut custom_schema_types: Vec<(TypeName, &TypeSchema)> = Vec::new();
        let mut custom_object_names: BTreeMap<String, TypeName> = BTreeMap::new();
        for (type_name, type_schema) in &schema.types {
            let type_name = TypeName::new(type_name);
            if registry.contains_type(&type_name) {
                let info = registry
                    .info_for(&type_name)
                    .ok_or_else(|| anyhow!("unsupported type {}", type_name))?;
                if !supports_feature(&info.features, &["custom-fields"]) {
                    continue;
                }
                let native = native_fields_for_type(self, &info, type_schema).await?;
                let content_type = content_type_of(registry, type_name.as_str());
                let existing = custom_fields_by_type.get(&content_type);
                for (field_name, field_schema) in &type_schema.fields {
                    if matches!(
                        field_schema.r#type,
                        FieldType::Ref { .. } | FieldType::ListRef { .. }
                    ) {
                        continue;
                    }
                    // a model column of the same name is netbox's, not ours.
                    if native.contains(field_name) {
                        continue;
                    }
                    if let Some(def) = existing.and_then(|fields| fields.get(field_name)) {
                        let payload = custom_field_payload(&content_type, field_name, field_schema);
                        let declared = format!("{type_name}.{field_name}");
                        let Some(field_id) = def.id else {
                            // netbox listed the field without an id, so it can be
                            // detected but not patched. saying so beats exiting 0
                            // with the divergence unreported.
                            if custom_field_update_payload(&def.current, &payload).is_some() {
                                tracing::warn!(
                                    field = %declared,
                                    "existing custom field diverges from the schema, but netbox reported no id to patch it by"
                                );
                            }
                            continue;
                        };
                        let shared =
                            shared_fields
                                .entry(field_id)
                                .or_insert_with(|| SharedCustomField {
                                    field_name: field_name.clone(),
                                    current: def.current.clone(),
                                    desired: Map::new(),
                                    declarations: Vec::new(),
                                });
                        if let Some(property) =
                            merge_shared_field_properties(&mut shared.desired, &payload)
                        {
                            return Err(anyhow!(
                                "custom field {} is one netbox field (id {field_id}) shared by {} and {declared}, which declare different {property}; make them agree or give each type its own field name",
                                shared.field_name,
                                shared.declarations.join(", "),
                            ));
                        }
                        shared.declarations.push(declared);
                        continue;
                    }
                    native_fields.push(PlannedNativeField {
                        type_name: type_name.clone(),
                        field_name: field_name.as_str(),
                        field_schema,
                    });
                }
                continue;
            }
            claim_custom_object_name(&mut custom_object_names, &type_name)?;
            custom_schema_types.push((type_name, type_schema));
        }

        // one patch per backend field, computed once every declaration on it has
        // been merged: a property another type already agrees with the backend on
        // must not be planned away by this one.
        let mut updated_fields = Vec::new();
        for (field_id, shared) in shared_fields {
            let Some(patch) =
                custom_field_update_payload(&shared.current, &Value::Object(shared.desired))
            else {
                continue;
            };
            // each declaration carries what the patch would write, so the
            // preview names the change rather than only the field.
            let changes = describe_custom_field_update(&shared.current, &patch).join(", ");
            updated_fields.push(PlannedFieldUpdate {
                declarations: shared
                    .declarations
                    .iter()
                    .map(|declared| format!("{declared}: {changes}"))
                    .collect(),
                field_id,
                patch,
            });
        }

        if !custom_schema_types.is_empty() && !custom_objects_available {
            let list = custom_schema_types
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(anyhow!(
                "schema includes custom type(s) but netbox custom objects are not available: {list}"
            ));
        }

        // custom object types: which to create and their would-be fields (id
        // resolution and the create-then-conflict-refetch stay in `ensure_schema`).
        let mut object_types = Vec::new();
        let mut updated_object_fields = Vec::new();
        for (type_name, type_schema) in custom_schema_types {
            let custom_name = custom_object_type_name(&type_name);
            let existing = custom_types_by_name.get(&custom_name).cloned();
            let existing_fields = existing
                .as_ref()
                .and_then(|existing| custom_fields_by_type_id.get(&existing.id));
            let existing_field_ids: BTreeMap<String, u64> = existing_fields
                .map(|fields| {
                    fields
                        .iter()
                        .map(|(name, field)| (name.clone(), field.id))
                        .collect()
                })
                .unwrap_or_default();

            let mut fields = Vec::new();
            let mut seen = BTreeSet::new();
            for (field_name, field_schema) in type_schema.key.iter().chain(&type_schema.fields) {
                if !seen.insert(field_name.as_str()) {
                    continue;
                }
                let is_key = type_schema.key.contains_key(field_name);
                match custom_object_field_action(
                    field_name,
                    existing_field_ids.contains_key(field_name),
                )? {
                    CustomObjectFieldAction::Skip => continue,
                    CustomObjectFieldAction::Create => fields.push(PlannedObjectField {
                        field_name: field_name.as_str(),
                        field_schema,
                        is_key,
                    }),
                    CustomObjectFieldAction::Converge => {
                        let Some(field) = existing_fields.and_then(|fields| fields.get(field_name))
                        else {
                            continue;
                        };
                        // desired is the create payload itself, as on the native path.
                        let desired = custom_object_field_payload(
                            registry,
                            field.custom_object_type,
                            field_name,
                            field_schema,
                            is_key,
                        )?;
                        let Some(patch) = custom_field_update_payload(&field.current, &desired)
                        else {
                            continue;
                        };
                        let changes =
                            describe_custom_field_update(&field.current, &patch).join(", ");
                        updated_object_fields.push(PlannedFieldUpdate {
                            declarations: vec![format!("{type_name}.{field_name}: {changes}")],
                            field_id: field.id,
                            patch,
                        });
                    }
                }
            }

            object_types.push(PlannedObjectType {
                type_name,
                custom_name,
                existing,
                existing_field_ids,
                fields,
            });
        }

        // deletes: alembic-owned custom types/fields the schema no longer declares
        // (same reserved-field skip and key+fields desired set as create).
        let mut deleted_object_fields = Vec::new();
        let mut deleted_object_types = Vec::new();
        if custom_objects_available {
            for (custom_name, custom_type) in &custom_types_by_name {
                let Some(recorded) = alembic_custom_object_name(custom_type) else {
                    continue;
                };
                let existing_fields = custom_fields_by_type_id.get(&custom_type.id);
                // which declared type claims this backend type is the question the
                // create and converge paths answer by its own name, so this reads
                // the same index rather than matching the recorded name as a
                // string: a declaration whose spelling changed into the same
                // custom object type name still owns it, and deleting it here
                // would race the patch planned for its fields above.
                if let Some(declared) = custom_object_names.get(custom_name) {
                    let type_name = declared.as_str().to_string();
                    let desired: BTreeSet<String> = schema
                        .types
                        .get(type_name.as_str())
                        .map(|ts| ts.key.keys().chain(ts.fields.keys()).cloned().collect())
                        .unwrap_or_default();
                    if let Some(existing_fields) = existing_fields {
                        for (field_name, field) in existing_fields {
                            if is_reserved_custom_object_field(field_name)
                                || desired.contains(field_name)
                            {
                                continue;
                            }
                            deleted_object_fields.push(PlannedFieldDelete {
                                type_name: type_name.clone(),
                                field_name: field_name.clone(),
                                field_id: field.id,
                            });
                        }
                    }
                } else {
                    let type_name = recorded;
                    if let Some(existing_fields) = existing_fields {
                        for (field_name, field) in existing_fields {
                            if is_reserved_custom_object_field(field_name) {
                                continue;
                            }
                            deleted_object_fields.push(PlannedFieldDelete {
                                type_name: type_name.clone(),
                                field_name: field_name.clone(),
                                field_id: field.id,
                            });
                        }
                    }
                    deleted_object_types.push(PlannedTypeDelete {
                        type_name,
                        type_id: custom_type.id,
                    });
                }
            }
        }

        Ok(ProvisionPlan {
            native_fields,
            updated_fields,
            object_types,
            updated_object_fields,
            deleted_object_fields,
            deleted_object_types,
        })
    }

    /// creates a custom object type, keeping the create-then-conflict-refetch
    /// fallback and registering it so field payloads can resolve it. reports the
    /// create (via `created_object_types`) only when the create actually
    /// succeeded, not when a concurrent create is discovered on refetch.
    async fn create_custom_object_type(
        &self,
        registry: &mut ObjectTypeRegistry,
        type_name: &TypeName,
        custom_name: &str,
        created_object_types: &mut Vec<String>,
    ) -> Result<u64> {
        let payload = Map::from_iter([
            ("name".to_string(), Value::String(custom_name.to_string())),
            ("slug".to_string(), Value::String(custom_name.to_string())),
            (
                "description".to_string(),
                Value::String(format!("alembic custom object for {}", type_name.as_str())),
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
                let (app_label, model) =
                    custom_object_type_parts(&created_type).unwrap_or_else(|| {
                        (CUSTOM_OBJECT_APP_LABEL.to_string(), custom_name.to_string())
                    });
                registry.insert_custom_object_type(
                    type_name.clone(),
                    custom_object_endpoint(custom_name),
                    custom_object_features(),
                    app_label,
                    model,
                );
                created_object_types.push(type_name.to_string());
                Ok(id)
            }
            Err(err) => {
                let Some(types) = self.client.fetch_custom_object_types().await? else {
                    return Err(err.into());
                };
                let Some(existing) = types.into_iter().find(|item| item.name == custom_name) else {
                    return Err(err.into());
                };
                let (app_label, model) = custom_object_type_parts(&existing).unwrap_or_else(|| {
                    (CUSTOM_OBJECT_APP_LABEL.to_string(), custom_name.to_string())
                });
                registry.insert_custom_object_type(
                    type_name.clone(),
                    custom_object_endpoint(custom_name),
                    custom_object_features(),
                    app_label,
                    model,
                );
                Ok(existing.id)
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
            // ref-typed key fields resolve to their canonical uid on read too, so
            // consult `.key` alongside `.fields` (the adapter-side counterpart of
            // the engine's `normalize_attrs_refs`, which walks `key.chain(fields)`).
            // a field in both resolves from `.fields`.
            let target_hint = type_schema
                .fields
                .get(&key)
                .or_else(|| type_schema.key.get(&key))
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
    // generic foreign keys (`assigned_object`, `scope`, terminations, ...) are
    // decoded uniformly from the schema-derived metadata in `decode_generic_fks`.
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
                // resolve the nested brief back to its canonical uid via recorded
                // mappings or the target's key fields.
                if let Some(uid) = resolve_ref_uid(&map, target_hint, schema, registry, mappings) {
                    return Value::String(uid.to_string());
                }
                // if it looks like a resource summary but isn't managed by us,
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

fn as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(num) => num.as_u64(),
        Value::String(raw) => raw.parse().ok(),
        _ => None,
    }
}

/// resolve a nested reference brief to its canonical uid, binding netbox's
/// integer id-space to the shared engine resolver.
fn resolve_ref_uid(
    map: &Map<String, Value>,
    target_hint: Option<&str>,
    schema: &Schema,
    registry: &ObjectTypeRegistry,
    mappings: &super::state::StateMappings,
) -> Option<Uid> {
    resolve_nested_ref_uid(
        map,
        target_hint,
        schema,
        |type_name, backend_id| match backend_id {
            BackendId::Int(id) => mappings.uid_for(type_name, *id),
            BackendId::String(_) => None,
        },
        |url| registry.type_name_for_endpoint(url).map(str::to_string),
    )
}

fn build_request_body(
    type_name: &TypeName,
    type_schema: &TypeSchema,
    attrs: &JsonMap,
    resolved: &BTreeMap<Uid, u64>,
    custom_fields: &BTreeSet<String>,
    features: &BTreeSet<String>,
    registry: &ObjectTypeRegistry,
) -> Result<Value> {
    let content_type = content_type_of(registry, type_name.as_str());
    let content_type = content_type.as_str();
    let mut body = Map::new();
    let mut custom = Map::new();

    for (key, value) in attrs.iter() {
        if key == "tags" {
            if !supports_feature(features, &["tags"]) {
                return Err(anyhow!("{} does not support tags", type_name));
            }
            let tags = tags_from_value(value)?;
            let tag_inputs = build_tag_inputs(&tags);
            body.insert(key.clone(), serde_json::to_value(tag_inputs)?);
            continue;
        }

        let field_schema = type_schema
            .fields
            .get(key)
            .ok_or_else(|| anyhow!("missing schema for field {key}"))?;

        // generic foreign keys are encoded from the schema-derived metadata: a
        // nested `{ object_type, object_id }` (single or array) or a split
        // `<field>_type` / `<field>_id` pair, depending on how NetBox models it.
        if let Some(encoding) = netbox::generic_fk_encoding(content_type, key) {
            encode_generic_fk(
                &mut body,
                key.as_str(),
                encoding,
                &field_schema.r#type,
                value.clone(),
                resolved,
                registry,
            )?;
            continue;
        }

        // a null clears the field
        let encoded = if value.is_null() {
            Value::Null
        } else {
            resolve_value_for_type(&field_schema.r#type, value.clone(), resolved)?
        };

        if custom_fields.contains(key) {
            if !supports_feature(features, &["custom-fields"]) {
                return Err(anyhow!("{} does not support custom fields", type_name));
            }
            custom.insert(key.clone(), encoded);
        } else {
            body.insert(key.clone(), encoded);
        }
    }

    if !custom.is_empty() {
        body.insert("custom_fields".to_string(), Value::Object(custom));
    }

    Ok(Value::Object(body))
}

/// the NetBox content type (`app_label.model`, e.g. `ipam.ipaddress`) for an
/// alembic `type_name` (`ipam.ip_address`), used to key generic-FK metadata and
/// to fill a generic FK's `object_type`. falls back to the input when the type
/// is not in the registry (it is then already a content type, e.g.
/// `dcim.interface`).
fn content_type_of(registry: &ObjectTypeRegistry, type_name: &str) -> String {
    registry
        .info_for(&TypeName::new(type_name))
        .map(|info| format!("{}.{}", info.app_label, info.model))
        .unwrap_or_else(|| type_name.to_string())
}

/// encodes a generic foreign key into `body` per its [`GenericFkEncoding`]. the
/// referenced object's content type comes from the schema field's target; its id
/// from the resolved uid->id map.
fn encode_generic_fk(
    body: &mut Map<String, Value>,
    key: &str,
    encoding: netbox::GenericFkEncoding,
    field_type: &FieldType,
    value: Value,
    resolved: &BTreeMap<Uid, u64>,
    registry: &ObjectTypeRegistry,
) -> Result<()> {
    if value.is_null() {
        // clear the generic fk
        match encoding {
            netbox::GenericFkEncoding::Split => {
                body.insert(format!("{key}_type"), Value::Null);
                body.insert(format!("{key}_id"), Value::Null);
            }
            netbox::GenericFkEncoding::Nested => {
                body.insert(key.to_string(), Value::Null);
            }
            netbox::GenericFkEncoding::NestedList => {
                body.insert(key.to_string(), Value::Array(Vec::new()));
            }
        }
        return Ok(());
    }
    let target = match field_type {
        FieldType::Ref { target } | FieldType::ListRef { target } => target.as_str(),
        other => {
            return Err(anyhow!(
                "generic foreign key {key} must be a ref, got {other:?}"
            ))
        }
    };
    let object_type = content_type_of(registry, target);
    let id = resolve_value_for_type(field_type, value, resolved)?;
    match encoding {
        netbox::GenericFkEncoding::Split => {
            body.insert(format!("{key}_type"), Value::String(object_type));
            body.insert(format!("{key}_id"), id);
        }
        netbox::GenericFkEncoding::Nested => {
            body.insert(
                key.to_string(),
                json!({ "object_type": object_type, "object_id": id }),
            );
        }
        netbox::GenericFkEncoding::NestedList => {
            let ids = id
                .as_array()
                .ok_or_else(|| anyhow!("generic foreign key {key} expected an array of ids"))?;
            let wrapped = ids
                .iter()
                .map(|id| json!({ "object_type": object_type, "object_id": id }))
                .collect();
            body.insert(key.to_string(), Value::Array(wrapped));
        }
    }
    Ok(())
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

    let mut custom_object_names: BTreeMap<String, TypeName> = BTreeMap::new();
    for type_name in missing {
        let custom_name = claim_custom_object_name(&mut custom_object_names, &type_name)?;
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
        if !matches!(
            custom_object_field_action(field_name, self.existing_fields.contains_key(field_name))?,
            CustomObjectFieldAction::Create
        ) {
            return Ok(());
        }
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

/// the create/delete decision `ensure_schema` and `preview_schema` share,
/// computed purely from the four backend reads and the schema without writing.
/// preview renders it into a `ProvisionReport`; ensure executes it and reports
/// what actually changed. one plan, two consumers -- so a preview can never claim
/// a change apply would not make, nor miss one.
struct ProvisionPlan<'a> {
    native_fields: Vec<PlannedNativeField<'a>>,
    updated_fields: Vec<PlannedFieldUpdate>,
    object_types: Vec<PlannedObjectType<'a>>,
    updated_object_fields: Vec<PlannedFieldUpdate>,
    deleted_object_fields: Vec<PlannedFieldDelete>,
    deleted_object_types: Vec<PlannedTypeDelete>,
}

/// a custom field to create on an existing (native) object type.
struct PlannedNativeField<'a> {
    type_name: TypeName,
    field_name: &'a str,
    field_schema: &'a FieldSchema,
}

/// an existing custom field to converge, with the patch that does it: only the
/// properties the schema declares and the backend disagrees on.
struct PlannedFieldUpdate {
    /// every `type.field` declaration this one backend field answers; on a custom
    /// object field exactly one, since colliding type names are refused.
    declarations: Vec<String>,
    field_id: u64,
    patch: Value,
}

/// the declarations landing on one backend custom field, accumulated so they can
/// be merged into a single patch or refused when they disagree.
struct SharedCustomField {
    field_name: String,
    current: ExistingCustomField,
    desired: Map<String, Value>,
    declarations: Vec<String>,
}

/// a custom object type to provision, with the object fields to create once its
/// backend id is known.
struct PlannedObjectType<'a> {
    type_name: TypeName,
    custom_name: String,
    /// `Some` when the type already exists on the backend (reuse its id, report
    /// no create); `None` when it must be created.
    existing: Option<CustomObjectType>,
    /// existing field ids, seeding the field provisioner for intra-type
    /// conflict handling.
    existing_field_ids: BTreeMap<String, u64>,
    /// fields to create, deduped key-then-fields, with reserved/existing/invalid
    /// names already filtered out.
    fields: Vec<PlannedObjectField<'a>>,
}

/// a custom object field to create, tagged whether it is part of the type key.
struct PlannedObjectField<'a> {
    field_name: &'a str,
    field_schema: &'a FieldSchema,
    is_key: bool,
}

/// an alembic-owned custom object field to delete, with its backend id.
struct PlannedFieldDelete {
    type_name: String,
    field_name: String,
    field_id: u64,
}

/// an alembic-owned custom object type to delete, with its backend id.
struct PlannedTypeDelete {
    type_name: String,
    type_id: u64,
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

/// two declared names can normalize onto one backend type name, and every
/// lookup keyed on it would then answer both.
fn claim_custom_object_name(
    claimed: &mut BTreeMap<String, TypeName>,
    type_name: &TypeName,
) -> Result<String> {
    let custom_name = custom_object_type_name(type_name);
    if let Some(other) = claimed.insert(custom_name.clone(), type_name.clone()) {
        return Err(anyhow!(
            "custom object type {custom_name} is one netbox type claimed by {other} and {type_name}, whose names collapse together once lowercased with every run of non-alphanumerics replaced by `_`; rename one so their custom object type names differ"
        ));
    }
    Ok(custom_name)
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

/// what a declared custom-object field needs.
enum CustomObjectFieldAction {
    /// netbox's own column: not ours to write.
    Skip,
    Create,
    Converge,
}

/// the decision `preview_schema` and `ensure_schema` share, erroring on a name
/// netbox rejects so both make it identically. a field that exists is not
/// name-checked: netbox took it already.
fn custom_object_field_action(
    field_name: &str,
    field_exists: bool,
) -> Result<CustomObjectFieldAction> {
    if is_reserved_custom_object_field(field_name) {
        return Ok(CustomObjectFieldAction::Skip);
    }
    if field_exists {
        return Ok(CustomObjectFieldAction::Converge);
    }
    validate_custom_object_field_name(field_name)?;
    Ok(CustomObjectFieldAction::Create)
}

/// netbox's `extras/custom-fields/` accepts more types than the netbox+nautobot
/// intersection the shared map stays on, so upgrade the cells netbox's own
/// custom-object path already carries. its object/multiobject arms have no
/// equivalent here: ref/listref are skipped before a native field is created.
fn native_custom_field_type(field_schema: &FieldSchema) -> String {
    match field_schema.r#type {
        FieldType::Float => "decimal".to_string(),
        FieldType::Text => "longtext".to_string(),
        _ => custom_field_type_for_schema(field_schema),
    }
}

/// the create payload for a custom field on a native netbox model.
fn custom_field_payload(content_type: &str, field_name: &str, field_schema: &FieldSchema) -> Value {
    let field_type = native_custom_field_type(field_schema);
    let validation_regex = validation_regex_for_schema(field_schema, &field_type);
    let mut payload = Map::new();
    payload.insert("name".to_string(), Value::String(field_name.to_string()));
    payload.insert("label".to_string(), Value::String(field_name.to_string()));
    payload.insert("type".to_string(), Value::String(field_type));
    // netbox keys a custom field's object_types by the django content type
    // (`ipam.ipaddress`), not the endpoint form (`ipam.ip_address`); the two
    // diverge for every multi-word model, and posting the endpoint form 400s.
    payload.insert(
        "object_types".to_string(),
        Value::Array(vec![Value::String(content_type.to_string())]),
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
    if let Some(pattern) = validation_regex {
        payload.insert(
            "validation_regex".to_string(),
            Value::String(pattern.to_string()),
        );
    }
    Value::Object(payload)
}

fn custom_object_field_payload(
    registry: &ObjectTypeRegistry,
    custom_object_type_id: u64,
    field_name: &str,
    field_schema: &FieldSchema,
    is_key: bool,
) -> Result<Value> {
    let field_type = custom_object_field_type(&field_schema.r#type);
    let mut payload = Map::new();
    payload.insert(
        "custom_object_type".to_string(),
        Value::Number(custom_object_type_id.into()),
    );
    payload.insert("name".to_string(), Value::String(field_name.to_string()));
    payload.insert("label".to_string(), Value::String(title_case(field_name)));
    payload.insert("type".to_string(), Value::String(field_type.to_string()));
    if is_key || field_schema.required {
        payload.insert("required".to_string(), Value::Bool(true));
    }
    if let Some(description) = &field_schema.description {
        payload.insert(
            "description".to_string(),
            Value::String(description.clone()),
        );
    }
    if let Some(pattern) = validation_regex_for_schema(field_schema, field_type) {
        payload.insert(
            "validation_regex".to_string(),
            Value::String(pattern.to_string()),
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
        FieldType::String
        | FieldType::Uuid
        | FieldType::Time
        | FieldType::IpAddress
        | FieldType::Cidr
        | FieldType::Prefix
        | FieldType::Mac
        | FieldType::Slug
        | FieldType::Enum { .. } => "text",
    }
}

fn is_404_error(err: &netbox::Error) -> bool {
    matches!(err, netbox::Error::ApiError { status: 404, .. })
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
    use alembic_core::{format_regex, FieldFormat, FieldSchema};
    use serde_json::json;

    #[test]
    fn test_normalize_value_netbox() {
        let registry = ObjectTypeRegistry::default();
        let mappings = super::super::state::StateMappings::default();
        let schema = Schema {
            types: BTreeMap::new(),
        };

        // test summary object to integer ID normalization
        let summary = json!({
            "id": 5,
            "url": "http://localhost/api/dcim/sites/5/",
            "display": "FRA1"
        });
        let normalized = normalize_value(summary, None, &schema, &registry, &mappings);
        assert_eq!(normalized, json!(5));

        // test value/label normalization
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

        // the literal backend `type` field is preserved as-is (interfaces and
        // everything else alike).
        normalize_attrs(&mut attrs, &type_schema, &schema, &registry, &mappings);
        assert_eq!(attrs.get("type").unwrap(), &json!("1000base-t"));
        assert!(!attrs.contains_key("if_type"));
    }

    #[test]
    fn test_interface_type_round_trips_literally() {
        let registry = ObjectTypeRegistry::default();
        let mappings = super::super::state::StateMappings::default();
        let mut fields = BTreeMap::new();
        fields.insert(
            "type".to_string(),
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
        let schema = Schema {
            types: BTreeMap::new(),
        };
        let mut attrs = JsonMap::default();
        attrs.insert("type".to_string(), json!("1000base-t"));

        // read: a `dcim.interface` `type` stays `type`.
        normalize_attrs(&mut attrs, &type_schema, &schema, &registry, &mappings);
        assert_eq!(attrs.get("type").unwrap(), &json!("1000base-t"));
        assert!(!attrs.contains_key("if_type"));

        // write: it goes back out under its own name, with no `if_type` remap.
        let body = build_request_body(
            &TypeName::new("dcim.interface"),
            &type_schema,
            &attrs,
            &BTreeMap::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &ObjectTypeRegistry::default(),
        )
        .unwrap();
        assert_eq!(body.get("type").unwrap(), &json!("1000base-t"));
        assert!(body.get("if_type").is_none());
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
            ("id".to_string(), json!(123)),
            ("name".to_string(), json!("router-01")),
        ]);

        let uid = resolve_ref_uid(&nested, Some("dcim.device"), &schema, &registry, &mappings);
        assert!(uid.is_some());

        // the UID should be deterministic: same inputs = same output
        let uid2 = resolve_ref_uid(&nested, Some("dcim.device"), &schema, &registry, &mappings);
        assert_eq!(uid, uid2);

        // different key value should produce different UID
        let nested2 = serde_json::Map::from_iter([
            ("id".to_string(), json!(456)),
            ("name".to_string(), json!("router-02")),
        ]);
        let uid3 = resolve_ref_uid(&nested2, Some("dcim.device"), &schema, &registry, &mappings);
        assert!(uid3.is_some());
        assert_ne!(uid, uid3);
    }

    #[test]
    fn test_normalize_attrs_resolves_ref_typed_key_field() {
        // a ref-typed field declared only in `.key` (not `.fields`) must resolve
        // to its target's canonical uid on read, exactly like a ref-typed `.fields`
        // field. this exercises the production `normalize_attrs` hint computation,
        // which `test_uid_from_key_fields` bypasses by passing the hint directly.
        let registry = ObjectTypeRegistry::default();
        let mappings = super::super::state::StateMappings::default();

        // target: dcim.device, keyed by `name`.
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
        let mut schema = Schema {
            types: BTreeMap::new(),
        };
        schema
            .types
            .insert("dcim.device".to_string(), device_schema);

        // parent object: a ref-typed key field `device` -> dcim.device, not
        // duplicated into `.fields`.
        let mut parent_schema = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::new(),
        };
        parent_schema.key.insert(
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

        // unmanaged brief: a resource summary (id + display) carrying the target's
        // key, but no recognized url, so the mappings lookup misses and only the
        // key-derivation stage can resolve it.
        let brief = json!({ "id": 123, "display": "router-01", "name": "router-01" });
        let mut attrs = JsonMap::default();
        attrs.insert("device".to_string(), brief.clone());

        normalize_attrs(&mut attrs, &parent_schema, &schema, &registry, &mappings);

        let expected = resolve_ref_uid(
            brief.as_object().unwrap(),
            Some("dcim.device"),
            &schema,
            &registry,
            &mappings,
        )
        .expect("key-derived uid");
        assert_eq!(attrs.get("device").unwrap(), &json!(expected.to_string()));
        // a hint miss here degrades to the raw backend id.
        assert_ne!(attrs.get("device").unwrap(), &json!(123));
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
            &ObjectTypeRegistry::default(),
        )
        .unwrap();
        assert_eq!(body.get("site").unwrap(), &json!(5));
    }

    #[test]
    fn test_build_request_body_passes_null_ref_through_to_clear_it() {
        let mut fields = BTreeMap::new();
        fields.insert(
            "rack".to_string(),
            FieldSchema {
                r#type: alembic_core::FieldType::Ref {
                    target: "dcim.rack".to_string(),
                },
                required: false,
                nullable: true,
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
        attrs.insert("rack".to_string(), json!(null));

        let body = build_request_body(
            &TypeName::new("dcim.device"),
            &type_schema,
            &attrs,
            &BTreeMap::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &ObjectTypeRegistry::default(),
        )
        .unwrap();
        assert_eq!(body.get("rack").unwrap(), &json!(null));
    }

    #[test]
    fn test_build_request_body_null_custom_field_clears_via_custom_data() {
        let mut fields = BTreeMap::new();
        fields.insert(
            "owner".to_string(),
            FieldSchema {
                r#type: alembic_core::FieldType::Ref {
                    target: "dcim.device".to_string(),
                },
                required: false,
                nullable: true,
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
        attrs.insert("owner".to_string(), json!(null));

        let body = build_request_body(
            &TypeName::new("dcim.device"),
            &type_schema,
            &attrs,
            &BTreeMap::new(),
            &BTreeSet::from(["owner".to_string()]),
            &BTreeSet::from(["custom-fields".to_string()]),
            &ObjectTypeRegistry::default(),
        )
        .unwrap();
        let custom = body
            .get("custom_fields")
            .and_then(Value::as_object)
            .unwrap();
        assert_eq!(custom.get("owner").unwrap(), &json!(null));
        assert!(body.get("owner").is_none());
    }

    #[test]
    fn test_resolve_value_for_type() {
        let resolved = BTreeMap::from([(Uid::from_u128(1), 5u64)]);

        // ref
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

    fn mappings_with(type_name: &str, id: u64, uid: Uid) -> super::super::state::StateMappings {
        let mut by_type = BTreeMap::new();
        by_type.insert(type_name.to_string(), BTreeMap::from([(id, uid)]));
        super::super::state::StateMappings { by_type }
    }

    #[test]
    fn test_encode_generic_fk_split() {
        // an ip address's `assigned_object` is written as a split
        // `assigned_object_type` / `assigned_object_id` pair.
        let registry = ObjectTypeRegistry::default();
        let resolved = BTreeMap::from([(Uid::from_u128(1), 42u64)]);
        let mut body = Map::new();
        encode_generic_fk(
            &mut body,
            "assigned_object",
            netbox::GenericFkEncoding::Split,
            &FieldType::Ref {
                target: "dcim.interface".to_string(),
            },
            json!(Uid::from_u128(1).to_string()),
            &resolved,
            &registry,
        )
        .unwrap();
        assert_eq!(
            body.get("assigned_object_type").unwrap(),
            &json!("dcim.interface")
        );
        assert_eq!(body.get("assigned_object_id").unwrap(), &json!(42));
    }

    #[test]
    fn test_encode_generic_fk_null_clears() {
        // a null generic fk clears: both split components null, the nested field
        // null, or an empty array for a nested list.
        let registry = ObjectTypeRegistry::default();
        let resolved: BTreeMap<Uid, u64> = BTreeMap::new();
        let ref_type = FieldType::Ref {
            target: "dcim.interface".to_string(),
        };

        let mut body = Map::new();
        encode_generic_fk(
            &mut body,
            "assigned_object",
            netbox::GenericFkEncoding::Split,
            &ref_type,
            json!(null),
            &resolved,
            &registry,
        )
        .unwrap();
        assert_eq!(body.get("assigned_object_type").unwrap(), &json!(null));
        assert_eq!(body.get("assigned_object_id").unwrap(), &json!(null));

        let mut body = Map::new();
        encode_generic_fk(
            &mut body,
            "scope",
            netbox::GenericFkEncoding::Nested,
            &ref_type,
            json!(null),
            &resolved,
            &registry,
        )
        .unwrap();
        assert_eq!(body.get("scope").unwrap(), &json!(null));

        let mut body = Map::new();
        encode_generic_fk(
            &mut body,
            "a_terminations",
            netbox::GenericFkEncoding::NestedList,
            &FieldType::ListRef {
                target: "dcim.interface".to_string(),
            },
            json!(null),
            &resolved,
            &registry,
        )
        .unwrap();
        assert_eq!(body.get("a_terminations").unwrap(), &json!([]));
    }

    #[test]
    fn test_encode_generic_fk_nested_list() {
        // a cable's terminations are written as an array of nested
        // `{ object_type, object_id }`.
        let registry = ObjectTypeRegistry::default();
        let resolved = BTreeMap::from([(Uid::from_u128(1), 42u64), (Uid::from_u128(2), 1000u64)]);
        let mut body = Map::new();
        encode_generic_fk(
            &mut body,
            "a_terminations",
            netbox::GenericFkEncoding::NestedList,
            &FieldType::ListRef {
                target: "dcim.interface".to_string(),
            },
            json!([Uid::from_u128(2).to_string(), Uid::from_u128(1).to_string()]),
            &resolved,
            &registry,
        )
        .unwrap();
        assert_eq!(
            body.get("a_terminations").unwrap(),
            &json!([
                {"object_type": "dcim.interface", "object_id": 1000},
                {"object_type": "dcim.interface", "object_id": 42}
            ])
        );
    }

    #[test]
    fn test_decode_generic_fks_nested_list() {
        // a cable termination read back as `{ object_type, object_id }` resolves
        // to the interface uid via the recorded mappings.
        let registry = ObjectTypeRegistry::default();
        let uid = Uid::from_u128(7);
        let mappings = mappings_with("dcim.interface", 1, uid);
        let mut map = Map::new();
        map.insert(
            "a_terminations".to_string(),
            json!([{ "object_type": "dcim.interface", "object_id": 1 }]),
        );
        let mut attrs: JsonMap = map.into_iter().collect::<BTreeMap<_, _>>().into();
        let schema = Schema {
            types: BTreeMap::new(),
        };
        decode_generic_fks(
            &mut attrs,
            &TypeName::new("dcim.cable"),
            &schema,
            &registry,
            &mappings,
        );
        assert_eq!(attrs["a_terminations"], json!([uid.to_string()]));
    }

    #[test]
    fn test_decode_generic_fks_split() {
        // a prefix's split `scope_type` / `scope_id` resolves to the site uid and
        // the wire fields are removed in favor of the `scope` ref.
        let registry = ObjectTypeRegistry::default();
        let uid = Uid::from_u128(9);
        let mappings = mappings_with("dcim.site", 3, uid);
        let mut map = Map::new();
        map.insert("scope_type".to_string(), json!("dcim.site"));
        map.insert("scope_id".to_string(), json!(3));
        let mut attrs: JsonMap = map.into_iter().collect::<BTreeMap<_, _>>().into();
        let schema = Schema {
            types: BTreeMap::new(),
        };
        decode_generic_fks(
            &mut attrs,
            &TypeName::new("ipam.prefix"),
            &schema,
            &registry,
            &mappings,
        );
        assert_eq!(attrs["scope"], json!(uid.to_string()));
        assert!(attrs.get("scope_type").is_none());
        assert!(attrs.get("scope_id").is_none());
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

    fn field_schema(r#type: FieldType, pattern: Option<&str>) -> FieldSchema {
        FieldSchema {
            r#type,
            required: false,
            nullable: true,
            description: None,
            format: None,
            pattern: pattern.map(str::to_string),
        }
    }

    #[test]
    fn test_custom_field_payload_carries_declared_pattern() {
        let payload = custom_field_payload(
            "dcim.device",
            "asset_tag",
            &field_schema(FieldType::String, Some("^[A-Z]{3}$")),
        );
        assert_eq!(payload.get("type").unwrap(), &json!("text"));
        assert_eq!(
            payload.get("validation_regex").unwrap(),
            &json!("^[A-Z]{3}$")
        );
    }

    #[test]
    fn test_custom_field_payload_omits_regex_without_pattern() {
        let payload = custom_field_payload(
            "dcim.device",
            "asset_tag",
            &field_schema(FieldType::String, None),
        );
        assert!(payload.get("validation_regex").is_none());
    }

    #[test]
    fn test_custom_field_payload_skips_pattern_on_json_field() {
        // core allows a pattern on `json`; the backend field it maps to would
        // enforce nothing, so the regex stays home.
        let payload = custom_field_payload(
            "dcim.device",
            "meta",
            &field_schema(FieldType::Json, Some("^[A-Z]{3}$")),
        );
        assert_eq!(payload.get("type").unwrap(), &json!("json"));
        assert!(payload.get("validation_regex").is_none());
    }

    #[test]
    fn test_custom_field_payload_carries_declared_format() {
        let mut schema = field_schema(FieldType::String, None);
        schema.format = Some(FieldFormat::Mac);
        let payload = custom_field_payload("dcim.device", "bmc_mac", &schema);
        assert_eq!(payload.get("type").unwrap(), &json!("text"));
        assert_eq!(
            payload.get("validation_regex").unwrap(),
            &json!(format_regex(&FieldFormat::Mac))
        );
    }

    #[test]
    fn test_custom_field_payload_carries_the_format_a_type_implies() {
        // netbox flattens every one of these to `text`, so the regex is the
        // only place the declared semantic survives.
        for (r#type, format) in [
            (FieldType::Mac, FieldFormat::Mac),
            (FieldType::Uuid, FieldFormat::Uuid),
            (FieldType::Cidr, FieldFormat::Cidr),
            (FieldType::Prefix, FieldFormat::Prefix),
            (FieldType::Slug, FieldFormat::Slug),
        ] {
            let payload = custom_field_payload("dcim.device", "f", &field_schema(r#type, None));
            assert_eq!(payload.get("type").unwrap(), &json!("text"));
            assert_eq!(
                payload.get("validation_regex").unwrap(),
                &json!(format_regex(&format))
            );
        }
    }

    #[test]
    fn test_custom_field_payload_leaves_ip_address_unconstrained() {
        // core checks an `ip_address`-typed value as a plain string, so any
        // regex here would reject values alembic's own validator accepts.
        let payload = custom_field_payload(
            "dcim.device",
            "mgmt",
            &field_schema(FieldType::IpAddress, None),
        );
        assert_eq!(payload.get("type").unwrap(), &json!("text"));
        assert!(payload.get("validation_regex").is_none());
    }

    #[test]
    fn test_custom_field_payload_prefers_pattern_over_format() {
        let mut schema = field_schema(FieldType::Mac, Some("^[A-Z]{3}$"));
        schema.format = Some(FieldFormat::Mac);
        let payload = custom_field_payload("dcim.device", "asset_tag", &schema);
        assert_eq!(
            payload.get("validation_regex").unwrap(),
            &json!("^[A-Z]{3}$")
        );
    }

    #[test]
    fn test_custom_field_payload_uses_netbox_native_types() {
        let float = custom_field_payload(
            "dcim.device",
            "ratio",
            &field_schema(FieldType::Float, None),
        );
        assert_eq!(float.get("type").unwrap(), &json!("decimal"));
        let text =
            custom_field_payload("dcim.device", "notes", &field_schema(FieldType::Text, None));
        assert_eq!(text.get("type").unwrap(), &json!("longtext"));
        // the two provisioning paths agree on the types both can express.
        for r#type in [FieldType::Float, FieldType::Text] {
            assert_eq!(
                custom_field_payload("dcim.device", "f", &field_schema(r#type.clone(), None))
                    .get("type")
                    .unwrap(),
                &json!(custom_object_field_type(&r#type)),
            );
        }
    }

    #[test]
    fn test_custom_field_payload_keeps_pattern_on_longtext() {
        // `longtext` is still text, so a declared pattern still constrains it.
        let payload = custom_field_payload(
            "dcim.device",
            "notes",
            &field_schema(FieldType::Text, Some("^[A-Z]{3}$")),
        );
        assert_eq!(payload.get("type").unwrap(), &json!("longtext"));
        assert_eq!(
            payload.get("validation_regex").unwrap(),
            &json!("^[A-Z]{3}$")
        );
    }

    #[test]
    fn test_custom_object_field_payload_guards_pattern_by_type() {
        let registry = ObjectTypeRegistry::default();
        let payload = custom_object_field_payload(
            &registry,
            7,
            "asset_tag",
            &field_schema(FieldType::String, Some("^[A-Z]{3}$")),
            false,
        )
        .unwrap();
        assert_eq!(
            payload.get("validation_regex").unwrap(),
            &json!("^[A-Z]{3}$")
        );

        let payload = custom_object_field_payload(
            &registry,
            7,
            "meta",
            &field_schema(FieldType::Json, Some("^[A-Z]{3}$")),
            false,
        )
        .unwrap();
        assert_eq!(payload.get("type").unwrap(), &json!("json"));
        assert!(payload.get("validation_regex").is_none());
    }
}
