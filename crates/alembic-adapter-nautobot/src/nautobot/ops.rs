use super::mapping::{
    build_tag_inputs, custom_field_type_for_schema, custom_field_update_payload,
    describe_custom_field_update, merge_shared_field_properties, slugify, supports_feature,
    tags_from_value, validation_regex_for_schema, ExistingCustomField,
};
use super::registry::ObjectTypeRegistry;
use super::state::{resolved_from_state, state_mappings};
use super::NautobotAdapter;
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

        let mut tasks = Vec::new();
        for type_name in requested {
            let info = registry
                .info_for(&type_name)
                .ok_or_else(|| anyhow!("unsupported type {}", type_name))?
                .clone();
            schema
                .types
                .get(type_name.as_str())
                .ok_or_else(|| anyhow!("missing schema for {}", type_name))?;
            let client = Arc::clone(&self.client);

            tasks.push(tokio::spawn(async move {
                let resource: Resource<Value> = client.resource(info.endpoint.clone());
                let objects = client.list_all(&resource, None).await?;
                let mut raw = Vec::new();
                for object in objects {
                    let (backend_id, attrs) = extract_attrs(object)?;
                    raw.push(RawNode {
                        type_name: type_name.clone(),
                        backend_id: BackendId::String(backend_id),
                        attrs,
                    });
                }
                Ok::<Vec<RawNode>, anyhow::Error>(raw)
            }));
        }

        let mut raw = Vec::new();
        for result in futures::future::join_all(tasks).await {
            raw.extend(result??);
        }

        let observed = resolve_ref_keyed_identity(
            &raw,
            schema,
            &mut mappings,
            |node, type_schema, mappings| {
                let mut attrs = node.attrs.clone();
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

            fn resume(&mut self, resumed: &[AppliedOp]) {
                for op in resumed {
                    if let Some(BackendId::String(id)) = &op.backend_id {
                        self.resolved.insert(op.uid, id.clone());
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
            apply_non_delete_journaled(state, "nautobot", &creates_updates, &mut driver).await?;

        if !retry_result.pending.is_empty() {
            let missing = describe_missing_refs(&retry_result.pending, &resolved);
            return Err(anyhow!("unresolved references: {missing}"));
        }

        let resumed = retry_result.resumed;
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
        let plan = self.plan_custom_fields(schema).await?;

        let mut created_fields = Vec::new();
        for (type_name, field_name, field_schema) in plan.missing {
            if self
                .create_custom_field(&type_name, field_name, field_schema)
                .await?
            {
                created_fields.push(format!("{type_name}.{field_name}"));
            }
        }

        // existing custom fields: converge only the properties the schema declares
        // and the declared choices the field does not offer yet.
        let mut updated_fields = Vec::new();
        // untyped: the patch response is not read, so it need not deserialize
        // into the vendor's custom field model.
        let custom_fields: Resource<Value> = self.client.resource("extras/custom-fields/".into());
        for update in &plan.updates {
            if let Some(patch) = &update.patch {
                custom_fields.patch(&update.field_id, patch).await?;
            }
            if !update.missing_choices.is_empty() {
                self.create_custom_field_choices(&update.field_id, &update.missing_choices)
                    .await?;
            }
            updated_fields.extend(update.declarations.iter().cloned());
        }

        Ok(ProvisionReport {
            created_fields,
            updated_fields,
            ..Default::default()
        })
    }

    async fn preview_schema(&self, schema: &Schema) -> Result<Option<ProvisionReport>> {
        let plan = self.plan_custom_fields(schema).await?;
        Ok(Some(ProvisionReport {
            created_fields: plan
                .missing
                .iter()
                .map(|(type_name, field_name, _)| format!("{type_name}.{field_name}"))
                .collect(),
            updated_fields: plan
                .updates
                .iter()
                .flat_map(|update| update.declarations.iter().cloned())
                .collect(),
            ..Default::default()
        }))
    }
}

impl Adapter for NautobotAdapter {}

/// what a provision has to do to the declared custom fields: create the ones the
/// backend lacks, converge the ones it has.
struct CustomFieldPlan<'a> {
    missing: Vec<(TypeName, &'a String, &'a FieldSchema)>,
    updates: Vec<PlannedFieldUpdate>,
}

/// an existing custom field to converge: the properties the schema declares and
/// the backend disagrees on, and the declared choices it does not offer yet.
struct PlannedFieldUpdate {
    /// every `type.field` declaration this one backend field answers.
    declarations: Vec<String>,
    field_id: String,
    /// `None` when only the choices move.
    patch: Option<Value>,
    missing_choices: Vec<(String, usize)>,
}

/// the declarations landing on one backend custom field, accumulated so they can
/// be merged into a single patch or refused when they disagree.
struct SharedCustomField {
    field_name: String,
    current: ExistingCustomField,
    desired: Map<String, Value>,
    choices: Vec<String>,
    declarations: Vec<String>,
}

/// fold one declaration's choices into what the others on the same field agreed
/// on, naming the property when they disagree: the create payload the engine
/// merges does not carry them. order is weight, and only a `select` has any.
fn merge_shared_field_choices(
    agreed: &mut Vec<String>,
    declared: &[String],
) -> Option<&'static str> {
    if declared.is_empty() {
        return None;
    }
    if agreed.is_empty() {
        *agreed = declared.to_vec();
        return None;
    }
    (agreed != declared).then_some("choices")
}

impl NautobotAdapter {
    /// read the live schema and compute which declared custom fields the backend
    /// lacks and which of the ones it has diverge from their declaration.
    /// read-only: shared by `ensure_schema` (which then writes them) and
    /// `preview_schema` (which only reports them), so the decision never drifts
    /// between preview and apply.
    async fn plan_custom_fields<'a>(&self, schema: &'a Schema) -> Result<CustomFieldPlan<'a>> {
        let registry: ObjectTypeRegistry = self.client.fetch_object_types().await?;
        let custom_fields_by_type = self.client.fetch_custom_field_defs().await?;
        let mut missing = Vec::new();
        // keyed by backend field id, not by (content type, field name): one nautobot
        // field carries a list of content types, so two declared types can land on
        // the same id and must produce one patch between them.
        let mut shared_fields: BTreeMap<String, SharedCustomField> = BTreeMap::new();

        for (type_name, type_schema) in &schema.types {
            let type_name = TypeName::new(type_name);
            let info = registry
                .info_for(&type_name)
                .ok_or_else(|| anyhow!("unsupported type {}", type_name))?;
            if !supports_feature(&info.features, &["custom-fields"]) {
                continue;
            }

            let native_fields = native_fields_for_type(self, &info, type_schema).await?;
            let existing = custom_fields_by_type.get(type_name.as_str());

            for (field_name, field_schema) in &type_schema.fields {
                if matches!(
                    field_schema.r#type,
                    FieldType::Ref { .. } | FieldType::ListRef { .. }
                ) {
                    continue;
                }
                // a model column of the same name is nautobot's, not ours.
                if native_fields.contains(field_name) {
                    continue;
                }
                if let Some(def) = existing.and_then(|fields| fields.get(field_name)) {
                    let payload =
                        custom_field_payload(type_name.as_str(), field_name, field_schema);
                    let declared = format!("{type_name}.{field_name}");
                    let Some(field_id) = def.id.clone() else {
                        // nautobot listed the field without an id, so it can be
                        // detected but neither patched nor keyed on to read its
                        // choices. saying so beats exiting 0 with the divergence
                        // unreported.
                        if custom_field_update_payload(&def.current, &payload).is_some()
                            || !declared_choices(field_schema).is_empty()
                        {
                            tracing::warn!(
                                field = %declared,
                                "existing custom field diverges from the schema or declares choices, but nautobot reported no id to write either by"
                            );
                        }
                        continue;
                    };
                    let shared = shared_fields.entry(field_id.clone()).or_insert_with(|| {
                        SharedCustomField {
                            field_name: field_name.clone(),
                            current: def.current.clone(),
                            desired: Map::new(),
                            choices: Vec::new(),
                            declarations: Vec::new(),
                        }
                    });
                    let disagreement = merge_shared_field_properties(&mut shared.desired, &payload)
                        .or_else(|| {
                            merge_shared_field_choices(
                                &mut shared.choices,
                                declared_choices(field_schema),
                            )
                        });
                    if let Some(property) = disagreement {
                        return Err(anyhow!(
                            "custom field {} is one nautobot field (id {field_id}) shared by {} and {declared}, which declare different {property}; make them agree or give each type its own field name",
                            shared.field_name,
                            shared.declarations.join(", "),
                        ));
                    }
                    shared.declarations.push(declared);
                    continue;
                }
                missing.push((type_name.clone(), field_name, field_schema));
            }
        }

        // one read for every field's choices, and only when a declaration that
        // landed on an existing field carries any: a model without enums must not
        // cost a request.
        let current_choices = if shared_fields
            .values()
            .any(|shared| !shared.choices.is_empty())
        {
            self.client.fetch_custom_field_choices().await?
        } else {
            BTreeMap::new()
        };

        // one patch per backend field, computed once every declaration on it has
        // been merged: a property another type already agrees with the backend on
        // must not be planned away by this one.
        let mut updates = Vec::new();
        for (field_id, shared) in shared_fields {
            let patch =
                custom_field_update_payload(&shared.current, &Value::Object(shared.desired));
            let choices = missing_choices(&shared.choices, current_choices.get(&field_id));
            if patch.is_none() && choices.is_empty() {
                continue;
            }
            // each declaration carries what the write would do, so the preview
            // names the change rather than only the field.
            let mut changes = patch
                .as_ref()
                .map(|patch| describe_custom_field_update(&shared.current, patch))
                .unwrap_or_default();
            if !choices.is_empty() {
                changes.push(describe_added_choices(&choices));
            }
            let changes = changes.join(", ");
            updates.push(PlannedFieldUpdate {
                declarations: shared
                    .declarations
                    .iter()
                    .map(|declared| format!("{declared}: {changes}"))
                    .collect(),
                field_id,
                patch,
                missing_choices: choices,
            });
        }

        Ok(CustomFieldPlan { missing, updates })
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
                if let Some(existing) = self
                    .lookup_backend_id(type_name, &info, type_schema, &desired.key, resolved)
                    .await?
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

    /// list the endpoint and return the backend id of the object whose key matches,
    /// or `None` when no such object exists. used to recover from a create conflict.
    async fn lookup_backend_id(
        &self,
        type_name: &TypeName,
        info: &super::registry::ObjectTypeInfo,
        type_schema: &TypeSchema,
        key: &Key,
        resolved: &BTreeMap<Uid, String>,
    ) -> Result<Option<String>> {
        let query = query_from_key(type_schema, key, resolved)?;
        let resource: Resource<Value> = self.client.resource(info.endpoint.clone());
        let page = resource.list(Some(query)).await?;
        let Some(item) = page.results.into_iter().next() else {
            return Ok(None);
        };
        let id = item
            .get("id")
            .and_then(Value::as_str)
            .map(|s| s.to_string())
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
        field_name: &str,
        field_schema: &FieldSchema,
    ) -> Result<bool> {
        let payload = custom_field_payload(type_name.as_str(), field_name, field_schema);
        let resource = self.client.extras().custom_fields();
        match resource.create(&payload).await {
            Ok(created) => {
                let choices = missing_choices(declared_choices(field_schema), None);
                if !choices.is_empty() {
                    let id = created
                        .id
                        .ok_or_else(|| anyhow!("custom field create returned no id"))?;
                    self.create_custom_field_choices(&id.to_string(), &choices)
                        .await?;
                }
                Ok(true)
            }
            Err(err) => {
                let existing = self.client.fetch_custom_fields().await?;
                if existing
                    .get(type_name.as_str())
                    .is_some_and(|fields| fields.contains(field_name))
                {
                    // the plan saw no field to converge, so posting choices here
                    // would duplicate whoever won the race. the next run finds the
                    // field and posts the ones it lacks.
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

    /// post each choice at the weight the caller computed. nautobot's writable
    /// custom field carries no inline `choices`, so they follow the create or a
    /// later converge.
    ///
    /// posts through a json resource, not `extras().custom_field_choices()`:
    /// that one decodes into the generated `CustomFieldChoice`, whose nested
    /// `custom_field.id` generates as an empty struct, so a create nautobot
    /// accepted would still fail to decode.
    async fn create_custom_field_choices(
        &self,
        field_id: &str,
        values: &[(String, usize)],
    ) -> Result<()> {
        let resource: Resource<Value> = self
            .client
            .resource("extras/custom-field-choices/".to_string());
        for (value, weight) in values {
            let payload = serde_json::json!({
                "value": value,
                "weight": weight,
                "custom_field": field_id,
            });
            resource
                .create(&payload)
                .await
                .with_context(|| format!("creating custom field choice {value}"))?;
        }
        Ok(())
    }
}

/// nautobot enforces a declared enum itself, so upgrade the cell the shared
/// netbox+nautobot map has to flatten to `text`: a `select` carries its values
/// as per-field choices, unlike netbox's separately named choice sets.
fn nautobot_custom_field_type(field_schema: &FieldSchema) -> String {
    match &field_schema.r#type {
        FieldType::Enum { .. } => "select".to_string(),
        FieldType::List { item } if matches!(**item, FieldType::Enum { .. }) => {
            "multi-select".to_string()
        }
        _ => custom_field_type_for_schema(field_schema),
    }
}

/// the values a `select`/`multi-select` field offers, in declaration order.
/// empty for every other type.
fn declared_choices(field_schema: &FieldSchema) -> &[String] {
    match &field_schema.r#type {
        FieldType::Enum { values } => values,
        FieldType::List { item } => match &**item {
            FieldType::Enum { values } => values,
            _ => &[],
        },
        _ => &[],
    }
}

/// the declared choices a field does not offer yet, each at the weight its
/// declared position implies, so a field created and later extended ends up with
/// the weights a create of the whole list would have written. a value declared
/// between two the backend already has can therefore tie one of their weights,
/// which nautobot allows and orders itself.
fn missing_choices(
    declared: &[String],
    current: Option<&BTreeSet<String>>,
) -> Vec<(String, usize)> {
    declared
        .iter()
        .enumerate()
        .filter(|(_, value)| !current.is_some_and(|current| current.contains(*value)))
        .map(|(index, value)| (value.clone(), (index + 1) * 100))
        .collect()
}

/// what posting them would add, worded the way `describe_custom_field_update`
/// words a property. additive, so only the values the field gains.
fn describe_added_choices(choices: &[(String, usize)]) -> String {
    let values: Vec<Value> = choices
        .iter()
        .map(|(value, _)| Value::String(value.clone()))
        .collect();
    format!("choices + {}", Value::Array(values))
}

/// the create payload for a custom field on a nautobot model.
fn custom_field_payload(content_type: &str, field_name: &str, field_schema: &FieldSchema) -> Value {
    let field_type = nautobot_custom_field_type(field_schema);
    let validation_regex = validation_regex_for_schema(field_schema, &field_type);
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
                // resolve the nested brief back to its canonical uid via recorded
                // mappings or the target's key fields.
                if let Some(uid) = resolve_ref_uid(&map, target_hint, schema, registry, mappings) {
                    return Value::String(uid.to_string());
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

/// resolve a nested reference brief to its canonical uid, binding nautobot's
/// uuid-string id-space to the shared engine resolver.
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
            BackendId::String(id) => mappings.uid_for(type_name, id),
            BackendId::Int(_) => None,
        },
        |url| registry.type_name_for_endpoint(url).map(str::to_string),
    )
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
        if key == "tags" {
            if !supports_feature(features, &["tags"]) {
                return Err(anyhow!("{} does not support tags", type_name));
            }
            let tags = tags_from_value(value)?;
            let tag_inputs = build_tag_inputs(&tags);
            body.insert(key.clone(), Value::Array(tag_inputs));
            continue;
        }

        let field_schema = type_schema
            .fields
            .get(key)
            .ok_or_else(|| anyhow!("missing schema for field {key}"))?;
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

    Ok(native)
}

fn is_404_error(err: &nautobot::Error) -> bool {
    matches!(err, nautobot::Error::ApiError { status: 404, .. })
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
    use alembic_core::{format_regex, FieldFormat, FieldSchema};
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

        // the literal backend `type` field is preserved as-is (interfaces and
        // everything else alike).
        normalize_attrs(&mut attrs, &type_schema, &schema, &registry, &mappings);
        assert_eq!(attrs.get("type").unwrap(), &json!("1000base-t"));
        assert!(!attrs.contains_key("if_type"));
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

        let uid = resolve_ref_uid(&nested, Some("dcim.device"), &schema, &registry, &mappings);
        assert!(uid.is_some());

        // the UID should be deterministic: same inputs = same output
        let uid2 = resolve_ref_uid(&nested, Some("dcim.device"), &schema, &registry, &mappings);
        assert_eq!(uid, uid2);

        // different key value should produce different UID
        let nested2 = serde_json::Map::from_iter([
            ("id".to_string(), json!("other-uuid")),
            ("name".to_string(), json!("router-02")),
        ]);
        let uid3 = resolve_ref_uid(&nested2, Some("dcim.device"), &schema, &registry, &mappings);
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
        let device_uid = resolve_ref_uid(
            &device_map,
            Some("dcim.device"),
            &schema,
            &registry,
            &mappings,
        )
        .expect("device uid");

        // expected: interface uid with the device key already resolved to its uid string
        let expected_map = serde_json::Map::from_iter([
            ("device".to_string(), json!(device_uid.to_string())),
            ("name".to_string(), json!("eth1")),
        ]);
        let expected = resolve_ref_uid(
            &expected_map,
            Some("dcim.interface"),
            &schema,
            &registry,
            &mappings,
        );

        // actual: interface uid with the device key as a nested brief
        let actual_map = serde_json::Map::from_iter([
            ("device".to_string(), json!({ "name": "router-01" })),
            ("name".to_string(), json!("eth1")),
        ]);
        let actual = resolve_ref_uid(
            &actual_map,
            Some("dcim.interface"),
            &schema,
            &registry,
            &mappings,
        );

        // the nested device brief got resolved to the device uid
        assert!(actual.is_some());
        assert_eq!(actual, expected);

        // eth1 under a different device must not collide with eth1 here
        let other_map = serde_json::Map::from_iter([
            ("device".to_string(), json!({ "name": "router-02" })),
            ("name".to_string(), json!("eth1")),
        ]);
        let other = resolve_ref_uid(
            &other_map,
            Some("dcim.interface"),
            &schema,
            &registry,
            &mappings,
        );
        assert_ne!(actual, other);
    }

    #[test]
    fn test_normalize_attrs_resolves_ref_typed_key_field() {
        // a ref-typed field declared only in `.key` (not `.fields`) must resolve
        // to its target's canonical uid on read, exactly like a ref-typed `.fields`
        // field. this exercises the production `normalize_attrs` hint computation,
        // which `test_uid_from_key_fields_resolves_ref_key` bypasses by passing the
        // hint directly.
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

        // unmanaged brief: a resource summary (uuid id + object_type) carrying the
        // target's key, but no recognized url, so the mappings lookup misses and
        // only the key-derivation stage can resolve it.
        let raw_id = "11111111-1111-1111-1111-111111111111";
        let brief = json!({ "id": raw_id, "object_type": "dcim.device", "name": "router-01" });
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
        assert_ne!(attrs.get("device").unwrap(), &json!(raw_id));
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
        )
        .unwrap();
        let custom = body
            .get("_custom_field_data")
            .and_then(Value::as_object)
            .unwrap();
        assert_eq!(custom.get("owner").unwrap(), &json!(null));
        assert!(body.get("owner").is_none());
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
        )
        .unwrap();
        assert_eq!(body.get("type").unwrap(), &json!("1000base-t"));
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
        // nautobot flattens every one of these to `text`, so the regex is the
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
    fn test_custom_field_payload_skips_format_on_select() {
        // an enum maps to `select`, which the text gate excludes; a `format:`
        // on one goes the same way a `pattern:` does.
        let mut schema = field_schema(
            FieldType::Enum {
                values: vec!["a".to_string(), "b".to_string()],
            },
            None,
        );
        schema.format = Some(FieldFormat::Slug);
        let payload = custom_field_payload("dcim.device", "state", &schema);
        assert_eq!(payload.get("type").unwrap(), &json!("select"));
        assert!(payload.get("validation_regex").is_none());
    }

    // the create's race fallback posts no choices: the plan that converges them
    // ran before this field existed, so posting here would duplicate whoever won
    // the race rather than converge anything.
    #[tokio::test]
    async fn test_existing_custom_field_posts_no_choices() {
        use httpmock::Method::{GET, POST};
        use httpmock::MockServer;

        let server = MockServer::start();
        let _create = server.mock(|when, then| {
            when.method(POST).path("/api/extras/custom-fields/");
            then.status(400)
                .json_body(json!({ "key": ["already exists"] }));
        });
        let _list = server.mock(|when, then| {
            when.method(GET).path("/api/extras/custom-fields/");
            then.status(200).json_body(json!({
                "count": 1,
                "next": null,
                "previous": null,
                "results": [{
                    "id": "44444444-4444-4444-4444-444444444444",
                    "key": "tier",
                    "label": "tier",
                    "content_types": ["dcim.site"],
                    "type": {},
                }],
            }));
        });
        let choices = server.mock(|when, then| {
            when.method(POST).path("/api/extras/custom-field-choices/");
            then.status(201).json_body(json!({ "value": "core" }));
        });

        let adapter = NautobotAdapter::new(&server.base_url(), "token").unwrap();
        let created = adapter
            .create_custom_field(
                &TypeName::new("dcim.site"),
                "tier",
                &field_schema(
                    FieldType::Enum {
                        values: vec!["core".to_string(), "edge".to_string()],
                    },
                    None,
                ),
            )
            .await
            .unwrap();

        assert!(!created);
        choices.assert_calls(0);
    }

    #[test]
    fn test_custom_field_payload_types_an_enum_as_select() {
        // nautobot's CustomFieldTypeChoices spells these `select` / `multi-select`.
        let payload = custom_field_payload(
            "dcim.device",
            "tier",
            &field_schema(
                FieldType::Enum {
                    values: vec!["core".to_string(), "edge".to_string()],
                },
                None,
            ),
        );
        assert_eq!(payload.get("type").unwrap(), &json!("select"));

        let payload = custom_field_payload(
            "dcim.device",
            "roles",
            &field_schema(
                FieldType::List {
                    item: Box::new(FieldType::Enum {
                        values: vec!["core".to_string()],
                    }),
                },
                None,
            ),
        );
        assert_eq!(payload.get("type").unwrap(), &json!("multi-select"));
    }

    #[test]
    fn test_custom_field_payload_types_a_plain_list_as_json() {
        // only a list *of enum* is a multi-select; every other list stays json.
        let payload = custom_field_payload(
            "dcim.device",
            "aliases",
            &field_schema(
                FieldType::List {
                    item: Box::new(FieldType::String),
                },
                None,
            ),
        );
        assert_eq!(payload.get("type").unwrap(), &json!("json"));
    }

    #[test]
    fn test_custom_field_payload_skips_pattern_on_enum_field() {
        // core allows a pattern alongside enum values; a select constrains by its
        // choices, and nautobot enforces validation_regex on text only.
        let payload = custom_field_payload(
            "dcim.device",
            "tier",
            &field_schema(
                FieldType::Enum {
                    values: vec!["core".to_string(), "edge".to_string()],
                },
                Some("^[a-z]+$"),
            ),
        );
        assert_eq!(payload.get("type").unwrap(), &json!("select"));
        assert!(payload.get("validation_regex").is_none());
    }

    #[test]
    fn test_missing_choices_weights_by_declared_position() {
        let declared = ["core", "agg", "edge"].map(str::to_string);

        // nothing offered yet is the create: the whole list, in order.
        assert_eq!(
            missing_choices(&declared, None),
            vec![
                ("core".to_string(), 100),
                ("agg".to_string(), 200),
                ("edge".to_string(), 300),
            ],
        );

        // one already offered is left alone, and the rest keep the weight their
        // declared position implies rather than being renumbered around it.
        let current = BTreeSet::from(["core".to_string(), "edge".to_string()]);
        assert_eq!(
            missing_choices(&declared, Some(&current)),
            vec![("agg".to_string(), 200)],
        );

        assert!(
            missing_choices(&declared, Some(&BTreeSet::from_iter(declared.clone()))).is_empty()
        );
    }

    #[test]
    fn test_declared_choices_keeps_declaration_order() {
        let values = vec!["core".to_string(), "agg".to_string(), "edge".to_string()];
        assert_eq!(
            declared_choices(&field_schema(
                FieldType::Enum {
                    values: values.clone()
                },
                None
            )),
            values.as_slice()
        );
        assert_eq!(
            declared_choices(&field_schema(
                FieldType::List {
                    item: Box::new(FieldType::Enum {
                        values: values.clone()
                    })
                },
                None
            )),
            values.as_slice()
        );
        assert!(declared_choices(&field_schema(FieldType::String, None)).is_empty());
    }

    #[test]
    fn test_custom_field_payload_types_a_float_as_json() {
        // nautobot's CustomFieldTypeChoices has no `decimal`, and its `text`
        // reads back as a json string, which the ir's float check rejects.
        let payload = custom_field_payload(
            "dcim.device",
            "ratio",
            &field_schema(FieldType::Float, None),
        );
        assert_eq!(payload.get("type").unwrap(), &json!("json"));
    }
}
