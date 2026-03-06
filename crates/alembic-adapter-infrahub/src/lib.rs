//! infrahub graphql adapter for alembic.

use alembic_core::{FieldType, JsonMap, Key, Schema, TypeName, Uid};
use alembic_engine::{
    apply_non_delete_with_retries, build_key_from_schema, resolve_value_for_type, Adapter,
    AdapterApplyError, AppliedOp, ApplyReport, BackendId, ObservedObject, ObservedState, Op,
    ProvisionReport, RetryApplyDriver, StateStore,
};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use graphql_parser::schema::{parse_schema, Definition, Type as GqlType, TypeDefinition};
use infrahub::{Client, ClientConfig};
use serde::Serialize;
use serde_json::{json, Map, Value};
use serde_yaml::{Mapping as YamlMapping, Value as YamlValue};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::process::Command;
use tokio::time::sleep;

/// schema provisioning mode for infrahub.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaApplyMode {
    Infrahubctl,
    Repository,
}

/// schema provisioning configuration for infrahub.
#[derive(Debug, Clone)]
pub struct SchemaPushConfig {
    pub schema_path: PathBuf,
    pub mode: SchemaApplyMode,
    pub repository_id: Option<String>,
    pub repository_name: Option<String>,
    pub repository_root: Option<PathBuf>,
    pub branch: Option<String>,
    pub infrahubctl_path: Option<PathBuf>,
}

impl SchemaPushConfig {
    pub fn infrahubctl(schema_path: PathBuf) -> Self {
        Self {
            schema_path,
            mode: SchemaApplyMode::Infrahubctl,
            repository_id: None,
            repository_name: None,
            repository_root: None,
            branch: None,
            infrahubctl_path: None,
        }
    }

    pub fn repository(
        schema_path: PathBuf,
        repository_id: String,
        repository_root: PathBuf,
    ) -> Self {
        Self {
            schema_path,
            mode: SchemaApplyMode::Repository,
            repository_id: Some(repository_id),
            repository_name: None,
            repository_root: Some(repository_root),
            branch: None,
            infrahubctl_path: None,
        }
    }
}

/// infrahub adapter.
pub struct InfrahubAdapter {
    client: Client,
    base_url: String,
    token: String,
    schema_push: Option<SchemaPushConfig>,
}

impl InfrahubAdapter {
    pub fn new(url: &str, token: &str, branch: Option<&str>) -> Result<Self> {
        let mut config = ClientConfig::new(url, token);
        if let Some(branch) = branch {
            config = config.with_default_branch(branch);
        }
        let client = Client::new(config)?;
        Ok(Self {
            client,
            base_url: url.to_string(),
            token: token.to_string(),
            schema_push: None,
        })
    }

    pub fn with_schema_push(mut self, schema_push: SchemaPushConfig) -> Self {
        self.schema_push = Some(schema_push);
        self
    }

    async fn load_schema_info(&self) -> Result<SchemaInfo> {
        let raw = self
            .client
            .fetch_schema(None)
            .await
            .context("fetch infrahub schema")?;
        SchemaInfo::parse(&raw)
    }

    async fn read_type_objects(
        &self,
        schema_info: &SchemaInfo,
        type_name: &TypeName,
        type_schema: &alembic_core::TypeSchema,
        mappings: &StateMappings,
    ) -> Result<Vec<ObservedObject>> {
        let gql_type = gql_type_name(type_name);
        let fields = field_names_for_schema(type_schema);
        let field_kinds = schema_info.field_kinds(&gql_type, type_schema, &fields)?;
        let selection = build_selection(&field_kinds);

        let query = format!(
            "query($offset: Int, $limit: Int) {{ {type_name}(offset: $offset, limit: $limit) {{ count edges {{ node {{ id hfid {selection} }} }} }} }}",
            type_name = gql_type,
            selection = selection
        );

        let mut observed = Vec::new();
        let mut offset = 0usize;
        let limit = 200usize;

        loop {
            let vars = json!({
                "offset": offset,
                "limit": limit,
            });
            let response = self
                .client
                .execute_raw(&query, Some(vars), None)
                .await
                .context("execute infrahub query")?;
            let data = response
                .data
                .ok_or_else(|| anyhow!("missing data in infrahub response"))?;
            let connection = data
                .get(&gql_type)
                .ok_or_else(|| anyhow!("missing {} in infrahub response", gql_type))?;
            let edges = connection
                .get("edges")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();

            if edges.is_empty() {
                break;
            }

            for edge in edges {
                let node = edge
                    .get("node")
                    .ok_or_else(|| anyhow!("missing node in infrahub response"))?;
                let backend_id = node
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("missing id in infrahub response"))?
                    .to_string();

                let attrs = extract_attrs(node, &field_kinds)?;
                let attrs = normalize_attrs_refs(&attrs, type_schema, mappings);
                let key = build_key_from_schema(type_schema, &attrs)?;

                observed.push(ObservedObject {
                    type_name: type_name.clone(),
                    key,
                    attrs,
                    backend_id: Some(BackendId::String(backend_id)),
                });
            }

            let count = connection.get("count").and_then(Value::as_u64).unwrap_or(0) as usize;
            offset += limit;
            if count > 0 && offset >= count {
                break;
            }
        }

        Ok(observed)
    }

    async fn apply_create(
        &self,
        op: &Op,
        schema: &Schema,
        resolved: &mut BTreeMap<Uid, BackendId>,
    ) -> Result<AppliedOp> {
        let (uid, type_name, desired) = match op {
            Op::Create {
                uid,
                type_name,
                desired,
            } => (*uid, type_name, desired),
            _ => return Err(anyhow!("expected create op")),
        };
        let type_schema = schema
            .types
            .get(type_name.as_str())
            .ok_or_else(|| anyhow!("missing schema for {}", type_name))?;
        let gql_type = gql_type_name(type_name);

        let data = build_input(&desired.attrs, type_schema, resolved)?;
        let mutation = format!(
            "mutation($data: {type_name}CreateInput!) {{ {type_name}Create(data: $data) {{ ok object {{ id }} }} }}",
            type_name = gql_type
        );

        let response = self
            .client
            .execute_raw(&mutation, Some(json!({ "data": data })), None)
            .await
            .context("execute infrahub create")?;
        let data = response
            .data
            .ok_or_else(|| anyhow!("missing data in infrahub response"))?;
        let root = data
            .get(format!("{}Create", gql_type))
            .ok_or_else(|| anyhow!("missing create response for {}", gql_type))?;
        let backend_id = root
            .get("object")
            .and_then(|obj| obj.get("id"))
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing id in infrahub create response"))?
            .to_string();

        let backend_id = BackendId::String(backend_id);
        resolved.insert(uid, backend_id.clone());

        Ok(AppliedOp {
            uid,
            type_name: type_name.clone(),
            backend_id: Some(backend_id),
        })
    }

    async fn apply_update(
        &self,
        op: &Op,
        schema: &Schema,
        resolved: &BTreeMap<Uid, BackendId>,
    ) -> Result<AppliedOp> {
        let (uid, type_name, desired, backend_id) = match op {
            Op::Update {
                uid,
                type_name,
                desired,
                backend_id,
                ..
            } => (*uid, type_name, desired, backend_id),
            _ => return Err(anyhow!("expected update op")),
        };
        let type_schema = schema
            .types
            .get(type_name.as_str())
            .ok_or_else(|| anyhow!("missing schema for {}", type_name))?;
        let gql_type = gql_type_name(type_name);

        let id = if let Some(BackendId::String(id)) = backend_id {
            id.clone()
        } else if let Some(BackendId::String(id)) = resolved.get(&uid) {
            id.clone()
        } else {
            let key = build_key_from_schema(type_schema, &desired.attrs)?;
            self.lookup_backend_id(type_name, type_schema, &key).await?
        };

        let mut data = build_input(&desired.attrs, type_schema, resolved)?;
        let map = data
            .as_object_mut()
            .ok_or_else(|| anyhow!("expected object for infrahub input"))?;
        map.insert("id".to_string(), Value::String(id.clone()));

        let mutation = format!(
            "mutation($data: {type_name}UpdateInput!) {{ {type_name}Update(data: $data) {{ ok object {{ id }} }} }}",
            type_name = gql_type
        );
        self.client
            .execute_raw(&mutation, Some(json!({ "data": data })), None)
            .await
            .context("execute infrahub update")?;

        Ok(AppliedOp {
            uid,
            type_name: type_name.clone(),
            backend_id: Some(BackendId::String(id)),
        })
    }

    async fn apply_delete(
        &self,
        op: &Op,
        schema: &Schema,
        resolved: &BTreeMap<Uid, BackendId>,
    ) -> Result<AppliedOp> {
        let (uid, type_name, backend_id, key) = match op {
            Op::Delete {
                uid,
                type_name,
                backend_id,
                key,
            } => (*uid, type_name, backend_id, key),
            _ => return Err(anyhow!("expected delete op")),
        };
        let type_schema = schema
            .types
            .get(type_name.as_str())
            .ok_or_else(|| anyhow!("missing schema for {}", type_name))?;
        let gql_type = gql_type_name(type_name);

        let id = if let Some(BackendId::String(id)) = backend_id {
            id.clone()
        } else if let Some(BackendId::String(id)) = resolved.get(&uid) {
            id.clone()
        } else {
            self.lookup_backend_id(type_name, type_schema, key).await?
        };

        let mutation = format!(
            "mutation($data: DeleteInput!) {{ {type_name}Delete(data: $data) {{ ok }} }}",
            type_name = gql_type
        );
        self.client
            .execute_raw(&mutation, Some(json!({ "data": { "id": id } })), None)
            .await
            .context("execute infrahub delete")?;

        Ok(AppliedOp {
            uid,
            type_name: type_name.clone(),
            backend_id: None,
        })
    }

    async fn lookup_backend_id(
        &self,
        type_name: &TypeName,
        type_schema: &alembic_core::TypeSchema,
        key: &Key,
    ) -> Result<String> {
        let schema_info = self.load_schema_info().await?;
        let mappings = StateMappings::default();
        let objects = self
            .read_type_objects(&schema_info, type_name, type_schema, &mappings)
            .await?;
        let key_string = serde_json::to_string(key).unwrap_or_default();
        for object in objects {
            if object.key == *key {
                if let Some(BackendId::String(id)) = object.backend_id {
                    return Ok(id);
                }
            }
        }
        Err(anyhow!("missing infrahub object with key {key_string}"))
    }

    async fn apply_schema_infrahubctl(&self, config: &SchemaPushConfig) -> Result<()> {
        let mut cmd = Command::new(
            config
                .infrahubctl_path
                .as_deref()
                .unwrap_or_else(|| Path::new("infrahubctl")),
        );
        cmd.arg("schema")
            .arg("load")
            .arg(&config.schema_path)
            .env("INFRAHUB_ADDRESS", &self.base_url)
            .env("INFRAHUB_API_TOKEN", &self.token);
        if let Some(branch) = &config.branch {
            cmd.arg("--branch").arg(branch);
        }

        let output = cmd.output().await.context("run infrahubctl schema load")?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(anyhow!(
                "infrahubctl schema load failed: {}\nstdout: {}\nstderr: {}",
                output.status,
                stdout.trim(),
                stderr.trim()
            ));
        }
        Ok(())
    }

    async fn apply_schema_repository(&self, config: &SchemaPushConfig) -> Result<()> {
        let repo_root = config
            .repository_root
            .as_ref()
            .ok_or_else(|| anyhow!("infrahub repository mode requires repository_root"))?;
        ensure_repository_config(repo_root, &config.schema_path)?;

        let repo_id = match (&config.repository_id, &config.repository_name) {
            (Some(id), _) => id.clone(),
            (None, Some(name)) => self.resolve_repository_id(name).await?,
            (None, None) => {
                return Err(anyhow!(
                    "infrahub repository mode requires repository_id or repository_name"
                ))
            }
        };

        self.process_repository(&repo_id).await?;
        Ok(())
    }

    async fn resolve_repository_id(&self, name: &str) -> Result<String> {
        let query = "query($name: String) { CoreRepository(name__value: $name, limit: 1) { edges { node { id } } } }";
        let response = self
            .client
            .execute_raw(query, Some(json!({ "name": name })), None)
            .await
            .context("query infrahub repository")?;
        let data = response
            .data
            .ok_or_else(|| anyhow!("missing data in infrahub repository response"))?;
        let edges = data
            .get("CoreRepository")
            .and_then(|value| value.get("edges"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for edge in edges {
            if let Some(id) = edge
                .get("node")
                .and_then(|node| node.get("id"))
                .and_then(Value::as_str)
            {
                return Ok(id.to_string());
            }
        }
        Err(anyhow!("infrahub repository not found: {name}"))
    }

    async fn process_repository(&self, repo_id: &str) -> Result<()> {
        let mutation = "mutation($data: IdentifierInput!) { InfrahubRepositoryProcess(data: $data) { ok task { id } } }";
        self.client
            .execute_raw(mutation, Some(json!({ "data": { "id": repo_id } })), None)
            .await
            .context("trigger infrahub repository process")?;
        Ok(())
    }
}

#[async_trait]
impl Adapter for InfrahubAdapter {
    async fn read(
        &self,
        schema: &Schema,
        types: &[TypeName],
        state_store: &StateStore,
    ) -> Result<ObservedState> {
        let schema_info = self.load_schema_info().await?;
        validate_schema(schema, &schema_info)?;

        let requested: Vec<TypeName> = if types.is_empty() {
            schema
                .types
                .keys()
                .map(|name| TypeName::new(name.clone()))
                .collect()
        } else {
            types.to_vec()
        };

        let mappings = state_mappings(state_store);
        let mut state = ObservedState::default();
        for type_name in requested {
            let type_schema = schema
                .types
                .get(type_name.as_str())
                .ok_or_else(|| anyhow!("missing schema for {}", type_name))?;
            let objects = self
                .read_type_objects(&schema_info, &type_name, type_schema, &mappings)
                .await?;
            for object in objects {
                state.insert(object);
            }
        }

        Ok(state)
    }

    async fn write(&self, schema: &Schema, ops: &[Op], state: &StateStore) -> Result<ApplyReport> {
        let schema_info = self.load_schema_info().await?;
        validate_schema(schema, &schema_info)?;

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
            adapter: &'a InfrahubAdapter,
            schema: &'a Schema,
            resolved: &'a mut BTreeMap<Uid, BackendId>,
        }

        #[async_trait]
        impl RetryApplyDriver for ApplyDriver<'_> {
            async fn apply_non_delete(&mut self, op: &Op) -> Result<AppliedOp> {
                match op {
                    Op::Create { .. } => {
                        self.adapter
                            .apply_create(op, self.schema, self.resolved)
                            .await
                    }
                    Op::Update { .. } => {
                        self.adapter
                            .apply_update(op, self.schema, self.resolved)
                            .await
                    }
                    Op::Delete { .. } => Err(anyhow!("delete ops not supported here")),
                }
            }

            fn is_retryable(&self, err: &anyhow::Error) -> bool {
                is_missing_ref_error(err)
            }
        }

        let mut driver = ApplyDriver {
            adapter: self,
            schema,
            resolved: &mut resolved,
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
            applied.push(self.apply_delete(&op, schema, &resolved).await?);
        }

        Ok(ApplyReport { applied })
    }

    async fn ensure_schema(&self, schema: &Schema) -> Result<ProvisionReport> {
        let schema_info = self.load_schema_info().await?;
        let Some(plan) = build_provision_plan(schema, &schema_info)? else {
            return Ok(ProvisionReport::default());
        };

        let config = self.schema_push.as_ref().ok_or_else(|| {
            anyhow!(
                "infrahub schema mismatch (configure schema provisioning): {}",
                plan.summary
            )
        })?;

        write_schema_document(&config.schema_path, &plan.document)?;

        match config.mode {
            SchemaApplyMode::Infrahubctl => self.apply_schema_infrahubctl(config).await?,
            SchemaApplyMode::Repository => self.apply_schema_repository(config).await?,
        }

        let mut refreshed = None;
        for _ in 0..5 {
            let schema_info = self.load_schema_info().await?;
            if validate_schema(schema, &schema_info).is_ok() {
                refreshed = Some(schema_info);
                break;
            }
            sleep(Duration::from_secs(1)).await;
        }
        if refreshed.is_none() {
            let schema_info = self.load_schema_info().await?;
            validate_schema(schema, &schema_info)?;
        }

        Ok(plan.report)
    }
}

#[derive(Debug, Clone)]
struct GraphField {
    base_type: String,
    is_list: bool,
}

#[derive(Debug, Default, Clone)]
struct SchemaInfo {
    attribute_types: BTreeSet<String>,
    type_fields: BTreeMap<String, BTreeMap<String, GraphField>>,
}

#[derive(Debug, Clone)]
enum RelationShape {
    RelatedNode,
    NestedEdged,
    NestedPaginated,
}

#[derive(Debug, Clone)]
enum FieldKind {
    Attribute,
    RelationSingle(RelationShape),
    RelationList(RelationShape),
}

impl SchemaInfo {
    fn parse(raw: &str) -> Result<Self> {
        let document = parse_schema::<String>(raw).map_err(|err| anyhow!(err.to_string()))?;
        let mut attribute_types = BTreeSet::new();
        let mut type_fields = BTreeMap::new();

        for def in document.definitions {
            let Definition::TypeDefinition(TypeDefinition::Object(obj)) = def else {
                continue;
            };
            if obj
                .implements_interfaces
                .iter()
                .any(|iface| iface == "AttributeInterface")
            {
                attribute_types.insert(obj.name.clone());
            }
            let mut fields = BTreeMap::new();
            for field in obj.fields {
                let (base_type, is_list) = unwrap_type(&field.field_type);
                fields.insert(field.name.clone(), GraphField { base_type, is_list });
            }
            type_fields.insert(obj.name.clone(), fields);
        }

        Ok(Self {
            attribute_types,
            type_fields,
        })
    }

    fn field_kinds(
        &self,
        type_name: &str,
        type_schema: &alembic_core::TypeSchema,
        fields: &[String],
    ) -> Result<BTreeMap<String, FieldKind>> {
        let info = self
            .type_fields
            .get(type_name)
            .ok_or_else(|| anyhow!("infrahub schema missing type {}", type_name))?;
        let mut kinds = BTreeMap::new();
        for field in fields {
            let graph = info
                .get(field)
                .ok_or_else(|| anyhow!("infrahub schema missing field {}.{}", type_name, field))?;
            let kind = self.kind_for_field(graph);
            let field_schema = field_schema_for(type_schema, field)
                .ok_or_else(|| anyhow!("missing alembic schema for {}.{}", type_name, field))?;
            validate_kind(type_name, field, &field_schema.r#type, &kind)?;
            kinds.insert(field.clone(), kind);
        }
        Ok(kinds)
    }

    fn kind_for_field(&self, graph: &GraphField) -> FieldKind {
        if self.attribute_types.contains(&graph.base_type) {
            return FieldKind::Attribute;
        }
        if graph.base_type == "RelatedNode" {
            return if graph.is_list {
                FieldKind::RelationList(RelationShape::RelatedNode)
            } else {
                FieldKind::RelationSingle(RelationShape::RelatedNode)
            };
        }
        if graph.base_type.starts_with("NestedPaginated") {
            return FieldKind::RelationList(RelationShape::NestedPaginated);
        }
        if graph.base_type.starts_with("NestedEdged") {
            return FieldKind::RelationSingle(RelationShape::NestedEdged);
        }
        FieldKind::Attribute
    }
}

fn unwrap_type(field_type: &GqlType<String>) -> (String, bool) {
    match field_type {
        GqlType::NamedType(name) => (name.clone(), false),
        GqlType::ListType(inner) => {
            let (name, _inner_list) = unwrap_type(inner);
            (name, true)
        }
        GqlType::NonNullType(inner) => unwrap_type(inner),
    }
}

fn field_names_for_schema(type_schema: &alembic_core::TypeSchema) -> Vec<String> {
    let mut fields = BTreeSet::new();
    for field in type_schema.key.keys() {
        fields.insert(field.clone());
    }
    for field in type_schema.fields.keys() {
        fields.insert(field.clone());
    }
    fields.into_iter().collect()
}

fn build_selection(field_kinds: &BTreeMap<String, FieldKind>) -> String {
    let mut parts = Vec::new();
    for (field, kind) in field_kinds {
        let selection = match kind {
            FieldKind::Attribute => format!("{field} {{ value }}"),
            FieldKind::RelationSingle(RelationShape::RelatedNode) => {
                format!("{field} {{ id kind }}")
            }
            FieldKind::RelationSingle(RelationShape::NestedEdged) => {
                format!("{field} {{ node {{ id }} }}")
            }
            FieldKind::RelationSingle(RelationShape::NestedPaginated) => {
                format!("{field} {{ node {{ id }} }}")
            }
            FieldKind::RelationList(RelationShape::RelatedNode) => {
                format!("{field} {{ id kind }}")
            }
            FieldKind::RelationList(RelationShape::NestedPaginated) => {
                format!("{field} {{ edges {{ node {{ id }} }} }}")
            }
            FieldKind::RelationList(RelationShape::NestedEdged) => {
                format!("{field} {{ node {{ id }} }}")
            }
        };
        parts.push(selection);
    }
    parts.join("\n")
}

fn extract_attrs(node: &Value, field_kinds: &BTreeMap<String, FieldKind>) -> Result<JsonMap> {
    let mut map = BTreeMap::new();
    for (field, kind) in field_kinds {
        let value = extract_field_value(node, field, kind)?;
        if let Some(value) = value {
            map.insert(field.clone(), value);
        }
    }
    Ok(JsonMap::from(map))
}

fn extract_field_value(node: &Value, field: &str, kind: &FieldKind) -> Result<Option<Value>> {
    let Some(field_val) = node.get(field) else {
        return Ok(None);
    };
    if field_val.is_null() {
        return Ok(None);
    }
    let value = match kind {
        FieldKind::Attribute => field_val.get("value").cloned().unwrap_or(Value::Null),
        FieldKind::RelationSingle(RelationShape::RelatedNode) => field_val
            .get("id")
            .and_then(Value::as_str)
            .map(|s| Value::String(s.to_string()))
            .unwrap_or(Value::Null),
        FieldKind::RelationSingle(RelationShape::NestedEdged) => field_val
            .get("node")
            .and_then(|node| node.get("id"))
            .and_then(Value::as_str)
            .map(|s| Value::String(s.to_string()))
            .unwrap_or(Value::Null),
        FieldKind::RelationSingle(RelationShape::NestedPaginated) => field_val
            .get("node")
            .and_then(|node| node.get("id"))
            .and_then(Value::as_str)
            .map(|s| Value::String(s.to_string()))
            .unwrap_or(Value::Null),
        FieldKind::RelationList(RelationShape::RelatedNode) => {
            let items = field_val
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|item| {
                    item.get("id")
                        .and_then(Value::as_str)
                        .map(|s| s.to_string())
                })
                .map(Value::String)
                .collect::<Vec<_>>();
            Value::Array(items)
        }
        FieldKind::RelationList(RelationShape::NestedPaginated) => {
            let items = field_val
                .get("edges")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|edge| {
                    edge.get("node")
                        .and_then(|node| node.get("id"))
                        .and_then(Value::as_str)
                        .map(|s| s.to_string())
                })
                .map(Value::String)
                .collect::<Vec<_>>();
            Value::Array(items)
        }
        FieldKind::RelationList(RelationShape::NestedEdged) => {
            let items = field_val
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|edge| {
                    edge.get("node")
                        .and_then(|node| node.get("id"))
                        .and_then(Value::as_str)
                        .map(|s| s.to_string())
                })
                .map(Value::String)
                .collect::<Vec<_>>();
            Value::Array(items)
        }
    };
    if value.is_null() {
        Ok(None)
    } else {
        Ok(Some(value))
    }
}

#[derive(Debug, Default)]
struct SchemaMissing {
    types: Vec<String>,
    fields: Vec<String>,
}

impl SchemaMissing {
    fn is_empty(&self) -> bool {
        self.types.is_empty() && self.fields.is_empty()
    }

    fn summary(&self) -> String {
        let mut parts = Vec::new();
        if !self.types.is_empty() {
            parts.push(format!("missing types: {}", self.types.join(", ")));
        }
        if !self.fields.is_empty() {
            parts.push(format!("missing fields: {}", self.fields.join(", ")));
        }
        parts.join("; ")
    }
}

fn schema_missing(schema: &Schema, schema_info: &SchemaInfo) -> SchemaMissing {
    let mut missing = SchemaMissing::default();
    for (type_name, type_schema) in &schema.types {
        let gql_type = gql_type_name_str(type_name);
        if !schema_info.type_fields.contains_key(&gql_type) {
            missing.types.push(type_name.clone());
            continue;
        }
        let fields = field_names_for_schema(type_schema);
        for field in fields {
            if schema_info
                .type_fields
                .get(&gql_type)
                .and_then(|fields| fields.get(&field))
                .is_none()
            {
                missing.fields.push(format!("{type_name}.{field}"));
            }
        }
    }
    missing
}

fn validate_schema(schema: &Schema, schema_info: &SchemaInfo) -> Result<()> {
    let missing = schema_missing(schema, schema_info);
    if missing.is_empty() {
        return Ok(());
    }
    Err(anyhow!(
        "infrahub schema mismatch (define missing types/fields before apply): {}",
        missing.summary()
    ))
}

#[derive(Debug)]
struct ProvisionPlan {
    document: SchemaDocument,
    report: ProvisionReport,
    summary: String,
}

fn build_provision_plan(
    schema: &Schema,
    schema_info: &SchemaInfo,
) -> Result<Option<ProvisionPlan>> {
    let missing = schema_missing(schema, schema_info);
    if missing.is_empty() {
        return Ok(None);
    }

    let mut nodes = Vec::new();
    let mut extensions = Vec::new();
    let mut created_object_types = Vec::new();
    let mut created_object_fields = Vec::new();

    for (type_name, type_schema) in &schema.types {
        let gql_type = gql_type_name_str(type_name);
        let Some(existing_fields) = schema_info.type_fields.get(&gql_type) else {
            let parts = type_name_parts(type_name)?;
            let (attributes, relationships, key_attrs) = collect_field_defs(type_schema, None)?;
            let mut human_friendly_id = Vec::new();
            if !key_attrs.is_empty() {
                human_friendly_id.extend(key_attrs.iter().map(|key| format!("{key}__value")));
            }
            let (display_label, default_filter) = display_label_for_keys(&key_attrs);
            let label = label_from_pascal(&parts.name);
            nodes.push(NodeDef {
                name: parts.name,
                namespace: parts.namespace,
                label: Some(label),
                description: None,
                human_friendly_id,
                display_label,
                default_filter,
                attributes,
                relationships,
            });
            created_object_types.push(type_name.clone());
            for field in field_names_for_schema(type_schema) {
                created_object_fields.push(format!("{type_name}.{field}"));
            }
            continue;
        };

        let mut missing_fields = BTreeSet::new();
        for field in field_names_for_schema(type_schema) {
            if !existing_fields.contains_key(&field) {
                missing_fields.insert(field.clone());
                created_object_fields.push(format!("{type_name}.{field}"));
            }
        }
        if missing_fields.is_empty() {
            continue;
        }
        let (attributes, relationships, _key_attrs) =
            collect_field_defs(type_schema, Some(&missing_fields))?;
        if attributes.is_empty() && relationships.is_empty() {
            continue;
        }
        extensions.push(NodeExtension {
            kind: gql_type,
            attributes,
            relationships,
        });
    }

    let document = SchemaDocument {
        version: "1.0".to_string(),
        nodes,
        extensions: SchemaExtensions { nodes: extensions },
    };

    let report = ProvisionReport {
        created_fields: Vec::new(),
        created_tags: Vec::new(),
        created_object_types,
        created_object_fields,
    };

    Ok(Some(ProvisionPlan {
        document,
        report,
        summary: missing.summary(),
    }))
}

fn write_schema_document(path: &Path, document: &SchemaDocument) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create schema directory {}", parent.display()))?;
    }
    let raw = serde_yaml::to_string(document).context("serialize infrahub schema")?;
    fs::write(path, raw).with_context(|| format!("write schema {}", path.display()))?;
    Ok(())
}

fn ensure_repository_config(repo_root: &Path, schema_path: &Path) -> Result<()> {
    let rel_path = schema_path
        .strip_prefix(repo_root)
        .with_context(|| {
            format!(
                "schema path {} must be inside repository root {}",
                schema_path.display(),
                repo_root.display()
            )
        })?
        .to_string_lossy()
        .replace('\\', "/");

    let config_path = repo_root.join(".infrahub.yml");
    let mut root = if config_path.exists() {
        let raw = fs::read_to_string(&config_path)
            .with_context(|| format!("read {}", config_path.display()))?;
        serde_yaml::from_str::<YamlValue>(&raw)
            .with_context(|| format!("parse {}", config_path.display()))?
    } else {
        YamlValue::Mapping(YamlMapping::new())
    };

    let mapping = root
        .as_mapping_mut()
        .ok_or_else(|| anyhow!(".infrahub.yml must be a mapping"))?;
    let schemas_key = YamlValue::String("schemas".to_string());
    let entry = mapping
        .entry(schemas_key)
        .or_insert_with(|| YamlValue::Sequence(Vec::new()));
    let list = entry
        .as_sequence_mut()
        .ok_or_else(|| anyhow!("schemas must be a list in .infrahub.yml"))?;
    if !list.iter().any(|v| v.as_str() == Some(&rel_path)) {
        list.push(YamlValue::String(rel_path));
    }

    let raw = serde_yaml::to_string(&root).context("serialize .infrahub.yml")?;
    fs::write(&config_path, raw).with_context(|| format!("write {}", config_path.display()))?;
    Ok(())
}

#[derive(Debug, Serialize)]
struct SchemaDocument {
    version: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    nodes: Vec<NodeDef>,
    #[serde(skip_serializing_if = "SchemaExtensions::is_empty")]
    extensions: SchemaExtensions,
}

#[derive(Debug, Default, Serialize)]
struct SchemaExtensions {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    nodes: Vec<NodeExtension>,
}

impl SchemaExtensions {
    fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

#[derive(Debug, Serialize)]
struct NodeDef {
    name: String,
    namespace: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    human_friendly_id: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    display_label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    default_filter: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    attributes: Vec<AttributeDef>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    relationships: Vec<RelationshipDef>,
}

#[derive(Debug, Serialize)]
struct NodeExtension {
    kind: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    attributes: Vec<AttributeDef>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    relationships: Vec<RelationshipDef>,
}

#[derive(Debug, Serialize)]
struct AttributeDef {
    name: String,
    kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    optional: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unique: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", rename = "enum")]
    enum_values: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    choices: Vec<ChoiceDef>,
}

#[derive(Debug, Serialize)]
struct ChoiceDef {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    color: Option<String>,
}

#[derive(Debug, Serialize)]
struct RelationshipDef {
    name: String,
    peer: String,
    kind: String,
    cardinality: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    optional: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    direction: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

#[derive(Debug, Clone)]
struct TypeNameParts {
    namespace: String,
    name: String,
}

fn type_name_parts(type_name: &str) -> Result<TypeNameParts> {
    if let Some((namespace_raw, name_raw)) = type_name.split_once('.') {
        let namespace = to_pascal_case(namespace_raw);
        let name = to_pascal_case(name_raw);
        return Ok(TypeNameParts { namespace, name });
    }

    let parts = split_camel_case(type_name);
    if parts.len() >= 2 {
        let namespace = parts[0].clone();
        let name = parts[1..].join("");
        return Ok(TypeNameParts { namespace, name });
    }

    Err(anyhow!(
        "infrahub schema provisioning requires namespaced types (e.g. dcim.site)"
    ))
}

fn gql_type_name(type_name: &TypeName) -> String {
    gql_type_name_str(type_name.as_str())
}

fn gql_type_name_str(type_name: &str) -> String {
    if let Some((namespace_raw, name_raw)) = type_name.split_once('.') {
        let namespace = to_pascal_case(namespace_raw);
        let name = to_pascal_case(name_raw);
        return format!("{namespace}{name}");
    }
    type_name.to_string()
}

fn collect_field_defs(
    type_schema: &alembic_core::TypeSchema,
    include_fields: Option<&BTreeSet<String>>,
) -> Result<(Vec<AttributeDef>, Vec<RelationshipDef>, Vec<String>)> {
    let mut attributes = Vec::new();
    let mut relationships = Vec::new();
    let mut key_attrs = Vec::new();
    let mut seen = BTreeSet::new();

    let mut handle_field =
        |field: &str, schema: &alembic_core::FieldSchema, is_key: bool| -> Result<()> {
            if let Some(include) = include_fields {
                if !include.contains(field) {
                    return Ok(());
                }
            }
            if !seen.insert(field.to_string()) {
                return Ok(());
            }
            match &schema.r#type {
                FieldType::Ref { target } => {
                    relationships.push(relationship_def(field, target, schema, "one")?);
                }
                FieldType::ListRef { target } => {
                    relationships.push(relationship_def(field, target, schema, "many")?);
                }
                _ => {
                    attributes.push(attribute_def(field, schema, is_key)?);
                    if is_key {
                        key_attrs.push(field.to_string());
                    }
                }
            }
            Ok(())
        };

    for (field, schema) in &type_schema.key {
        handle_field(field, schema, true)?;
    }
    for (field, schema) in &type_schema.fields {
        handle_field(field, schema, false)?;
    }

    Ok((attributes, relationships, key_attrs))
}

fn attribute_def(
    field: &str,
    schema: &alembic_core::FieldSchema,
    is_key: bool,
) -> Result<AttributeDef> {
    let kind = attribute_kind_for_field(&schema.r#type);
    let optional = Some(!schema.required);
    let unique = if is_key { Some(true) } else { None };
    let enum_values = Vec::new();
    let mut choices = Vec::new();
    if let FieldType::Enum { values } = &schema.r#type {
        for value in values {
            choices.push(ChoiceDef {
                name: value.clone(),
                description: None,
                color: None,
            });
        }
    }

    Ok(AttributeDef {
        name: field.to_string(),
        kind,
        optional,
        unique,
        description: schema.description.clone(),
        enum_values,
        choices,
    })
}

fn relationship_def(
    field: &str,
    target: &str,
    schema: &alembic_core::FieldSchema,
    cardinality: &str,
) -> Result<RelationshipDef> {
    Ok(RelationshipDef {
        name: field.to_string(),
        peer: gql_type_name_str(target),
        kind: "Attribute".to_string(),
        cardinality: cardinality.to_string(),
        optional: Some(!schema.required),
        direction: Some("outbound".to_string()),
        description: schema.description.clone(),
    })
}

fn attribute_kind_for_field(field_type: &FieldType) -> String {
    match field_type {
        FieldType::String | FieldType::Text | FieldType::Uuid | FieldType::Slug => {
            "Text".to_string()
        }
        FieldType::Enum { .. } => "Dropdown".to_string(),
        FieldType::Int | FieldType::Float => "Number".to_string(),
        FieldType::Bool => "Boolean".to_string(),
        FieldType::Date | FieldType::Datetime | FieldType::Time => "DateTime".to_string(),
        FieldType::Json | FieldType::Map { .. } => "JSON".to_string(),
        FieldType::List { .. } => "List".to_string(),
        FieldType::IpAddress => "IPHost".to_string(),
        FieldType::Cidr | FieldType::Prefix => "IPNetwork".to_string(),
        FieldType::Mac => "MacAddress".to_string(),
        FieldType::Ref { .. } | FieldType::ListRef { .. } => "Text".to_string(),
    }
}

fn display_label_for_keys(keys: &[String]) -> (Option<String>, Option<String>) {
    let Some(primary) = keys.first() else {
        return (None, None);
    };
    (
        Some(format!("{{{{ {}__value }}}}", primary)),
        Some(format!("{}__value", primary)),
    )
}

fn label_from_pascal(name: &str) -> String {
    let mut label = String::new();
    for (idx, ch) in name.chars().enumerate() {
        if idx > 0 && ch.is_uppercase() {
            label.push(' ');
        }
        label.push(ch);
    }
    label
}

fn to_pascal_case(raw: &str) -> String {
    raw.split(['_', '-', ' ', '.'])
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            let Some(first) = chars.next() else {
                return String::new();
            };
            let mut out = String::new();
            out.extend(first.to_uppercase());
            out.push_str(&chars.as_str().to_lowercase());
            out
        })
        .collect::<Vec<_>>()
        .join("")
}

fn split_camel_case(raw: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    for ch in raw.chars() {
        if ch.is_uppercase() && !current.is_empty() {
            parts.push(current.clone());
            current.clear();
        }
        current.push(ch);
    }
    if !current.is_empty() {
        parts.push(current);
    }
    parts
}

fn validate_kind(
    type_name: &str,
    field: &str,
    field_type: &FieldType,
    kind: &FieldKind,
) -> Result<()> {
    match field_type {
        FieldType::Ref { .. } => match kind {
            FieldKind::RelationSingle(_) => Ok(()),
            _ => Err(anyhow!(
                "expected {}.{} to be a single relation in infrahub",
                type_name,
                field
            )),
        },
        FieldType::ListRef { .. } => match kind {
            FieldKind::RelationList(_) => Ok(()),
            _ => Err(anyhow!(
                "expected {}.{} to be a list relation in infrahub",
                type_name,
                field
            )),
        },
        _ => match kind {
            FieldKind::Attribute => Ok(()),
            _ => Err(anyhow!(
                "expected {}.{} to be an attribute in infrahub",
                type_name,
                field
            )),
        },
    }
}

fn field_schema_for<'a>(
    type_schema: &'a alembic_core::TypeSchema,
    field: &str,
) -> Option<&'a alembic_core::FieldSchema> {
    type_schema
        .fields
        .get(field)
        .or_else(|| type_schema.key.get(field))
}

fn build_input(
    attrs: &JsonMap,
    type_schema: &alembic_core::TypeSchema,
    resolved: &BTreeMap<Uid, BackendId>,
) -> Result<Value> {
    let mut map = Map::new();
    for (field, value) in attrs.iter() {
        let field_schema = field_schema_for(type_schema, field)
            .ok_or_else(|| anyhow!("missing schema for field {field}"))?;
        if value.is_null() {
            map.insert(field.clone(), Value::Null);
            continue;
        }
        validate_value(field, &field_schema.r#type, value)?;
        let resolved_value = resolve_value_for_type(
            &field_schema.r#type,
            value.clone(),
            resolved,
            |id| match id {
                BackendId::Int(n) => json!({ "id": n.to_string() }),
                BackendId::String(s) => json!({ "id": s }),
            },
        )?;
        match field_schema.r#type {
            FieldType::Ref { .. } | FieldType::ListRef { .. } => {
                map.insert(field.clone(), resolved_value);
            }
            _ => {
                map.insert(field.clone(), json!({ "value": resolved_value }));
            }
        }
    }
    Ok(Value::Object(map))
}

fn validate_value(field: &str, field_type: &FieldType, value: &Value) -> Result<()> {
    if value.is_null() {
        return Ok(());
    }
    match field_type {
        FieldType::String
        | FieldType::Text
        | FieldType::Uuid
        | FieldType::Date
        | FieldType::Datetime
        | FieldType::Time
        | FieldType::IpAddress
        | FieldType::Cidr
        | FieldType::Prefix
        | FieldType::Mac
        | FieldType::Slug
        | FieldType::Enum { .. } => {
            if !value.is_string() {
                return Err(anyhow!("field {field} expects a string"));
            }
        }
        FieldType::Int => {
            if value.as_i64().is_none() && value.as_u64().is_none() {
                return Err(anyhow!("field {field} expects an integer"));
            }
        }
        FieldType::Float => {
            if !value.is_number() {
                return Err(anyhow!("field {field} expects a number"));
            }
        }
        FieldType::Bool => {
            if !value.is_boolean() {
                return Err(anyhow!("field {field} expects a boolean"));
            }
        }
        FieldType::List { .. } => {
            if !value.is_array() {
                return Err(anyhow!("field {field} expects a list"));
            }
        }
        FieldType::Map { .. } => {
            if !value.is_object() {
                return Err(anyhow!("field {field} expects a map"));
            }
        }
        FieldType::Json => {}
        FieldType::Ref { .. } => {
            if !value.is_string() {
                return Err(anyhow!("field {field} expects a ref uid string"));
            }
        }
        FieldType::ListRef { .. } => {
            if !value.is_array() {
                return Err(anyhow!("field {field} expects a list of ref uids"));
            }
        }
    }
    Ok(())
}

#[derive(Debug, Default, Clone)]
struct StateMappings {
    by_type: BTreeMap<String, BTreeMap<BackendId, Uid>>,
}

impl StateMappings {
    fn uid_for(&self, type_name: &str, backend_id: &BackendId) -> Option<Uid> {
        self.by_type
            .get(type_name)
            .and_then(|mapping| mapping.get(backend_id).copied())
    }
}

fn state_mappings(state: &StateStore) -> StateMappings {
    let mut by_type = BTreeMap::new();
    for (type_name, mapping) in state.all_mappings() {
        let mut id_to_uid = BTreeMap::new();
        for (uid, backend_id) in mapping {
            id_to_uid.insert(backend_id.clone(), *uid);
        }
        by_type.insert(type_name.as_str().to_string(), id_to_uid);
    }
    StateMappings { by_type }
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

fn normalize_attrs_refs(
    attrs: &JsonMap,
    type_schema: &alembic_core::TypeSchema,
    mappings: &StateMappings,
) -> JsonMap {
    let mut normalized = attrs.clone();
    for (field, schema) in &type_schema.fields {
        match &schema.r#type {
            FieldType::Ref { target } => {
                if let Some(value) = attrs.get(field) {
                    normalized.insert(
                        field.clone(),
                        normalize_ref_value(value.clone(), target, mappings),
                    );
                }
            }
            FieldType::ListRef { target } => {
                if let Some(value) = attrs.get(field) {
                    let updated = if let Value::Array(items) = value {
                        let mapped = items
                            .iter()
                            .cloned()
                            .map(|item| normalize_ref_value(item, target, mappings))
                            .collect::<Vec<_>>();
                        Value::Array(mapped)
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

fn normalize_ref_value(value: Value, target: &str, mappings: &StateMappings) -> Value {
    if value.is_null() {
        return value;
    }
    let backend_id = match backend_id_from_value(&value) {
        Some(id) => id,
        None => return value,
    };
    mappings
        .uid_for(target, &backend_id)
        .map(|uid| Value::String(uid.to_string()))
        .unwrap_or(value)
}

fn backend_id_from_value(value: &Value) -> Option<BackendId> {
    match value {
        Value::Number(n) => n.as_u64().map(BackendId::Int).or_else(|| {
            n.as_i64()
                .and_then(|v| u64::try_from(v).ok())
                .map(BackendId::Int)
        }),
        Value::String(s) => Some(BackendId::String(s.clone())),
        Value::Object(map) => map.get("id").and_then(backend_id_from_value),
        _ => None,
    }
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
                if let Some(uid) = extract_ref_uid(value) {
                    if !resolved.contains_key(&uid) {
                        missing.insert(uid);
                    }
                }
            }
        }
    }
    missing
        .into_iter()
        .map(|uid| uid.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn extract_ref_uid(value: &Value) -> Option<Uid> {
    match value {
        Value::String(raw) => Uid::parse_str(raw).ok(),
        Value::Array(items) => items.iter().find_map(extract_ref_uid),
        _ => None,
    }
}
