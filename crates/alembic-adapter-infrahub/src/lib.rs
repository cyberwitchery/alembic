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
use reqwest::header::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
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
    api: reqwest::Client,
    base_url: String,
    token: String,
    schema_push: Option<SchemaPushConfig>,
}

impl InfrahubAdapter {
    pub fn new(url: &str, token: &str, branch: Option<&str>) -> Result<Self> {
        let mut config = ClientConfig::new(url, token);
        config = config.with_http_client_builder(|builder| builder.no_proxy());
        if let Some(branch) = branch {
            config = config.with_default_branch(branch);
        }
        let client = Client::new(config)?;
        let mut headers = HeaderMap::new();
        headers.insert(
            "X-INFRAHUB-KEY",
            HeaderValue::from_str(token)
                .map_err(|err| anyhow!("invalid infrahub token header: {err}"))?,
        );
        let api = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(30))
            .no_proxy()
            .build()
            .context("build infrahub http client")?;
        Ok(Self {
            client,
            api,
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

    async fn load_schema_snapshot(&self) -> Result<SchemaSnapshot> {
        let base = self.base_url.trim_end_matches('/');
        let url = format!("{base}/api/schema");
        let response = self
            .api
            .get(url)
            .send()
            .await
            .context("fetch infrahub schema snapshot")?;
        let status = response.status();
        let text = response
            .text()
            .await
            .context("read infrahub schema snapshot")?;
        if !status.is_success() {
            return Err(anyhow!("infrahub schema snapshot http error: {}", status));
        }
        serde_json::from_str(&text).context("parse infrahub schema snapshot")
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
        let schema_snapshot = self.load_schema_snapshot().await?;
        let Some(plan) = build_provision_plan(schema, &schema_info, &schema_snapshot)? else {
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

#[derive(Debug, Default, Clone, Deserialize)]
struct SchemaSnapshot {
    #[serde(default)]
    nodes: Vec<SchemaNodeSnapshot>,
}

#[derive(Debug, Default, Clone, Deserialize)]
struct SchemaNodeSnapshot {
    name: String,
    namespace: String,
    #[serde(default)]
    inherit_from: Vec<String>,
    #[serde(default)]
    include_in_menu: bool,
}

impl SchemaNodeSnapshot {
    fn key(&self) -> NodeKey {
        NodeKey::new(self.namespace.clone(), self.name.clone())
    }

    fn qualified_name(&self) -> String {
        format!("{}.{}", self.namespace, self.name)
    }
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
    schema_snapshot: &SchemaSnapshot,
) -> Result<Option<ProvisionPlan>> {
    let missing = schema_missing(schema, schema_info);
    let menu_anchors = menu_anchor_map(schema)?;
    let mut nodes = Vec::new();
    let mut extensions = Vec::new();
    let mut created_object_types = Vec::new();
    let mut created_object_fields = Vec::new();
    let mut deprecated_object_types = Vec::new();

    let mut desired_node_keys = BTreeSet::new();
    for type_name in schema.types.keys() {
        let parts = type_name_parts(type_name)?;
        desired_node_keys.insert(NodeKey::new(parts.namespace, parts.name));
    }

    for (type_name, type_schema) in &schema.types {
        let gql_type = gql_type_name_str(type_name);
        let parts = type_name_parts(type_name)?;
        let menu_placement = menu_placement_for(&menu_anchors, &parts, &gql_type);
        let menu_node = NodeDef {
            name: parts.name.clone(),
            namespace: parts.namespace.clone(),
            label: None,
            description: None,
            icon: None,
            include_in_menu: Some(false),
            menu_placement,
            inherit_from: Vec::new(),
            human_friendly_id: Vec::new(),
            display_label: None,
            default_filter: None,
            attributes: Vec::new(),
            relationships: Vec::new(),
        };

        let Some(existing_fields) = schema_info.type_fields.get(&gql_type) else {
            let (attributes, relationships, key_attrs) =
                collect_field_defs(type_name, type_schema, None)?;
            let mut human_friendly_id = Vec::new();
            if !key_attrs.is_empty() {
                human_friendly_id.extend(key_attrs.iter().map(|key| format!("{key}__value")));
            }
            let (display_label, default_filter) = display_label_for_keys(&key_attrs);
            let name = parts.name.clone();
            let namespace = parts.namespace.clone();
            let label = label_from_pascal(&name);
            nodes.push(NodeDef {
                name,
                namespace: namespace.clone(),
                label: Some(label),
                description: None,
                icon: None,
                include_in_menu: Some(false),
                menu_placement: menu_node.menu_placement.clone(),
                inherit_from: Vec::new(),
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

        nodes.push(menu_node);

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
            collect_field_defs(type_name, type_schema, Some(&missing_fields))?;
        if attributes.is_empty() && relationships.is_empty() {
            continue;
        }
        extensions.push(NodeExtension {
            kind: gql_type,
            attributes,
            relationships,
        });
    }

    for node in &schema_snapshot.nodes {
        if !node.include_in_menu {
            continue;
        }
        let key = node.key();
        if desired_node_keys.contains(&key) {
            continue;
        }
        deprecated_object_types.push(node.qualified_name());
        nodes.push(NodeDef {
            name: node.name.clone(),
            namespace: node.namespace.clone(),
            label: None,
            description: None,
            icon: None,
            include_in_menu: Some(false),
            menu_placement: None,
            human_friendly_id: Vec::new(),
            display_label: None,
            default_filter: None,
            attributes: Vec::new(),
            relationships: Vec::new(),
            inherit_from: node.inherit_from.clone(),
        });
    }

    if nodes.is_empty() && extensions.is_empty() {
        return Ok(None);
    }

    let document = SchemaDocument {
        version: "1.0".to_string(),
        nodes,
        extensions: SchemaExtensions { nodes: extensions },
    };

    let mut summary = missing.summary();
    if !deprecated_object_types.is_empty() {
        if !summary.is_empty() {
            summary.push_str("; ");
        }
        summary.push_str(&format!(
            "stale menu types: {}",
            deprecated_object_types.join(", ")
        ));
    }

    let report = ProvisionReport {
        created_fields: Vec::new(),
        created_tags: Vec::new(),
        created_object_types,
        created_object_fields,
        deprecated_object_types,
        deprecated_object_fields: Vec::new(),
        deleted_object_types: Vec::new(),
        deleted_object_fields: Vec::new(),
    };

    Ok(Some(ProvisionPlan {
        document,
        report,
        summary,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    icon: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    include_in_menu: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    menu_placement: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    inherit_from: Vec<String>,
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
    identifier: Option<String>,
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct NodeKey {
    namespace: String,
    name: String,
}

impl NodeKey {
    fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }
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
        "infrahub schema provisioning requires namespaced types (e.g. namespace.type)"
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
    type_name: &str,
    type_schema: &alembic_core::TypeSchema,
    include_fields: Option<&BTreeSet<String>>,
) -> Result<(Vec<AttributeDef>, Vec<RelationshipDef>, Vec<String>)> {
    let mut attributes = Vec::new();
    let mut relationships = Vec::new();
    let mut key_attrs = Vec::new();
    let mut seen = BTreeSet::new();
    let source_kind = identifier_part(&gql_type_name_str(type_name));

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
                    relationships.push(relationship_def(
                        field,
                        target,
                        schema,
                        "one",
                        &source_kind,
                    )?);
                }
                FieldType::ListRef { target } => {
                    relationships.push(relationship_def(
                        field,
                        target,
                        schema,
                        "many",
                        &source_kind,
                    )?);
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
    source_kind: &str,
) -> Result<RelationshipDef> {
    Ok(RelationshipDef {
        name: field.to_string(),
        peer: gql_type_name_str(target),
        kind: "Attribute".to_string(),
        cardinality: cardinality.to_string(),
        identifier: Some(relationship_identifier(source_kind, field)),
        optional: Some(!schema.required),
        direction: Some("outbound".to_string()),
        description: schema.description.clone(),
    })
}

fn relationship_identifier(source_kind: &str, field: &str) -> String {
    format!(
        "{}__{}",
        identifier_part(source_kind),
        identifier_part(field)
    )
}

fn identifier_part(raw: &str) -> String {
    raw.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
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

fn menu_anchor_map(schema: &Schema) -> Result<BTreeMap<String, String>> {
    let mut by_namespace: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
    for type_name in schema.types.keys() {
        let parts = type_name_parts(type_name)?;
        by_namespace
            .entry(parts.namespace)
            .or_default()
            .push((parts.name, type_name.clone()));
    }

    let mut anchors = BTreeMap::new();
    for (namespace, entries) in by_namespace {
        let anchor = pick_menu_anchor(&entries);
        anchors.insert(namespace, anchor);
    }
    Ok(anchors)
}

fn pick_menu_anchor(entries: &[(String, String)]) -> String {
    entries
        .iter()
        .map(|(_, type_name)| type_name)
        .min()
        .cloned()
        .unwrap_or_default()
}

fn menu_placement_for(
    anchors: &BTreeMap<String, String>,
    parts: &TypeNameParts,
    gql_type: &str,
) -> Option<String> {
    let anchor = anchors
        .get(&parts.namespace)
        .map(|type_name| gql_type_name_str(type_name))?;
    if anchor == gql_type {
        None
    } else {
        Some(anchor)
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use alembic_core::{
        key_string, FieldSchema, FieldType, JsonMap, Key, Object, Schema, TypeName, TypeSchema,
    };
    use alembic_engine::{AdapterApplyError, BackendId, Op, StateData, StateStore};
    use httpmock::prelude::*;
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    const GRAPHQL_SCHEMA: &str = r#"
interface AttributeInterface { value: String }
type TextAttribute implements AttributeInterface { value: String }
type RelatedNode { id: String kind: String }
type Owner { id: String }
type Peer { id: String }
type NestedEdgedOwner { node: Owner }
type NestedPaginatedPeerEdge { node: Peer }
type NestedPaginatedPeerConnection { edges: [NestedPaginatedPeerEdge] }
type DcimSite {
  id: ID
  hfid: String
  name: TextAttribute
  parent: RelatedNode
  children: [RelatedNode]
  owner: NestedEdgedOwner
  peers: NestedPaginatedPeerConnection
}
type DcimSiteEdge { node: DcimSite }
type DcimSiteConnection { count: Int edges: [DcimSiteEdge] }
type Query { DcimSite(offset: Int, limit: Int): DcimSiteConnection }
schema { query: Query }
"#;

    fn field_schema(field_type: FieldType, required: bool) -> FieldSchema {
        FieldSchema {
            r#type: field_type,
            required,
            nullable: false,
            format: None,
            pattern: None,
            description: None,
        }
    }

    fn type_schema(
        key_fields: Vec<(&str, FieldSchema)>,
        fields: Vec<(&str, FieldSchema)>,
    ) -> TypeSchema {
        let mut key = BTreeMap::new();
        for (name, schema) in key_fields {
            key.insert(name.to_string(), schema);
        }
        let mut field_map = BTreeMap::new();
        for (name, schema) in fields {
            field_map.insert(name.to_string(), schema);
        }
        TypeSchema {
            key,
            fields: field_map,
        }
    }

    fn schema_with(types: Vec<(&str, TypeSchema)>) -> Schema {
        let mut map = BTreeMap::new();
        for (name, schema) in types {
            map.insert(name.to_string(), schema);
        }
        Schema { types: map }
    }

    fn temp_dir(prefix: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        dir.push(format!("alembic-{prefix}-{now}-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[cfg(unix)]
    fn make_executable(path: &Path) {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).unwrap();
    }

    fn write_executable(path: &Path, contents: &str) {
        fs::write(path, contents).unwrap();
        #[cfg(unix)]
        make_executable(path);
    }

    #[test]
    fn schema_info_parse_and_field_kinds() {
        let schema_info = SchemaInfo::parse(GRAPHQL_SCHEMA).unwrap();
        assert!(schema_info.attribute_types.contains("TextAttribute"));

        let type_schema = type_schema(
            vec![("name", field_schema(FieldType::String, true))],
            vec![
                (
                    "parent",
                    field_schema(
                        FieldType::Ref {
                            target: "dcim.site".to_string(),
                        },
                        false,
                    ),
                ),
                (
                    "children",
                    field_schema(
                        FieldType::ListRef {
                            target: "dcim.site".to_string(),
                        },
                        false,
                    ),
                ),
                (
                    "owner",
                    field_schema(
                        FieldType::Ref {
                            target: "dcim.owner".to_string(),
                        },
                        false,
                    ),
                ),
                (
                    "peers",
                    field_schema(
                        FieldType::ListRef {
                            target: "dcim.peer".to_string(),
                        },
                        false,
                    ),
                ),
            ],
        );

        let fields = field_names_for_schema(&type_schema);
        let kinds = schema_info
            .field_kinds("DcimSite", &type_schema, &fields)
            .unwrap();

        assert!(matches!(kinds.get("name"), Some(FieldKind::Attribute)));
        assert!(matches!(
            kinds.get("parent"),
            Some(FieldKind::RelationSingle(RelationShape::RelatedNode))
        ));
        assert!(matches!(
            kinds.get("children"),
            Some(FieldKind::RelationList(RelationShape::RelatedNode))
        ));
        assert!(matches!(
            kinds.get("owner"),
            Some(FieldKind::RelationSingle(RelationShape::NestedEdged))
        ));
        assert!(matches!(
            kinds.get("peers"),
            Some(FieldKind::RelationList(RelationShape::NestedPaginated))
        ));

        let err = validate_kind(
            "DcimSite",
            "name",
            &FieldType::Ref {
                target: "dcim.site".to_string(),
            },
            &FieldKind::Attribute,
        )
        .unwrap_err();
        assert!(err.to_string().contains("expected DcimSite.name"));
    }

    #[test]
    fn build_selection_and_extract_attrs() {
        let mut kinds = BTreeMap::new();
        kinds.insert("attr".to_string(), FieldKind::Attribute);
        kinds.insert(
            "rel_one".to_string(),
            FieldKind::RelationSingle(RelationShape::RelatedNode),
        );
        kinds.insert(
            "rel_edge".to_string(),
            FieldKind::RelationSingle(RelationShape::NestedEdged),
        );
        kinds.insert(
            "rel_page".to_string(),
            FieldKind::RelationSingle(RelationShape::NestedPaginated),
        );
        kinds.insert(
            "rel_many".to_string(),
            FieldKind::RelationList(RelationShape::RelatedNode),
        );
        kinds.insert(
            "rel_many_page".to_string(),
            FieldKind::RelationList(RelationShape::NestedPaginated),
        );
        kinds.insert(
            "rel_many_edge".to_string(),
            FieldKind::RelationList(RelationShape::NestedEdged),
        );

        let selection = build_selection(&kinds);
        assert!(selection.contains("attr { value }"));
        assert!(selection.contains("rel_one { id kind }"));
        assert!(selection.contains("rel_edge { node { id } }"));
        assert!(selection.contains("rel_page { node { id } }"));
        assert!(selection.contains("rel_many { id kind }"));
        assert!(selection.contains("rel_many_page { edges { node { id } } }"));
        assert!(selection.contains("rel_many_edge { node { id } }"));

        let node = json!({
            "attr": {"value": "alpha"},
            "rel_one": {"id": "r1", "kind": "DcimSite"},
            "rel_edge": {"node": {"id": "r2"}},
            "rel_page": {"node": {"id": "r3"}},
            "rel_many": [{"id": "m1", "kind": "DcimSite"}, {"id": "m2", "kind": "DcimSite"}],
            "rel_many_page": {"edges": [{"node": {"id": "p1"}}, {"node": {"id": "p2"}}]},
            "rel_many_edge": [{"node": {"id": "e1"}}, {"node": {"id": "e2"}}],
            "missing": null
        });

        let attrs = extract_attrs(&node, &kinds).unwrap();
        assert_eq!(attrs.get("attr"), Some(&json!("alpha")));
        assert_eq!(attrs.get("rel_one"), Some(&json!("r1")));
        assert_eq!(attrs.get("rel_edge"), Some(&json!("r2")));
        assert_eq!(attrs.get("rel_page"), Some(&json!("r3")));
        assert_eq!(attrs.get("rel_many"), Some(&json!(["m1", "m2"])));
        assert_eq!(attrs.get("rel_many_page"), Some(&json!(["p1", "p2"])));
        assert_eq!(attrs.get("rel_many_edge"), Some(&json!(["e1", "e2"])));
        assert!(!attrs.contains_key("missing"));
    }

    #[test]
    fn schema_missing_and_validate_schema() {
        let type_schema = type_schema(
            vec![("name", field_schema(FieldType::String, true))],
            vec![(
                "region",
                field_schema(
                    FieldType::Ref {
                        target: "dcim.region".to_string(),
                    },
                    false,
                ),
            )],
        );
        let schema = schema_with(vec![("dcim.site", type_schema)]);

        let mut fields = BTreeMap::new();
        fields.insert(
            "name".to_string(),
            GraphField {
                base_type: "TextAttribute".to_string(),
                is_list: false,
            },
        );
        let mut type_fields = BTreeMap::new();
        type_fields.insert("DcimSite".to_string(), fields);
        let schema_info = SchemaInfo {
            attribute_types: BTreeSet::new(),
            type_fields,
        };

        let missing = schema_missing(&schema, &schema_info);
        assert!(missing
            .fields
            .iter()
            .any(|field| field == "dcim.site.region"));

        let err = validate_schema(&schema, &schema_info).unwrap_err();
        assert!(err.to_string().contains("infrahub schema mismatch"));
    }

    #[test]
    fn build_provision_plan_creates_nodes_and_extensions() {
        let site_schema = type_schema(
            vec![("name", field_schema(FieldType::String, true))],
            vec![(
                "region",
                field_schema(
                    FieldType::Ref {
                        target: "dcim.region".to_string(),
                    },
                    false,
                ),
            )],
        );
        let prefix_schema = type_schema(
            vec![("prefix", field_schema(FieldType::Cidr, true))],
            vec![(
                "site",
                field_schema(
                    FieldType::Ref {
                        target: "dcim.site".to_string(),
                    },
                    false,
                ),
            )],
        );
        let schema = schema_with(vec![
            ("dcim.site", site_schema),
            ("ipam.prefix", prefix_schema),
        ]);

        let mut fields = BTreeMap::new();
        fields.insert(
            "name".to_string(),
            GraphField {
                base_type: "TextAttribute".to_string(),
                is_list: false,
            },
        );
        let mut type_fields = BTreeMap::new();
        type_fields.insert("DcimSite".to_string(), fields);
        let schema_info = SchemaInfo {
            attribute_types: BTreeSet::new(),
            type_fields,
        };

        let snapshot = SchemaSnapshot::default();
        let plan = build_provision_plan(&schema, &schema_info, &snapshot)
            .unwrap()
            .unwrap();
        assert!(plan
            .report
            .created_object_types
            .contains(&"ipam.prefix".to_string()));
        assert!(plan
            .report
            .created_object_fields
            .contains(&"dcim.site.region".to_string()));
        assert_eq!(plan.document.nodes.len(), 2);
        assert_eq!(plan.document.extensions.nodes.len(), 1);
        let mut names = plan
            .document
            .nodes
            .iter()
            .map(|node| format!("{}.{}", node.namespace, node.name))
            .collect::<Vec<_>>();
        names.sort();
        assert!(names.contains(&"Dcim.Site".to_string()));
        assert!(names.contains(&"Ipam.Prefix".to_string()));
    }

    #[test]
    fn write_schema_document_and_repository_config() {
        let doc = SchemaDocument {
            version: "1.0".to_string(),
            nodes: vec![NodeDef {
                name: "Site".to_string(),
                namespace: "Dcim".to_string(),
                label: Some("Site".to_string()),
                description: None,
                icon: None,
                include_in_menu: None,
                menu_placement: None,
                inherit_from: Vec::new(),
                human_friendly_id: vec!["name__value".to_string()],
                display_label: Some("{{ name__value }}".to_string()),
                default_filter: Some("name__value".to_string()),
                attributes: Vec::new(),
                relationships: Vec::new(),
            }],
            extensions: SchemaExtensions::default(),
        };

        let dir = temp_dir("schema-doc");
        let schema_path = dir.join("schema/schema.yaml");
        write_schema_document(&schema_path, &doc).unwrap();
        let raw = fs::read_to_string(&schema_path).unwrap();
        assert!(raw.contains("version"));

        let repo_root = temp_dir("repo");
        let nested = repo_root.join("schemas/site.yaml");
        fs::create_dir_all(nested.parent().unwrap()).unwrap();
        fs::write(&nested, "version: 1.0").unwrap();
        ensure_repository_config(&repo_root, &nested).unwrap();
        ensure_repository_config(&repo_root, &nested).unwrap();
        let config_path = repo_root.join(".infrahub.yml");
        let config = fs::read_to_string(&config_path).unwrap();
        assert!(config.contains("schemas"));
        assert!(config.contains("schemas/site.yaml"));
        assert_eq!(config.matches("schemas/site.yaml").count(), 1);

        let outside = temp_dir("outside").join("schema.yaml");
        fs::write(&outside, "version: 1.0").unwrap();
        let err = ensure_repository_config(&repo_root, &outside).unwrap_err();
        assert!(err.to_string().contains("must be inside repository root"));
    }

    #[test]
    fn build_input_and_validate_value() {
        let type_schema = type_schema(
            Vec::new(),
            vec![
                ("name", field_schema(FieldType::String, true)),
                ("count", field_schema(FieldType::Int, false)),
                (
                    "parent",
                    field_schema(
                        FieldType::Ref {
                            target: "dcim.site".to_string(),
                        },
                        false,
                    ),
                ),
                (
                    "tags",
                    field_schema(
                        FieldType::ListRef {
                            target: "dcim.tag".to_string(),
                        },
                        false,
                    ),
                ),
            ],
        );

        let uid_parent = Uid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let uid_tag_a = Uid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
        let uid_tag_b = Uid::parse_str("00000000-0000-0000-0000-000000000003").unwrap();

        let attrs = JsonMap::from(BTreeMap::from([
            ("name".to_string(), json!("Site-1")),
            ("count".to_string(), json!(5)),
            ("parent".to_string(), json!(uid_parent.to_string())),
            (
                "tags".to_string(),
                json!([uid_tag_a.to_string(), uid_tag_b.to_string()]),
            ),
        ]));

        let mut resolved = BTreeMap::new();
        resolved.insert(uid_parent, BackendId::String("p1".to_string()));
        resolved.insert(uid_tag_a, BackendId::String("t1".to_string()));
        resolved.insert(uid_tag_b, BackendId::String("t2".to_string()));

        let input = build_input(&attrs, &type_schema, &resolved).unwrap();
        assert_eq!(
            input,
            json!({
                "name": {"value": "Site-1"},
                "count": {"value": 5},
                "parent": {"id": "p1"},
                "tags": [{"id": "t1"}, {"id": "t2"}],
            })
        );

        let err = validate_value("count", &FieldType::Int, &json!("oops")).unwrap_err();
        assert!(err.to_string().contains("expects an integer"));
    }

    #[test]
    fn normalize_refs_and_backend_id() {
        let uid = Uid::parse_str("00000000-0000-0000-0000-000000000010").unwrap();
        let mut mappings = StateMappings::default();
        mappings.by_type.insert(
            "dcim.site".to_string(),
            BTreeMap::from([(BackendId::String("site-1".to_string()), uid)]),
        );

        let type_schema = type_schema(
            Vec::new(),
            vec![
                (
                    "parent",
                    field_schema(
                        FieldType::Ref {
                            target: "dcim.site".to_string(),
                        },
                        false,
                    ),
                ),
                (
                    "children",
                    field_schema(
                        FieldType::ListRef {
                            target: "dcim.site".to_string(),
                        },
                        false,
                    ),
                ),
            ],
        );

        let attrs = JsonMap::from(BTreeMap::from([
            ("parent".to_string(), json!("site-1")),
            ("children".to_string(), json!(["site-1", "site-2"])),
        ]));

        let normalized = normalize_attrs_refs(&attrs, &type_schema, &mappings);
        assert_eq!(normalized.get("parent"), Some(&json!(uid.to_string())));
        assert_eq!(
            normalized.get("children"),
            Some(&json!([uid.to_string(), "site-2"]))
        );

        assert_eq!(
            backend_id_from_value(&json!({"id": "abc"})),
            Some(BackendId::String("abc".to_string()))
        );
        assert_eq!(backend_id_from_value(&json!(42)), Some(BackendId::Int(42)));
        assert_eq!(backend_id_from_value(&json!(-1)), None);
    }

    #[test]
    fn describe_missing_refs_and_extract_ref_uid() {
        let uid_present = Uid::parse_str("00000000-0000-0000-0000-000000000020").unwrap();
        let uid_missing = Uid::parse_str("00000000-0000-0000-0000-000000000021").unwrap();

        let attrs = JsonMap::from(BTreeMap::from([
            ("ref".to_string(), json!(uid_missing.to_string())),
            ("refs".to_string(), json!([uid_present.to_string()])),
        ]));

        let key = Key::from(BTreeMap::from([("name".to_string(), json!("site"))]));
        let obj = Object {
            uid: uid_present,
            type_name: TypeName::new("dcim.site"),
            key,
            attrs,
            source: None,
        };

        let op = Op::Create {
            uid: uid_present,
            type_name: TypeName::new("dcim.site"),
            desired: obj,
        };

        let mut resolved = BTreeMap::new();
        resolved.insert(uid_present, BackendId::String("ok".to_string()));

        let missing = describe_missing_refs(&[op], &resolved);
        assert!(missing.contains(&uid_missing.to_string()));

        let err = anyhow::Error::new(AdapterApplyError::MissingRef { uid: uid_missing });
        assert!(is_missing_ref_error(&err));

        let extracted = extract_ref_uid(&json!([uid_present.to_string(), uid_missing.to_string()]));
        assert_eq!(extracted, Some(uid_present));
    }

    #[test]
    fn attribute_kind_for_field_variants() {
        let cases = vec![
            (FieldType::String, "Text"),
            (FieldType::Text, "Text"),
            (FieldType::Uuid, "Text"),
            (FieldType::Slug, "Text"),
            (
                FieldType::Enum {
                    values: vec!["a".to_string()],
                },
                "Dropdown",
            ),
            (FieldType::Int, "Number"),
            (FieldType::Float, "Number"),
            (FieldType::Bool, "Boolean"),
            (FieldType::Date, "DateTime"),
            (FieldType::Datetime, "DateTime"),
            (FieldType::Time, "DateTime"),
            (FieldType::Json, "JSON"),
            (
                FieldType::Map {
                    value: Box::new(FieldType::String),
                },
                "JSON",
            ),
            (
                FieldType::List {
                    item: Box::new(FieldType::String),
                },
                "List",
            ),
            (FieldType::IpAddress, "IPHost"),
            (FieldType::Cidr, "IPNetwork"),
            (FieldType::Prefix, "IPNetwork"),
            (FieldType::Mac, "MacAddress"),
            (
                FieldType::Ref {
                    target: "dcim.site".to_string(),
                },
                "Text",
            ),
            (
                FieldType::ListRef {
                    target: "dcim.site".to_string(),
                },
                "Text",
            ),
        ];

        for (field_type, expected) in cases {
            assert_eq!(attribute_kind_for_field(&field_type), expected);
        }
    }

    #[test]
    fn string_helpers() {
        assert_eq!(to_pascal_case("dcim_site"), "DcimSite");
        assert_eq!(to_pascal_case("ipam-prefix"), "IpamPrefix");
        assert_eq!(label_from_pascal("DeviceType"), "Device Type");
        assert_eq!(
            split_camel_case("DcimSite"),
            vec!["Dcim".to_string(), "Site".to_string()]
        );
        assert_eq!(gql_type_name_str("dcim.site"), "DcimSite");
        assert_eq!(gql_type_name_str("Device"), "Device");
        assert_eq!(display_label_for_keys(&[]), (None, None));
    }

    #[tokio::test]
    async fn apply_schema_infrahubctl_executes() {
        let dir = temp_dir("infrahubctl");
        let args_path = dir.join("args.txt");
        let addr_path = dir.join("addr.txt");
        let token_path = dir.join("token.txt");
        let script_path = dir.join("infrahubctl");
        let script = format!(
            "#!/usr/bin/env bash\nset -euo pipefail\nprintf '%s' \"$*\" > \"{}\"\nprintf '%s' \"$INFRAHUB_ADDRESS\" > \"{}\"\nprintf '%s' \"$INFRAHUB_API_TOKEN\" > \"{}\"\n",
            args_path.display(),
            addr_path.display(),
            token_path.display()
        );
        write_executable(&script_path, &script);

        let schema_path = dir.join("schema.yaml");
        fs::write(&schema_path, "version: 1.0").unwrap();

        let adapter = InfrahubAdapter::new("http://example.test", "token-123", None).unwrap();
        let mut config = SchemaPushConfig::infrahubctl(schema_path.clone());
        config.infrahubctl_path = Some(script_path);
        config.branch = Some("main".to_string());

        adapter.apply_schema_infrahubctl(&config).await.unwrap();

        let args = fs::read_to_string(&args_path).unwrap();
        assert!(args.contains("schema load"));
        assert!(args.contains(schema_path.to_str().unwrap()));
        assert!(args.contains("--branch main"));
        assert_eq!(
            fs::read_to_string(&addr_path).unwrap(),
            "http://example.test"
        );
        assert_eq!(fs::read_to_string(&token_path).unwrap(), "token-123");
    }

    #[tokio::test]
    async fn apply_schema_repository_flow() {
        let server = MockServer::start();
        let repo_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/graphql")
                .body_contains("CoreRepository");
            then.status(200).json_body(json!({
                "data": {
                    "CoreRepository": { "edges": [ { "node": { "id": "repo-1" } } ] }
                },
                "errors": []
            }));
        });
        let process_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/graphql")
                .body_contains("InfrahubRepositoryProcess");
            then.status(200).json_body(json!({
                "data": {
                    "InfrahubRepositoryProcess": { "ok": true, "task": { "id": "task-1" } }
                },
                "errors": []
            }));
        });

        let repo_root = temp_dir("repo");
        let schema_path = repo_root.join("schemas/site.yaml");
        fs::create_dir_all(schema_path.parent().unwrap()).unwrap();
        fs::write(&schema_path, "version: 1.0").unwrap();

        let adapter = InfrahubAdapter::new(&server.base_url(), "token", None).unwrap();
        let config = SchemaPushConfig {
            schema_path: schema_path.clone(),
            mode: SchemaApplyMode::Repository,
            repository_id: None,
            repository_name: Some("repo-name".to_string()),
            repository_root: Some(repo_root.clone()),
            branch: None,
            infrahubctl_path: None,
        };

        adapter.apply_schema_repository(&config).await.unwrap();
        repo_mock.assert();
        process_mock.assert();
        let config_path = repo_root.join(".infrahub.yml");
        let config_raw = fs::read_to_string(&config_path).unwrap();
        assert!(config_raw.contains("schemas/site.yaml"));
    }

    #[tokio::test]
    async fn read_observes_objects() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path("/schema.graphql");
            then.status(200).body(GRAPHQL_SCHEMA);
        });
        server.mock(|when, then| {
            when.method(POST)
                .path("/graphql")
                .body_contains("DcimSite");
            then.status(200).json_body(json!({
                "data": {
                    "DcimSite": {
                        "count": 1,
                        "edges": [
                            { "node": { "id": "site-1", "hfid": "site-1", "name": { "value": "Site One" } } }
                        ]
                    }
                },
                "errors": []
            }));
        });

        let adapter = InfrahubAdapter::new(&server.base_url(), "token", None).unwrap();
        let schema = schema_with(vec![(
            "dcim.site",
            type_schema(
                vec![("name", field_schema(FieldType::String, true))],
                vec![],
            ),
        )]);
        let state = StateStore::new(None, StateData::default());
        let observed = adapter.read(&schema, &[], &state).await.unwrap();
        assert_eq!(observed.by_key.len(), 1);
        let key = Key::from(BTreeMap::from([("name".to_string(), json!("Site One"))]));
        let object = observed
            .by_key
            .get(&(TypeName::new("dcim.site"), key_string(&key)))
            .unwrap();
        assert_eq!(object.attrs.get("name"), Some(&json!("Site One")));
    }

    #[tokio::test]
    async fn write_applies_create_update_delete() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path("/schema.graphql");
            then.status(200).body(GRAPHQL_SCHEMA);
        });
        server.mock(|when, then| {
            when.method(POST).path("/graphql").body_contains("Create");
            then.status(200).json_body(json!({
                "data": { "DcimSiteCreate": { "ok": true, "object": { "id": "site-1" } } },
                "errors": []
            }));
        });
        server.mock(|when, then| {
            when.method(POST).path("/graphql").body_contains("Update");
            then.status(200).json_body(json!({
                "data": { "DcimSiteUpdate": { "ok": true, "object": { "id": "site-2" } } },
                "errors": []
            }));
        });
        server.mock(|when, then| {
            when.method(POST).path("/graphql").body_contains("Delete");
            then.status(200).json_body(json!({
                "data": { "DcimSiteDelete": { "ok": true } },
                "errors": []
            }));
        });

        let adapter = InfrahubAdapter::new(&server.base_url(), "token", None).unwrap();
        let schema = schema_with(vec![(
            "dcim.site",
            type_schema(
                vec![("name", field_schema(FieldType::String, true))],
                vec![],
            ),
        )]);

        let uid_create = Uid::parse_str("00000000-0000-0000-0000-000000000100").unwrap();
        let uid_update = Uid::parse_str("00000000-0000-0000-0000-000000000101").unwrap();
        let uid_delete = Uid::parse_str("00000000-0000-0000-0000-000000000102").unwrap();

        let key = Key::from(BTreeMap::from([("name".to_string(), json!("Site A"))]));
        let create_obj = Object {
            uid: uid_create,
            type_name: TypeName::new("dcim.site"),
            key: key.clone(),
            attrs: JsonMap::from(BTreeMap::from([("name".to_string(), json!("Site A"))])),
            source: None,
        };
        let update_obj = Object {
            uid: uid_update,
            type_name: TypeName::new("dcim.site"),
            key: key.clone(),
            attrs: JsonMap::from(BTreeMap::from([("name".to_string(), json!("Site A"))])),
            source: None,
        };

        let ops = vec![
            Op::Create {
                uid: uid_create,
                type_name: TypeName::new("dcim.site"),
                desired: create_obj,
            },
            Op::Update {
                uid: uid_update,
                type_name: TypeName::new("dcim.site"),
                desired: update_obj,
                changes: Vec::new(),
                backend_id: Some(BackendId::String("site-2".to_string())),
            },
            Op::Delete {
                uid: uid_delete,
                type_name: TypeName::new("dcim.site"),
                key,
                backend_id: Some(BackendId::String("site-3".to_string())),
            },
        ];

        let state = StateStore::new(None, StateData::default());
        let report = adapter.write(&schema, &ops, &state).await.unwrap();
        assert_eq!(report.applied.len(), 3);
    }

    #[tokio::test]
    async fn lookup_backend_id_resolves() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path("/schema.graphql");
            then.status(200).body(GRAPHQL_SCHEMA);
        });
        server.mock(|when, then| {
            when.method(POST)
                .path("/graphql")
                .body_contains("DcimSite");
            then.status(200).json_body(json!({
                "data": {
                    "DcimSite": {
                        "count": 1,
                        "edges": [
                            { "node": { "id": "site-42", "hfid": "site-42", "name": { "value": "Site Z" } } }
                        ]
                    }
                },
                "errors": []
            }));
        });

        let adapter = InfrahubAdapter::new(&server.base_url(), "token", None).unwrap();
        let type_schema = type_schema(
            vec![("name", field_schema(FieldType::String, true))],
            vec![],
        );
        let key = Key::from(BTreeMap::from([("name".to_string(), json!("Site Z"))]));
        let id = adapter
            .lookup_backend_id(&TypeName::new("dcim.site"), &type_schema, &key)
            .await
            .unwrap();
        assert_eq!(id, "site-42");
    }
}
