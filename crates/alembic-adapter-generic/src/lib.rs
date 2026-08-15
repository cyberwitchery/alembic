//! generic rest adapter for alembic.

use alembic_core::{key_string, JsonMap, Key, Schema, TypeName, TypeSchema, Uid};
use alembic_engine::{
    apply_non_delete_journaled, build_key_from_schema, bullet_list, describe_missing_refs,
    is_missing_ref_error, normalize_attrs_refs, resolve_ref_keyed_identity, resolved_ids_identity,
    Adapter, AppliedOp, ApplyReport, BackendId, Emitter, ObservedState, Observer, Op, RawNode,
    RetryApplyDriver, StateMappings,
};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// configuration for the generic rest adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct EndpointConfig {
    /// path for listing and creating objects.
    pub path: String,
    /// json path to the results array in the list response (default: root).
    pub results_path: Option<String>,
    /// json path to the next-page url in the list response (default: unset, one
    /// request per list).
    pub next_path: Option<String>,
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

/// reqwest's own default, kept because the custom redirect policy replaces it.
const REDIRECT_LIMIT: usize = 10;

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
            .redirect(same_origin_redirects())
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

            // a path of only separators walks nowhere and would report the
            // response root as the wrong shape on every read.
            if let Some(next_path) = &endpoint.next_path {
                if next_path.split('.').all(str::is_empty) {
                    return Err(anyhow!(
                        "invalid next_path {:?} for type {} (no path segments; omit the key for a single-page endpoint)",
                        next_path,
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
                let key = key_from_inventory(type_schema, desired, type_name)?;
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
            let listed = key_from_response(
                type_schema,
                &attrs,
                type_name,
                endpoint.path.trim_end_matches('/'),
            )?;
            if listed == *key {
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

    /// refuse a plan holding a delete this config cannot perform, before any op
    /// is applied: config presence, delete strategy and backend id, in one
    /// pass. found in the delete phase instead, the creates and updates have
    /// landed and the journal makes every re-run fail at the same delete.
    fn guard_deletable_ops(&self, ops: &[Op]) -> Result<()> {
        let mut unconfigured = BTreeSet::new();
        let mut declining = BTreeSet::new();
        let mut unidentified = BTreeSet::new();

        for op in ops {
            let Op::Delete {
                type_name,
                key,
                backend_id,
                ..
            } = op
            else {
                continue;
            };
            match self.config.types.get(type_name.as_str()) {
                None => {
                    unconfigured.insert(format!(
                        "no config for {type_name}; add a types: entry for it"
                    ));
                }
                Some(endpoint) => match endpoint.delete_strategy {
                    DeleteStrategy::None => {
                        declining.insert(format!(
                            "delete not supported for type {type_name} (delete_strategy: none); set delete_strategy: standard"
                        ));
                    }
                    DeleteStrategy::Standard => {}
                },
            }
            if backend_id.is_none() {
                unidentified.insert(format!(
                    "delete requires backend id for {type_name} {}; re-plan against the backend",
                    key_string(key)
                ));
            }
        }

        // grouped by cause and named, not counted: each is its own edit, and
        // stopping at the first leaves the rest to be found one apply at a time.
        let blocked: Vec<String> = unconfigured
            .into_iter()
            .chain(declining)
            .chain(unidentified)
            .collect();
        if blocked.is_empty() {
            return Ok(());
        }
        Err(anyhow!(
            "plan holds deletes this config cannot apply; nothing was applied:\n{}",
            bullet_list(&blocked)
        ))
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
            schema
                .types
                .get(type_name.as_str())
                .ok_or_else(|| anyhow!("missing schema for {}", type_name))?;

            let client = self.client.clone();
            let base_url = self.config.base_url.clone();

            tasks.push(tokio::spawn(async move {
                let results =
                    list_endpoint_results(&client, &base_url, &endpoint, &type_name).await?;

                let mut raw = Vec::new();
                for item in results {
                    let id_val = resolve_path(&item, &endpoint.id_path)?;
                    let backend_id = parse_backend_id(id_val)?;

                    let attrs = match item {
                        serde_json::Value::Object(map) => {
                            map.into_iter().collect::<BTreeMap<_, _>>().into()
                        }
                        _ => return Err(anyhow!("expected object in results")),
                    };

                    raw.push(RawNode {
                        type_name: type_name.clone(),
                        backend_id,
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

        let mut mappings = StateMappings::from_state(state_store);
        let observed = resolve_ref_keyed_identity(
            &raw,
            schema,
            &mut mappings,
            |node, type_schema, mappings| normalize_attrs_refs(&node.attrs, type_schema, mappings),
            |node, type_schema, attrs| {
                let path = self
                    .config
                    .types
                    .get(node.type_name.as_str())
                    .map(|e| e.path.trim_end_matches('/'))
                    .unwrap_or_default();
                key_from_response(
                    type_schema,
                    attrs,
                    &node.type_name,
                    &format!("{}/{}", path, node.backend_id),
                )
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
impl Emitter for GenericAdapter {
    async fn write(
        &self,
        schema: &Schema,
        ops: &[Op],
        state: &alembic_engine::StateStore,
    ) -> Result<ApplyReport> {
        self.guard_deletable_ops(ops)?;

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
        let (retry_result, previously_applied_count, journal) =
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
        journal.finish()?;

        Ok(ApplyReport {
            applied,
            resumed,
            previously_applied_count,
            ..Default::default()
        })
    }
}

impl Adapter for GenericAdapter {}

/// list an endpoint and return its results array (from `results_path` when set,
/// else the response root), following `next_path` across pages when it is set.
/// shared by `read` and `lookup_backend_id` so the 409-recovery path fetches and
/// extracts identically to observation.
async fn list_endpoint_results(
    client: &reqwest::Client,
    base_url: &str,
    endpoint: &EndpointConfig,
    type_name: &TypeName,
) -> Result<Vec<serde_json::Value>> {
    let mut url = format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        endpoint.path.trim_start_matches('/')
    );
    let mut results = Vec::new();
    let mut visited = BTreeSet::new();

    loop {
        if !visited.insert(url.clone()) {
            return Err(anyhow!(
                "pagination loop for {}: {} was already fetched",
                type_name,
                url
            ));
        }

        let resp = client.get(&url).send().await?.error_for_status()?;
        let body: serde_json::Value = resp.json().await?;
        results.extend(extract_results(&body, endpoint, type_name, &url)?);

        let Some(next_path) = &endpoint.next_path else {
            break;
        };
        let first_page = visited.len() == 1;
        let Some(next) = next_page_url(&body, next_path, type_name, &url, first_page)? else {
            break;
        };
        url = resolve_next_url(base_url, &url, &next, type_name)?;
    }

    Ok(results)
}

/// extract one page's results array. `url` names the page in errors, since a
/// shape that broke on page four says nothing without it.
fn extract_results(
    body: &serde_json::Value,
    endpoint: &EndpointConfig,
    type_name: &TypeName,
    url: &str,
) -> Result<Vec<serde_json::Value>> {
    if let Some(path) = &endpoint.results_path {
        Ok(resolve_path(body, path)
            .map_err(|err| anyhow!("{} for {} at {}", err, type_name, url))?
            .as_array()
            .ok_or_else(|| {
                anyhow!(
                    "expected array at path {} for {} at {}",
                    path,
                    type_name,
                    url
                )
            })?
            .clone())
    } else if let Some(arr) = body.as_array() {
        Ok(arr.clone())
    } else {
        Err(anyhow!(
            "expected array in list response for {} at {}",
            type_name,
            url
        ))
    }
}

/// read the next-page url out of a list response. an absent key, an explicit
/// null and an empty string all mean "last page"; any other shape is an error,
/// because silently stopping on an unexpected one is the bug this follows.
///
/// a segment above the final key missing from the *first* page is a mistyped
/// `next_path` rather than a last page, and errors as `results_path` does. a
/// later page is allowed to drop the envelope once it has nothing left to link.
fn next_page_url(
    body: &serde_json::Value,
    path: &str,
    type_name: &TypeName,
    url: &str,
    first_page: bool,
) -> Result<Option<String>> {
    let mut current = body;
    let mut segments = path
        .split('.')
        .filter(|segment| !segment.is_empty())
        .peekable();
    while let Some(segment) = segments.next() {
        match current.get(segment) {
            Some(value) => current = value,
            None if segments.peek().is_none() || !first_page => return Ok(None),
            None => {
                return Err(anyhow!(
                    "path segment not found: {} at next_path {} for {} at {}",
                    segment,
                    path,
                    type_name,
                    url
                ))
            }
        }
    }

    match current {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(next) if next.is_empty() => Ok(None),
        serde_json::Value::String(next) => Ok(Some(next.clone())),
        other => Err(anyhow!(
            "expected string or null at next_path {} for {} at {}, got {}",
            path,
            type_name,
            url,
            json_kind(other)
        )),
    }
}

/// resolve a next-page url against the page that returned it and refuse one that
/// leaves `base_url`'s origin. the client carries the operator's auth headers on
/// every request it makes, so following an off-host next would hand that token
/// to a third party.
fn resolve_next_url(
    base_url: &str,
    current_url: &str,
    next: &str,
    type_name: &TypeName,
) -> Result<String> {
    let base = reqwest::Url::parse(base_url)
        .map_err(|err| anyhow!("invalid base_url {} for {}: {}", base_url, type_name, err))?;
    let current = reqwest::Url::parse(current_url).map_err(|err| {
        anyhow!(
            "invalid page url {} for {}: {}",
            current_url,
            type_name,
            err
        )
    })?;
    let resolved = current
        .join(next)
        .map_err(|err| anyhow!("invalid next url {} for {}: {}", next, type_name, err))?;

    if resolved.origin() != base.origin() {
        return Err(anyhow!(
            "next url {} for {} leaves the configured origin {}; refusing to send credentials to another host",
            resolved,
            type_name,
            base.origin().ascii_serialization()
        ));
    }

    Ok(resolved.to_string())
}

/// follow a redirect only while it stays on the origin that issued it, the same
/// test `resolve_next_url` applies to a next url. reqwest strips `authorization`
/// and friends across hosts, but `headers` is an arbitrary map, so a token
/// configured as `X-API-Key` would ride along.
fn same_origin_redirects() -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(|attempt| {
        let Some(origin) = attempt.previous().last().map(reqwest::Url::origin) else {
            return attempt.follow();
        };
        if origin != attempt.url().origin() {
            let refused = anyhow!(
                "redirect to {} leaves the origin {}; refusing to send credentials to another host",
                attempt.url(),
                origin.ascii_serialization()
            );
            return attempt.error(refused);
        }
        // a custom policy does not get reqwest's default hop limit.
        if attempt.previous().len() > REDIRECT_LIMIT {
            return attempt.error(anyhow!("too many redirects (limit {})", REDIRECT_LIMIT));
        }
        attempt.follow()
    })
}

fn json_kind(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
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

/// build an object's key from what the api returned, naming where it came from.
fn key_from_response(
    type_schema: &TypeSchema,
    attrs: &JsonMap,
    type_name: &TypeName,
    at: &str,
) -> Result<Key> {
    build_key_from_schema(type_schema, attrs).with_context(|| {
        format!(
            "build key for {type_name} at {at}: a key field must be declared in `fields:` \
             and carried in `attrs:` to be sent on create, unless the backend derives it \
             and returns it"
        )
    })
}

/// build an object's key from the inventory's own attrs, naming the object.
fn key_from_inventory(
    type_schema: &TypeSchema,
    desired: &alembic_core::Object,
    type_name: &TypeName,
) -> Result<Key> {
    build_key_from_schema(type_schema, &desired.attrs).with_context(|| {
        format!(
            "build key for {type_name} {}: a key field declared in `key:` must also be \
             carried in `attrs:` to be sent on create",
            key_string(&desired.key)
        )
    })
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
