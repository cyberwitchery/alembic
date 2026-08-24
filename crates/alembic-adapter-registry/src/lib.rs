//! adapter registry and config loading for alembic.

use alembic_engine::{
    Adapter, ApplyReport, Backend, BackendIdentity, Emitter, ExternalCapabilities,
    ExternalEnvelopeRef, ExternalObject, ExternalRequestRef, ExternalResponse, ExternalRole,
    ObservedObject, ObservedState, Observer, Op, ProvisionReport, StateData, StateStore,
    EXTERNAL_PROTOCOL_VERSION,
};
use anyhow::{anyhow, Context, Result};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::time::timeout;

#[cfg(feature = "django")]
use alembic_adapter_django::emit::DjangoConfig;

const SUPPORTED_BACKENDS: &[&str] = &[
    #[cfg(feature = "netbox")]
    "netbox",
    #[cfg(feature = "nautobot")]
    "nautobot",
    #[cfg(feature = "infrahub")]
    "infrahub",
    #[cfg(feature = "generic")]
    "generic",
    #[cfg(feature = "peeringdb")]
    "peeringdb",
    #[cfg(feature = "django")]
    "django",
    "external",
];

#[derive(Debug, Deserialize)]
#[serde(tag = "backend", rename_all = "kebab-case")]
pub enum AdapterConfig {
    #[cfg(feature = "netbox")]
    Netbox(NetboxConfig),
    #[cfg(feature = "nautobot")]
    Nautobot(NautobotConfig),
    #[cfg(feature = "infrahub")]
    Infrahub(InfrahubConfig),
    #[cfg(feature = "generic")]
    Generic(GenericConfig),
    #[cfg(feature = "peeringdb")]
    Peeringdb(PeeringdbConfig),
    #[cfg(feature = "django")]
    Django(DjangoConfig),
    External(ExternalConfig),
}

#[cfg(feature = "netbox")]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetboxConfig {
    pub url: Option<String>,
    pub token: Option<String>,
    /// stable backend-instance identity override; defaults to the normalized url.
    pub instance: Option<String>,
}

#[cfg(feature = "nautobot")]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NautobotConfig {
    pub url: Option<String>,
    pub token: Option<String>,
    /// stable backend-instance identity override; defaults to the normalized url.
    pub instance: Option<String>,
}

#[cfg(feature = "infrahub")]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InfrahubConfig {
    pub url: Option<String>,
    pub token: Option<String>,
    pub branch: Option<String>,
    /// stable backend-instance identity override; defaults to the normalized url.
    pub instance: Option<String>,
    #[serde(default)]
    pub schema: Option<InfrahubSchemaConfig>,
}

#[cfg(feature = "generic")]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenericConfig {
    pub config: Option<alembic_adapter_generic::GenericConfig>,
    pub config_path: Option<PathBuf>,
    /// stable backend-instance identity override; defaults to the normalized base_url.
    pub instance: Option<String>,
}

/// takes no keys; the credential is `PEERINGDB_API_KEY`. it exists so a stray
/// key is rejected by name, which the unit variant it replaces could not do.
#[cfg(feature = "peeringdb")]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeeringdbConfig {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalConfig {
    pub command: Option<String>,
    #[serde(default)]
    pub args: Vec<String>,
    pub working_dir: Option<PathBuf>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    pub timeout_seconds: Option<u64>,
    #[serde(default)]
    pub setup: serde_yaml::Value,
    /// stable backend-instance identity. an external adapter's config carries
    /// no endpoint the host can read, so without this the identity falls back
    /// to a fingerprint of the whole config, which changes when the config does.
    pub instance: Option<String>,
}

#[cfg(feature = "infrahub")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum InfrahubSchemaMode {
    #[default]
    None,
    Infrahubctl,
    Repository,
}

#[cfg(feature = "infrahub")]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InfrahubSchemaConfig {
    #[serde(default)]
    pub mode: InfrahubSchemaMode,
    pub schema_path: Option<PathBuf>,
    pub repository_id: Option<String>,
    pub repository_name: Option<String>,
    pub repository_root: Option<PathBuf>,
    pub branch: Option<String>,
    pub infrahubctl_path: Option<PathBuf>,
}

impl AdapterConfig {
    fn backend_name(&self) -> &'static str {
        match self {
            #[cfg(feature = "netbox")]
            AdapterConfig::Netbox(_) => "netbox",
            #[cfg(feature = "nautobot")]
            AdapterConfig::Nautobot(_) => "nautobot",
            #[cfg(feature = "infrahub")]
            AdapterConfig::Infrahub(_) => "infrahub",
            #[cfg(feature = "generic")]
            AdapterConfig::Generic(_) => "generic",
            #[cfg(feature = "peeringdb")]
            AdapterConfig::Peeringdb(_) => "peeringdb",
            #[cfg(feature = "django")]
            AdapterConfig::Django(_) => "django",
            AdapterConfig::External(_) => "external",
        }
    }

    fn from_env(plugins: &[Plugin], backend: &str) -> Result<Self> {
        match backend.to_lowercase().as_str() {
            #[cfg(feature = "netbox")]
            "netbox" => Ok(AdapterConfig::Netbox(NetboxConfig {
                url: None,
                token: None,
                instance: None,
            })),
            #[cfg(feature = "nautobot")]
            "nautobot" => Ok(AdapterConfig::Nautobot(NautobotConfig {
                url: None,
                token: None,
                instance: None,
            })),
            #[cfg(feature = "infrahub")]
            "infrahub" => Ok(AdapterConfig::Infrahub(InfrahubConfig {
                url: None,
                token: None,
                branch: None,
                instance: None,
                schema: None,
            })),
            #[cfg(feature = "generic")]
            "generic" => Ok(AdapterConfig::Generic(GenericConfig {
                config: None,
                config_path: None,
                instance: None,
            })),
            #[cfg(feature = "peeringdb")]
            "peeringdb" => Ok(AdapterConfig::Peeringdb(PeeringdbConfig {})),
            #[cfg(feature = "django")]
            "django" => Ok(AdapterConfig::Django(DjangoConfig {
                ..DjangoConfig::default()
            })),
            "external" => Ok(AdapterConfig::External(ExternalConfig {
                command: None,
                args: Vec::new(),
                working_dir: None,
                env: BTreeMap::new(),
                timeout_seconds: None,
                setup: serde_yaml::Value::default(),
                instance: None,
            })),
            other => Err(anyhow!(
                "unsupported backend {other} (expected one of: {}{})",
                SUPPORTED_BACKENDS.join(", "),
                if plugins.is_empty() {
                    "".to_string()
                } else {
                    format!(
                        "; OR one of the plugins: {}",
                        plugins
                            .iter()
                            .map(|p| p.name.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }
            )),
        }
    }

    /// build the backend plus the identity of the instance it talks to. state
    /// is identity memory scoped to one backend instance, and the config layer
    /// is where the instance is known, so both come out of one call.
    pub fn build(self) -> Result<(Backend, BackendIdentity)> {
        match self {
            #[cfg(feature = "netbox")]
            AdapterConfig::Netbox(cfg) => {
                let (url, token) = resolve_credentials("NETBOX", cfg.url, cfg.token)?;
                let identity = BackendIdentity::new(
                    "netbox",
                    cfg.instance.unwrap_or_else(|| normalize_instance_url(&url)),
                );
                Ok((
                    Backend::Adapter(Box::new(alembic_adapter_netbox::NetBoxAdapter::new(
                        &url, &token,
                    )?)),
                    identity,
                ))
            }
            #[cfg(feature = "nautobot")]
            AdapterConfig::Nautobot(cfg) => {
                let (url, token) = resolve_credentials("NAUTOBOT", cfg.url, cfg.token)?;
                let identity = BackendIdentity::new(
                    "nautobot",
                    cfg.instance.unwrap_or_else(|| normalize_instance_url(&url)),
                );
                Ok((
                    Backend::Adapter(Box::new(alembic_adapter_nautobot::NautobotAdapter::new(
                        &url, &token,
                    )?)),
                    identity,
                ))
            }
            #[cfg(feature = "infrahub")]
            AdapterConfig::Infrahub(cfg) => {
                let (url, token) = resolve_credentials("INFRAHUB", cfg.url, cfg.token)?;
                let identity = BackendIdentity::new(
                    "infrahub",
                    cfg.instance.unwrap_or_else(|| normalize_instance_url(&url)),
                );
                let mut adapter = alembic_adapter_infrahub::InfrahubAdapter::new(
                    &url,
                    &token,
                    cfg.branch.as_deref(),
                )?;
                if let Some(schema_cfg) = cfg.schema {
                    if let Some(schema_push) = schema_cfg.build()? {
                        adapter = adapter.with_schema_push(schema_push);
                    }
                }
                Ok((Backend::Adapter(Box::new(adapter)), identity))
            }
            #[cfg(feature = "generic")]
            AdapterConfig::Generic(cfg) => {
                if cfg.config.is_some() && cfg.config_path.is_some() {
                    return Err(anyhow!(
                        "generic adapter config cannot include both config and config_path"
                    ));
                }
                let config = if let Some(config) = cfg.config {
                    config
                } else {
                    let path = cfg
                        .config_path
                        .or_else(|| std::env::var("GENERIC_CONFIG").ok().map(PathBuf::from));
                    let path =
                        path.ok_or_else(|| anyhow!("generic backend requires config_path"))?;
                    let content = fs::read_to_string(&path)
                        .with_context(|| format!("read generic config: {}", path.display()))?;
                    serde_yaml::from_str(&content)
                        .with_context(|| format!("parse generic config: {}", path.display()))?
                };
                let identity = BackendIdentity::new(
                    "generic",
                    cfg.instance
                        .unwrap_or_else(|| normalize_instance_url(&config.base_url)),
                );
                Ok((
                    Backend::Adapter(Box::new(alembic_adapter_generic::GenericAdapter::new(
                        config,
                    )?)),
                    identity,
                ))
            }
            #[cfg(feature = "peeringdb")]
            AdapterConfig::Peeringdb(_) => Ok((
                Backend::Observer(Box::new(alembic_adapter_peeringdb::PeeringDBAdapter::new())),
                // one public instance; there is nothing else to identify.
                BackendIdentity::new("peeringdb", "public"),
            )),
            #[cfg(feature = "django")]
            AdapterConfig::Django(cfg) => {
                // an emitter assigns no backend ids, but the stamp still holds a
                // state file to the one output it describes.
                let identity = BackendIdentity::new("django", cfg.output.display().to_string());
                Ok((
                    Backend::Emitter(Box::new(alembic_adapter_django::DjangoAdapter::new(cfg))),
                    identity,
                ))
            }
            AdapterConfig::External(cfg) => {
                let instance = cfg.instance.clone().unwrap_or_else(|| {
                    // no endpoint the host can read: fall back to a fingerprint
                    // of the config, which changes when the config does. set
                    // `instance:` for an identity that survives config edits.
                    let fingerprint = format!(
                        "{:?}\n{:?}\n{:?}\n{:?}\n{}",
                        cfg.command,
                        cfg.args,
                        cfg.working_dir,
                        cfg.env,
                        serde_yaml::to_string(&cfg.setup).unwrap_or_default(),
                    );
                    format!(
                        "config-{}",
                        &alembic_core::uid_v5("alembic.external", &fingerprint)
                            .simple()
                            .to_string()[..12]
                    )
                });
                let identity = BackendIdentity::new("external", instance);
                let adapter = ProcessAdapter::new(cfg)?;
                // box the adapter into the backend variant matching its declared
                // role, so an emit-only external adapter gets the same handling a
                // built-in emitter like django gets (all-creates plan, up-front
                // import error) instead of silently observing nothing.
                let backend = match adapter.probe_role() {
                    ExternalRole::Observer => Backend::Observer(Box::new(adapter)),
                    ExternalRole::Emitter => Backend::Emitter(Box::new(adapter)),
                    ExternalRole::Adapter => Backend::Adapter(Box::new(adapter)),
                };
                Ok((backend, identity))
            }
        }
    }
}

/// normalize an endpoint url into a stable instance identity: lowercase the
/// scheme and host, trim trailing slashes, keep port and path as given (two
/// instances behind one host stay distinct).
fn normalize_instance_url(url: &str) -> String {
    let url = url.trim().trim_end_matches('/');
    match url.split_once("://") {
        Some((scheme, rest)) => {
            let (authority, path) = match rest.split_once('/') {
                Some((authority, path)) => (authority, Some(path)),
                None => (rest, None),
            };
            let mut out = format!(
                "{}://{}",
                scheme.to_ascii_lowercase(),
                authority.to_ascii_lowercase()
            );
            if let Some(path) = path {
                out.push('/');
                out.push_str(path);
            }
            out
        }
        None => url.to_string(),
    }
}

/// default external-adapter request timeout, in seconds.
const DEFAULT_EXTERNAL_TIMEOUT_SECS: u64 = 120;

#[derive(Debug)]
struct ProcessAdapter {
    command: String,
    args: Vec<String>,
    working_dir: Option<PathBuf>,
    env: BTreeMap<String, String>,
    timeout: Duration,
    setup: serde_yaml::Value,
}

impl ProcessAdapter {
    fn new(cfg: ExternalConfig) -> Result<Self> {
        let command = cfg
            .command
            .or_else(|| std::env::var("EXTERNAL_COMMAND").ok())
            .ok_or_else(|| anyhow!("external backend requires command"))?;
        let timeout =
            Duration::from_secs(cfg.timeout_seconds.unwrap_or(DEFAULT_EXTERNAL_TIMEOUT_SECS));
        Ok(Self {
            command,
            args: cfg.args,
            working_dir: cfg.working_dir,
            env: cfg.env,
            timeout,
            setup: cfg.setup,
        })
    }

    /// send a request and return the raw success payload, or `None` when the adapter
    /// reported success with a null/absent `result` field.
    async fn call_raw(&self, request: ExternalRequestRef<'_>) -> Result<Option<JsonValue>> {
        // bound before the envelope takes the request; the exits below name it.
        let method = request.method();
        let envelope = ExternalEnvelopeRef {
            version: EXTERNAL_PROTOCOL_VERSION,
            request,
            setup: self.setup.clone(),
        };
        let payload = serde_json::to_vec(&envelope).context("serialize external request")?;
        let output = self.run(payload).await?;
        let stdout = String::from_utf8(output.stdout)
            .with_context(|| format!("external adapter {method} response not utf-8"))?;
        let response: ExternalResponse<JsonValue> = serde_json::from_str(&stdout)
            .with_context(|| format!("parse external adapter {method} response"))?;
        if !response.ok {
            let message = response
                .error
                .unwrap_or_else(|| format!("external adapter {method} error"));
            return Err(anyhow!(message));
        }
        Ok(response.result)
    }

    async fn call<R: DeserializeOwned>(&self, request: ExternalRequestRef<'_>) -> Result<R> {
        let method = request.method();
        let result = self
            .call_raw(request)
            .await?
            .ok_or_else(|| anyhow!("external adapter {method} response missing result"))?;
        serde_json::from_value(result)
            .with_context(|| format!("deserialize external adapter {method} result"))
    }

    /// like [`ProcessAdapter::call`] but tolerates a null/absent result, mapping it to
    /// `None` -- used for optional responses such as schema preview.
    async fn call_optional<R: DeserializeOwned>(
        &self,
        request: ExternalRequestRef<'_>,
    ) -> Result<Option<R>> {
        let method = request.method();
        match self.call_raw(request).await? {
            None | Some(JsonValue::Null) => Ok(None),
            Some(value) => serde_json::from_value(value)
                .map(Some)
                .with_context(|| format!("deserialize external adapter {method} result")),
        }
    }

    /// ask the adapter which role it implements, once at construction. any probe
    /// failure (an old adapter answering unknown-method, a crash, a garbage
    /// payload) defaults to the full read+write role, the pre-capabilities
    /// behavior, so the probe can never fail construction.
    fn probe_role(&self) -> ExternalRole {
        // build() is sync but may run inside the cli's tokio runtime, where
        // blocking on a future in place is not allowed; a scoped thread with its
        // own small runtime works from both sync and async callers.
        let probed = std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()?;
                    runtime.block_on(
                        self.call::<ExternalCapabilities>(ExternalRequestRef::Capabilities),
                    )
                })
                .join()
        });
        match probed {
            Ok(Ok(capabilities)) => capabilities.role,
            Ok(Err(_)) | Err(_) => ExternalRole::Adapter,
        }
    }

    async fn run(&self, payload: Vec<u8>) -> Result<std::process::Output> {
        let mut cmd = Command::new(&self.command);
        // kill the child when we drop it on timeout, so it can't keep writing to the backend.
        cmd.args(&self.args)
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(dir) = &self.working_dir {
            cmd.current_dir(dir);
        }
        for (key, value) in &self.env {
            cmd.env(key, value);
        }

        let mut child = cmd.spawn().context("spawn external adapter")?;
        let stdin = child.stdin.take();
        // write the request to the child's stdin concurrently with draining its
        // stdout/stderr, all under the one timeout, so a child that floods its output or
        // never reads its stdin can't deadlock the write and hang the cli forever, but
        // instead trips the timeout like any other stall.
        let output = timeout(self.timeout, async move {
            let write = async move {
                if let Some(mut stdin) = stdin {
                    // ignore a BrokenPipe from a crashed adapter so its real exit error surfaces below.
                    let _ = stdin.write_all(&payload).await;
                }
            };
            let (_, output) = tokio::join!(write, child.wait_with_output());
            output
        })
        .await
        .context("external adapter timed out")?
        .context("wait for external adapter")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            return Err(anyhow!(
                "external adapter exited with {}: {}",
                output.status,
                stderr
            ));
        }

        // forward adapter warnings so they aren't lost on a successful run.
        if !output.stderr.is_empty() {
            eprint!("{}", String::from_utf8_lossy(&output.stderr));
        }

        Ok(output)
    }
}

#[async_trait::async_trait]
impl Observer for ProcessAdapter {
    async fn read(
        &self,
        schema: &alembic_core::Schema,
        types: &[alembic_core::TypeName],
        state: &StateStore,
    ) -> Result<ObservedState> {
        let state = StateData::from(state);
        let objects: Vec<ExternalObject> = self
            .call(ExternalRequestRef::Read {
                schema,
                types,
                state,
            })
            .await?;
        let mut observed = ObservedState::default();
        for object in objects {
            observed.insert(ObservedObject {
                type_name: object.type_name,
                key: object.key,
                attrs: object.attrs,
                backend_id: object.backend_id,
            })?;
        }
        Ok(observed)
    }
}

#[async_trait::async_trait]
impl Emitter for ProcessAdapter {
    async fn write(
        &self,
        schema: &alembic_core::Schema,
        ops: &[Op],
        state: &StateStore,
    ) -> Result<ApplyReport> {
        let state = StateData::from(state);
        self.call(ExternalRequestRef::Write { schema, ops, state })
            .await
    }

    async fn ensure_schema(&self, schema: &alembic_core::Schema) -> Result<ProvisionReport> {
        self.call(ExternalRequestRef::EnsureSchema { schema }).await
    }

    async fn preview_schema(
        &self,
        schema: &alembic_core::Schema,
    ) -> Result<Option<ProvisionReport>> {
        self.call_optional(ExternalRequestRef::PreviewSchema { schema })
            .await
    }
}

impl Adapter for ProcessAdapter {}

#[cfg(feature = "infrahub")]
impl InfrahubSchemaConfig {
    fn build(self) -> Result<Option<alembic_adapter_infrahub::SchemaPushConfig>> {
        if self.mode == InfrahubSchemaMode::None {
            return Ok(None);
        }
        let schema_path = self
            .schema_path
            .ok_or_else(|| anyhow!("infrahub schema requires schema_path"))?;
        let mode = match self.mode {
            InfrahubSchemaMode::Infrahubctl => {
                alembic_adapter_infrahub::SchemaApplyMode::Infrahubctl
            }
            InfrahubSchemaMode::Repository => alembic_adapter_infrahub::SchemaApplyMode::Repository,
            InfrahubSchemaMode::None => alembic_adapter_infrahub::SchemaApplyMode::Infrahubctl,
        };
        let config = alembic_adapter_infrahub::SchemaPushConfig {
            schema_path,
            mode,
            repository_id: self.repository_id,
            repository_name: self.repository_name,
            repository_root: self.repository_root,
            branch: self.branch,
            infrahubctl_path: self.infrahubctl_path,
        };

        if config.mode == alembic_adapter_infrahub::SchemaApplyMode::Repository {
            if config.repository_root.is_none() {
                return Err(anyhow!(
                    "infrahub schema repository mode requires repository_root"
                ));
            }
            if config.repository_id.is_none() && config.repository_name.is_none() {
                return Err(anyhow!(
                    "infrahub schema repository mode requires repository_id or repository_name"
                ));
            }
        }

        Ok(Some(config))
    }
}

/// a plugin is an external backend that can be
/// referred to using its name instead of passing
/// `--backend external --backend-config <path>` manually.
#[derive(Debug)]
pub struct Plugin {
    /// should match the name passed to `--backend` flag
    pub name: String,
    /// path to the backend config yaml file that describes the plugin
    pub path: PathBuf,
}

pub fn create_backend(
    plugins: &[Plugin],
    backend: Option<&str>,
    config_path: Option<PathBuf>,
) -> Result<(Backend, BackendIdentity)> {
    let config = if let Some(path) = config_path {
        let config = load_config(&path)?;
        if let Some(backend) = backend {
            if backend.to_lowercase() != config.backend_name() {
                return Err(anyhow!(
                    "backend {backend} does not match config backend {}",
                    config.backend_name()
                ));
            }
        }
        config
    } else {
        let backend =
            backend.ok_or_else(|| anyhow!("--backend or --backend-config is required"))?;

        if let Some(plugin) = plugins.iter().find(|p| p.name == backend.to_lowercase()) {
            load_config(&plugin.path)?
        } else {
            AdapterConfig::from_env(plugins, backend)?
        }
    };

    config.build()
}

fn load_config(path: &PathBuf) -> Result<AdapterConfig> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("read adapter config: {}", path.display()))?;
    let config: AdapterConfig = serde_yaml::from_str(&content)
        .with_context(|| format!("parse adapter config: {}", path.display()))?;
    Ok(config)
}

pub fn resolve_credentials(
    prefix: &str,
    url: Option<String>,
    token: Option<String>,
) -> Result<(String, String)> {
    let env_url = format!("{}_URL", prefix);
    let env_token = format!("{}_TOKEN", prefix);
    let url = url
        .or_else(|| std::env::var(&env_url).ok())
        .ok_or_else(|| anyhow!("missing {env_url} (or url in backend config)"))?;
    let token = token
        .or_else(|| std::env::var(&env_token).ok())
        .ok_or_else(|| anyhow!("missing {env_token} (or token in backend config)"))?;
    Ok((url, token))
}

#[cfg(test)]
mod tests {
    use super::resolve_credentials;
    use super::AdapterConfig;
    use super::ExternalConfig;
    #[cfg(feature = "infrahub")]
    use super::InfrahubSchemaConfig;
    #[cfg(unix)]
    use super::ProcessAdapter;
    use alembic_core::{JsonMap, Key, Object, Schema, TypeName, Uid};
    use alembic_engine::{Backend, BackendId, Op, StateData, StateStore};
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::Path;
    use std::sync::{Mutex, OnceLock};
    #[cfg(unix)]
    use std::time::Duration;
    use tempfile::tempdir;

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    /// serializes the tests that spawn a child process (via [`ProcessAdapter`] or
    /// an external backend). exec-ing a just-written script can race a sibling
    /// test thread's `fork` and fail to spawn with `ETXTBSY`; holding this lock
    /// for the whole spawn keeps no two of these tests running concurrently, so
    /// no fork can observe a half-written-then-exec'd file. it is a
    /// `tokio::sync::Mutex` so the async spawn tests can hold the guard across
    /// `.await` without tripping `clippy::await_holding_lock`.
    fn spawn_lock() -> &'static tokio::sync::Mutex<()> {
        static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    #[test]
    fn resolve_credentials_prefers_args() {
        let _guard = env_lock().lock().unwrap();
        let creds = resolve_credentials(
            "NETBOX",
            Some("http://example".to_string()),
            Some("token".to_string()),
        )
        .unwrap();
        assert_eq!(creds.0, "http://example");
        assert_eq!(creds.1, "token");
    }

    #[test]
    fn resolve_credentials_from_env() {
        let _guard = env_lock().lock().unwrap();
        let old_url = std::env::var("NETBOX_URL").ok();
        let old_token = std::env::var("NETBOX_TOKEN").ok();
        std::env::set_var("NETBOX_URL", "http://env");
        std::env::set_var("NETBOX_TOKEN", "envtoken");

        let result = std::panic::catch_unwind(|| {
            let creds = resolve_credentials("NETBOX", None, None).unwrap();
            assert_eq!(creds.0, "http://env");
            assert_eq!(creds.1, "envtoken");
        });

        if let Some(value) = old_url {
            std::env::set_var("NETBOX_URL", value);
        } else {
            std::env::remove_var("NETBOX_URL");
        }
        if let Some(value) = old_token {
            std::env::set_var("NETBOX_TOKEN", value);
        } else {
            std::env::remove_var("NETBOX_TOKEN");
        }

        assert!(result.is_ok());
    }

    #[test]
    fn resolve_credentials_missing_is_error() {
        let _guard = env_lock().lock().unwrap();
        let old_url = std::env::var("NETBOX_URL").ok();
        let old_token = std::env::var("NETBOX_TOKEN").ok();
        std::env::remove_var("NETBOX_URL");
        std::env::remove_var("NETBOX_TOKEN");

        let result = resolve_credentials("NETBOX", None, None);
        assert!(result.is_err());

        if let Some(value) = old_url {
            std::env::set_var("NETBOX_URL", value);
        }
        if let Some(value) = old_token {
            std::env::set_var("NETBOX_TOKEN", value);
        }
    }

    #[cfg(feature = "infrahub")]
    #[test]
    fn infrahub_schema_none_is_noop() {
        use crate::InfrahubSchemaMode;
        let config = InfrahubSchemaConfig {
            mode: InfrahubSchemaMode::None,
            schema_path: None,
            repository_id: None,
            repository_name: None,
            repository_root: None,
            branch: None,
            infrahubctl_path: None,
        };
        assert!(config.build().unwrap().is_none());
    }

    /// a typo'd key must not be discarded in silence: the run would take the
    /// default instead, and for a boolean like django's `no_migrate` that means
    /// migrating a database the user meant to leave untouched, reporting success.
    #[test]
    fn unknown_backend_config_key_is_rejected() {
        let cases: &[(&str, &str)] = &[
            #[cfg(feature = "netbox")]
            ("netbox", "backend: netbox\nurl: http://nb\ntokn: secret\n"),
            #[cfg(feature = "nautobot")]
            (
                "nautobot",
                "backend: nautobot\nurl: http://nb\ntokn: secret\n",
            ),
            #[cfg(feature = "infrahub")]
            (
                "infrahub",
                "backend: infrahub\nurl: http://ih\nbranchh: main\n",
            ),
            #[cfg(feature = "generic")]
            ("generic", "backend: generic\nconfig_pth: ./g.yaml\n"),
            #[cfg(feature = "peeringdb")]
            ("peeringdb", "backend: peeringdb\ntoken: secret\n"),
            #[cfg(feature = "django")]
            ("django", "backend: django\noutput: ./out\nno_admn: true\n"),
            (
                "external",
                "backend: external\ncommand: ./a\ntimeout_second: 5\n",
            ),
        ];

        for (backend, yaml) in cases {
            let err = serde_yaml::from_str::<AdapterConfig>(yaml)
                .expect_err(&format!("{backend}: unknown key must be rejected"));
            assert!(
                err.to_string().contains("unknown field"),
                "{backend}: expected an unknown-field error, got: {err}"
            );
        }
    }

    /// the tag the enum dispatches on is not itself an unknown field.
    #[test]
    fn known_backend_config_still_parses() {
        let config: AdapterConfig =
            serde_yaml::from_str("backend: external\ncommand: ./a\ntimeout_seconds: 5\n").unwrap();
        assert_eq!(config.backend_name(), "external");
    }

    /// peeringdb takes the credential from the environment, so a `token:`
    /// copied from a netbox config has to name itself rather than be dropped on
    /// the way to an unauthenticated read; both ways to select it keep working.
    #[cfg(feature = "peeringdb")]
    #[test]
    fn peeringdb_takes_no_config_keys() {
        let bare: AdapterConfig = serde_yaml::from_str("backend: peeringdb\n").unwrap();
        assert_eq!(bare.backend_name(), "peeringdb");
        assert_eq!(
            AdapterConfig::from_env(&[], "peeringdb")
                .unwrap()
                .backend_name(),
            "peeringdb"
        );

        let err = serde_yaml::from_str::<AdapterConfig>("backend: peeringdb\ntoken: secret\n")
            .expect_err("a key peeringdb does not read must be rejected");
        assert!(
            err.to_string().contains("token"),
            "expected the error to name the key, got: {err}"
        );
    }

    #[cfg(feature = "infrahub")]
    #[test]
    fn unknown_nested_infrahub_schema_key_is_rejected() {
        let err = serde_yaml::from_str::<AdapterConfig>(
            "backend: infrahub\nurl: http://ih\nschema:\n  mode: infrahubctl\n  schema_pth: ./s.yaml\n",
        )
        .expect_err("unknown key in a nested config must be rejected");
        assert!(
            err.to_string().contains("unknown field"),
            "expected an unknown-field error, got: {err}"
        );
    }

    #[cfg(unix)]
    fn make_executable(path: &Path) {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).unwrap();
    }

    fn write_script(path: &Path, contents: &str) {
        fs::write(path, contents).unwrap();
        #[cfg(unix)]
        make_executable(path);
    }

    #[tokio::test]
    async fn external_adapter_roundtrip() {
        let _spawn = spawn_lock().lock().await;
        let dir = tempdir().unwrap();
        let script_path = dir.path().join("adapter.sh");
        let script = r#"#!/usr/bin/env bash
set -euo pipefail
input="$(cat)"
if [[ "$input" == *"\"method\":\"read\""* ]]; then
  cat <<'JSON'
{"ok":true,"result":[{"type_name":"dcim.site","key":{"name":"site-a"},"attrs":{"name":"Site A"},"backend_id":"site-1"}]}
JSON
elif [[ "$input" == *"\"method\":\"write\""* ]]; then
  cat <<'JSON'
{"ok":true,"result":{"applied":[{"uid":"00000000-0000-0000-0000-000000000001","type_name":"dcim.site","backend_id":"site-1"}]}}
JSON
elif [[ "$input" == *"\"method\":\"ensure_schema\""* ]]; then
  cat <<'JSON'
{"ok":true,"result":{"created_fields":["field1"],"created_tags":[],"created_object_types":["dcim.site"],"created_object_fields":["dcim.site.name"]}}
JSON
else
  echo '{"ok":false,"error":"unknown method"}'
fi
"#;
        write_script(&script_path, script);

        let config = AdapterConfig::External(ExternalConfig {
            command: Some(script_path.to_string_lossy().to_string()),
            args: Vec::new(),
            working_dir: None,
            env: BTreeMap::new(),
            timeout_seconds: Some(5),
            setup: serde_yaml::Value::default(),
            instance: None,
        });
        let (backend, _) = config.build().unwrap();
        let schema = Schema {
            types: BTreeMap::new(),
        };
        let state = StateStore::new(None, StateData::default());

        let observed = backend
            .observer()
            .unwrap()
            .read(&schema, &[], &state)
            .await
            .unwrap();
        assert_eq!(observed.len(), 1);

        let uid = Uid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let key = Key::from(BTreeMap::from([("name".to_string(), json!("site-a"))]));
        let obj = Object {
            uid,
            type_name: TypeName::new("dcim.site"),
            key,
            attrs: JsonMap::from(BTreeMap::from([("name".to_string(), json!("Site A"))])),
            source: None,
        };
        let ops = vec![Op::Create {
            uid,
            type_name: TypeName::new("dcim.site"),
            desired: obj,
        }];
        let report = backend
            .emitter()
            .unwrap()
            .write(&schema, &ops, &state)
            .await
            .unwrap();
        assert_eq!(report.applied.len(), 1);
        assert_eq!(
            report.applied[0].backend_id,
            Some(BackendId::String("site-1".to_string()))
        );

        let provision = backend
            .emitter()
            .unwrap()
            .ensure_schema(&schema)
            .await
            .unwrap();
        assert!(provision
            .created_object_types
            .contains(&"dcim.site".to_string()));
    }

    /// write a script to `name` in `dir` and build an external backend around it.
    /// callers must hold the spawn lock: build() probes the adapter's capabilities,
    /// which spawns the script.
    fn build_external(dir: &Path, name: &str, script: &str) -> Backend {
        let script_path = dir.join(name);
        write_script(&script_path, script);
        AdapterConfig::External(ExternalConfig {
            command: Some(script_path.to_string_lossy().to_string()),
            args: Vec::new(),
            working_dir: None,
            env: BTreeMap::new(),
            timeout_seconds: Some(5),
            setup: serde_yaml::Value::default(),
            instance: None,
        })
        .build()
        .unwrap()
        .0
    }

    /// a script that answers capabilities with `response` and errors on all else.
    fn capabilities_script(response: &str) -> String {
        format!(
            r#"#!/usr/bin/env bash
req="$(cat)"
case "$req" in
  *'"method":"capabilities"'*) printf '{response}' ;;
  *) printf '{{"ok":false,"error":"unsupported"}}' ;;
esac
"#
        )
    }

    #[tokio::test]
    async fn external_capabilities_role_selects_the_backend_variant() {
        let _spawn = spawn_lock().lock().await;
        let dir = tempdir().unwrap();

        let observer = build_external(
            dir.path(),
            "observer.sh",
            &capabilities_script(r#"{"ok":true,"result":{"role":"observer"}}"#),
        );
        assert!(matches!(observer, Backend::Observer(_)));

        let emitter = build_external(
            dir.path(),
            "emitter.sh",
            &capabilities_script(r#"{"ok":true,"result":{"role":"emitter"}}"#),
        );
        assert!(matches!(emitter, Backend::Emitter(_)));

        let adapter = build_external(
            dir.path(),
            "adapter.sh",
            &capabilities_script(r#"{"ok":true,"result":{"role":"adapter"}}"#),
        );
        assert!(matches!(adapter, Backend::Adapter(_)));
    }

    #[tokio::test]
    async fn external_capabilities_probe_failure_defaults_to_adapter() {
        let _spawn = spawn_lock().lock().await;
        let dir = tempdir().unwrap();

        // an old adapter answering unknown-method, a garbage role, a non-json
        // answer, and a crash all default to the full read+write role; the probe
        // never fails construction.
        let scripts = [
            (
                "old.sh",
                capabilities_script(r#"{"ok":false,"error":"invalid request: unknown method"}"#),
            ),
            (
                "garbage.sh",
                capabilities_script(r#"{"ok":true,"result":{"role":"frobnicator"}}"#),
            ),
            (
                "nonjson.sh",
                "#!/usr/bin/env bash\ncat >/dev/null\nprintf 'not json'\n".to_string(),
            ),
            (
                "crash.sh",
                "#!/usr/bin/env bash\ncat >/dev/null\nexit 1\n".to_string(),
            ),
        ];
        for (name, script) in scripts {
            let backend = build_external(dir.path(), name, &script);
            assert!(
                matches!(backend, Backend::Adapter(_)),
                "{name} did not default to the adapter role"
            );
        }
    }

    /// a script that answers capabilities as an adapter and `answer` to everything else.
    fn answering_script(answer: &str) -> String {
        format!(
            r#"#!/usr/bin/env bash
req="$(cat)"
case "$req" in
  *'"method":"capabilities"'*) printf '{{"ok":true,"result":{{"role":"adapter"}}}}' ;;
  *) {answer} ;;
esac
"#
        )
    }

    /// every exit that reads what the adapter answered names the method it was sent,
    /// so a hand-written adapter's broken reply says which call failed.
    #[tokio::test]
    async fn external_answer_errors_name_the_method() {
        let _spawn = spawn_lock().lock().await;
        let dir = tempdir().unwrap();
        let schema = Schema {
            types: BTreeMap::new(),
        };
        // ensure_schema, because no exit's own wording could carry it by accident.
        let cases = [
            (
                "not-utf8.sh",
                r"printf '\377\376'",
                "external adapter ensure_schema response not utf-8",
            ),
            (
                "stray-line.sh",
                r#"printf 'warning: sourced a profile\n{"ok":true,"result":{}}'"#,
                "parse external adapter ensure_schema response",
            ),
            (
                "no-error.sh",
                r#"printf '{"ok":false}'"#,
                "external adapter ensure_schema error",
            ),
            (
                "no-result.sh",
                r#"printf '{"ok":true}'"#,
                "external adapter ensure_schema response missing result",
            ),
        ];

        let mut unnamed = Vec::new();
        for (name, answer, expected) in cases {
            let backend = build_external(dir.path(), name, &answering_script(answer));
            let err = backend
                .emitter()
                .unwrap()
                .ensure_schema(&schema)
                .await
                .unwrap_err();
            let message = err.to_string();
            if !message.contains(expected) {
                unnamed.push(format!("{name}: expected {expected:?}, got {message:?}"));
            }
        }
        // collected, not asserted in the loop, so a regression names every exit it hit.
        assert!(unnamed.is_empty(), "{}", unnamed.join("\n"));
    }

    #[tokio::test]
    async fn external_emitter_errors_on_observe_like_django() {
        let _spawn = spawn_lock().lock().await;
        let dir = tempdir().unwrap();
        let backend = build_external(
            dir.path(),
            "emit-only.sh",
            &capabilities_script(r#"{"ok":true,"result":{"role":"emitter"}}"#),
        );

        // plan and import observe through Backend::observer(); a declared emitter
        // gets the same up-front error as a built-in emitter; it must never
        // silently observe nothing.
        let Err(err) = backend.observer() else {
            panic!("a declared emitter must not observe");
        };
        assert!(
            err.to_string()
                .contains("backend is write-only; it cannot observe state"),
            "unexpected observer error: {err}"
        );
        // the write side stays reachable.
        assert!(backend.emitter().is_ok());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_kills_child_on_timeout() {
        // serialize against the other spawning tests so no sibling fork can race
        // the write-then-exec of the script below (see `spawn_lock`).
        let _spawn = spawn_lock().lock().await;
        let dir = tempdir().unwrap();
        let script_path = dir.path().join("slow.sh");
        write_script(&script_path, "#!/usr/bin/env bash\nsleep 2\ntouch \"$1\"\n");
        let sentinel = dir.path().join("sentinel");

        let adapter = ProcessAdapter {
            command: script_path.to_string_lossy().to_string(),
            args: vec![sentinel.to_string_lossy().to_string()],
            working_dir: None,
            env: BTreeMap::new(),
            timeout: Duration::from_secs(1),
            setup: serde_yaml::Value::default(),
        };

        let err = adapter.run(Vec::new()).await.unwrap_err();
        assert!(
            err.to_string().contains("timed out"),
            "expected timeout error, got: {err}"
        );

        // the child is SIGKILLed at ~1s so the `touch` after `sleep 2` never runs.
        for _ in 0..30 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(
                !sentinel.exists(),
                "adapter child kept running after timeout and touched the sentinel"
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_surfaces_adapter_exit_not_stdin_error() {
        // serialize against the other spawning tests so no sibling fork can race
        // the write-then-exec of the script below (see `spawn_lock`).
        let _spawn = spawn_lock().lock().await;
        let dir = tempdir().unwrap();
        let script_path = dir.path().join("crash.sh");
        // the adapter writes to stderr and exits 1 without reading stdin, so the
        // oversized stdin write below still hits BrokenPipe as a crashing adapter
        // would.
        write_script(
            &script_path,
            "#!/usr/bin/env bash\necho \"boom: bad config\" >&2\nexit 1\n",
        );

        let adapter = ProcessAdapter {
            command: script_path.to_string_lossy().to_string(),
            args: Vec::new(),
            working_dir: None,
            env: BTreeMap::new(),
            timeout: Duration::from_secs(5),
            setup: serde_yaml::Value::default(),
        };

        // 100 KB > the 64 KB pipe buffer, so the unread write hits BrokenPipe.
        let err = adapter.run(vec![b'x'; 100_000]).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("boom"),
            "expected the adapter's real stderr, got: {msg}"
        );
        assert!(
            !msg.contains("write external adapter stdin"),
            "stdin write error masked the real failure: {msg}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_times_out_when_child_never_drains_stdin() {
        // serialize against the other spawning tests so no sibling fork can race
        // the write-then-exec of the script below (see `spawn_lock`).
        let _spawn = spawn_lock().lock().await;
        let dir = tempdir().unwrap();
        let script_path = dir.path().join("stuck.sh");
        // stays alive for the whole run without ever reading its stdin, so the
        // oversized write below can only complete once we drain concurrently.
        write_script(&script_path, "#!/usr/bin/env bash\nsleep 30\n");

        let adapter = ProcessAdapter {
            command: script_path.to_string_lossy().to_string(),
            args: Vec::new(),
            working_dir: None,
            env: BTreeMap::new(),
            timeout: Duration::from_secs(1),
            setup: serde_yaml::Value::default(),
        };

        // 256 KB > the 64 KB pipe buffer, so an un-drained write blocks once the
        // buffer fills; it must trip the 1s timeout, not hang run().
        let payload = vec![b'x'; 256 * 1024];
        let result = tokio::time::timeout(Duration::from_secs(10), adapter.run(payload)).await;
        let run_result =
            result.expect("run() hung: stdin write deadlocked outside the timeout guard");
        assert!(
            run_result.unwrap_err().to_string().contains("timed out"),
            "expected a timeout error once the un-drained stdin write is under the guard"
        );
    }
}
