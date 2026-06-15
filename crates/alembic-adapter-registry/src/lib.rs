//! adapter registry and config loading for alembic.

use alembic_engine::{
    Adapter, ApplyReport, BackendId, ObservedObject, ObservedState, Op, ProvisionReport, StateData,
    StateStore,
};
use anyhow::{anyhow, Context, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
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
use alembic_adapter_django::cast_django::DjangoConfig;

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
    Peeringdb,
    #[cfg(feature = "django")]
    Django(DjangoConfig),
    External(ExternalConfig),
}

#[cfg(feature = "netbox")]
#[derive(Debug, Deserialize)]
pub struct NetboxConfig {
    pub url: Option<String>,
    pub token: Option<String>,
}

#[cfg(feature = "nautobot")]
#[derive(Debug, Deserialize)]
pub struct NautobotConfig {
    pub url: Option<String>,
    pub token: Option<String>,
}

#[cfg(feature = "infrahub")]
#[derive(Debug, Deserialize)]
pub struct InfrahubConfig {
    pub url: Option<String>,
    pub token: Option<String>,
    pub branch: Option<String>,
    #[serde(default)]
    pub schema: Option<InfrahubSchemaConfig>,
}

#[cfg(feature = "generic")]
#[derive(Debug, Deserialize)]
pub struct GenericConfig {
    pub config: Option<alembic_adapter_generic::GenericConfig>,
    pub config_path: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
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
            AdapterConfig::Peeringdb => "peeringdb",
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
            })),
            #[cfg(feature = "nautobot")]
            "nautobot" => Ok(AdapterConfig::Nautobot(NautobotConfig {
                url: None,
                token: None,
            })),
            #[cfg(feature = "infrahub")]
            "infrahub" => Ok(AdapterConfig::Infrahub(InfrahubConfig {
                url: None,
                token: None,
                branch: None,
                schema: None,
            })),
            #[cfg(feature = "generic")]
            "generic" => Ok(AdapterConfig::Generic(GenericConfig {
                config: None,
                config_path: None,
            })),
            #[cfg(feature = "peeringdb")]
            "peeringdb" => Ok(AdapterConfig::Peeringdb),
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

    pub fn build(self) -> Result<Box<dyn Adapter>> {
        match self {
            #[cfg(feature = "netbox")]
            AdapterConfig::Netbox(cfg) => {
                let (url, token) = resolve_credentials("NETBOX", cfg.url, cfg.token)?;
                Ok(Box::new(alembic_adapter_netbox::NetBoxAdapter::new(
                    &url, &token,
                )?))
            }
            #[cfg(feature = "nautobot")]
            AdapterConfig::Nautobot(cfg) => {
                let (url, token) = resolve_credentials("NAUTOBOT", cfg.url, cfg.token)?;
                Ok(Box::new(alembic_adapter_nautobot::NautobotAdapter::new(
                    &url, &token,
                )?))
            }
            #[cfg(feature = "infrahub")]
            AdapterConfig::Infrahub(cfg) => {
                let (url, token) = resolve_credentials("INFRAHUB", cfg.url, cfg.token)?;
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
                Ok(Box::new(adapter))
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
                Ok(Box::new(alembic_adapter_generic::GenericAdapter::new(
                    config,
                )?))
            }
            #[cfg(feature = "peeringdb")]
            AdapterConfig::Peeringdb => {
                Ok(Box::new(alembic_adapter_peeringdb::PeeringDBAdapter::new()))
            }
            #[cfg(feature = "django")]
            AdapterConfig::Django(cfg) => {
                Ok(Box::new(alembic_adapter_django::DjangoAdapter::new(cfg)))
            }
            AdapterConfig::External(cfg) => Ok(Box::new(ProcessAdapter::new(cfg)?)),
        }
    }
}

#[derive(Debug, Clone)]
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
        let timeout = Duration::from_secs(cfg.timeout_seconds.unwrap_or(120));
        Ok(Self {
            command,
            args: cfg.args,
            working_dir: cfg.working_dir,
            env: cfg.env,
            timeout,
            setup: cfg.setup,
        })
    }

    async fn call<R: DeserializeOwned>(&self, request: ExternalRequest<'_>) -> Result<R> {
        let envelope = ExternalEnvelope {
            version: 1,
            request,
            setup: self.setup.clone(),
        };
        let payload = serde_json::to_vec(&envelope).context("serialize external request")?;
        let output = self.run(payload).await?;
        let stdout =
            String::from_utf8(output.stdout).context("external adapter response not utf-8")?;
        let response: ExternalResponse<JsonValue> =
            serde_json::from_str(&stdout).context("parse external adapter response")?;
        if !response.ok {
            let message = response
                .error
                .unwrap_or_else(|| "external adapter error".to_string());
            return Err(anyhow!(message));
        }
        let result = response
            .result
            .ok_or_else(|| anyhow!("external adapter response missing result"))?;
        serde_json::from_value(result).context("deserialize external adapter result")
    }

    async fn run(&self, payload: Vec<u8>) -> Result<std::process::Output> {
        let mut cmd = Command::new(&self.command);
        cmd.args(&self.args)
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
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(&payload)
                .await
                .context("write external adapter stdin")?;
        }

        let output = timeout(self.timeout, child.wait_with_output())
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

        Ok(output)
    }
}

#[async_trait::async_trait]
impl Adapter for ProcessAdapter {
    async fn read(
        &self,
        schema: &alembic_core::Schema,
        types: &[alembic_core::TypeName],
        state: &StateStore,
    ) -> Result<ObservedState> {
        let state = StateData {
            mappings: state.all_mappings().clone(),
        };
        let objects: Vec<ObservedObjectData> = self
            .call(ExternalRequest::Read {
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

    async fn write(
        &self,
        schema: &alembic_core::Schema,
        ops: &[Op],
        state: &StateStore,
    ) -> Result<ApplyReport> {
        let state = StateData {
            mappings: state.all_mappings().clone(),
        };
        self.call(ExternalRequest::Write { schema, ops, state })
            .await
    }

    async fn ensure_schema(&self, schema: &alembic_core::Schema) -> Result<ProvisionReport> {
        self.call(ExternalRequest::EnsureSchema { schema }).await
    }
}

#[derive(Debug, Serialize)]
struct ExternalEnvelope<'a> {
    version: u8,
    setup: serde_yaml::Value,
    #[serde(flatten)]
    request: ExternalRequest<'a>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "method", rename_all = "snake_case")]
enum ExternalRequest<'a> {
    Read {
        schema: &'a alembic_core::Schema,
        types: &'a [alembic_core::TypeName],
        state: StateData,
    },
    Write {
        schema: &'a alembic_core::Schema,
        ops: &'a [Op],
        state: StateData,
    },
    EnsureSchema {
        schema: &'a alembic_core::Schema,
    },
}

#[derive(Debug, Deserialize)]
struct ExternalResponse<T> {
    ok: bool,
    result: Option<T>,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ObservedObjectData {
    type_name: alembic_core::TypeName,
    key: alembic_core::Key,
    attrs: alembic_core::JsonMap,
    backend_id: Option<BackendId>,
}

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

pub fn create_adapter(
    plugins: &[Plugin],
    backend: Option<&str>,
    config_path: Option<PathBuf>,
) -> Result<Box<dyn Adapter>> {
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
    use alembic_core::{JsonMap, Key, Object, Schema, TypeName, Uid};
    use alembic_engine::{BackendId, Op, StateData, StateStore};
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::Path;
    use std::sync::{Mutex, OnceLock};
    use tempfile::tempdir;

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
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
        });
        let adapter = config.build().unwrap();
        let schema = Schema {
            types: BTreeMap::new(),
        };
        let state = StateStore::new(None, StateData::default());

        let observed = adapter.read(&schema, &[], &state).await.unwrap();
        assert_eq!(observed.by_key.len(), 1);

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
        let report = adapter.write(&schema, &ops, &state).await.unwrap();
        assert_eq!(report.applied.len(), 1);
        assert_eq!(
            report.applied[0].backend_id,
            Some(BackendId::String("site-1".to_string()))
        );

        let provision = adapter.ensure_schema(&schema).await.unwrap();
        assert!(provision
            .created_object_types
            .contains(&"dcim.site".to_string()));
    }
}
