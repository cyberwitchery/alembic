//! adapter registry and config loading for alembic.

use alembic_engine::Adapter;
use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;

const SUPPORTED_BACKENDS: &[&str] = &["netbox", "nautobot", "infrahub", "generic", "peeringdb"];

#[derive(Debug, Deserialize)]
#[serde(tag = "backend", rename_all = "kebab-case")]
pub enum AdapterConfig {
    Netbox(NetboxConfig),
    Nautobot(NautobotConfig),
    Infrahub(InfrahubConfig),
    Generic(GenericConfig),
    Peeringdb,
}

#[derive(Debug, Deserialize)]
pub struct NetboxConfig {
    pub url: Option<String>,
    pub token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct NautobotConfig {
    pub url: Option<String>,
    pub token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct InfrahubConfig {
    pub url: Option<String>,
    pub token: Option<String>,
    pub branch: Option<String>,
    #[serde(default)]
    pub schema: Option<InfrahubSchemaConfig>,
}

#[derive(Debug, Deserialize)]
pub struct GenericConfig {
    pub config: Option<alembic_adapter_generic::GenericConfig>,
    pub config_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum InfrahubSchemaMode {
    #[default]
    None,
    Infrahubctl,
    Repository,
}

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
            AdapterConfig::Netbox(_) => "netbox",
            AdapterConfig::Nautobot(_) => "nautobot",
            AdapterConfig::Infrahub(_) => "infrahub",
            AdapterConfig::Generic(_) => "generic",
            AdapterConfig::Peeringdb => "peeringdb",
        }
    }

    fn from_env(backend: &str) -> Result<Self> {
        match backend.to_lowercase().as_str() {
            "netbox" => Ok(AdapterConfig::Netbox(NetboxConfig {
                url: None,
                token: None,
            })),
            "nautobot" => Ok(AdapterConfig::Nautobot(NautobotConfig {
                url: None,
                token: None,
            })),
            "infrahub" => Ok(AdapterConfig::Infrahub(InfrahubConfig {
                url: None,
                token: None,
                branch: None,
                schema: None,
            })),
            "generic" => Ok(AdapterConfig::Generic(GenericConfig {
                config: None,
                config_path: None,
            })),
            "peeringdb" => Ok(AdapterConfig::Peeringdb),
            other => Err(anyhow!(
                "unsupported backend {other} (expected one of: {})",
                SUPPORTED_BACKENDS.join(", ")
            )),
        }
    }

    fn build(self) -> Result<Box<dyn Adapter>> {
        match self {
            AdapterConfig::Netbox(cfg) => {
                let (url, token) = resolve_credentials("NETBOX", cfg.url, cfg.token)?;
                Ok(Box::new(alembic_adapter_netbox::NetBoxAdapter::new(
                    &url, &token,
                )?))
            }
            AdapterConfig::Nautobot(cfg) => {
                let (url, token) = resolve_credentials("NAUTOBOT", cfg.url, cfg.token)?;
                Ok(Box::new(alembic_adapter_nautobot::NautobotAdapter::new(
                    &url, &token,
                )?))
            }
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
            AdapterConfig::Peeringdb => {
                Ok(Box::new(alembic_adapter_peeringdb::PeeringDBAdapter::new()))
            }
        }
    }
}

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

pub fn create_adapter(
    backend: Option<&str>,
    config_path: Option<PathBuf>,
) -> Result<Box<dyn Adapter>> {
    let config = if let Some(path) = config_path {
        let content = fs::read_to_string(&path)
            .with_context(|| format!("read adapter config: {}", path.display()))?;
        let config: AdapterConfig = serde_yaml::from_str(&content)
            .with_context(|| format!("parse adapter config: {}", path.display()))?;
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
        AdapterConfig::from_env(backend)?
    };

    config.build()
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
    use super::InfrahubSchemaConfig;
    use super::InfrahubSchemaMode;
    use std::sync::{Mutex, OnceLock};

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

    #[test]
    fn infrahub_schema_none_is_noop() {
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
}
