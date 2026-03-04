use super::Backend;
use alembic_adapter_nautobot::NautobotAdapter;
use alembic_adapter_netbox::NetBoxAdapter;
use alembic_engine::Adapter;
use anyhow::{anyhow, Context, Result};
use std::fs;
use std::path::PathBuf;

pub(super) fn create_adapter(
    backend: Backend,
    netbox_url: Option<String>,
    netbox_token: Option<String>,
    nautobot_url: Option<String>,
    nautobot_token: Option<String>,
    generic_config: Option<PathBuf>,
) -> Result<Box<dyn Adapter>> {
    match backend {
        Backend::Netbox => {
            let (url, token) = resolve_credentials("NETBOX", netbox_url, netbox_token)?;
            Ok(Box::new(NetBoxAdapter::new(&url, &token)?))
        }
        Backend::Nautobot => {
            let (url, token) = resolve_credentials("NAUTOBOT", nautobot_url, nautobot_token)?;
            Ok(Box::new(NautobotAdapter::new(&url, &token)?))
        }
        Backend::Generic => {
            let path = generic_config
                .ok_or_else(|| anyhow!("--generic-config required for generic backend"))?;
            let content = fs::read_to_string(&path)
                .with_context(|| format!("read generic config: {}", path.display()))?;
            let config: alembic_adapter_generic::GenericConfig = serde_yaml::from_str(&content)
                .with_context(|| format!("parse generic config: {}", path.display()))?;
            Ok(Box::new(alembic_adapter_generic::GenericAdapter::new(
                config,
            )?))
        }
        Backend::Peeringdb => {
            // API key is read from PEERINGDB_API_KEY env var by the peeringdb-rs crate
            Ok(Box::new(alembic_adapter_peeringdb::PeeringDBAdapter::new()))
        }
    }
}

pub(super) fn resolve_credentials(
    prefix: &str,
    url: Option<String>,
    token: Option<String>,
) -> Result<(String, String)> {
    let env_url = format!("{}_URL", prefix);
    let env_token = format!("{}_TOKEN", prefix);
    let url = url
        .or_else(|| std::env::var(&env_url).ok())
        .ok_or_else(|| anyhow!("missing --{}-url or {}", prefix.to_lowercase(), env_url))?;
    let token = token
        .or_else(|| std::env::var(&env_token).ok())
        .ok_or_else(|| anyhow!("missing --{}-token or {}", prefix.to_lowercase(), env_token))?;
    Ok((url, token))
}
