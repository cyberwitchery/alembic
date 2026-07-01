use alembic_engine::{PostgresTlsMode, StateStore};
use anyhow::{anyhow, Result};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum StateBackendConfig {
    Local {
        path: PathBuf,
    },
    Postgres {
        url: String,
        key: String,
        tls_mode: PostgresTlsMode,
    },
}

pub(super) async fn load_state() -> Result<StateStore> {
    let root = Path::new(".");
    let store = match resolve_state_backend_config(root)? {
        StateBackendConfig::Local { path } => StateStore::load(path)?,
        StateBackendConfig::Postgres { url, key, tls_mode } => {
            StateStore::load_postgres(url, key, tls_mode).await?
        }
    };
    // apply journals are local scratch; keep them alongside state under `.alembic/`
    // even when the state backend is postgres.
    Ok(store.with_journal_dir(root.join(".alembic")))
}

pub(super) fn state_path(root: &Path) -> PathBuf {
    root.join(".alembic").join("state.json")
}

pub(super) fn resolve_state_backend_config(root: &Path) -> Result<StateBackendConfig> {
    let backend = std::env::var("ALEMBIC_STATE_BACKEND").unwrap_or_else(|_| "local".to_string());
    match backend.to_lowercase().as_str() {
        "local" | "file" => {
            let path = std::env::var("ALEMBIC_STATE_PATH")
                .map(PathBuf::from)
                .unwrap_or_else(|_| state_path(root));
            Ok(StateBackendConfig::Local { path })
        }
        "postgres" | "postgresql" => {
            let url = std::env::var("ALEMBIC_STATE_POSTGRES_URL")
                .map_err(|_| anyhow!("missing ALEMBIC_STATE_POSTGRES_URL"))?;
            let key = std::env::var("ALEMBIC_STATE_KEY").unwrap_or_else(|_| "default".to_string());
            let tls_mode = resolve_postgres_tls_mode()?;
            Ok(StateBackendConfig::Postgres { url, key, tls_mode })
        }
        other => Err(anyhow!(
            "unsupported ALEMBIC_STATE_BACKEND '{}'; expected local|file|postgres",
            other
        )),
    }
}

fn resolve_postgres_tls_mode() -> Result<PostgresTlsMode> {
    let raw = std::env::var("ALEMBIC_STATE_POSTGRES_TLS").unwrap_or_else(|_| "disable".to_string());
    match raw.to_lowercase().as_str() {
        "disable" | "off" | "false" | "no" => Ok(PostgresTlsMode::Disable),
        "require" | "on" | "true" | "yes" => Ok(PostgresTlsMode::Require),
        other => Err(anyhow!(
            "unsupported ALEMBIC_STATE_POSTGRES_TLS '{}'; expected disable|require",
            other
        )),
    }
}
