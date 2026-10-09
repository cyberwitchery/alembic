use alembic_engine::{BackendIdentity, PostgresTlsMode, StateContext, StateLock, StateStore};
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

/// load the state store for one backend instance and hold it to that identity:
/// the default path is scoped per backend, an explicit ALEMBIC_STATE_PATH still
/// answers to the stamp inside the file, and a mismatch is a hard error.
pub(super) async fn load_state(lock: StateLock, identity: &BackendIdentity) -> Result<StateStore> {
    Ok(load_state_with_context(lock, identity).await?.0)
}

/// `load_state`, plus what was loaded, taken before `ensure_backend` stamps an
/// empty store.
pub(super) async fn load_state_with_context(
    lock: StateLock,
    identity: &BackendIdentity,
) -> Result<(StateStore, StateContext)> {
    let root = Path::new(".");
    let (mut store, storage, location, present) =
        match resolve_state_backend_config(root, identity)? {
            StateBackendConfig::Local { path } => {
                let store = StateStore::load_with(&path, lock)?;
                let present = path.try_exists()?;
                (store, "local", path.display().to_string(), present)
            }
            StateBackendConfig::Postgres { url, key, tls_mode } => {
                let store = StateStore::load_postgres(url, key.clone(), tls_mode).await?;
                // a saved row is stamped, a new one is not. the location is the
                // row key: the url may carry credentials.
                let present = store.backend_identity().is_some();
                (store, "postgres", key, present)
            }
        };
    let context = StateContext {
        storage: storage.to_string(),
        location,
        present,
        backend: identity.clone(),
        bindings_loaded: store.all_mappings().values().map(|m| m.len()).sum(),
    };
    store.ensure_backend(identity)?;
    // apply journals are local scratch; keep them alongside state under `.alembic/`
    // even when the state backend is postgres.
    Ok((store.with_journal_dir(root.join(".alembic")), context))
}

pub(super) fn print_state_context(context: &StateContext) {
    if context.present {
        eprintln!(
            "state: {} for {}, {} bindings",
            context.location, context.backend, context.bindings_loaded
        );
    } else {
        eprintln!(
            "state: none at {} for {}, 0 bindings",
            context.location, context.backend
        );
    }
}

pub(super) fn state_path(root: &Path, identity: &BackendIdentity) -> PathBuf {
    root.join(".alembic").join("state").join(format!(
        "{}-{}.json",
        identity.adapter,
        identity.scope_hash()
    ))
}

pub(super) fn resolve_state_backend_config(
    root: &Path,
    identity: &BackendIdentity,
) -> Result<StateBackendConfig> {
    let backend = std::env::var("ALEMBIC_STATE_BACKEND").unwrap_or_else(|_| "local".to_string());
    match backend.to_lowercase().as_str() {
        "local" | "file" => {
            let path = std::env::var("ALEMBIC_STATE_PATH")
                .map(PathBuf::from)
                .unwrap_or_else(|_| state_path(root, identity));
            Ok(StateBackendConfig::Local { path })
        }
        "postgres" | "postgresql" => {
            let url = std::env::var("ALEMBIC_STATE_POSTGRES_URL")
                .map_err(|_| anyhow!("missing ALEMBIC_STATE_POSTGRES_URL"))?;
            // the row key is backend-scoped like the local path, so several
            // backends share one database without sharing a row. the env var is
            // the workspace namespace, not the final key.
            let workspace =
                std::env::var("ALEMBIC_STATE_KEY").unwrap_or_else(|_| "default".to_string());
            let key = format!("{workspace}/{}-{}", identity.adapter, identity.scope_hash());
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
