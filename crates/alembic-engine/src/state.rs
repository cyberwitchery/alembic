//! local uid -> backend id state store.

use crate::types::BackendId;
use alembic_core::{TypeName, Uid};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use std::sync::Arc;

/// on-disk state schema.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StateData {
    #[serde(default)]
    pub mappings: BTreeMap<TypeName, BTreeMap<Uid, BackendId>>,
}

/// TLS configuration for postgres state backend connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresTlsMode {
    Disable,
    Require,
}

/// trait for pluggable state backends.
#[async_trait::async_trait]
pub trait StateBackend: Send + Sync + std::fmt::Debug {
    async fn load(&self) -> Result<StateData>;
    async fn save(&self, data: &StateData) -> Result<()>;
}

/// state store wrapper with load/save helpers.
#[derive(Debug, Clone)]
pub struct StateStore {
    backend: Option<Arc<dyn StateBackend>>,
    data: StateData,
}

impl StateStore {
    /// create a new state store with an optional backend.
    pub fn new(backend: Option<Arc<dyn StateBackend>>, data: StateData) -> Self {
        Self { backend, data }
    }

    /// load state from a file path.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let data = if path.exists() {
            let raw = fs::read_to_string(&path)
                .with_context(|| format!("read state: {}", path.display()))?;
            serde_json::from_str::<StateData>(&raw)
                .with_context(|| format!("parse state: {}", path.display()))?
        } else {
            StateData::default()
        };
        Ok(Self::new(Some(Arc::new(LocalBackend { path })), data))
    }

    /// load state from a postgres backend.
    pub async fn load_postgres(
        url: impl Into<String>,
        key: impl Into<String>,
        tls_mode: PostgresTlsMode,
    ) -> Result<Self> {
        let backend: Arc<dyn StateBackend> = Arc::new(PostgresBackend {
            url: url.into(),
            key: key.into(),
            tls_mode,
        });
        let data = backend.load().await?;
        Ok(Self::new(Some(backend), data))
    }

    /// load state from the configured backend.
    pub async fn load_async(&mut self) -> Result<()> {
        if let Some(backend) = &self.backend {
            self.data = backend.load().await?;
        }
        Ok(())
    }

    /// persist state to the configured backend.
    pub async fn save_async(&self) -> Result<()> {
        if let Some(backend) = &self.backend {
            backend.save(&self.data).await?;
        }
        Ok(())
    }

    /// lookup a backend id by type + uid.
    pub fn backend_id(&self, type_name: TypeName, uid: Uid) -> Option<BackendId> {
        self.data
            .mappings
            .get(&type_name)
            .and_then(|map| map.get(&uid).cloned())
    }

    /// set a backend id mapping.
    pub fn set_backend_id(&mut self, type_name: TypeName, uid: Uid, backend_id: BackendId) {
        self.data
            .mappings
            .entry(type_name)
            .or_default()
            .insert(uid, backend_id);
    }

    /// remove a backend id mapping.
    pub fn remove_backend_id(&mut self, type_name: TypeName, uid: Uid) {
        if let Some(type_map) = self.data.mappings.get_mut(&type_name) {
            type_map.remove(&uid);
        }
    }

    /// return all mappings for external use.
    pub fn all_mappings(&self) -> &BTreeMap<TypeName, BTreeMap<Uid, BackendId>> {
        &self.data.mappings
    }
}

#[derive(Debug)]
struct LocalBackend {
    path: PathBuf,
}

#[async_trait::async_trait]
impl StateBackend for LocalBackend {
    async fn load(&self) -> Result<StateData> {
        if self.path.exists() {
            let raw = fs::read_to_string(&self.path)
                .with_context(|| format!("read state: {}", self.path.display()))?;
            let data = serde_json::from_str::<StateData>(&raw)
                .with_context(|| format!("parse state: {}", self.path.display()))?;
            Ok(data)
        } else {
            Ok(StateData::default())
        }
    }

    async fn save(&self, data: &StateData) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create state dir: {}", parent.display()))?;
        }
        let raw = serde_json::to_string_pretty(data)?;
        let tmp = self.path.with_extension("json.tmp");
        fs::write(&tmp, &raw).with_context(|| format!("write state tmp: {}", tmp.display()))?;
        fs::rename(&tmp, &self.path)
            .with_context(|| format!("write state: {}", self.path.display()))?;
        Ok(())
    }
}

#[derive(Debug)]
struct PostgresBackend {
    url: String,
    key: String,
    tls_mode: PostgresTlsMode,
}

impl PostgresBackend {
    async fn connect(&self) -> Result<tokio_postgres::Client> {
        match self.tls_mode {
            PostgresTlsMode::Disable => {
                let (client, connection) =
                    tokio_postgres::connect(&self.url, tokio_postgres::NoTls)
                        .await
                        .with_context(|| "connect postgres state backend")?;
                tokio::spawn(async move {
                    if let Err(err) = connection.await {
                        tracing::warn!("postgres state backend connection error: {err}");
                    }
                });
                Ok(client)
            }
            PostgresTlsMode::Require => {
                let connector = native_tls::TlsConnector::builder()
                    .build()
                    .with_context(|| "build postgres TLS connector")?;
                let connector = postgres_native_tls::MakeTlsConnector::new(connector);
                let (client, connection) = tokio_postgres::connect(&self.url, connector)
                    .await
                    .with_context(|| "connect postgres state backend")?;
                tokio::spawn(async move {
                    if let Err(err) = connection.await {
                        tracing::warn!("postgres state backend connection error: {err}");
                    }
                });
                Ok(client)
            }
        }
    }

    // The postgres table is expected to be pre-provisioned.
}

#[async_trait::async_trait]
impl StateBackend for PostgresBackend {
    async fn load(&self) -> Result<StateData> {
        let client = self.connect().await?;

        let row = client
            .query_opt(
                "SELECT payload::text FROM alembic_state WHERE state_key = $1",
                &[&self.key],
            )
            .await
            .with_context(|| "load postgres state payload")?;

        let Some(row) = row else {
            return Ok(StateData::default());
        };

        let raw: String = row
            .try_get(0)
            .with_context(|| "decode postgres state payload")?;
        serde_json::from_str::<StateData>(&raw).with_context(|| "parse postgres state payload")
    }

    async fn save(&self, data: &StateData) -> Result<()> {
        let client = self.connect().await?;

        let payload = serde_json::to_string(data)?;
        client
            .execute(
                "INSERT INTO alembic_state (state_key, payload, updated_at)
                 VALUES ($1, CAST($2 AS TEXT)::jsonb, NOW())
                 ON CONFLICT (state_key)
                 DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW()",
                &[&self.key, &payload],
            )
            .await
            .with_context(|| "save postgres state payload")?;
        Ok(())
    }
}
