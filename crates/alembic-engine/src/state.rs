//! local uid -> backend id state store.

use crate::types::BackendId;
use alembic_core::{TypeName, Uid};
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::Client;

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
    async fn load(&mut self) -> Result<StateData>;
    async fn save(&mut self, data: &StateData) -> Result<()>;
}

/// state store wrapper with load/save helpers.
#[derive(Debug, Clone)]
pub struct StateStore {
    backend: Option<Arc<Mutex<dyn StateBackend>>>,
    data: StateData,
}

impl StateStore {
    /// create a new state store with an optional backend.
    pub fn new(backend: Option<Arc<Mutex<dyn StateBackend>>>, data: StateData) -> Self {
        Self { backend, data }
    }

    /// load state from a file path.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        // Always create a backend so we can save to the same path later.
        // The backend's load() method handles missing files gracefully.
        let backend: Option<Arc<Mutex<dyn StateBackend>>> =
            Some(Arc::new(Mutex::new(LocalBackend { path: path.clone() }))
                as Arc<Mutex<dyn StateBackend>>);
        let data = if path.exists() {
            let raw = fs::read_to_string(&path)
                .with_context(|| format!("read state: {}", path.display()))?;
            serde_json::from_str::<StateData>(&raw)
                .with_context(|| format!("parse state: {}", path.display()))?
        } else {
            StateData::default()
        };
        Ok(Self::new(backend, data))
    }

    /// load state from a postgres backend.
    pub async fn load_postgres(
        url: impl Into<String>,
        key: impl Into<String>,
        tls_mode: PostgresTlsMode,
    ) -> Result<Self> {
        let mut postgres_backend = PostgresBackend {
            url: url.into(),
            key: key.into(),
            tls_mode,
            loaded_version: None,
            table_ensured: false,
        };
        let data = postgres_backend.load().await?;
        let backend: Arc<Mutex<dyn StateBackend>> = Arc::new(Mutex::new(postgres_backend));
        Ok(Self::new(Some(backend), data))
    }

    /// load state from the configured backend.
    pub async fn load_async(&mut self) -> Result<()> {
        if let Some(backend) = &self.backend {
            self.data = backend.lock().await.load().await?;
        }
        Ok(())
    }

    /// persist state to the configured backend.
    pub async fn save_async(&self) -> Result<()> {
        if let Some(backend) = &self.backend {
            backend.lock().await.save(&self.data).await?;
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
    async fn load(&mut self) -> Result<StateData> {
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

    async fn save(&mut self, data: &StateData) -> Result<()> {
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
    loaded_version: Option<i32>,
    table_ensured: bool,
}

#[async_trait::async_trait]
impl StateBackend for PostgresBackend {
    async fn load(&mut self) -> Result<StateData> {
        let client = self.connect().await?;

        let row = client
            .query_opt(
                "SELECT payload::text, loaded_version FROM alembic_state WHERE state_key = $1",
                &[&self.key],
            )
            .await
            .with_context(|| "load postgres state payload")?;

        let Some(row) = row else {
            self.loaded_version = Some(0);
            return Ok(StateData::default());
        };

        self.loaded_version = Some(row.try_get::<_, i32>("loaded_version")?);

        let raw: String = row
            .try_get(0)
            .with_context(|| "decode postgres state payload")?;
        serde_json::from_str::<StateData>(&raw).with_context(|| "parse postgres state payload")
    }

    async fn save(&mut self, data: &StateData) -> Result<()> {
        let client = self.connect().await?;
        let Some(loaded_version) = self.loaded_version else {
            return Err(anyhow!("must load state before saving"));
        };

        let payload = serde_json::to_string(data)?;
        let rows_modified = client
            .execute(
                "INSERT INTO alembic_state (state_key, payload, updated_at)
                 VALUES ($1, CAST($2 AS TEXT)::jsonb, NOW())
                 ON CONFLICT (state_key)
                 DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW(), loaded_version = $3+1
                 WHERE alembic_state.loaded_version = $3",
                &[&self.key, &payload, &loaded_version],
            )
            .await
            .with_context(|| "save postgres state payload")?;

        if rows_modified == 0 {
            return Err(anyhow!(
                "failed to save postgres state for key '{}': optimistic lock failed (expected loaded_version={})",
                self.key,
                loaded_version
            ));
        }

        self.loaded_version = Some(loaded_version + 1);

        Ok(())
    }
}

impl PostgresBackend {
    async fn ensure_table(&mut self, client: &Client) -> Result<()> {
        if self.table_ensured {
            return Ok(());
        }

        client
            .execute(
                "CREATE TABLE IF NOT EXISTS alembic_state (\
                state_key TEXT PRIMARY KEY, \
                payload JSONB NOT NULL, \
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), \
                loaded_version INTEGER NOT NULL DEFAULT 1\
                )",
                &[],
            )
            .await
            .with_context(|| "ensure postgres alembic_state table exists")?;

        // If the table already existed, CREATE TABLE IF NOT EXISTS won't add new columns.
        client
            .execute(
                "ALTER TABLE alembic_state ADD COLUMN IF NOT EXISTS loaded_version INTEGER NOT NULL DEFAULT 1",
                &[],
            )
            .await
            .with_context(|| "ensure postgres alembic_state.loaded_version column exists")?;

        self.table_ensured = true;
        Ok(())
    }

    async fn connect(&mut self) -> Result<tokio_postgres::Client> {
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
                self.ensure_table(&client).await?;
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
                self.ensure_table(&client).await?;
                Ok(client)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn t(s: &str) -> TypeName {
        TypeName::new(s)
    }

    fn uid(n: u128) -> Uid {
        Uid::from_u128(n)
    }

    #[test]
    fn state_data_default_is_empty() {
        let data = StateData::default();
        assert!(data.mappings.is_empty());
    }

    #[test]
    fn backend_id_returns_none_for_missing_type() {
        let store = StateStore::new(None, StateData::default());
        assert_eq!(store.backend_id(t("site"), uid(1)), None);
    }

    #[test]
    fn backend_id_returns_none_for_missing_uid() {
        let mut data = StateData::default();
        data.mappings
            .entry(t("site"))
            .or_default()
            .insert(uid(1), BackendId::Int(42));
        let store = StateStore::new(None, data);
        assert_eq!(store.backend_id(t("site"), uid(2)), None);
    }

    #[test]
    fn backend_id_returns_value_for_existing_mapping() {
        let mut data = StateData::default();
        data.mappings
            .entry(t("site"))
            .or_default()
            .insert(uid(1), BackendId::Int(42));
        let store = StateStore::new(None, data);
        assert_eq!(
            store.backend_id(t("site"), uid(1)),
            Some(BackendId::Int(42))
        );
    }

    #[test]
    fn set_backend_id_creates_mapping() {
        let mut store = StateStore::new(None, StateData::default());
        store.set_backend_id(t("site"), uid(1), BackendId::Int(42));
        assert_eq!(
            store.backend_id(t("site"), uid(1)),
            Some(BackendId::Int(42))
        );
    }

    #[test]
    fn set_backend_id_overwrites_existing() {
        let mut data = StateData::default();
        data.mappings
            .entry(t("site"))
            .or_default()
            .insert(uid(1), BackendId::Int(42));
        let mut store = StateStore::new(None, data);
        store.set_backend_id(t("site"), uid(1), BackendId::Int(99));
        assert_eq!(
            store.backend_id(t("site"), uid(1)),
            Some(BackendId::Int(99))
        );
    }

    #[test]
    fn remove_backend_id_removes_mapping() {
        let mut data = StateData::default();
        data.mappings
            .entry(t("site"))
            .or_default()
            .insert(uid(1), BackendId::Int(42));
        let mut store = StateStore::new(None, data);
        store.remove_backend_id(t("site"), uid(1));
        assert_eq!(store.backend_id(t("site"), uid(1)), None);
    }

    #[test]
    fn remove_backend_id_noop_for_missing() {
        let mut store = StateStore::new(None, StateData::default());
        store.remove_backend_id(t("site"), uid(1));
        // Should not panic
    }

    #[test]
    fn all_mappings_returns_internal_reference() {
        let store = StateStore::new(None, StateData::default());
        assert!(store.all_mappings().is_empty());
    }

    #[tokio::test]
    async fn local_backend_load_missing_returns_empty() {
        let dir = TempDir::new().unwrap();
        let mut backend = LocalBackend {
            path: dir.path().join("nope.json"),
        };
        let data = backend.load().await.unwrap();
        assert!(data.mappings.is_empty());
    }

    #[tokio::test]
    async fn local_backend_save_load_round_trip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("sub").join("state.json");
        let mut backend = LocalBackend { path: path.clone() };

        let mut data = StateData::default();
        data.mappings
            .entry(t("site"))
            .or_default()
            .insert(uid(10), BackendId::String("site-001".into()));

        backend.save(&data).await.unwrap();
        assert!(path.exists());

        let loaded = backend.load().await.unwrap();
        assert_eq!(
            loaded.mappings[&t("site")][&uid(10)],
            BackendId::String("site-001".into())
        );
    }

    #[tokio::test]
    async fn store_save_without_backend_is_noop() {
        let store = StateStore::new(None, StateData::default());
        store.save_async().await.unwrap();
    }

    #[tokio::test]
    async fn store_load_async_without_backend_is_noop() {
        let mut store = StateStore::new(None, StateData::default());
        store.set_backend_id(t("x"), uid(1), BackendId::Int(1));
        store.load_async().await.unwrap();
        assert_eq!(store.backend_id(t("x"), uid(1)), Some(BackendId::Int(1)));
    }
}
