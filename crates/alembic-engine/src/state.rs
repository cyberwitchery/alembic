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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::BackendId;
    use alembic_core::{TypeName, Uid};
    use tempfile::tempdir;
    use uuid::Uuid;

    fn uid(n: u128) -> Uid {
        Uuid::from_u128(n)
    }

    fn t(name: &str) -> TypeName {
        TypeName::new(name)
    }

    #[test]
    fn state_data_default_is_empty() {
        let data = StateData::default();
        assert!(data.mappings.is_empty());
    }

    #[test]
    fn state_data_serde_empty() {
        let data = StateData::default();
        let json = serde_json::to_string(&data).unwrap();
        let back: StateData = serde_json::from_str(&json).unwrap();
        assert!(back.mappings.is_empty());
    }

    #[test]
    fn state_data_serde_round_trip() {
        let mut data = StateData::default();
        data.mappings
            .entry(t("device"))
            .or_default()
            .insert(uid(1), BackendId::Int(100));
        data.mappings
            .entry(t("device"))
            .or_default()
            .insert(uid(2), BackendId::String("abc".into()));
        data.mappings
            .entry(t("interface"))
            .or_default()
            .insert(uid(3), BackendId::Int(200));

        let json = serde_json::to_string_pretty(&data).unwrap();
        let back: StateData = serde_json::from_str(&json).unwrap();

        assert_eq!(back.mappings.len(), 2);
        assert_eq!(back.mappings[&t("device")][&uid(1)], BackendId::Int(100));
        assert_eq!(
            back.mappings[&t("device")][&uid(2)],
            BackendId::String("abc".into())
        );
        assert_eq!(back.mappings[&t("interface")][&uid(3)], BackendId::Int(200));
    }

    #[test]
    fn state_data_deserializes_missing_mappings_as_empty() {
        let data: StateData = serde_json::from_str("{}").unwrap();
        assert!(data.mappings.is_empty());
    }

    #[test]
    fn store_set_and_get_backend_id() {
        let mut store = StateStore::new(None, StateData::default());
        assert!(store.backend_id(t("device"), uid(1)).is_none());

        store.set_backend_id(t("device"), uid(1), BackendId::Int(42));
        assert_eq!(
            store.backend_id(t("device"), uid(1)),
            Some(BackendId::Int(42))
        );
    }

    #[test]
    fn store_remove_backend_id() {
        let mut store = StateStore::new(None, StateData::default());
        store.set_backend_id(t("device"), uid(1), BackendId::Int(42));
        store.remove_backend_id(t("device"), uid(1));
        assert!(store.backend_id(t("device"), uid(1)).is_none());
    }

    #[test]
    fn store_remove_nonexistent_is_noop() {
        let mut store = StateStore::new(None, StateData::default());
        store.remove_backend_id(t("device"), uid(99));
        assert!(store.all_mappings().is_empty());
    }

    #[test]
    fn store_all_mappings() {
        let mut store = StateStore::new(None, StateData::default());
        store.set_backend_id(t("device"), uid(1), BackendId::Int(1));
        store.set_backend_id(t("interface"), uid(2), BackendId::Int(2));
        assert_eq!(store.all_mappings().len(), 2);
    }

    #[test]
    fn store_overwrite_backend_id() {
        let mut store = StateStore::new(None, StateData::default());
        store.set_backend_id(t("device"), uid(1), BackendId::Int(1));
        store.set_backend_id(t("device"), uid(1), BackendId::Int(99));
        assert_eq!(
            store.backend_id(t("device"), uid(1)),
            Some(BackendId::Int(99))
        );
    }

    #[test]
    fn load_missing_file_returns_empty_state() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("does_not_exist.json");
        let store = StateStore::load(&path).unwrap();
        assert!(store.all_mappings().is_empty());
    }

    #[test]
    fn load_existing_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.json");
        let mut data = StateData::default();
        data.mappings
            .entry(t("device"))
            .or_default()
            .insert(uid(1), BackendId::Int(42));
        std::fs::write(&path, serde_json::to_string(&data).unwrap()).unwrap();

        let store = StateStore::load(&path).unwrap();
        assert_eq!(
            store.backend_id(t("device"), uid(1)),
            Some(BackendId::Int(42))
        );
    }

    #[tokio::test]
    async fn local_backend_save_load_round_trip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sub").join("state.json");
        let backend = LocalBackend { path: path.clone() };

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
    async fn local_backend_load_missing_returns_empty() {
        let dir = tempdir().unwrap();
        let backend = LocalBackend {
            path: dir.path().join("nope.json"),
        };
        let data = backend.load().await.unwrap();
        assert!(data.mappings.is_empty());
    }

    #[tokio::test]
    async fn store_save_and_reload_async() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.json");

        let mut store = StateStore::load(&path).unwrap();
        store.set_backend_id(t("device"), uid(1), BackendId::Int(7));
        store.save_async().await.unwrap();

        let mut store2 = StateStore::load(&path).unwrap();
        assert_eq!(
            store2.backend_id(t("device"), uid(1)),
            Some(BackendId::Int(7))
        );

        store2.set_backend_id(t("device"), uid(2), BackendId::Int(8));
        store2.save_async().await.unwrap();

        let mut reloaded = StateStore::load(&path).unwrap();
        reloaded.load_async().await.unwrap();
        assert_eq!(
            reloaded.backend_id(t("device"), uid(1)),
            Some(BackendId::Int(7))
        );
        assert_eq!(
            reloaded.backend_id(t("device"), uid(2)),
            Some(BackendId::Int(8))
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
