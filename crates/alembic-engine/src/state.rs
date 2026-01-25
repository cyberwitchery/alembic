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

    /// load state from a file path (local-only legacy helper).
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if path.exists() {
            let raw = fs::read_to_string(&path)
                .with_context(|| format!("read state: {}", path.display()))?;
            let data = serde_json::from_str::<StateData>(&raw)
                .with_context(|| format!("parse state: {}", path.display()))?;
            Ok(Self::new(Some(Arc::new(LocalBackend { path })), data))
        } else {
            Ok(Self::new(
                Some(Arc::new(LocalBackend { path })),
                StateData::default(),
            ))
        }
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

    /// persist state to disk (legacy helper).
    pub fn save(&self) -> Result<()> {
        if let Some(backend) = &self.backend {
            // Note: This is a hack to allow sync save for now,
            // should eventually move everything to async.
            let data = self.data.clone();
            let backend = Arc::clone(backend);
            // Check if we're in a tokio runtime
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                tokio::task::block_in_place(move || {
                    handle.block_on(async move { backend.save(&data).await })
                })?;
            } else {
                // No runtime, create one for this operation
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?
                    .block_on(async move { backend.save(&data).await })?;
            }
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
        fs::write(&self.path, raw)
            .with_context(|| format!("write state: {}", self.path.display()))?;
        Ok(())
    }
}
