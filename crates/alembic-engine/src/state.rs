//! local uid -> backend id state store.

use alembic_core::{Kind, Uid};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

/// on-disk state schema.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct StateData {
    #[serde(default)]
    mappings: BTreeMap<Kind, BTreeMap<Uid, u64>>,
}

/// state store wrapper with load/save helpers.
#[derive(Debug, Clone)]
pub struct StateStore {
    path: PathBuf,
    data: StateData,
}

impl StateStore {
    /// load state from disk (or create empty when absent).
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if path.exists() {
            let raw = fs::read_to_string(&path)
                .with_context(|| format!("read state: {}", path.display()))?;
            let data = serde_json::from_str::<StateData>(&raw)
                .with_context(|| format!("parse state: {}", path.display()))?;
            Ok(Self { path, data })
        } else {
            Ok(Self {
                path,
                data: StateData::default(),
            })
        }
    }

    /// persist state to disk.
    pub fn save(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create state dir: {}", parent.display()))?;
        }
        let raw = serde_json::to_string_pretty(&self.data)?;
        fs::write(&self.path, raw)
            .with_context(|| format!("write state: {}", self.path.display()))?;
        Ok(())
    }

    /// lookup a backend id by kind + uid.
    pub fn backend_id(&self, kind: Kind, uid: Uid) -> Option<u64> {
        self.data
            .mappings
            .get(&kind)
            .and_then(|map| map.get(&uid).copied())
    }

    /// set a backend id mapping.
    pub fn set_backend_id(&mut self, kind: Kind, uid: Uid, backend_id: u64) {
        self.data
            .mappings
            .entry(kind)
            .or_default()
            .insert(uid, backend_id);
    }

    /// remove a backend id mapping.
    pub fn remove_backend_id(&mut self, kind: Kind, uid: Uid) {
        if let Some(kind_map) = self.data.mappings.get_mut(&kind) {
            kind_map.remove(&uid);
        }
    }

    /// return all mappings for external use.
    pub fn all_mappings(&self) -> &BTreeMap<Kind, BTreeMap<Uid, u64>> {
        &self.data.mappings
    }
}
