use crate::types::BackendId;
use alembic_core::Uid;
use std::collections::BTreeMap;

/// per-type `backend-id -> uid` map for read-side ref normalization.
#[derive(Debug, Default, Clone)]
pub struct StateMappings {
    pub by_type: BTreeMap<String, BTreeMap<BackendId, Uid>>,
}

impl StateMappings {
    /// the canonical uid a backend id maps to for `type_name`, if known.
    pub fn uid_for(&self, type_name: &str, backend_id: &BackendId) -> Option<Uid> {
        self.by_type
            .get(type_name)
            .and_then(|mapping| mapping.get(backend_id).copied())
    }

    /// record a `backend-id -> uid` mapping for `type_name`.
    pub fn insert(&mut self, type_name: &str, backend_id: BackendId, uid: Uid) {
        self.by_type
            .entry(type_name.to_string())
            .or_default()
            .insert(backend_id, uid);
    }
}
