use alembic_core::Uid;
use alembic_engine::{BackendId, StateStore};
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub(super) struct StateMappings {
    pub(super) by_type: BTreeMap<String, BTreeMap<String, Uid>>,
}

impl StateMappings {
    pub(super) fn uid_for(&self, type_name: &str, backend_id: &str) -> Option<Uid> {
        self.by_type
            .get(type_name)
            .and_then(|mapping| mapping.get(backend_id).copied())
    }
}

pub(super) fn state_mappings(state: &StateStore) -> StateMappings {
    let mut by_type = BTreeMap::new();

    for (type_name, mapping) in state.all_mappings() {
        let mut id_to_uid = BTreeMap::new();
        for (uid, backend_id) in mapping {
            if let BackendId::String(id) = backend_id {
                id_to_uid.insert(id.clone(), *uid);
            }
        }
        by_type.insert(type_name.as_str().to_string(), id_to_uid);
    }

    StateMappings { by_type }
}

pub(super) fn resolved_from_state(state: &StateStore) -> BTreeMap<Uid, String> {
    let mut resolved = BTreeMap::new();
    for mapping in state.all_mappings().values() {
        for (uid, backend_id) in mapping {
            if let BackendId::String(id) = backend_id {
                resolved.insert(*uid, id.clone());
            }
        }
    }
    resolved
}
