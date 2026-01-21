use alembic_core::Uid;
use alembic_engine::StateStore;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub(super) struct StateMappings {
    pub(super) by_type: BTreeMap<String, BTreeMap<u64, Uid>>,
}

impl StateMappings {
    pub(super) fn uid_for(&self, type_name: &str, backend_id: u64) -> Option<Uid> {
        self.by_type
            .get(type_name)
            .and_then(|mapping| mapping.get(&backend_id).copied())
    }
}

pub(super) fn state_mappings(state: &StateStore) -> StateMappings {
    let mut by_type = BTreeMap::new();

    for (type_name, mapping) in state.all_mappings() {
        let mut id_to_uid = BTreeMap::new();
        for (uid, backend_id) in mapping {
            id_to_uid.insert(*backend_id, *uid);
        }
        by_type.insert(type_name.as_str().to_string(), id_to_uid);
    }

    StateMappings { by_type }
}

pub(super) fn resolved_from_state(state: &StateStore) -> BTreeMap<Uid, u64> {
    let mut resolved = BTreeMap::new();
    for mapping in state.all_mappings().values() {
        for (uid, backend_id) in mapping {
            resolved.insert(*uid, *backend_id);
        }
    }
    resolved
}
