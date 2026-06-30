use alembic_core::Uid;
use alembic_engine::{resolved_ids_from_state, state_mappings_by_id, BackendId, StateStore};
use std::collections::BTreeMap;

#[derive(Debug, Default, Clone)]
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
    StateMappings {
        by_type: state_mappings_by_id(state, |b| match b {
            BackendId::String(id) => Some(id.clone()),
            _ => None,
        }),
    }
}

pub(super) fn resolved_from_state(state: &StateStore) -> BTreeMap<Uid, String> {
    resolved_ids_from_state(state, |b| match b {
        BackendId::String(id) => Some(id.clone()),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alembic_core::TypeName;
    use tempfile::tempdir;

    #[test]
    fn test_state_mappings() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.json");
        let mut store = StateStore::load(path).unwrap();
        let uid = Uid::from_u128(1);
        let type_name = TypeName::new("dcim.site");
        store.set_backend_id(
            type_name.clone(),
            uid,
            BackendId::String("uuid-1".to_string()),
        );

        let mappings = state_mappings(&store);
        assert_eq!(mappings.uid_for("dcim.site", "uuid-1"), Some(uid));
        assert_eq!(mappings.uid_for("dcim.site", "none"), None);
    }

    #[test]
    fn test_resolved_from_state() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.json");
        let mut store = StateStore::load(path).unwrap();
        let uid = Uid::from_u128(1);
        store.set_backend_id(
            TypeName::new("t"),
            uid,
            BackendId::String("uuid-1".to_string()),
        );

        let resolved = resolved_from_state(&store);
        assert_eq!(resolved.get(&uid).unwrap(), "uuid-1");
    }
}
