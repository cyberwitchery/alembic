use alembic_core::Uid;
use alembic_engine::{BackendId, StateStore};
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
