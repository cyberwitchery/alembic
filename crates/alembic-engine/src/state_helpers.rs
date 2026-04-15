use crate::{BackendId, StateStore};
use alembic_core::Uid;
use std::collections::BTreeMap;

#[derive(Debug, Default, Clone)]
pub struct StateMappings {
    pub by_type: BTreeMap<String, BTreeMap<BackendId, Uid>>,
}

impl StateMappings {
    pub fn uid_for(&self, type_name: &str, backend_id: &BackendId) -> Option<Uid> {
        self.by_type
            .get(type_name)
            .and_then(|mapping| mapping.get(backend_id).copied())
    }
}

pub fn state_mappings(state: &StateStore) -> StateMappings {
    let mut by_type = BTreeMap::new();
    for (type_name, mapping) in state.all_mappings() {
        let mut id_to_uid = BTreeMap::new();
        for (uid, backend_id) in mapping {
            id_to_uid.insert(backend_id.clone(), *uid);
        }
        by_type.insert(type_name.as_str().to_string(), id_to_uid);
    }
    StateMappings { by_type }
}

pub fn resolved_from_state(state: &StateStore) -> BTreeMap<Uid, BackendId> {
    let mut resolved = BTreeMap::new();
    for mapping in state.all_mappings().values() {
        for (uid, backend_id) in mapping {
            resolved.insert(*uid, backend_id.clone());
        }
    }
    resolved
}

#[cfg(test)]
mod tests {
    use super::*;
    use alembic_core::TypeName;

    fn new_state_store() -> StateStore {
        StateStore::new(None, crate::StateData::default())
    }

    #[test]
    fn state_mappings_empty() {
        let store = new_state_store();
        let mappings = state_mappings(&store);
        assert!(mappings.by_type.is_empty());
    }

    #[test]
    fn state_mappings_lookup() {
        let mut store = new_state_store();
        let uid = Uid::from_u128(1);
        store.set_backend_id(
            TypeName::new("dcim.site"),
            uid,
            BackendId::String("site-1".to_string()),
        );

        let mappings = state_mappings(&store);
        assert_eq!(
            mappings.uid_for("dcim.site", &BackendId::String("site-1".to_string())),
            Some(uid)
        );
        assert_eq!(mappings.uid_for("dcim.site", &BackendId::Int(999)), None);
    }

    #[test]
    fn resolved_from_state_empty() {
        let store = new_state_store();
        let resolved = resolved_from_state(&store);
        assert!(resolved.is_empty());
    }

    #[test]
    fn resolved_from_state_with_mappings() {
        let mut store = new_state_store();
        let uid = Uid::from_u128(1);
        store.set_backend_id(TypeName::new("t"), uid, BackendId::Int(5));

        let resolved = resolved_from_state(&store);
        assert_eq!(resolved.get(&uid), Some(&BackendId::Int(5)));
    }
}
