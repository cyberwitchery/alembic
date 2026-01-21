use alembic_core::Uid;
use alembic_engine::StateStore;
use std::collections::BTreeMap;

use super::{TYPE_DCIM_DEVICE, TYPE_DCIM_INTERFACE, TYPE_DCIM_SITE};

pub(super) struct StateMappings {
    pub(super) site_id_to_uid: BTreeMap<u64, Uid>,
    pub(super) device_id_to_uid: BTreeMap<u64, Uid>,
    pub(super) interface_id_to_uid: BTreeMap<u64, Uid>,
}

pub(super) fn state_mappings(state: &StateStore) -> StateMappings {
    let mut site_id_to_uid = BTreeMap::new();
    let mut device_id_to_uid = BTreeMap::new();
    let mut interface_id_to_uid = BTreeMap::new();

    for (type_name, mapping) in state.all_mappings() {
        match type_name.as_str() {
            TYPE_DCIM_SITE => {
                for (uid, backend_id) in mapping {
                    site_id_to_uid.insert(*backend_id, *uid);
                }
            }
            TYPE_DCIM_DEVICE => {
                for (uid, backend_id) in mapping {
                    device_id_to_uid.insert(*backend_id, *uid);
                }
            }
            TYPE_DCIM_INTERFACE => {
                for (uid, backend_id) in mapping {
                    interface_id_to_uid.insert(*backend_id, *uid);
                }
            }
            _ => {}
        }
    }

    StateMappings {
        site_id_to_uid,
        device_id_to_uid,
        interface_id_to_uid,
    }
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
