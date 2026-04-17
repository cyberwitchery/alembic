//! diff and plan generation.

use crate::state::StateStore;
use crate::types::{FieldChange, ObservedState, Op, Plan};
use alembic_core::{key_string, uid_v5, JsonMap, Key, Object, TypeName};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// build a deterministic plan from desired and observed state.
pub fn plan(
    desired: &[Object],
    observed: &ObservedState,
    state: &StateStore,
    schema: &alembic_core::Schema,
    allow_delete: bool,
) -> Plan {
    let mut ops = Vec::new();
    let mut matched = BTreeSet::new();
    let mut backend_to_uid = BTreeMap::new();

    for (type_name, mapping) in state.all_mappings() {
        for (uid, backend_id) in mapping {
            backend_to_uid.insert((backend_id.clone(), type_name.clone()), *uid);
        }
    }

    let mut desired_sorted = desired.to_vec();
    desired_sorted.sort_by_key(|a| op_sort_key(&a.type_name, &a.key));

    for object in desired_sorted.iter() {
        let observed_object = state
            .backend_id(object.type_name.clone(), object.uid)
            .and_then(|id| observed.by_backend_id.get(&(object.type_name.clone(), id)))
            .or_else(|| {
                observed
                    .by_key
                    .get(&(object.type_name.clone(), key_string(&object.key)))
            });

        if let Some(obs) = observed_object {
            let changes = diff_object(obs, object);
            if !changes.is_empty() {
                ops.push(Op::Update {
                    uid: object.uid,
                    type_name: object.type_name.clone(),
                    desired: object.clone(),
                    changes,
                    backend_id: obs.backend_id.clone(),
                });
            }
            if let Some(backend_id) = &obs.backend_id {
                matched.insert(backend_id.clone());
            }
        } else {
            ops.push(Op::Create {
                uid: object.uid,
                type_name: object.type_name.clone(),
                desired: object.clone(),
            });
        }
    }

    if allow_delete {
        for ((type_name, backend_id), obs) in &observed.by_backend_id {
            if matched.contains(backend_id) {
                continue;
            }
            let uid = backend_to_uid
                .get(&(backend_id.clone(), type_name.clone()))
                .copied()
                .unwrap_or_else(|| uid_v5(type_name.as_str(), &key_string(&obs.key)));
            ops.push(Op::Delete {
                uid,
                type_name: type_name.clone(),
                key: obs.key.clone(),
                backend_id: Some(backend_id.clone()),
            });
        }
    }

    ops.sort_by_key(op_order_key);

    let mut plan = Plan {
        schema: schema.clone(),
        ops,
        summary: None,
    };
    plan.summary = Some(plan.summary());
    plan
}

/// compute field-level diffs for attrs.
fn diff_attrs(existing: &JsonMap, desired: &JsonMap) -> Vec<FieldChange> {
    let mut changes = Vec::new();
    let keys: BTreeSet<String> = existing.keys().chain(desired.keys()).cloned().collect();

    for key in keys.iter() {
        let from = existing.get(key).cloned().unwrap_or(Value::Null);
        let desired_has = desired.contains_key(key);
        if !desired_has {
            continue;
        }
        let to = desired.get(key).cloned().unwrap_or(Value::Null);
        if from != to {
            changes.push(FieldChange {
                field: key.clone(),
                from,
                to,
            });
        }
    }

    changes
}

fn diff_object(existing: &crate::types::ObservedObject, desired: &Object) -> Vec<FieldChange> {
    diff_attrs(&existing.attrs, &desired.attrs)
}

/// stable sort key for desired objects.
fn op_sort_key(type_name: &TypeName, key: &Key) -> (String, String) {
    (type_name.as_str().to_string(), key_string(key))
}

/// stable sort key for plan operations.
fn op_order_key(op: &Op) -> (String, u8, String) {
    let (type_name, key, weight) = match op {
        Op::Create {
            type_name, desired, ..
        } => (type_name.clone(), key_string(&desired.key), 0u8),
        Op::Update {
            type_name, desired, ..
        } => (type_name.clone(), key_string(&desired.key), 1u8),
        Op::Delete { type_name, key, .. } => (type_name.clone(), key_string(key), 2u8),
    };
    (type_name.as_str().to_string(), weight, key)
}

/// order operations for apply (creates/updates first, deletes last).
pub fn sort_ops_for_apply(ops: &[Op]) -> Vec<Op> {
    let mut creates_updates = Vec::new();
    let mut deletes = Vec::new();

    for op in ops {
        match op {
            Op::Delete { .. } => deletes.push(op.clone()),
            _ => creates_updates.push(op.clone()),
        }
    }

    creates_updates.sort_by_key(op_order_key);
    deletes.sort_by_key(op_order_key);
    deletes.reverse();

    creates_updates.into_iter().chain(deletes).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{StateData, StateStore};
    use crate::types::{BackendId, ObservedObject, ObservedState};
    use alembic_core::{JsonMap, Key, Object, Schema, TypeName, Uid};
    use serde_json::json;
    use std::collections::BTreeMap;

    fn make_key(slug: &str) -> Key {
        let mut k = BTreeMap::new();
        k.insert("slug".to_string(), json!(slug));
        Key::from(k)
    }

    fn make_attrs(pairs: &[(&str, serde_json::Value)]) -> JsonMap {
        let mut m = BTreeMap::new();
        for (k, v) in pairs {
            m.insert(k.to_string(), v.clone());
        }
        JsonMap::from(m)
    }

    fn make_object(uid: u128, type_name: &str, slug: &str, attrs: JsonMap) -> Object {
        Object::new(
            Uid::from_u128(uid),
            TypeName::new(type_name),
            make_key(slug),
            attrs,
        )
        .unwrap()
    }

    fn empty_schema() -> Schema {
        Schema {
            types: BTreeMap::new(),
        }
    }

    fn empty_state() -> StateStore {
        StateStore::new(None, StateData::default())
    }

    // --- diff_attrs ---

    #[test]
    fn diff_attrs_identical_maps() {
        let attrs = make_attrs(&[("name", json!("FRA1"))]);
        let changes = diff_attrs(&attrs, &attrs);
        assert!(changes.is_empty());
    }

    #[test]
    fn diff_attrs_field_changed() {
        let existing = make_attrs(&[("name", json!("FRA1"))]);
        let desired = make_attrs(&[("name", json!("FRA2"))]);
        let changes = diff_attrs(&existing, &desired);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].field, "name");
        assert_eq!(changes[0].from, json!("FRA1"));
        assert_eq!(changes[0].to, json!("FRA2"));
    }

    #[test]
    fn diff_attrs_field_added() {
        let existing = make_attrs(&[]);
        let desired = make_attrs(&[("name", json!("FRA1"))]);
        let changes = diff_attrs(&existing, &desired);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].field, "name");
        assert_eq!(changes[0].from, json!(null));
        assert_eq!(changes[0].to, json!("FRA1"));
    }

    #[test]
    fn diff_attrs_field_removed_in_desired_is_ignored() {
        let existing = make_attrs(&[("name", json!("FRA1")), ("extra", json!(true))]);
        let desired = make_attrs(&[("name", json!("FRA1"))]);
        let changes = diff_attrs(&existing, &desired);
        assert!(changes.is_empty());
    }

    #[test]
    fn diff_attrs_multiple_changes() {
        let existing = make_attrs(&[("a", json!(1)), ("b", json!(2))]);
        let desired = make_attrs(&[("a", json!(10)), ("b", json!(20))]);
        let changes = diff_attrs(&existing, &desired);
        assert_eq!(changes.len(), 2);
        let fields: Vec<&str> = changes.iter().map(|c| c.field.as_str()).collect();
        assert!(fields.contains(&"a"));
        assert!(fields.contains(&"b"));
    }

    #[test]
    fn diff_attrs_type_change() {
        let existing = make_attrs(&[("val", json!("string"))]);
        let desired = make_attrs(&[("val", json!(42))]);
        let changes = diff_attrs(&existing, &desired);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].from, json!("string"));
        assert_eq!(changes[0].to, json!(42));
    }

    // --- plan() ---

    #[test]
    fn plan_creates_for_new_objects() {
        let desired = vec![make_object(
            1,
            "dcim.site",
            "fra1",
            make_attrs(&[("name", json!("FRA1"))]),
        )];
        let observed = ObservedState::default();
        let result = plan(&desired, &observed, &empty_state(), &empty_schema(), false);
        assert_eq!(result.ops.len(), 1);
        assert!(matches!(&result.ops[0], Op::Create { uid, type_name, .. }
            if *uid == Uid::from_u128(1) && type_name.as_str() == "dcim.site"));
        let summary = result.summary.unwrap();
        assert_eq!(summary.create, 1);
        assert_eq!(summary.update, 0);
        assert_eq!(summary.delete, 0);
    }

    #[test]
    fn plan_updates_when_attrs_differ() {
        let desired = vec![make_object(
            1,
            "dcim.site",
            "fra1",
            make_attrs(&[("name", json!("FRA2"))]),
        )];
        let mut observed = ObservedState::default();
        observed.insert(ObservedObject {
            type_name: TypeName::new("dcim.site"),
            key: make_key("fra1"),
            attrs: make_attrs(&[("name", json!("FRA1"))]),
            backend_id: Some(BackendId::Int(100)),
        });
        let result = plan(&desired, &observed, &empty_state(), &empty_schema(), false);
        assert_eq!(result.ops.len(), 1);
        match &result.ops[0] {
            Op::Update {
                changes,
                backend_id,
                ..
            } => {
                assert_eq!(changes.len(), 1);
                assert_eq!(changes[0].field, "name");
                assert_eq!(backend_id, &Some(BackendId::Int(100)));
            }
            other => panic!("expected Update, got {:?}", other),
        }
    }

    #[test]
    fn plan_no_op_when_identical() {
        let desired = vec![make_object(
            1,
            "dcim.site",
            "fra1",
            make_attrs(&[("name", json!("FRA1"))]),
        )];
        let mut observed = ObservedState::default();
        observed.insert(ObservedObject {
            type_name: TypeName::new("dcim.site"),
            key: make_key("fra1"),
            attrs: make_attrs(&[("name", json!("FRA1"))]),
            backend_id: Some(BackendId::Int(100)),
        });
        let result = plan(&desired, &observed, &empty_state(), &empty_schema(), false);
        assert!(result.ops.is_empty());
    }

    #[test]
    fn plan_deletes_unmatched_when_allowed() {
        let desired = vec![];
        let mut observed = ObservedState::default();
        observed.insert(ObservedObject {
            type_name: TypeName::new("dcim.site"),
            key: make_key("fra1"),
            attrs: make_attrs(&[("name", json!("FRA1"))]),
            backend_id: Some(BackendId::Int(100)),
        });
        let result = plan(&desired, &observed, &empty_state(), &empty_schema(), true);
        assert_eq!(result.ops.len(), 1);
        assert!(matches!(
            &result.ops[0],
            Op::Delete {
                backend_id: Some(BackendId::Int(100)),
                ..
            }
        ));
    }

    #[test]
    fn plan_no_deletes_when_disallowed() {
        let desired = vec![];
        let mut observed = ObservedState::default();
        observed.insert(ObservedObject {
            type_name: TypeName::new("dcim.site"),
            key: make_key("fra1"),
            attrs: make_attrs(&[("name", json!("FRA1"))]),
            backend_id: Some(BackendId::Int(100)),
        });
        let result = plan(&desired, &observed, &empty_state(), &empty_schema(), false);
        assert!(result.ops.is_empty());
    }

    #[test]
    fn plan_matched_objects_not_deleted() {
        let desired = vec![make_object(
            1,
            "dcim.site",
            "fra1",
            make_attrs(&[("name", json!("FRA1"))]),
        )];
        let mut observed = ObservedState::default();
        observed.insert(ObservedObject {
            type_name: TypeName::new("dcim.site"),
            key: make_key("fra1"),
            attrs: make_attrs(&[("name", json!("FRA1"))]),
            backend_id: Some(BackendId::Int(100)),
        });
        let result = plan(&desired, &observed, &empty_state(), &empty_schema(), true);
        assert!(result.ops.is_empty());
    }

    #[test]
    fn plan_mixed_create_update_delete() {
        let desired = vec![
            make_object(
                1,
                "dcim.site",
                "fra1",
                make_attrs(&[("name", json!("FRA1-new"))]),
            ),
            make_object(
                2,
                "dcim.site",
                "ams1",
                make_attrs(&[("name", json!("AMS1"))]),
            ),
        ];
        let mut observed = ObservedState::default();
        observed.insert(ObservedObject {
            type_name: TypeName::new("dcim.site"),
            key: make_key("fra1"),
            attrs: make_attrs(&[("name", json!("FRA1"))]),
            backend_id: Some(BackendId::Int(100)),
        });
        observed.insert(ObservedObject {
            type_name: TypeName::new("dcim.site"),
            key: make_key("lhr1"),
            attrs: make_attrs(&[("name", json!("LHR1"))]),
            backend_id: Some(BackendId::Int(200)),
        });
        let result = plan(&desired, &observed, &empty_state(), &empty_schema(), true);

        let creates: Vec<_> = result
            .ops
            .iter()
            .filter(|op| matches!(op, Op::Create { .. }))
            .collect();
        let updates: Vec<_> = result
            .ops
            .iter()
            .filter(|op| matches!(op, Op::Update { .. }))
            .collect();
        let deletes: Vec<_> = result
            .ops
            .iter()
            .filter(|op| matches!(op, Op::Delete { .. }))
            .collect();
        assert_eq!(creates.len(), 1);
        assert_eq!(updates.len(), 1);
        assert_eq!(deletes.len(), 1);

        let summary = result.summary.unwrap();
        assert_eq!(summary.create, 1);
        assert_eq!(summary.update, 1);
        assert_eq!(summary.delete, 1);
    }

    #[test]
    fn plan_uses_state_mapping_for_lookup() {
        let mut state_data = StateData::default();
        state_data
            .mappings
            .entry(TypeName::new("dcim.site"))
            .or_default()
            .insert(Uid::from_u128(1), BackendId::Int(100));
        let state = StateStore::new(None, state_data);

        let desired = vec![make_object(
            1,
            "dcim.site",
            "fra1",
            make_attrs(&[("name", json!("FRA2"))]),
        )];
        let mut observed = ObservedState::default();
        observed.insert(ObservedObject {
            type_name: TypeName::new("dcim.site"),
            key: make_key("fra1"),
            attrs: make_attrs(&[("name", json!("FRA1"))]),
            backend_id: Some(BackendId::Int(100)),
        });
        let result = plan(&desired, &observed, &state, &empty_schema(), false);
        assert_eq!(result.ops.len(), 1);
        assert!(matches!(&result.ops[0], Op::Update { .. }));
    }

    // --- sort_ops_for_apply ---

    #[test]
    fn sort_ops_creates_before_deletes() {
        let ops = vec![
            Op::Delete {
                uid: Uid::from_u128(1),
                type_name: TypeName::new("dcim.site"),
                key: make_key("fra1"),
                backend_id: Some(BackendId::Int(100)),
            },
            Op::Create {
                uid: Uid::from_u128(2),
                type_name: TypeName::new("dcim.site"),
                desired: make_object(2, "dcim.site", "ams1", make_attrs(&[])),
            },
        ];
        let sorted = sort_ops_for_apply(&ops);
        assert!(matches!(&sorted[0], Op::Create { .. }));
        assert!(matches!(&sorted[1], Op::Delete { .. }));
    }

    #[test]
    fn sort_ops_updates_before_deletes() {
        let ops = vec![
            Op::Delete {
                uid: Uid::from_u128(1),
                type_name: TypeName::new("dcim.site"),
                key: make_key("fra1"),
                backend_id: None,
            },
            Op::Update {
                uid: Uid::from_u128(2),
                type_name: TypeName::new("dcim.site"),
                desired: make_object(2, "dcim.site", "ams1", make_attrs(&[])),
                changes: vec![],
                backend_id: None,
            },
        ];
        let sorted = sort_ops_for_apply(&ops);
        assert!(matches!(&sorted[0], Op::Update { .. }));
        assert!(matches!(&sorted[1], Op::Delete { .. }));
    }

    #[test]
    fn sort_ops_deletes_reversed() {
        let ops = vec![
            Op::Delete {
                uid: Uid::from_u128(1),
                type_name: TypeName::new("a.type"),
                key: make_key("a"),
                backend_id: None,
            },
            Op::Delete {
                uid: Uid::from_u128(2),
                type_name: TypeName::new("z.type"),
                key: make_key("z"),
                backend_id: None,
            },
        ];
        let sorted = sort_ops_for_apply(&ops);
        assert!(
            matches!(&sorted[0], Op::Delete { type_name, .. } if type_name.as_str() == "z.type")
        );
        assert!(
            matches!(&sorted[1], Op::Delete { type_name, .. } if type_name.as_str() == "a.type")
        );
    }

    #[test]
    fn sort_ops_empty_input() {
        let sorted = sort_ops_for_apply(&[]);
        assert!(sorted.is_empty());
    }

    #[test]
    fn sort_ops_preserves_create_update_order() {
        let ops = vec![
            Op::Update {
                uid: Uid::from_u128(2),
                type_name: TypeName::new("dcim.site"),
                desired: make_object(2, "dcim.site", "ams1", make_attrs(&[])),
                changes: vec![],
                backend_id: None,
            },
            Op::Create {
                uid: Uid::from_u128(1),
                type_name: TypeName::new("dcim.site"),
                desired: make_object(1, "dcim.site", "aaa1", make_attrs(&[])),
            },
        ];
        let sorted = sort_ops_for_apply(&ops);
        assert!(matches!(&sorted[0], Op::Create { .. }));
        assert!(matches!(&sorted[1], Op::Update { .. }));
    }
}
