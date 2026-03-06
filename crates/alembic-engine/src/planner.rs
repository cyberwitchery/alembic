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
    desired_sorted
        .sort_by(|a, b| op_sort_key(&a.type_name, &a.key).cmp(&op_sort_key(&b.type_name, &b.key)));

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
