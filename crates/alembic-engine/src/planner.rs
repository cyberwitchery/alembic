//! diff and plan generation.

use crate::state::StateStore;
use crate::types::{FieldChange, ObservedState, Op, Plan};
use alembic_core::{Attrs, Inventory, Kind, Uid};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// build a deterministic plan from desired and observed state.
pub fn plan(
    desired: &Inventory,
    observed: &ObservedState,
    state: &StateStore,
    allow_delete: bool,
) -> Plan {
    let mut ops = Vec::new();
    let mut matched = BTreeSet::new();
    let mut backend_to_uid = BTreeMap::new();

    for (kind, mapping) in state.all_mappings() {
        for (uid, backend_id) in mapping {
            backend_to_uid.insert((*backend_id, *kind), *uid);
        }
    }

    let mut desired_sorted = desired.objects.clone();
    desired_sorted.sort_by(|a, b| op_sort_key(a.kind, &a.key).cmp(&op_sort_key(b.kind, &b.key)));

    for object in desired_sorted.iter() {
        let observed_object = state
            .backend_id(object.kind, object.uid)
            .and_then(|id| observed.by_backend_id.get(&id))
            .or_else(|| observed.by_key.get(&object.key));

        if let Some(obs) = observed_object {
            let changes = diff_attrs(&obs.attrs, &object.attrs);
            if !changes.is_empty() {
                ops.push(Op::Update {
                    uid: object.uid,
                    kind: object.kind,
                    desired: object.clone(),
                    changes,
                    backend_id: obs.backend_id,
                });
            }
            if let Some(backend_id) = obs.backend_id {
                matched.insert(backend_id);
            }
        } else {
            ops.push(Op::Create {
                uid: object.uid,
                kind: object.kind,
                desired: object.clone(),
            });
        }
    }

    if allow_delete {
        for (backend_id, obs) in &observed.by_backend_id {
            if matched.contains(backend_id) {
                continue;
            }
            let uid = backend_to_uid
                .get(&(*backend_id, obs.kind))
                .copied()
                .unwrap_or_else(Uid::nil);
            ops.push(Op::Delete {
                uid,
                kind: obs.kind,
                key: obs.key.clone(),
                backend_id: Some(*backend_id),
            });
        }
    }

    ops.sort_by_key(op_order_key);

    Plan { ops }
}

/// compute field-level diffs for attrs.
fn diff_attrs(existing: &Attrs, desired: &Attrs) -> Vec<FieldChange> {
    if existing.kind() != desired.kind() {
        return vec![FieldChange {
            field: "kind".to_string(),
            from: Value::String(existing.kind().as_str().to_string()),
            to: Value::String(desired.kind().as_str().to_string()),
        }];
    }

    let existing_value = serde_json::to_value(existing).unwrap_or(Value::Null);
    let desired_value = serde_json::to_value(desired).unwrap_or(Value::Null);
    let mut changes = Vec::new();

    let (Value::Object(existing_map), Value::Object(desired_map)) =
        (&existing_value, &desired_value)
    else {
        if existing_value != desired_value {
            changes.push(FieldChange {
                field: "attrs".to_string(),
                from: existing_value,
                to: desired_value,
            });
        }
        return changes;
    };

    let keys: BTreeSet<String> = existing_map
        .keys()
        .chain(desired_map.keys())
        .cloned()
        .collect();

    for key in keys.iter() {
        let from = existing_map.get(key).cloned().unwrap_or(Value::Null);
        let to = desired_map.get(key).cloned().unwrap_or(Value::Null);
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

/// stable sort key for desired objects.
fn op_sort_key(kind: Kind, key: &str) -> (u8, String) {
    (kind_rank(kind), key.to_string())
}

/// stable sort key for plan operations.
fn op_order_key(op: &Op) -> (u8, u8, String) {
    let (kind, key, weight) = match op {
        Op::Create { kind, desired, .. } => (*kind, desired.key.clone(), 0),
        Op::Update { kind, desired, .. } => (*kind, desired.key.clone(), 1),
        Op::Delete { kind, key, .. } => (*kind, key.clone(), 2),
    };
    (kind_rank(kind), weight, key)
}

/// dependency rank for kinds.
fn kind_rank(kind: Kind) -> u8 {
    match kind {
        Kind::DcimSite => 0,
        Kind::DcimDevice => 1,
        Kind::DcimInterface => 2,
        Kind::IpamPrefix => 3,
        Kind::IpamIpAddress => 4,
    }
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
