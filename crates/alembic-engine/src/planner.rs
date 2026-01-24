//! diff and plan generation.

use crate::projection::ProjectedInventory;
use crate::state::StateStore;
use crate::types::{FieldChange, ObservedState, Op, Plan};
use alembic_core::{key_string, uid_v5, JsonMap, Key, TypeName};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// build a deterministic plan from desired and observed state.
pub fn plan(
    desired: &ProjectedInventory,
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

    let mut desired_sorted = desired.objects.clone();
    desired_sorted.sort_by(|a, b| {
        op_sort_key(&a.base.type_name, &a.base.key)
            .cmp(&op_sort_key(&b.base.type_name, &b.base.key))
    });

    for object in desired_sorted.iter() {
        let observed_object = state
            .backend_id(object.base.type_name.clone(), object.base.uid)
            .and_then(|id| {
                observed
                    .by_backend_id
                    .get(&(object.base.type_name.clone(), id))
            })
            .or_else(|| {
                observed
                    .by_key
                    .get(&(object.base.type_name.clone(), key_string(&object.base.key)))
            });

        if let Some(obs) = observed_object {
            let changes = diff_object(obs, object);
            if !changes.is_empty() {
                ops.push(Op::Update {
                    uid: object.base.uid,
                    type_name: object.base.type_name.clone(),
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
                uid: object.base.uid,
                type_name: object.base.type_name.clone(),
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

    Plan {
        schema: schema.clone(),
        ops,
    }
}

/// compute field-level diffs for attrs.
fn diff_attrs(
    existing: &JsonMap,
    desired: &JsonMap,
    ignore: &BTreeSet<String>,
) -> Vec<FieldChange> {
    let mut changes = Vec::new();
    let keys: BTreeSet<String> = existing.keys().chain(desired.keys()).cloned().collect();

    for key in keys.iter() {
        if ignore.contains(key) {
            continue;
        }
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

fn diff_object(
    existing: &crate::types::ObservedObject,
    desired: &crate::projection::ProjectedObject,
) -> Vec<FieldChange> {
    let mut changes = diff_attrs(
        &existing.attrs,
        &desired.base.attrs,
        &desired.projection_inputs,
    );

    if let Some(desired_fields) = &desired.projection.custom_fields {
        let existing_subset = existing
            .projection
            .custom_fields
            .as_ref()
            .map(|fields| {
                fields
                    .iter()
                    .filter_map(|(key, value)| {
                        if desired_fields.contains_key(key) {
                            Some((key.clone(), value.clone()))
                        } else {
                            None
                        }
                    })
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default();
        if existing_subset != *desired_fields {
            changes.push(FieldChange {
                field: "custom_fields".to_string(),
                from: serde_json::to_value(existing_subset).unwrap_or(Value::Null),
                to: serde_json::to_value(desired_fields).unwrap_or(Value::Null),
            });
        }
    }
    if desired.projection.tags.is_some() && existing.projection.tags != desired.projection.tags {
        changes.push(FieldChange {
            field: "tags".to_string(),
            from: serde_json::to_value(&existing.projection.tags).unwrap_or(Value::Null),
            to: serde_json::to_value(&desired.projection.tags).unwrap_or(Value::Null),
        });
    }
    if desired.projection.local_context.is_some()
        && existing.projection.local_context != desired.projection.local_context
    {
        changes.push(FieldChange {
            field: "local_context".to_string(),
            from: serde_json::to_value(&existing.projection.local_context).unwrap_or(Value::Null),
            to: serde_json::to_value(&desired.projection.local_context).unwrap_or(Value::Null),
        });
    }

    changes
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
        } => (type_name.clone(), key_string(&desired.base.key), 0u8),
        Op::Update {
            type_name, desired, ..
        } => (type_name.clone(), key_string(&desired.base.key), 1u8),
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

    creates_updates.into_iter().chain(deletes).collect()
}
