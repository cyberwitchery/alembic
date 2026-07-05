//! diff and plan generation.

use crate::state::StateStore;
use crate::types::{FieldChange, ObservedState, Op, Plan};
use alembic_core::{
    key_string, uid_v5, FieldType, JsonMap, Key, Object, Schema, TypeName, TypeSchema, Uid,
};
use serde_json::Value;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

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
            backend_to_uid.insert((type_name.clone(), backend_id.clone()), *uid);
        }
    }

    let mut desired_sorted: Vec<&Object> = desired.iter().collect();
    desired_sorted.sort_by_key(|a| op_sort_key(&a.type_name, &a.key));

    for object in desired_sorted {
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
                matched.insert((object.type_name.clone(), backend_id.clone()));
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
            if matched.contains(&(type_name.clone(), backend_id.clone())) {
                continue;
            }
            let uid = backend_to_uid
                .get(&(type_name.clone(), backend_id.clone()))
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
        schema_preview: None,
    };
    plan.summary = Some(plan.summary());
    plan
}

/// compute field-level diffs for attrs.
fn diff_attrs(existing: &JsonMap, desired: &JsonMap) -> Vec<FieldChange> {
    let mut changes = Vec::new();
    for (field, to) in desired.iter() {
        let from = existing.get(field).cloned().unwrap_or(Value::Null);
        if from != *to {
            changes.push(FieldChange {
                field: field.clone(),
                from,
                to: to.clone(),
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

/// deterministic ordering key for a plan op: (type name, op weight, key string).
type OrderKey = (String, u8, String);

/// stable sort key for plan operations.
fn op_order_key(op: &Op) -> OrderKey {
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

/// collect the referenced uids carried by a value, recursing through list and
/// map containers, using the field's schema to know where refs live.
fn collect_refs_in_value(field_type: &FieldType, value: &Value, out: &mut BTreeSet<Uid>) {
    match field_type {
        FieldType::Ref { .. } => {
            if let Some(uid) = value.as_str().and_then(|raw| Uid::parse_str(raw).ok()) {
                out.insert(uid);
            }
        }
        FieldType::ListRef { .. } => {
            if let Value::Array(items) = value {
                for item in items {
                    if let Some(uid) = item.as_str().and_then(|raw| Uid::parse_str(raw).ok()) {
                        out.insert(uid);
                    }
                }
            }
        }
        FieldType::List { item } => {
            if let Value::Array(items) = value {
                for item_value in items {
                    collect_refs_in_value(item, item_value, out);
                }
            }
        }
        FieldType::Map { value: inner } => {
            if let Value::Object(map) = value {
                for entry in map.values() {
                    collect_refs_in_value(inner, entry, out);
                }
            }
        }
        _ => {}
    }
}

/// collect every uid referenced by an object's attrs (or key) given its schema.
/// fields are looked up in `fields` first, falling back to `key` so that
/// reference-typed key components are picked up too.
fn collect_referenced_uids(
    type_schema: &TypeSchema,
    attrs: &BTreeMap<String, Value>,
) -> BTreeSet<Uid> {
    let mut uids = BTreeSet::new();
    for (field, value) in attrs {
        if let Some(field_schema) = type_schema
            .fields
            .get(field)
            .or_else(|| type_schema.key.get(field))
        {
            collect_refs_in_value(&field_schema.r#type, value, &mut uids);
        }
    }
    uids
}

/// the uids an op depends on. creates/updates draw refs from the desired attrs
/// and key; deletes only carry a key, so refs come from reference-typed key
/// components (the op has no attrs to inspect).
fn op_referenced_uids(op: &Op, schema: &Schema) -> BTreeSet<Uid> {
    let Some(type_schema) = schema.types.get(op.type_name().as_str()) else {
        return BTreeSet::new();
    };
    match op {
        Op::Create { desired, .. } | Op::Update { desired, .. } => {
            let mut uids = collect_referenced_uids(type_schema, &desired.attrs);
            uids.extend(collect_referenced_uids(type_schema, &desired.key));
            uids
        }
        Op::Delete { key, .. } => collect_referenced_uids(type_schema, key),
    }
}

/// Kahn's algorithm with `op_order_key` as a deterministic tie-breaker.
/// `edges[a]` holds the nodes that must come *after* node `a`. Nodes that
/// remain in a reference cycle never reach in-degree zero; they are appended in
/// stable `op_order_key` order so the result is always a total order (the
/// apply_retry fixpoint resolves any residual ordering for them at apply time).
fn stable_toposort(ops: &[&Op], edges: &[BTreeSet<usize>]) -> Vec<usize> {
    let n = ops.len();
    let keys: Vec<OrderKey> = ops.iter().map(|&op| op_order_key(op)).collect();

    let mut indegree = vec![0usize; n];
    for succs in edges {
        for &b in succs {
            indegree[b] += 1;
        }
    }

    let mut ready: BinaryHeap<Reverse<(OrderKey, usize)>> = BinaryHeap::new();
    for (i, &deg) in indegree.iter().enumerate() {
        if deg == 0 {
            ready.push(Reverse((keys[i].clone(), i)));
        }
    }

    let mut order = Vec::with_capacity(n);
    while let Some(Reverse((_, i))) = ready.pop() {
        order.push(i);
        for &b in &edges[i] {
            indegree[b] -= 1;
            if indegree[b] == 0 {
                ready.push(Reverse((keys[b].clone(), b)));
            }
        }
    }

    // cyclic remainder: nodes that never reached in-degree zero. fall back to
    // stable ordering for them so we still emit a total order without panicking.
    if order.len() < n {
        let mut placed = vec![false; n];
        for &i in &order {
            placed[i] = true;
        }
        let mut remaining: Vec<usize> = (0..n).filter(|&i| !placed[i]).collect();
        remaining.sort_by(|&a, &b| keys[a].cmp(&keys[b]).then(a.cmp(&b)));
        order.extend(remaining);
    }

    order
}

/// order ops by reference dependency. when `reverse` is false (creates/updates)
/// an op is placed after the ops that create the uids it references; when true
/// (deletes) an op is placed before the ops it references, so an object is
/// removed only after everything referencing it.
fn ordered_by_refs(ops: &[&Op], schema: &Schema, reverse: bool) -> Vec<Op> {
    let uid_to_node: BTreeMap<Uid, usize> = ops
        .iter()
        .enumerate()
        .map(|(i, op)| (op.uid(), i))
        .collect();

    let mut edges: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); ops.len()];
    for (i, &op) in ops.iter().enumerate() {
        for referenced in op_referenced_uids(op, schema) {
            let Some(&j) = uid_to_node.get(&referenced) else {
                continue;
            };
            if i == j {
                continue;
            }
            if reverse {
                edges[i].insert(j); // i references j -> delete i before j
            } else {
                edges[j].insert(i); // i references j -> create j before i
            }
        }
    }

    stable_toposort(ops, &edges)
        .into_iter()
        .map(|i| ops[i].clone())
        .collect()
}

/// order operations for apply: creates/updates first (topologically sorted so
/// referenced objects are created before the objects that reference them),
/// then deletes (reverse-toposorted so an object is deleted only after
/// everything referencing it). reference cycles fall back to a stable order.
pub fn sort_ops_for_apply(ops: &[Op], schema: &Schema) -> Vec<Op> {
    let (non_deletes, deletes): (Vec<&Op>, Vec<&Op>) =
        ops.iter().partition(|op| !matches!(op, Op::Delete { .. }));

    let mut result = ordered_by_refs(&non_deletes, schema, false);
    result.extend(ordered_by_refs(&deletes, schema, true));
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{StateData, StateStore};
    use crate::types::{BackendId, ObservedObject, ObservedState};
    use alembic_core::{
        FieldSchema, FieldType, JsonMap, Key, Object, Schema, TypeName, TypeSchema, Uid,
    };
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
        observed
            .insert(ObservedObject {
                type_name: TypeName::new("dcim.site"),
                key: make_key("fra1"),
                attrs: make_attrs(&[("name", json!("FRA1"))]),
                backend_id: Some(BackendId::Int(100)),
            })
            .unwrap();
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
        observed
            .insert(ObservedObject {
                type_name: TypeName::new("dcim.site"),
                key: make_key("fra1"),
                attrs: make_attrs(&[("name", json!("FRA1"))]),
                backend_id: Some(BackendId::Int(100)),
            })
            .unwrap();
        let result = plan(&desired, &observed, &empty_state(), &empty_schema(), false);
        assert!(result.ops.is_empty());
    }

    #[test]
    fn plan_deletes_unmatched_when_allowed() {
        let desired = vec![];
        let mut observed = ObservedState::default();
        observed
            .insert(ObservedObject {
                type_name: TypeName::new("dcim.site"),
                key: make_key("fra1"),
                attrs: make_attrs(&[("name", json!("FRA1"))]),
                backend_id: Some(BackendId::Int(100)),
            })
            .unwrap();
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
        observed
            .insert(ObservedObject {
                type_name: TypeName::new("dcim.site"),
                key: make_key("fra1"),
                attrs: make_attrs(&[("name", json!("FRA1"))]),
                backend_id: Some(BackendId::Int(100)),
            })
            .unwrap();
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
        observed
            .insert(ObservedObject {
                type_name: TypeName::new("dcim.site"),
                key: make_key("fra1"),
                attrs: make_attrs(&[("name", json!("FRA1"))]),
                backend_id: Some(BackendId::Int(100)),
            })
            .unwrap();
        let result = plan(&desired, &observed, &empty_state(), &empty_schema(), true);
        assert!(result.ops.is_empty());
    }

    #[test]
    fn plan_deletes_cross_type_id_collision() {
        // both types carry backend id 100; declaring only the site must not
        // suppress the undeclared device's delete.
        let desired = vec![make_object(
            1,
            "dcim.site",
            "fra1",
            make_attrs(&[("name", json!("FRA1"))]),
        )];
        let mut observed = ObservedState::default();
        observed
            .insert(ObservedObject {
                type_name: TypeName::new("dcim.site"),
                key: make_key("fra1"),
                attrs: make_attrs(&[("name", json!("FRA1"))]),
                backend_id: Some(BackendId::Int(100)),
            })
            .unwrap();
        observed
            .insert(ObservedObject {
                type_name: TypeName::new("dcim.device"),
                key: make_key("leaf01"),
                attrs: make_attrs(&[]),
                backend_id: Some(BackendId::Int(100)),
            })
            .unwrap();
        let result = plan(&desired, &observed, &empty_state(), &empty_schema(), true);
        let deletes: Vec<&Op> = result
            .ops
            .iter()
            .filter(|op| matches!(op, Op::Delete { .. }))
            .collect();
        assert_eq!(deletes.len(), 1);
        assert!(matches!(
            deletes[0],
            Op::Delete { type_name, .. } if type_name.as_str() == "dcim.device"
        ));
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
        observed
            .insert(ObservedObject {
                type_name: TypeName::new("dcim.site"),
                key: make_key("fra1"),
                attrs: make_attrs(&[("name", json!("FRA1"))]),
                backend_id: Some(BackendId::Int(100)),
            })
            .unwrap();
        observed
            .insert(ObservedObject {
                type_name: TypeName::new("dcim.site"),
                key: make_key("lhr1"),
                attrs: make_attrs(&[("name", json!("LHR1"))]),
                backend_id: Some(BackendId::Int(200)),
            })
            .unwrap();
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
        observed
            .insert(ObservedObject {
                type_name: TypeName::new("dcim.site"),
                key: make_key("fra1"),
                attrs: make_attrs(&[("name", json!("FRA1"))]),
                backend_id: Some(BackendId::Int(100)),
            })
            .unwrap();
        let result = plan(&desired, &observed, &state, &empty_schema(), false);
        assert_eq!(result.ops.len(), 1);
        assert!(matches!(&result.ops[0], Op::Update { .. }));
    }

    #[test]
    fn plan_matches_renamed_object_by_backend_id() {
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
            "fra1-renamed",
            make_attrs(&[("name", json!("FRA2"))]),
        )];
        let mut observed = ObservedState::default();
        observed
            .insert(ObservedObject {
                type_name: TypeName::new("dcim.site"),
                key: make_key("fra1-old"),
                attrs: make_attrs(&[("name", json!("FRA1"))]),
                backend_id: Some(BackendId::Int(100)),
            })
            .unwrap();
        let result = plan(&desired, &observed, &state, &empty_schema(), true);
        assert_eq!(result.ops.len(), 1);
        assert!(matches!(
            &result.ops[0],
            Op::Update {
                backend_id: Some(BackendId::Int(100)),
                ..
            }
        ));
    }

    #[test]
    fn plan_delete_uses_tracked_uid() {
        let mut state_data = StateData::default();
        state_data
            .mappings
            .entry(TypeName::new("dcim.site"))
            .or_default()
            .insert(Uid::from_u128(42), BackendId::Int(100));
        let state = StateStore::new(None, state_data);

        let mut observed = ObservedState::default();
        observed
            .insert(ObservedObject {
                type_name: TypeName::new("dcim.site"),
                key: make_key("fra1"),
                attrs: make_attrs(&[]),
                backend_id: Some(BackendId::Int(100)),
            })
            .unwrap();

        let result = plan(&[], &observed, &state, &empty_schema(), true);
        assert_eq!(result.ops.len(), 1);
        match &result.ops[0] {
            Op::Delete { uid, .. } => {
                assert_eq!(*uid, Uid::from_u128(42));
                assert_ne!(*uid, uid_v5("dcim.site", &key_string(&make_key("fra1"))));
            }
            other => panic!("expected delete, got {other:?}"),
        }
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
        let sorted = sort_ops_for_apply(&ops, &empty_schema());
        assert!(matches!(&sorted[0], Op::Create { .. }));
        assert!(matches!(&sorted[1], Op::Delete { .. }));
    }

    #[test]
    fn sort_ops_deletes_last() {
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
        let sorted = sort_ops_for_apply(&ops, &empty_schema());
        // both deletes come last, sorted alphabetically by type
        assert!(
            matches!(&sorted[0], Op::Delete { type_name, .. } if type_name.as_str() == "a.type")
        );
        assert!(
            matches!(&sorted[1], Op::Delete { type_name, .. } if type_name.as_str() == "z.type")
        );
    }

    #[test]
    fn sort_ops_empty_input() {
        let sorted = sort_ops_for_apply(&[], &empty_schema());
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
        let sorted = sort_ops_for_apply(&ops, &empty_schema());
        assert!(matches!(&sorted[0], Op::Create { .. }));
        assert!(matches!(&sorted[1], Op::Update { .. }));
    }

    // --- sort_ops_for_apply: topological ordering by reference (issue #47) ---

    fn field(r#type: FieldType) -> FieldSchema {
        FieldSchema {
            r#type,
            required: false,
            nullable: true,
            format: None,
            pattern: None,
            description: None,
        }
    }

    fn ref_t() -> FieldType {
        FieldType::Ref {
            target: "node".to_string(),
        }
    }

    fn list_ref_t() -> FieldType {
        FieldType::ListRef {
            target: "node".to_string(),
        }
    }

    /// schema with a single type `node` keyed by `slug` plus the given fields.
    fn node_schema(fields: &[(&str, FieldType)]) -> Schema {
        let mut field_map = BTreeMap::new();
        for (name, ty) in fields {
            field_map.insert(name.to_string(), field(ty.clone()));
        }
        let mut key = BTreeMap::new();
        key.insert("slug".to_string(), field(FieldType::Slug));
        let mut types = BTreeMap::new();
        types.insert(
            "node".to_string(),
            TypeSchema {
                key,
                fields: field_map,
            },
        );
        Schema { types }
    }

    /// a uuid string ref value for the given uid.
    fn uref(uid: u128) -> serde_json::Value {
        json!(Uid::from_u128(uid).to_string())
    }

    fn create_node(uid: u128, slug: &str, attrs: JsonMap) -> Op {
        Op::Create {
            uid: Uid::from_u128(uid),
            type_name: TypeName::new("node"),
            desired: make_object(uid, "node", slug, attrs),
        }
    }

    /// position of the op carrying `uid` in the ordered output.
    fn order_index(ops: &[Op], uid: u128) -> usize {
        ops.iter()
            .position(|op| op.uid().as_u128() == uid)
            .expect("uid present in ordered ops")
    }

    #[test]
    fn toposort_linear_ref_chain() {
        let schema = node_schema(&[("next", ref_t())]);
        let ops = vec![
            create_node(1, "a", make_attrs(&[("next", uref(2))])),
            create_node(2, "b", make_attrs(&[("next", uref(3))])),
            create_node(3, "c", make_attrs(&[])),
        ];
        let sorted = sort_ops_for_apply(&ops, &schema);
        // referenced objects are created before the objects referencing them.
        assert!(order_index(&sorted, 3) < order_index(&sorted, 2));
        assert!(order_index(&sorted, 2) < order_index(&sorted, 1));
    }

    #[test]
    fn toposort_diamond_shared_dep() {
        let schema = node_schema(&[("parent", ref_t()), ("left", ref_t()), ("right", ref_t())]);
        let ops = vec![
            create_node(1, "d", make_attrs(&[("left", uref(2)), ("right", uref(3))])),
            create_node(2, "b", make_attrs(&[("parent", uref(4))])),
            create_node(3, "c", make_attrs(&[("parent", uref(4))])),
            create_node(4, "a", make_attrs(&[])),
        ];
        let sorted = sort_ops_for_apply(&ops, &schema);
        let (a, b, c, d) = (
            order_index(&sorted, 4),
            order_index(&sorted, 2),
            order_index(&sorted, 3),
            order_index(&sorted, 1),
        );
        assert!(a < b && a < c, "shared dep created first");
        assert!(b < d && c < d, "both branches created before the joiner");
    }

    #[test]
    fn toposort_list_ref_fan_out() {
        let schema = node_schema(&[("members", list_ref_t())]);
        let ops = vec![
            create_node(
                1,
                "a",
                make_attrs(&[("members", json!([uref(2), uref(3), uref(4)]))]),
            ),
            create_node(2, "b", make_attrs(&[])),
            create_node(3, "c", make_attrs(&[])),
            create_node(4, "d", make_attrs(&[])),
        ];
        let sorted = sort_ops_for_apply(&ops, &schema);
        let a = order_index(&sorted, 1);
        assert!(order_index(&sorted, 2) < a);
        assert!(order_index(&sorted, 3) < a);
        assert!(order_index(&sorted, 4) < a);
    }

    #[test]
    fn toposort_refs_nested_in_list_and_map() {
        let schema = node_schema(&[
            (
                "group",
                FieldType::List {
                    item: Box::new(ref_t()),
                },
            ),
            (
                "lookup",
                FieldType::Map {
                    value: Box::new(ref_t()),
                },
            ),
        ]);
        let ops = vec![
            create_node(
                1,
                "a",
                make_attrs(&[
                    ("group", json!([uref(2)])),
                    ("lookup", json!({ "k": uref(3) })),
                ]),
            ),
            create_node(2, "b", make_attrs(&[])),
            create_node(3, "c", make_attrs(&[])),
        ];
        let sorted = sort_ops_for_apply(&ops, &schema);
        let a = order_index(&sorted, 1);
        assert!(order_index(&sorted, 2) < a, "ref nested in list");
        assert!(order_index(&sorted, 3) < a, "ref nested in map");
    }

    #[test]
    fn toposort_reference_cycle_falls_back_to_stable_order() {
        let schema = node_schema(&[("next", ref_t())]);
        let ops = vec![
            create_node(1, "aaa", make_attrs(&[("next", uref(2))])),
            create_node(2, "bbb", make_attrs(&[("next", uref(1))])),
        ];
        let sorted = sort_ops_for_apply(&ops, &schema);
        // a cycle still yields a total order without panicking or dropping ops.
        assert_eq!(sorted.len(), 2);
        // the cyclic remainder is emitted in stable op_order_key (slug) order.
        assert_eq!(order_index(&sorted, 1), 0);
        assert_eq!(order_index(&sorted, 2), 1);
    }

    #[test]
    fn reverse_toposort_deletes_child_before_parent() {
        // `child` is keyed by a reference to `node`, so deleting the parent
        // node must wait until the referencing child is gone.
        let mut node_key = BTreeMap::new();
        node_key.insert("slug".to_string(), field(FieldType::Slug));
        let mut child_key = BTreeMap::new();
        child_key.insert("parent".to_string(), field(ref_t()));
        child_key.insert("slug".to_string(), field(FieldType::Slug));
        let mut types = BTreeMap::new();
        types.insert(
            "node".to_string(),
            TypeSchema {
                key: node_key,
                fields: BTreeMap::new(),
            },
        );
        types.insert(
            "child".to_string(),
            TypeSchema {
                key: child_key,
                fields: BTreeMap::new(),
            },
        );
        let schema = Schema { types };

        let parent_key = Key::from(BTreeMap::from([("slug".to_string(), json!("p"))]));
        let child_key_val = Key::from(BTreeMap::from([
            ("parent".to_string(), uref(1)),
            ("slug".to_string(), json!("c")),
        ]));
        let ops = vec![
            Op::Delete {
                uid: Uid::from_u128(1),
                type_name: TypeName::new("node"),
                key: parent_key,
                backend_id: None,
            },
            Op::Delete {
                uid: Uid::from_u128(2),
                type_name: TypeName::new("child"),
                key: child_key_val,
                backend_id: None,
            },
        ];
        let sorted = sort_ops_for_apply(&ops, &schema);
        assert!(
            order_index(&sorted, 2) < order_index(&sorted, 1),
            "child deleted before the parent it references"
        );
        assert!(sorted.iter().all(|op| matches!(op, Op::Delete { .. })));
    }

    #[test]
    fn toposort_keeps_deletes_after_creates_and_updates() {
        let schema = node_schema(&[("next", ref_t())]);
        let ops = vec![
            Op::Delete {
                uid: Uid::from_u128(9),
                type_name: TypeName::new("node"),
                key: make_key("z"),
                backend_id: None,
            },
            create_node(1, "a", make_attrs(&[("next", uref(2))])),
            create_node(2, "b", make_attrs(&[])),
        ];
        let sorted = sort_ops_for_apply(&ops, &schema);
        let non_delete_count = sorted
            .iter()
            .filter(|op| !matches!(op, Op::Delete { .. }))
            .count();
        let delete_pos = sorted
            .iter()
            .position(|op| matches!(op, Op::Delete { .. }))
            .unwrap();
        // every create/update precedes the single delete.
        assert_eq!(delete_pos, non_delete_count);
        // and the create chain is still toposorted within its block.
        assert!(order_index(&sorted, 2) < order_index(&sorted, 1));
    }
}
