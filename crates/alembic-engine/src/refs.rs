//! the ref contract import and the plan path share: a ref-typed field names the
//! target's uid, never the backend's own id.

use crate::adapter_ops::backend_id_from_value;
use crate::pretty_printing::bullet_list;
use crate::types::{ObservedObject, ObservedState};
use crate::StateStore;
use alembic_adapter_sdk::BackendId;
use alembic_core::{
    key_string, uid_v5, FieldType, JsonMap, Key, Schema, TypeName, TypeSchema, Uid,
};
use anyhow::{anyhow, Result};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;

/// the backend id a ref-typed value still holds. a uid, a null and a value that
/// is not a ref shape at all each resolve to nothing to report.
pub(crate) fn unrewritten_backend_id(value: &Value) -> Option<BackendId> {
    if value.is_null()
        || value
            .as_str()
            .is_some_and(|raw| Uid::parse_str(raw).is_ok())
    {
        return None;
    }
    backend_id_from_value(value)
}

/// visit every ref-typed leaf of one object's key and attrs, labelled as
/// validation labels it (`<field>`, key fields under `key.`).
pub(crate) fn visit_ref_leaves(
    type_schema: &TypeSchema,
    key: &Key,
    attrs: &JsonMap,
    visit: &mut impl FnMut(String, &str, &Value),
) {
    visit_key_ref_leaves(type_schema, key, visit);
    for (field, schema) in &type_schema.fields {
        if let Some(value) = attrs.get(field) {
            scan(field, &schema.r#type, value, visit);
        }
    }
}

/// the key half of [`visit_ref_leaves`], for the question an object's own uid
/// derives from its key alone.
fn visit_key_ref_leaves(
    type_schema: &TypeSchema,
    key: &Key,
    visit: &mut impl FnMut(String, &str, &Value),
) {
    for (field, schema) in &type_schema.key {
        if let Some(value) = key.get(field) {
            scan(&format!("key.{field}"), &schema.r#type, value, visit);
        }
    }
}

/// whether an observed object's own key still holds a backend id.
fn key_holds_backend_id(type_schema: &TypeSchema, key: &Key) -> bool {
    let mut held = false;
    visit_key_ref_leaves(type_schema, key, &mut |_, _, value| {
        held |= unrewritten_backend_id(value).is_some();
    });
    held
}

fn scan(
    field: &str,
    field_type: &FieldType,
    value: &Value,
    visit: &mut impl FnMut(String, &str, &Value),
) {
    match field_type {
        FieldType::Ref { target } => visit(field.to_string(), target, value),
        FieldType::ListRef { target } => {
            if let Value::Array(items) = value {
                for item in items {
                    visit(field.to_string(), target, item);
                }
            }
        }
        FieldType::List { item } => {
            if let Value::Array(items) = value {
                for elem in items {
                    scan(field, item, elem, visit);
                }
            }
        }
        FieldType::Map { value: inner } => {
            if let Value::Object(map) = value {
                for elem in map.values() {
                    scan(field, inner, elem, visit);
                }
            }
        }
        // enumerated as in `normalize_value_for_type`, so a new ref-bearing
        // variant has to answer in both places.
        FieldType::String
        | FieldType::Text
        | FieldType::Int
        | FieldType::Float
        | FieldType::Bool
        | FieldType::Uuid
        | FieldType::Date
        | FieldType::Datetime
        | FieldType::Time
        | FieldType::Json
        | FieldType::IpAddress
        | FieldType::Cidr
        | FieldType::Prefix
        | FieldType::Mac
        | FieldType::Slug
        | FieldType::Enum { .. } => {}
    }
}

/// a ref an adapter reported as a backend id, with what the observation itself
/// says about the target.
struct BackendIdRef {
    field: String,
    target: String,
    value: Value,
    cause: BackendIdCause,
}

/// what the observation holds for the target of a ref reported as a backend id.
enum BackendIdCause {
    /// no object with that backend id was observed.
    Unobserved,
    /// observed with a key already in uid space, so a uid derives for it.
    Rewritable,
    /// observed, but its own key still holds a backend id.
    KeyUnresolved,
}

impl fmt::Display for BackendIdRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} -> {} {}: ", self.field, self.target, self.value)?;
        match self.cause {
            BackendIdCause::Unobserved => write!(
                f,
                "no {} with that backend id was observed, so there is no uid to point at",
                self.target
            ),
            BackendIdCause::Rewritable => write!(
                f,
                "the {} it names was observed, so the adapter can rewrite the id without reading again",
                self.target
            ),
            BackendIdCause::KeyUnresolved => write!(
                f,
                "the {} it names was observed, but its own key still holds a backend id",
                self.target
            ),
        }
    }
}

/// refuse an observation holding refs in backend-id space. plan matches desired
/// against observed by key and diffs the rest, and both sides are uids.
pub(crate) fn refuse_backend_id_refs(observed: &ObservedState, schema: &Schema) -> Result<()> {
    let mut found = Vec::new();
    for object in observed.objects() {
        let Some(type_schema) = schema.types.get(object.type_name.as_str()) else {
            continue;
        };
        visit_ref_leaves(
            type_schema,
            &object.key,
            &object.attrs,
            &mut |field, target, value| {
                let Some(backend_id) = unrewritten_backend_id(value) else {
                    return;
                };
                found.push(BackendIdRef {
                    field: format!("{}.{field}", object.type_name),
                    target: target.to_string(),
                    value: value.clone(),
                    cause: classify(observed, schema, target, backend_id),
                });
            },
        );
    }
    if found.is_empty() {
        return Ok(());
    }
    Err(anyhow!(
        "the adapter reported {} reference(s) as backend ids, but a ref-typed field names the target's uid:\n{}\nsee docs/external-adapters.md for the read contract.",
        found.len(),
        bullet_list(&found)
    ))
}

/// classify one ref by what the observation says about its target. a target
/// whose type the schema does not declare has no key to walk, as above.
fn classify(
    observed: &ObservedState,
    schema: &Schema,
    target: &str,
    backend_id: BackendId,
) -> BackendIdCause {
    let Some(object) = observed.by_backend_id(&TypeName::new(target), &backend_id) else {
        return BackendIdCause::Unobserved;
    };
    match schema.types.get(target) {
        Some(type_schema) if key_holds_backend_id(type_schema, &object.key) => {
            BackendIdCause::KeyUnresolved
        }
        _ => BackendIdCause::Rewritable,
    }
}

/// the uid each observed object answered to when it was read: the one state
/// bound it to, or else the one its key derives, which is how an adapter writes
/// a ref to a target state does not know. index-aligned with the observation.
pub(crate) fn read_uids(observed: &ObservedState, state: &StateStore) -> Vec<Uid> {
    observed
        .objects()
        .map(|object| {
            object
                .backend_id
                .as_ref()
                .and_then(|id| state.uid_for_backend_id(&object.type_name, id))
                .unwrap_or_else(|| uid_v5(object.type_name.as_str(), &key_string(&object.key)))
        })
        .collect()
}

/// point observed refs at the uids adoption just bound. a ref to an object that
/// was unbound at read time holds the uid its key derives (or, on a backend whose
/// ids are strings, its bare backend id); once adoption binds a declared uid to
/// that object, every ref to it, in keys as in attrs, is rewritten to the declared
/// one. returns whether any key changed, since a re-keyed object may now match a
/// declared key it could not before.
pub(crate) fn rebind_adopted_refs(
    observed: ObservedState,
    read_uids: &[Uid],
    schema: &Schema,
    state: &StateStore,
) -> Result<(ObservedState, bool)> {
    let mut rebound: BTreeMap<(String, String), Uid> = BTreeMap::new();
    for (object, read_uid) in observed.objects().zip(read_uids) {
        let Some(backend_id) = &object.backend_id else {
            continue;
        };
        let Some(bound) = state.uid_for_backend_id(&object.type_name, backend_id) else {
            continue;
        };
        if bound == *read_uid {
            continue;
        }
        let type_name = object.type_name.to_string();
        rebound.insert((type_name.clone(), read_uid.to_string()), bound);
        if let BackendId::String(id) = backend_id {
            rebound.insert((type_name, id.clone()), bound);
        }
    }

    let objects = observed.into_objects();
    let mut keys_changed = false;
    let mut next = ObservedState::default();
    for mut object in objects {
        if !rebound.is_empty() {
            if let Some(type_schema) = schema.types.get(object.type_name.as_str()) {
                keys_changed |= rebind_object(&mut object, type_schema, &rebound);
            }
        }
        next.insert(object)?;
    }
    Ok((next, keys_changed))
}

/// rewrite one object's ref leaves through `rebound`; true when its key changed.
fn rebind_object(
    object: &mut ObservedObject,
    type_schema: &TypeSchema,
    rebound: &BTreeMap<(String, String), Uid>,
) -> bool {
    let mut key_changed = false;
    for (field, schema) in &type_schema.key {
        if let Some(value) = object.key.get_mut(field) {
            key_changed |= rebind_value(&schema.r#type, value, rebound);
        }
    }
    for (field, schema) in type_schema.key.iter().chain(&type_schema.fields) {
        if let Some(value) = object.attrs.get_mut(field) {
            rebind_value(&schema.r#type, value, rebound);
        }
    }
    key_changed
}

/// the mutable counterpart of `scan`: rewrite each ref leaf `rebound` names.
fn rebind_value(
    field_type: &FieldType,
    value: &mut Value,
    rebound: &BTreeMap<(String, String), Uid>,
) -> bool {
    let leaf = |target: &str, value: &mut Value| {
        let Some(uid) = value
            .as_str()
            .and_then(|raw| rebound.get(&(target.to_string(), raw.to_string())))
        else {
            return false;
        };
        *value = Value::String(uid.to_string());
        true
    };
    match field_type {
        FieldType::Ref { target } => leaf(target, value),
        FieldType::ListRef { target } => match value {
            Value::Array(items) => items
                .iter_mut()
                .fold(false, |changed, item| leaf(target, item) | changed),
            _ => false,
        },
        FieldType::List { item } => match value {
            Value::Array(items) => items.iter_mut().fold(false, |changed, elem| {
                rebind_value(item, elem, rebound) | changed
            }),
            _ => false,
        },
        FieldType::Map { value: inner } => match value {
            Value::Object(map) => map.values_mut().fold(false, |changed, elem| {
                rebind_value(inner, elem, rebound) | changed
            }),
            _ => false,
        },
        // enumerated as in `scan`, so a new ref-bearing variant has to answer here.
        FieldType::String
        | FieldType::Text
        | FieldType::Int
        | FieldType::Float
        | FieldType::Bool
        | FieldType::Uuid
        | FieldType::Date
        | FieldType::Datetime
        | FieldType::Time
        | FieldType::Json
        | FieldType::IpAddress
        | FieldType::Cidr
        | FieldType::Prefix
        | FieldType::Mac
        | FieldType::Slug
        | FieldType::Enum { .. } => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // a null is not a backend id to report.
    #[test]
    fn null_value_is_none() {
        assert_eq!(unrewritten_backend_id(&json!(null)), None);
    }

    // a value already in uid space is not a backend id to report.
    #[test]
    fn valid_uid_string_is_none() {
        let uid = Uid::parse_str("0198d3f0-0000-8000-8000-000000000000").unwrap();
        assert_eq!(unrewritten_backend_id(&json!(uid.to_string())), None);
    }

    // an id-shaped backend id like 42 is a real backend id.
    #[test]
    fn int_shaped_value_is_some() {
        assert_eq!(unrewritten_backend_id(&json!(42)), Some(BackendId::Int(42)));
    }
}
