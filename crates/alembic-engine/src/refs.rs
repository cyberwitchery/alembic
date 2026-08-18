//! the ref contract import and the plan path share: a ref-typed field names the
//! target's uid, never the backend's own id.

use crate::adapter_ops::backend_id_from_value;
use crate::pretty_printing::bullet_list;
use crate::types::{BackendId, ObservedState};
use alembic_core::{FieldType, JsonMap, Key, Schema, TypeName, TypeSchema, Uid};
use anyhow::{anyhow, Result};
use serde_json::Value;
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
    for object in observed.by_key.values() {
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
    let Some(object) = observed
        .by_backend_id
        .get(&(TypeName::new(target), backend_id))
    else {
        return BackendIdCause::Unobserved;
    };
    match schema.types.get(target) {
        Some(type_schema) if key_holds_backend_id(type_schema, &object.key) => {
            BackendIdCause::KeyUnresolved
        }
        _ => BackendIdCause::Rewritable,
    }
}
