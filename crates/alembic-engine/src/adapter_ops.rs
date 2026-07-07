use crate::{AdapterApplyError, BackendId, StateStore};
use alembic_core::{FieldType, JsonMap, Key, TypeSchema, Uid};
use anyhow::{anyhow, Result};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

pub fn build_key_from_schema(type_schema: &TypeSchema, attrs: &JsonMap) -> Result<Key> {
    let mut map = BTreeMap::new();
    for field in type_schema.key.keys() {
        let Some(value) = attrs.get(field) else {
            return Err(anyhow!("missing key field {field}"));
        };
        map.insert(field.clone(), value.clone());
    }
    Ok(Key::from(map))
}

pub fn build_request_body<Id, F>(
    type_schema: &TypeSchema,
    attrs: &JsonMap,
    resolved: &BTreeMap<Uid, Id>,
    encode_ref: F,
) -> Result<Value>
where
    F: Fn(&Id) -> Value + Copy,
{
    let mut map = Map::new();
    for (key, value) in attrs.iter() {
        let field_schema = type_schema
            .fields
            .get(key)
            .ok_or_else(|| anyhow!("missing schema for field {key}"))?;
        if value.is_null() {
            map.insert(key.clone(), Value::Null);
            continue;
        }
        map.insert(
            key.clone(),
            resolve_value_for_type(&field_schema.r#type, value.clone(), resolved, encode_ref)?,
        );
    }
    Ok(Value::Object(map))
}

pub fn resolve_value_for_type<Id, F>(
    field_type: &FieldType,
    value: Value,
    resolved: &BTreeMap<Uid, Id>,
    encode_ref: F,
) -> Result<Value>
where
    F: Fn(&Id) -> Value + Copy,
{
    match field_type {
        FieldType::Ref { .. } => resolve_ref_value(value, resolved, encode_ref),
        FieldType::ListRef { .. } => resolve_list_ref_value(value, resolved, encode_ref),
        FieldType::List { item } => resolve_list_value(item, value, resolved, encode_ref),
        FieldType::Map { value: inner } => resolve_map_value(inner, value, resolved, encode_ref),
        _ => Ok(value),
    }
}

pub fn query_filters_from_key<Id>(
    type_schema: &TypeSchema,
    key: &Key,
    resolved: &BTreeMap<Uid, Id>,
) -> Result<Vec<(String, String)>>
where
    Id: ToString,
{
    let mut filters = Vec::new();
    for (field, value) in key.iter() {
        let field_schema = type_schema
            .key
            .get(field)
            .ok_or_else(|| anyhow!("missing schema for key field {field}"))?;
        add_query_filters(&mut filters, field, &field_schema.r#type, value, resolved)?;
    }
    Ok(filters)
}

fn resolve_ref_value<Id, F>(
    value: Value,
    resolved: &BTreeMap<Uid, Id>,
    encode_ref: F,
) -> Result<Value>
where
    F: Fn(&Id) -> Value + Copy,
{
    let Value::String(raw) = value else {
        return Err(anyhow!("ref value must be a uuid string"));
    };
    let uid = Uid::parse_str(&raw).map_err(|_| anyhow!("ref value is not a uuid: {raw}"))?;
    let id = resolved
        .get(&uid)
        .ok_or(AdapterApplyError::MissingRef { uid })?;
    Ok(encode_ref(id))
}

fn resolve_list_ref_value<Id, F>(
    value: Value,
    resolved: &BTreeMap<Uid, Id>,
    encode_ref: F,
) -> Result<Value>
where
    F: Fn(&Id) -> Value + Copy,
{
    let Value::Array(items) = value else {
        return Err(anyhow!("list_ref value must be an array"));
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        out.push(resolve_ref_value(item, resolved, encode_ref)?);
    }
    Ok(Value::Array(out))
}

fn resolve_list_value<Id, F>(
    item_type: &FieldType,
    value: Value,
    resolved: &BTreeMap<Uid, Id>,
    encode_ref: F,
) -> Result<Value>
where
    F: Fn(&Id) -> Value + Copy,
{
    let Value::Array(items) = value else {
        return Err(anyhow!("list value must be an array"));
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        out.push(resolve_value_for_type(
            item_type, item, resolved, encode_ref,
        )?);
    }
    Ok(Value::Array(out))
}

fn resolve_map_value<Id, F>(
    value_type: &FieldType,
    value: Value,
    resolved: &BTreeMap<Uid, Id>,
    encode_ref: F,
) -> Result<Value>
where
    F: Fn(&Id) -> Value + Copy,
{
    let Value::Object(map) = value else {
        return Err(anyhow!("map value must be an object"));
    };
    let mut out = Map::new();
    for (key, value) in map {
        out.insert(
            key,
            resolve_value_for_type(value_type, value, resolved, encode_ref)?,
        );
    }
    Ok(Value::Object(out))
}

fn add_query_filters<Id>(
    filters: &mut Vec<(String, String)>,
    field: &str,
    field_type: &FieldType,
    value: &Value,
    resolved: &BTreeMap<Uid, Id>,
) -> Result<()>
where
    Id: ToString,
{
    match field_type {
        FieldType::Ref { .. } => {
            let id = resolve_query_ref(value, resolved)?;
            filters.push((field.to_string(), id));
            Ok(())
        }
        FieldType::ListRef { .. } => {
            let Value::Array(items) = value else {
                return Err(anyhow!("key field {field} must be an array"));
            };
            for item in items {
                let id = resolve_query_ref(item, resolved)?;
                filters.push((field.to_string(), id));
            }
            Ok(())
        }
        _ => {
            let scalar = value_to_query_value(value)?;
            filters.push((field.to_string(), scalar));
            Ok(())
        }
    }
}

fn resolve_query_ref<Id>(value: &Value, resolved: &BTreeMap<Uid, Id>) -> Result<String>
where
    Id: ToString,
{
    let Value::String(raw) = value else {
        return Err(anyhow!("ref value must be a uuid string"));
    };
    let uid = Uid::parse_str(raw).map_err(|_| anyhow!("ref value is not a uuid: {raw}"))?;
    let id = resolved
        .get(&uid)
        .ok_or(AdapterApplyError::MissingRef { uid })?;
    Ok(id.to_string())
}

fn value_to_query_value(value: &Value) -> Result<String> {
    match value {
        Value::String(raw) => Ok(raw.clone()),
        Value::Number(num) => Ok(num.to_string()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Null => Err(anyhow!("key value is null")),
        Value::Array(_) | Value::Object(_) => Err(anyhow!("key value must be scalar")),
    }
}

/// project the state store into a per-type `backend-id -> uid` map, keeping only
/// the backend ids `extract` accepts (the variant a given adapter retains).
pub fn state_mappings_by_id<I: Ord>(
    state: &StateStore,
    extract: impl Fn(&BackendId) -> Option<I>,
) -> BTreeMap<String, BTreeMap<I, Uid>> {
    let mut by_type = BTreeMap::new();
    for (type_name, mapping) in state.all_mappings() {
        let mut id_to_uid = BTreeMap::new();
        for (uid, backend_id) in mapping {
            if let Some(id) = extract(backend_id) {
                id_to_uid.insert(id, *uid);
            }
        }
        by_type.insert(type_name.as_str().to_string(), id_to_uid);
    }
    by_type
}

/// project the state store into a flat `uid -> backend-id` map, keeping only the
/// backend ids `extract` accepts. companion to [`state_mappings_by_id`].
pub fn resolved_ids_from_state<I>(
    state: &StateStore,
    extract: impl Fn(&BackendId) -> Option<I>,
) -> BTreeMap<Uid, I> {
    let mut resolved = BTreeMap::new();
    for mapping in state.all_mappings().values() {
        for (uid, backend_id) in mapping {
            if let Some(id) = extract(backend_id) {
                resolved.insert(*uid, id);
            }
        }
    }
    resolved
}

/// per-type `backend-id -> uid` map for read-side ref normalization.
#[derive(Debug, Default, Clone)]
pub struct StateMappings {
    by_type: BTreeMap<String, BTreeMap<BackendId, Uid>>,
}

impl StateMappings {
    /// the canonical uid a backend id maps to for `type_name`, if known.
    pub fn uid_for(&self, type_name: &str, backend_id: &BackendId) -> Option<Uid> {
        self.by_type
            .get(type_name)
            .and_then(|mapping| mapping.get(backend_id).copied())
    }

    /// record a `backend-id -> uid` mapping for `type_name`.
    pub fn insert(&mut self, type_name: &str, backend_id: BackendId, uid: Uid) {
        self.by_type
            .entry(type_name.to_string())
            .or_default()
            .insert(backend_id, uid);
    }

    /// project the whole state store into per-type backend-id -> uid mappings.
    pub fn from_state(state: &StateStore) -> Self {
        StateMappings {
            by_type: state_mappings_by_id(state, |b| Some(b.clone())),
        }
    }
}

/// project the state store into a flat `uid -> backend-id` map, keeping every
/// mapping. identity companion to [`resolved_ids_from_state`].
pub fn resolved_ids_identity(state: &StateStore) -> BTreeMap<Uid, BackendId> {
    resolved_ids_from_state(state, |b| Some(b.clone()))
}

/// rewrite every reference-typed field of `attrs` from backend ids back to
/// canonical uids. read-side inverse of `build_request_body`.
pub fn normalize_attrs_refs(
    attrs: &JsonMap,
    type_schema: &TypeSchema,
    mappings: &StateMappings,
) -> JsonMap {
    let mut normalized = attrs.clone();
    // key fields can be ref-typed too (validated as first-class refs, and read
    // back out by `build_key_from_schema`), so normalize them alongside `fields`.
    // a field declared in both is normalized idempotently (the `fields` pass wins
    // with the same result).
    for (field, schema) in type_schema.key.iter().chain(&type_schema.fields) {
        if let Some(value) = attrs.get(field) {
            normalized.insert(
                field.clone(),
                normalize_value_for_type(&schema.r#type, value.clone(), mappings),
            );
        }
    }
    normalized
}

/// read-side mirror of `resolve_value_for_type`: maps backend ids back to uids
/// at each ref leaf, recursing into `list` and `map` field types.
fn normalize_value_for_type(
    field_type: &FieldType,
    value: Value,
    mappings: &StateMappings,
) -> Value {
    match field_type {
        FieldType::Ref { target } => normalize_ref_value(value, target, mappings),
        FieldType::ListRef { target } => match value {
            Value::Array(items) => Value::Array(
                items
                    .into_iter()
                    .map(|item| normalize_ref_value(item, target, mappings))
                    .collect(),
            ),
            other => other,
        },
        FieldType::List { item } => match value {
            Value::Array(items) => Value::Array(
                items
                    .into_iter()
                    .map(|elem| normalize_value_for_type(item, elem, mappings))
                    .collect(),
            ),
            other => other,
        },
        FieldType::Map { value: inner } => match value {
            Value::Object(obj) => Value::Object(
                obj.into_iter()
                    .map(|(k, v)| (k, normalize_value_for_type(inner, v, mappings)))
                    .collect(),
            ),
            other => other,
        },
        _ => value,
    }
}

/// map one ref value's backend id back to its canonical uid, or leave it as-is
/// when the id is unknown or the value is not a ref shape.
fn normalize_ref_value(value: Value, target: &str, mappings: &StateMappings) -> Value {
    if value.is_null() {
        return value;
    }
    let backend_id = match backend_id_from_value(&value) {
        Some(id) => id,
        None => return value,
    };
    mappings
        .uid_for(target, &backend_id)
        .map(|uid| Value::String(uid.to_string()))
        .unwrap_or(value)
}

/// read a backend id out of a raw json value: a number, a string, or an object
/// with an `id` field (a nested brief).
pub fn backend_id_from_value(value: &Value) -> Option<BackendId> {
    match value {
        Value::Number(n) => n.as_u64().map(BackendId::Int).or_else(|| {
            n.as_i64()
                .and_then(|v| u64::try_from(v).ok())
                .map(BackendId::Int)
        }),
        Value::String(s) => Some(BackendId::String(s.clone())),
        Value::Object(map) => map.get("id").and_then(backend_id_from_value),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alembic_core::{FieldSchema, FieldType, JsonMap, Key, TypeSchema};
    use serde_json::json;
    use uuid::Uuid;

    fn field_schema(r#type: FieldType) -> FieldSchema {
        FieldSchema {
            r#type,
            required: false,
            nullable: false,
            format: None,
            pattern: None,
            description: None,
        }
    }

    fn simple_type_schema() -> TypeSchema {
        TypeSchema {
            key: BTreeMap::from([("slug".to_string(), field_schema(FieldType::Slug))]),
            fields: BTreeMap::from([
                ("name".to_string(), field_schema(FieldType::String)),
                ("count".to_string(), field_schema(FieldType::Int)),
            ]),
        }
    }

    fn attrs(pairs: Vec<(&str, Value)>) -> JsonMap {
        JsonMap::from(
            pairs
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect::<BTreeMap<_, _>>(),
        )
    }

    fn empty_resolved() -> BTreeMap<Uid, i64> {
        BTreeMap::new()
    }

    fn encode_ref(id: &i64) -> Value {
        json!(id)
    }

    // --- build_key_from_schema ---

    #[test]
    fn build_key_from_schema_extracts_key_fields() {
        let schema = simple_type_schema();
        let a = attrs(vec![("slug", json!("fra1")), ("name", json!("FRA1"))]);
        let key = build_key_from_schema(&schema, &a).unwrap();
        assert_eq!(key.get("slug"), Some(&json!("fra1")));
        assert_eq!(key.len(), 1);
    }

    #[test]
    fn build_key_from_schema_composite_key() {
        let schema = TypeSchema {
            key: BTreeMap::from([
                ("site".to_string(), field_schema(FieldType::String)),
                ("name".to_string(), field_schema(FieldType::String)),
            ]),
            fields: BTreeMap::new(),
        };
        let a = attrs(vec![("site", json!("fra1")), ("name", json!("eth0"))]);
        let key = build_key_from_schema(&schema, &a).unwrap();
        assert_eq!(key.len(), 2);
        assert_eq!(key.get("site"), Some(&json!("fra1")));
        assert_eq!(key.get("name"), Some(&json!("eth0")));
    }

    #[test]
    fn build_key_from_schema_missing_field_errors() {
        let schema = simple_type_schema();
        let a = attrs(vec![("name", json!("FRA1"))]);
        let err = build_key_from_schema(&schema, &a).unwrap_err();
        assert!(err.to_string().contains("missing key field slug"));
    }

    // --- build_request_body ---

    #[test]
    fn build_request_body_scalar_fields() {
        let schema = simple_type_schema();
        let a = attrs(vec![("name", json!("FRA1")), ("count", json!(42))]);
        let body = build_request_body(&schema, &a, &empty_resolved(), encode_ref).unwrap();
        let obj = body.as_object().unwrap();
        assert_eq!(obj.get("name"), Some(&json!("FRA1")));
        assert_eq!(obj.get("count"), Some(&json!(42)));
    }

    #[test]
    fn build_request_body_null_value_passes_through() {
        let schema = simple_type_schema();
        let a = attrs(vec![("name", Value::Null)]);
        let body = build_request_body(&schema, &a, &empty_resolved(), encode_ref).unwrap();
        assert_eq!(body.as_object().unwrap().get("name"), Some(&Value::Null));
    }

    #[test]
    fn build_request_body_resolves_ref() {
        let uid = Uuid::from_u128(1);
        let schema = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::from([(
                "site".to_string(),
                field_schema(FieldType::Ref {
                    target: "dcim.site".to_string(),
                }),
            )]),
        };
        let a = attrs(vec![("site", json!(uid.to_string()))]);
        let mut resolved = BTreeMap::new();
        resolved.insert(uid, 99_i64);
        let body = build_request_body(&schema, &a, &resolved, encode_ref).unwrap();
        assert_eq!(body.as_object().unwrap().get("site"), Some(&json!(99)));
    }

    #[test]
    fn build_request_body_missing_schema_errors() {
        let schema = simple_type_schema();
        let a = attrs(vec![("nonexistent", json!("x"))]);
        let err = build_request_body(&schema, &a, &empty_resolved(), encode_ref).unwrap_err();
        assert!(err.to_string().contains("missing schema for field"));
    }

    // --- resolve_value_for_type ---

    #[test]
    fn resolve_value_scalar_passthrough() {
        let val = json!("hello");
        let result = resolve_value_for_type(
            &FieldType::String,
            val.clone(),
            &empty_resolved(),
            encode_ref,
        )
        .unwrap();
        assert_eq!(result, val);
    }

    #[test]
    fn resolve_value_ref() {
        let uid = Uuid::from_u128(5);
        let mut resolved = BTreeMap::new();
        resolved.insert(uid, 42_i64);
        let result = resolve_value_for_type(
            &FieldType::Ref {
                target: "t".to_string(),
            },
            json!(uid.to_string()),
            &resolved,
            encode_ref,
        )
        .unwrap();
        assert_eq!(result, json!(42));
    }

    #[test]
    fn resolve_value_ref_missing_uid_errors() {
        let uid = Uuid::from_u128(99);
        let err = resolve_value_for_type(
            &FieldType::Ref {
                target: "t".to_string(),
            },
            json!(uid.to_string()),
            &empty_resolved(),
            encode_ref,
        )
        .unwrap_err();
        assert!(err.to_string().contains("missing referenced uid"));
    }

    #[test]
    fn resolve_value_ref_non_string_errors() {
        let err = resolve_value_for_type(
            &FieldType::Ref {
                target: "t".to_string(),
            },
            json!(123),
            &empty_resolved(),
            encode_ref,
        )
        .unwrap_err();
        assert!(err.to_string().contains("ref value must be a uuid string"));
    }

    #[test]
    fn resolve_value_ref_invalid_uuid_errors() {
        let err = resolve_value_for_type(
            &FieldType::Ref {
                target: "t".to_string(),
            },
            json!("not-a-uuid"),
            &empty_resolved(),
            encode_ref,
        )
        .unwrap_err();
        assert!(err.to_string().contains("ref value is not a uuid"));
    }

    #[test]
    fn resolve_value_list_ref() {
        let uid1 = Uuid::from_u128(1);
        let uid2 = Uuid::from_u128(2);
        let mut resolved = BTreeMap::new();
        resolved.insert(uid1, 10_i64);
        resolved.insert(uid2, 20_i64);
        let result = resolve_value_for_type(
            &FieldType::ListRef {
                target: "t".to_string(),
            },
            json!([uid1.to_string(), uid2.to_string()]),
            &resolved,
            encode_ref,
        )
        .unwrap();
        assert_eq!(result, json!([10, 20]));
    }

    #[test]
    fn resolve_value_list_ref_non_array_errors() {
        let err = resolve_value_for_type(
            &FieldType::ListRef {
                target: "t".to_string(),
            },
            json!("not-array"),
            &empty_resolved(),
            encode_ref,
        )
        .unwrap_err();
        assert!(err.to_string().contains("list_ref value must be an array"));
    }

    #[test]
    fn resolve_value_list_scalars() {
        let result = resolve_value_for_type(
            &FieldType::List {
                item: Box::new(FieldType::String),
            },
            json!(["a", "b"]),
            &empty_resolved(),
            encode_ref,
        )
        .unwrap();
        assert_eq!(result, json!(["a", "b"]));
    }

    #[test]
    fn resolve_value_list_of_refs() {
        let uid = Uuid::from_u128(3);
        let mut resolved = BTreeMap::new();
        resolved.insert(uid, 7_i64);
        let result = resolve_value_for_type(
            &FieldType::List {
                item: Box::new(FieldType::Ref {
                    target: "t".to_string(),
                }),
            },
            json!([uid.to_string()]),
            &resolved,
            encode_ref,
        )
        .unwrap();
        assert_eq!(result, json!([7]));
    }

    #[test]
    fn resolve_value_list_non_array_errors() {
        let err = resolve_value_for_type(
            &FieldType::List {
                item: Box::new(FieldType::String),
            },
            json!("not-array"),
            &empty_resolved(),
            encode_ref,
        )
        .unwrap_err();
        assert!(err.to_string().contains("list value must be an array"));
    }

    #[test]
    fn resolve_value_map_scalars() {
        let result = resolve_value_for_type(
            &FieldType::Map {
                value: Box::new(FieldType::Int),
            },
            json!({"a": 1, "b": 2}),
            &empty_resolved(),
            encode_ref,
        )
        .unwrap();
        let obj = result.as_object().unwrap();
        assert_eq!(obj.get("a"), Some(&json!(1)));
        assert_eq!(obj.get("b"), Some(&json!(2)));
    }

    #[test]
    fn resolve_value_map_with_refs() {
        let uid = Uuid::from_u128(4);
        let mut resolved = BTreeMap::new();
        resolved.insert(uid, 50_i64);
        let result = resolve_value_for_type(
            &FieldType::Map {
                value: Box::new(FieldType::Ref {
                    target: "t".to_string(),
                }),
            },
            json!({"x": uid.to_string()}),
            &resolved,
            encode_ref,
        )
        .unwrap();
        assert_eq!(result.as_object().unwrap().get("x"), Some(&json!(50)));
    }

    #[test]
    fn resolve_value_map_non_object_errors() {
        let err = resolve_value_for_type(
            &FieldType::Map {
                value: Box::new(FieldType::String),
            },
            json!("not-object"),
            &empty_resolved(),
            encode_ref,
        )
        .unwrap_err();
        assert!(err.to_string().contains("map value must be an object"));
    }

    // --- query_filters_from_key ---

    #[test]
    fn query_filters_scalar_key() {
        let schema = simple_type_schema();
        let key = Key::from(BTreeMap::from([("slug".to_string(), json!("fra1"))]));
        let filters = query_filters_from_key(&schema, &key, &empty_resolved()).unwrap();
        assert_eq!(filters, vec![("slug".to_string(), "fra1".to_string())]);
    }

    #[test]
    fn query_filters_numeric_key() {
        let schema = TypeSchema {
            key: BTreeMap::from([("id".to_string(), field_schema(FieldType::Int))]),
            fields: BTreeMap::new(),
        };
        let key = Key::from(BTreeMap::from([("id".to_string(), json!(42))]));
        let filters = query_filters_from_key(&schema, &key, &empty_resolved()).unwrap();
        assert_eq!(filters, vec![("id".to_string(), "42".to_string())]);
    }

    #[test]
    fn query_filters_bool_key() {
        let schema = TypeSchema {
            key: BTreeMap::from([("active".to_string(), field_schema(FieldType::Bool))]),
            fields: BTreeMap::new(),
        };
        let key = Key::from(BTreeMap::from([("active".to_string(), json!(true))]));
        let filters = query_filters_from_key(&schema, &key, &empty_resolved()).unwrap();
        assert_eq!(filters, vec![("active".to_string(), "true".to_string())]);
    }

    #[test]
    fn query_filters_ref_key() {
        let uid = Uuid::from_u128(10);
        let schema = TypeSchema {
            key: BTreeMap::from([(
                "site".to_string(),
                field_schema(FieldType::Ref {
                    target: "dcim.site".to_string(),
                }),
            )]),
            fields: BTreeMap::new(),
        };
        let key = Key::from(BTreeMap::from([(
            "site".to_string(),
            json!(uid.to_string()),
        )]));
        let mut resolved = BTreeMap::new();
        resolved.insert(uid, 77_i64);
        let filters = query_filters_from_key(&schema, &key, &resolved).unwrap();
        assert_eq!(filters, vec![("site".to_string(), "77".to_string())]);
    }

    #[test]
    fn query_filters_list_ref_key() {
        let uid1 = Uuid::from_u128(1);
        let uid2 = Uuid::from_u128(2);
        let schema = TypeSchema {
            key: BTreeMap::from([(
                "tags".to_string(),
                field_schema(FieldType::ListRef {
                    target: "extras.tag".to_string(),
                }),
            )]),
            fields: BTreeMap::new(),
        };
        let key = Key::from(BTreeMap::from([(
            "tags".to_string(),
            json!([uid1.to_string(), uid2.to_string()]),
        )]));
        let mut resolved = BTreeMap::new();
        resolved.insert(uid1, 100_i64);
        resolved.insert(uid2, 200_i64);
        let filters = query_filters_from_key(&schema, &key, &resolved).unwrap();
        assert_eq!(
            filters,
            vec![
                ("tags".to_string(), "100".to_string()),
                ("tags".to_string(), "200".to_string()),
            ]
        );
    }

    #[test]
    fn query_filters_missing_key_schema_errors() {
        let schema = simple_type_schema();
        let key = Key::from(BTreeMap::from([("nonexistent".to_string(), json!("x"))]));
        let err = query_filters_from_key(&schema, &key, &empty_resolved()).unwrap_err();
        assert!(err.to_string().contains("missing schema for key field"));
    }

    #[test]
    fn query_filters_null_scalar_errors() {
        let schema = simple_type_schema();
        let key = Key::from(BTreeMap::from([("slug".to_string(), Value::Null)]));
        let err = query_filters_from_key(&schema, &key, &empty_resolved()).unwrap_err();
        assert!(err.to_string().contains("key value is null"));
    }

    #[test]
    fn query_filters_list_ref_non_array_errors() {
        let schema = TypeSchema {
            key: BTreeMap::from([(
                "tags".to_string(),
                field_schema(FieldType::ListRef {
                    target: "t".to_string(),
                }),
            )]),
            fields: BTreeMap::new(),
        };
        let key = Key::from(BTreeMap::from([("tags".to_string(), json!("not-array"))]));
        let err = query_filters_from_key(&schema, &key, &empty_resolved()).unwrap_err();
        assert!(err.to_string().contains("key field tags must be an array"));
    }

    // --- state projection helpers ---

    fn store_with_mixed_ids(type_name: &str) -> StateStore {
        use crate::StateData;
        use alembic_core::TypeName;

        let mut data = StateData::default();
        data.mappings
            .entry(TypeName::new(type_name))
            .or_default()
            .extend([
                (Uid::from_u128(1), BackendId::Int(5)),
                (Uid::from_u128(2), BackendId::String("uuid-2".to_string())),
            ]);
        StateStore::new(None, data)
    }

    fn keep_int(b: &BackendId) -> Option<u64> {
        match b {
            BackendId::Int(i) => Some(*i),
            _ => None,
        }
    }

    fn keep_string(b: &BackendId) -> Option<String> {
        match b {
            BackendId::String(s) => Some(s.clone()),
            _ => None,
        }
    }

    #[test]
    fn state_mappings_by_id_keeps_only_accepted_variant() {
        let store = store_with_mixed_ids("dcim.site");

        let ints = state_mappings_by_id(&store, keep_int);
        assert_eq!(ints["dcim.site"].get(&5), Some(&Uid::from_u128(1)));
        assert_eq!(ints["dcim.site"].len(), 1);

        let strings = state_mappings_by_id(&store, keep_string);
        assert_eq!(strings["dcim.site"].get("uuid-2"), Some(&Uid::from_u128(2)));
        assert_eq!(strings["dcim.site"].len(), 1);
    }

    #[test]
    fn resolved_ids_from_state_filters_by_variant() {
        let store = store_with_mixed_ids("t");

        let ints = resolved_ids_from_state(&store, keep_int);
        assert_eq!(ints, BTreeMap::from([(Uid::from_u128(1), 5u64)]));

        let strings = resolved_ids_from_state(&store, keep_string);
        assert_eq!(
            strings,
            BTreeMap::from([(Uid::from_u128(2), "uuid-2".to_string())])
        );
    }

    // --- read-side ref normalization ---

    fn ref_type(target: &str) -> FieldType {
        FieldType::Ref {
            target: target.to_string(),
        }
    }

    fn mappings_with(type_name: &str, backend_id: BackendId, uid: Uid) -> StateMappings {
        let mut m = StateMappings::default();
        m.insert(type_name, backend_id, uid);
        m
    }

    #[test]
    fn normalize_value_ref_maps_backend_id_to_uid() {
        let uid = Uuid::from_u128(1);
        let m = mappings_with("t", BackendId::Int(7), uid);
        let out = normalize_value_for_type(&ref_type("t"), json!(7), &m);
        assert_eq!(out, json!(uid.to_string()));
    }

    #[test]
    fn normalize_value_ref_unknown_id_passes_through() {
        let out = normalize_value_for_type(&ref_type("t"), json!(7), &StateMappings::default());
        assert_eq!(out, json!(7));
    }

    #[test]
    fn normalize_value_list_ref_maps_each_known() {
        let uid = Uuid::from_u128(2);
        let m = mappings_with("t", BackendId::Int(8), uid);
        let out = normalize_value_for_type(
            &FieldType::ListRef {
                target: "t".to_string(),
            },
            json!([8, 9]),
            &m,
        );
        assert_eq!(out, json!([uid.to_string(), 9]));
    }

    #[test]
    fn normalize_value_ref_nested_in_list() {
        let uid = Uuid::from_u128(3);
        let m = mappings_with("t", BackendId::Int(10), uid);
        let out = normalize_value_for_type(
            &FieldType::List {
                item: Box::new(ref_type("t")),
            },
            json!([10]),
            &m,
        );
        assert_eq!(out, json!([uid.to_string()]));
    }

    #[test]
    fn normalize_value_ref_nested_in_map() {
        let uid = Uuid::from_u128(4);
        let m = mappings_with("t", BackendId::String("s".to_string()), uid);
        let out = normalize_value_for_type(
            &FieldType::Map {
                value: Box::new(ref_type("t")),
            },
            json!({ "k": "s" }),
            &m,
        );
        assert_eq!(out, json!({ "k": uid.to_string() }));
    }

    #[test]
    fn normalize_ref_value_object_shaped_brief() {
        let uid = Uuid::from_u128(5);
        let m = mappings_with("t", BackendId::Int(11), uid);
        let out = normalize_ref_value(json!({ "id": 11, "display": "x" }), "t", &m);
        assert_eq!(out, json!(uid.to_string()));
    }

    #[test]
    fn backend_id_from_value_variants() {
        assert_eq!(backend_id_from_value(&json!(42)), Some(BackendId::Int(42)));
        assert_eq!(backend_id_from_value(&json!(-1)), None);
        assert_eq!(
            backend_id_from_value(&json!("abc")),
            Some(BackendId::String("abc".to_string()))
        );
        assert_eq!(
            backend_id_from_value(&json!({ "id": 3 })),
            Some(BackendId::Int(3))
        );
        assert_eq!(backend_id_from_value(&Value::Null), None);
    }

    #[test]
    fn normalize_attrs_refs_resolves_declared_fields() {
        let uid = Uuid::from_u128(6);
        let m = mappings_with("dcim.site", BackendId::Int(12), uid);
        let schema = TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::from([("site".to_string(), field_schema(ref_type("dcim.site")))]),
        };
        let a = attrs(vec![("site", json!(12))]);
        let out = normalize_attrs_refs(&a, &schema, &m);
        assert_eq!(out.get("site"), Some(&json!(uid.to_string())));
    }

    #[test]
    fn normalize_attrs_refs_resolves_ref_typed_key_field() {
        let uid = Uuid::from_u128(7);
        let m = mappings_with("dcim.device", BackendId::Int(42), uid);
        let schema = TypeSchema {
            key: BTreeMap::from([("device".to_string(), field_schema(ref_type("dcim.device")))]),
            fields: BTreeMap::new(),
        };
        let a = attrs(vec![("device", json!(42))]);
        let out = normalize_attrs_refs(&a, &schema, &m);
        assert_eq!(out.get("device"), Some(&json!(uid.to_string())));
    }

    #[test]
    fn from_state_and_resolved_ids_identity_roundtrip() {
        let store = store_with_mixed_ids("dcim.site");
        let m = StateMappings::from_state(&store);
        assert_eq!(
            m.uid_for("dcim.site", &BackendId::Int(5)),
            Some(Uid::from_u128(1))
        );
        let resolved = resolved_ids_identity(&store);
        assert_eq!(resolved.get(&Uid::from_u128(1)), Some(&BackendId::Int(5)));
        assert_eq!(
            resolved.get(&Uid::from_u128(2)),
            Some(&BackendId::String("uuid-2".to_string()))
        );
    }
}
