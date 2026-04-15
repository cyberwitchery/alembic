use crate::AdapterApplyError;
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
}
