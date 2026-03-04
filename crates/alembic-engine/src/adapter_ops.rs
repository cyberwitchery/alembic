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
