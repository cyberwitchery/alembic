//! canonical ir types for alembic.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;
use std::ops::{Deref, DerefMut};
use uuid::Uuid;

/// stable object identifier (uuid).
pub type Uid = Uuid;

/// json object wrapper for typed access and stricter boundaries.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JsonMap(pub BTreeMap<String, Value>);

impl JsonMap {
    pub fn into_inner(self) -> BTreeMap<String, Value> {
        self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.get(key)?.as_str()
    }

    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.get(key)?.as_bool()
    }

    pub fn get_i64(&self, key: &str) -> Option<i64> {
        self.get(key)?.as_i64()
    }

    pub fn get_f64(&self, key: &str) -> Option<f64> {
        self.get(key)?.as_f64()
    }
}

impl Deref for JsonMap {
    type Target = BTreeMap<String, Value>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for JsonMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<BTreeMap<String, Value>> for JsonMap {
    fn from(map: BTreeMap<String, Value>) -> Self {
        Self(map)
    }
}

impl From<JsonMap> for BTreeMap<String, Value> {
    fn from(map: JsonMap) -> Self {
        map.0
    }
}

/// structured key for object identity.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Key(pub BTreeMap<String, Value>);

impl Key {
    pub fn into_inner(self) -> BTreeMap<String, Value> {
        self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Deref for Key {
    type Target = BTreeMap<String, Value>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Key {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<BTreeMap<String, Value>> for Key {
    fn from(map: BTreeMap<String, Value>) -> Self {
        Self(map)
    }
}

impl From<Key> for BTreeMap<String, Value> {
    fn from(map: Key) -> Self {
        map.0
    }
}

pub fn key_string(key: &Key) -> String {
    key.0
        .iter()
        .map(|(field, value)| format!("{field}={}", key_value_to_string(value)))
        .collect::<Vec<_>>()
        .join("/")
}

fn key_value_to_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Null => "null".to_string(),
        _ => serde_json::to_string(value).unwrap_or_default(),
    }
}

pub const ALEMBIC_UID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x45, 0x93, 0x1a, 0x5f, 0x6c, 0x2b, 0x49, 0x6a, 0x9b, 0x6f, 0x8f, 0x77, 0x7d, 0x4f, 0x3a, 0x1c,
]);

pub fn uid_v5(type_name: &str, stable: &str) -> Uid {
    let name = format!("{type_name}:{stable}");
    Uuid::new_v5(&ALEMBIC_UID_NAMESPACE, name.as_bytes())
}

/// canonical object type name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TypeName(String);

impl TypeName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.trim().is_empty()
    }
}

impl fmt::Display for TypeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// field type definition in the schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FieldType {
    String,
    Text,
    Int,
    Float,
    Bool,
    Uuid,
    Date,
    Datetime,
    Time,
    Json,
    IpAddress,
    Cidr,
    Prefix,
    Mac,
    Slug,
    Enum { values: Vec<String> },
    List { item: Box<FieldType> },
    Map { value: Box<FieldType> },
    Ref { target: String },
    ListRef { target: String },
}

/// schema metadata for a single field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldSchema {
    pub r#type: FieldType,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// schema metadata for a type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TypeSchema {
    pub key: BTreeMap<String, FieldSchema>,
    #[serde(default)]
    pub fields: BTreeMap<String, FieldSchema>,
}

/// collection of schema definitions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    #[serde(default)]
    pub types: BTreeMap<String, TypeSchema>,
}

/// object envelope for the ir.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Object {
    /// stable identifier for the object.
    pub uid: Uid,
    /// canonical type for the object.
    #[serde(rename = "type", alias = "kind")]
    pub type_name: TypeName,
    /// structured key used for matching when state is missing.
    pub key: Key,
    /// attributes payload for this object.
    #[serde(default, rename = "attrs")]
    pub attrs: JsonMap,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectError {
    MissingType,
    MissingKey,
}

impl fmt::Display for ObjectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ObjectError::MissingType => f.write_str("object type must be set"),
            ObjectError::MissingKey => f.write_str("object key must be set"),
        }
    }
}

impl std::error::Error for ObjectError {}

impl Object {
    /// create an object with a type name.
    pub fn new(
        uid: Uid,
        type_name: TypeName,
        key: Key,
        attrs: JsonMap,
    ) -> Result<Self, ObjectError> {
        if type_name.is_empty() {
            return Err(ObjectError::MissingType);
        }
        if key.is_empty() {
            return Err(ObjectError::MissingKey);
        }
        Ok(Self {
            uid,
            type_name,
            key,
            attrs,
        })
    }
}

/// top-level inventory of objects.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Inventory {
    /// schema definitions for type metadata.
    pub schema: Schema,
    /// list of objects in this inventory.
    #[serde(default)]
    pub objects: Vec<Object>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_roundtrip_json() {
        let mut key = BTreeMap::new();
        key.insert("slug".to_string(), serde_json::json!("fra1"));
        let mut attrs = BTreeMap::new();
        attrs.insert("name".to_string(), serde_json::json!("FRA1"));
        let object = Object::new(
            Uuid::from_u128(1),
            TypeName::new("dcim.site"),
            Key::from(key),
            attrs.into(),
        )
        .unwrap();

        let value = serde_json::to_value(&object).unwrap();
        let decoded: Object = serde_json::from_value(value).unwrap();
        assert_eq!(decoded.uid, object.uid);
        assert_eq!(decoded.type_name, object.type_name);
        assert_eq!(decoded.key, object.key);
        assert_eq!(decoded.attrs, object.attrs);
    }

    #[test]
    fn object_roundtrip_json_only_attrs() {
        let mut key = BTreeMap::new();
        key.insert("slug".to_string(), serde_json::json!("fra1"));
        let mut attrs = BTreeMap::new();
        attrs.insert("name".to_string(), serde_json::json!("FRA1"));
        attrs.insert("extra".to_string(), serde_json::json!(true));
        let object = Object::new(
            Uuid::from_u128(2),
            TypeName::new("dcim.site"),
            Key::from(key),
            attrs.into(),
        )
        .unwrap();

        let value = serde_json::to_value(&object).unwrap();
        let decoded: Object = serde_json::from_value(value).unwrap();
        assert_eq!(decoded.attrs.get("extra"), Some(&serde_json::json!(true)));
    }
}
