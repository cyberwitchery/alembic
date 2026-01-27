//! canonical ir types for alembic.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use uuid::Uuid;

/// Source location for tracking where an object was defined.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceLocation {
    /// Path to the source file.
    pub file: PathBuf,
    /// Line number in the file (1-indexed), if known.
    pub line: Option<usize>,
    /// Column number in the file (1-indexed), if known.
    pub column: Option<usize>,
}

impl SourceLocation {
    /// Create a source location with just a file path.
    pub fn file(path: impl Into<PathBuf>) -> Self {
        Self {
            file: path.into(),
            line: None,
            column: None,
        }
    }

    /// Create a source location with file and line number.
    pub fn file_line(path: impl Into<PathBuf>, line: usize) -> Self {
        Self {
            file: path.into(),
            line: Some(line),
            column: None,
        }
    }
}

impl fmt::Display for SourceLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.file.display())?;
        if let Some(line) = self.line {
            write!(f, ":{}", line)?;
            if let Some(col) = self.column {
                write!(f, ":{}", col)?;
            }
        }
        Ok(())
    }
}

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
#[derive(Debug, Clone, PartialEq)]
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

impl Serialize for FieldType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        match self {
            FieldType::String => serializer.serialize_str("string"),
            FieldType::Text => serializer.serialize_str("text"),
            FieldType::Int => serializer.serialize_str("int"),
            FieldType::Float => serializer.serialize_str("float"),
            FieldType::Bool => serializer.serialize_str("bool"),
            FieldType::Uuid => serializer.serialize_str("uuid"),
            FieldType::Date => serializer.serialize_str("date"),
            FieldType::Datetime => serializer.serialize_str("datetime"),
            FieldType::Time => serializer.serialize_str("time"),
            FieldType::Json => serializer.serialize_str("json"),
            FieldType::IpAddress => serializer.serialize_str("ip_address"),
            FieldType::Cidr => serializer.serialize_str("cidr"),
            FieldType::Prefix => serializer.serialize_str("prefix"),
            FieldType::Mac => serializer.serialize_str("mac"),
            FieldType::Slug => serializer.serialize_str("slug"),
            FieldType::Enum { values } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "enum")?;
                map.serialize_entry("values", values)?;
                map.end()
            }
            FieldType::List { item } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "list")?;
                map.serialize_entry("item", item)?;
                map.end()
            }
            FieldType::Map { value } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "map")?;
                map.serialize_entry("value", value)?;
                map.end()
            }
            FieldType::Ref { target } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "ref")?;
                map.serialize_entry("target", target)?;
                map.end()
            }
            FieldType::ListRef { target } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "list_ref")?;
                map.serialize_entry("target", target)?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for FieldType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        parse_field_type_value(&value).map_err(serde::de::Error::custom)
    }
}

fn parse_field_type_value(value: &serde_json::Value) -> Result<FieldType, String> {
    match value {
        serde_json::Value::String(raw) => parse_simple_field_type(raw),
        serde_json::Value::Object(map) => {
            let raw_type = map
                .get("type")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "field type requires a string 'type' key".to_string())?;
            match raw_type {
                "enum" => {
                    let values = map
                        .get("values")
                        .and_then(serde_json::Value::as_array)
                        .ok_or_else(|| "enum type requires values array".to_string())?
                        .iter()
                        .map(|value| {
                            value
                                .as_str()
                                .map(str::to_string)
                                .ok_or_else(|| "enum values must be strings".to_string())
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(FieldType::Enum { values })
                }
                "list" => {
                    let item = map
                        .get("item")
                        .ok_or_else(|| "list type requires item".to_string())?;
                    Ok(FieldType::List {
                        item: Box::new(parse_field_type_value(item)?),
                    })
                }
                "map" => {
                    let value = map
                        .get("value")
                        .ok_or_else(|| "map type requires value".to_string())?;
                    Ok(FieldType::Map {
                        value: Box::new(parse_field_type_value(value)?),
                    })
                }
                "ref" => {
                    let target = map
                        .get("target")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| "ref type requires target".to_string())?;
                    Ok(FieldType::Ref {
                        target: target.to_string(),
                    })
                }
                "list_ref" => {
                    let target = map
                        .get("target")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| "list_ref type requires target".to_string())?;
                    Ok(FieldType::ListRef {
                        target: target.to_string(),
                    })
                }
                _ => {
                    if map.len() != 1 {
                        return Err(format!("unknown field type {raw_type}"));
                    }
                    parse_simple_field_type(raw_type)
                }
            }
        }
        _ => Err("field type must be a string or map".to_string()),
    }
}

fn parse_simple_field_type(raw: &str) -> Result<FieldType, String> {
    match raw {
        "string" => Ok(FieldType::String),
        "text" => Ok(FieldType::Text),
        "int" => Ok(FieldType::Int),
        "float" => Ok(FieldType::Float),
        "bool" => Ok(FieldType::Bool),
        "uuid" => Ok(FieldType::Uuid),
        "date" => Ok(FieldType::Date),
        "datetime" => Ok(FieldType::Datetime),
        "time" => Ok(FieldType::Time),
        "json" => Ok(FieldType::Json),
        "ip_address" => Ok(FieldType::IpAddress),
        "cidr" => Ok(FieldType::Cidr),
        "prefix" => Ok(FieldType::Prefix),
        "mac" => Ok(FieldType::Mac),
        "slug" => Ok(FieldType::Slug),
        _ => Err(format!("unknown field type {raw}")),
    }
}

/// schema metadata for a single field.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FieldSchema {
    pub r#type: FieldType,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl<'de> Deserialize<'de> for FieldSchema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        let map = value
            .as_object()
            .ok_or_else(|| serde::de::Error::custom("field schema must be an object"))?;

        let required = map
            .get("required")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let nullable = map
            .get("nullable")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let description = map
            .get("description")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);

        let type_value = map
            .get("type")
            .ok_or_else(|| serde::de::Error::custom("field schema requires type"))?;
        let field_type = match type_value {
            serde_json::Value::String(raw) => match raw.as_str() {
                "list" => {
                    let item = map
                        .get("item")
                        .ok_or_else(|| serde::de::Error::custom("list type requires item"))?;
                    FieldType::List {
                        item: Box::new(
                            parse_field_type_value(item).map_err(serde::de::Error::custom)?,
                        ),
                    }
                }
                "map" => {
                    let value = map
                        .get("value")
                        .ok_or_else(|| serde::de::Error::custom("map type requires value"))?;
                    FieldType::Map {
                        value: Box::new(
                            parse_field_type_value(value).map_err(serde::de::Error::custom)?,
                        ),
                    }
                }
                "enum" => {
                    let values = map
                        .get("values")
                        .and_then(serde_json::Value::as_array)
                        .ok_or_else(|| serde::de::Error::custom("enum type requires values"))?
                        .iter()
                        .map(|value| {
                            value.as_str().map(str::to_string).ok_or_else(|| {
                                serde::de::Error::custom("enum values must be strings")
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    FieldType::Enum { values }
                }
                "ref" => {
                    let target = map
                        .get("target")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| serde::de::Error::custom("ref type requires target"))?;
                    FieldType::Ref {
                        target: target.to_string(),
                    }
                }
                "list_ref" => {
                    let target = map
                        .get("target")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| serde::de::Error::custom("list_ref type requires target"))?;
                    FieldType::ListRef {
                        target: target.to_string(),
                    }
                }
                _ => parse_simple_field_type(raw).map_err(serde::de::Error::custom)?,
            },
            _ => parse_field_type_value(type_value).map_err(serde::de::Error::custom)?,
        };

        Ok(FieldSchema {
            r#type: field_type,
            required,
            nullable,
            description,
        })
    }
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// source location where this object was defined (not serialized).
    #[serde(skip)]
    pub source: Option<SourceLocation>,
}

impl PartialEq for Object {
    fn eq(&self, other: &Self) -> bool {
        // Source location is intentionally excluded from equality
        self.uid == other.uid
            && self.type_name == other.type_name
            && self.key == other.key
            && self.attrs == other.attrs
    }
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
            source: None,
        })
    }

    /// Set the source location for this object.
    pub fn with_source(mut self, source: SourceLocation) -> Self {
        self.source = Some(source);
        self
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

    #[test]
    fn field_type_roundtrip() {
        let cases = vec![
            FieldType::String,
            FieldType::Int,
            FieldType::Enum {
                values: vec!["a".to_string()],
            },
            FieldType::Ref {
                target: "test".to_string(),
            },
            FieldType::List {
                item: Box::new(FieldType::Bool),
            },
        ];
        for case in cases {
            let json = serde_json::to_string(&case).unwrap();
            let back: FieldType = serde_json::from_str(&json).unwrap();
            assert_eq!(back, case);
        }
    }

    #[test]
    fn json_map_helpers() {
        let mut map = JsonMap::default();
        map.insert("s".to_string(), serde_json::json!("val"));
        map.insert("b".to_string(), serde_json::json!(true));
        map.insert("i".to_string(), serde_json::json!(123));
        map.insert("f".to_string(), serde_json::json!(1.23));

        assert_eq!(map.get_str("s"), Some("val"));
        assert_eq!(map.get_bool("b"), Some(true));
        assert_eq!(map.get_i64("i"), Some(123));
        assert_eq!(map.get_f64("f"), Some(1.23));

        assert_eq!(map.get_str("none"), None);
        assert_eq!(map.get_str("b"), None); // wrong type
    }

    #[test]
    fn test_key_string() {
        let mut k = BTreeMap::new();
        k.insert("a".to_string(), serde_json::json!(1));
        k.insert("b".to_string(), serde_json::json!("s"));
        let key = Key::from(k);
        let s = key_string(&key);
        assert!(s.contains("a=1"));
        assert!(s.contains("b=s"));
    }

    #[test]
    fn field_schema_deserialization() {
        // Simple type
        let json = serde_json::json!({ "type": "string" });
        let schema: FieldSchema = serde_json::from_value(json).unwrap();
        assert_eq!(schema.r#type, FieldType::String);

        // Map type
        let json = serde_json::json!({
            "type": "map",
            "value": "int"
        });
        let schema: FieldSchema = serde_json::from_value(json).unwrap();
        assert_eq!(
            schema.r#type,
            FieldType::Map {
                value: Box::new(FieldType::Int)
            }
        );

        // Enum type
        let json = serde_json::json!({
            "type": "enum",
            "values": ["a", "b"]
        });
        let schema: FieldSchema = serde_json::from_value(json).unwrap();
        assert_eq!(
            schema.r#type,
            FieldType::Enum {
                values: vec!["a".to_string(), "b".to_string()]
            }
        );

        // Complex nested
        let json = serde_json::json!({
            "type": "list",
            "item": { "type": "ref", "target": "test" }
        });
        let schema: FieldSchema = serde_json::from_value(json).unwrap();
        assert_eq!(
            schema.r#type,
            FieldType::List {
                item: Box::new(FieldType::Ref {
                    target: "test".to_string()
                })
            }
        );
    }

    #[test]
    fn test_type_name() {
        let t = TypeName::new("test");
        assert_eq!(t.as_str(), "test");
        assert!(!t.is_empty());
        assert_eq!(format!("{}", t), "test");

        let empty = TypeName::new("");
        assert!(empty.is_empty());
    }

    #[test]
    fn test_field_schema_defaults() {
        let json = serde_json::json!({ "type": "string" });
        let schema: FieldSchema = serde_json::from_value(json).unwrap();
        assert!(!schema.required);
        assert!(!schema.nullable);
        assert!(schema.description.is_none());
    }

    #[test]
    fn field_type_all_simple_variants() {
        let simple_types = vec![
            ("string", FieldType::String),
            ("int", FieldType::Int),
            ("float", FieldType::Float),
            ("bool", FieldType::Bool),
            ("uuid", FieldType::Uuid),
            ("date", FieldType::Date),
            ("datetime", FieldType::Datetime),
            ("time", FieldType::Time),
            ("json", FieldType::Json),
            ("ip_address", FieldType::IpAddress),
            ("cidr", FieldType::Cidr),
            ("prefix", FieldType::Prefix),
            ("mac", FieldType::Mac),
            ("slug", FieldType::Slug),
        ];
        for (name, expected) in simple_types {
            let json = serde_json::json!({ "type": name });
            let schema: FieldSchema = serde_json::from_value(json).unwrap();
            assert_eq!(schema.r#type, expected, "failed for {}", name);
        }
    }

    #[test]
    fn field_type_list_ref() {
        let json = serde_json::json!({
            "type": "list_ref",
            "target": "dcim.device"
        });
        let schema: FieldSchema = serde_json::from_value(json).unwrap();
        assert_eq!(
            schema.r#type,
            FieldType::ListRef {
                target: "dcim.device".to_string()
            }
        );
    }

    #[test]
    fn field_type_unknown_errors() {
        let json = serde_json::json!({ "type": "unknown_type" });
        let result: Result<FieldSchema, _> = serde_json::from_value(json);
        assert!(result.is_err());
    }

    #[test]
    fn field_type_enum_missing_values_errors() {
        let json = serde_json::json!({ "type": "enum" });
        let result: Result<FieldSchema, _> = serde_json::from_value(json);
        assert!(result.is_err());
    }

    #[test]
    fn field_type_list_missing_item_errors() {
        let json = serde_json::json!({ "type": "list" });
        let result: Result<FieldSchema, _> = serde_json::from_value(json);
        assert!(result.is_err());
    }

    #[test]
    fn field_type_map_missing_value_errors() {
        let json = serde_json::json!({ "type": "map" });
        let result: Result<FieldSchema, _> = serde_json::from_value(json);
        assert!(result.is_err());
    }

    #[test]
    fn field_type_ref_missing_target_errors() {
        let json = serde_json::json!({ "type": "ref" });
        let result: Result<FieldSchema, _> = serde_json::from_value(json);
        assert!(result.is_err());
    }

    #[test]
    fn key_into_inner_and_is_empty() {
        let key = Key::default();
        assert!(key.is_empty());
        let inner = key.into_inner();
        assert!(inner.is_empty());

        let mut k = BTreeMap::new();
        k.insert("a".to_string(), serde_json::json!(1));
        let key = Key::from(k);
        assert!(!key.is_empty());
    }

    #[test]
    fn json_map_into_inner_and_is_empty() {
        let map = JsonMap::default();
        assert!(map.is_empty());
        let inner = map.into_inner();
        assert!(inner.is_empty());
    }

    #[test]
    fn object_with_empty_key_errors() {
        let key = Key::default();
        let attrs = JsonMap::default();
        let result = Object::new(Uuid::from_u128(1), TypeName::new("dcim.site"), key, attrs);
        assert!(result.is_err());
    }
}
