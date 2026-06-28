//! canonical ir types for alembic.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use uuid::Uuid;

/// source location for tracking where an object was defined.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceLocation {
    /// path to the source file.
    pub file: PathBuf,
    /// line number in the file (1-indexed), if known.
    pub line: Option<usize>,
    /// column number in the file (1-indexed), if known.
    pub column: Option<usize>,
}

impl SourceLocation {
    /// create a source location with just a file path.
    pub fn file(path: impl Into<PathBuf>) -> Self {
        Self {
            file: path.into(),
            line: None,
            column: None,
        }
    }

    /// create a source location with file and line number.
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
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, Eq, Hash)]
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
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, Eq, Hash)]
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
    let new_key: Key = Key(key
        .0
        .iter()
        .map(|(k, v)| (k.clone(), canonicalize_number(v.clone())))
        .collect());
    serde_json::to_string(&new_key).unwrap_or_default()
}

/// prefer integer representation if lossless
fn canonicalize_number(v: Value) -> Value {
    match v {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Number(i.into())
            } else if let Some(f) = n.as_f64() {
                let i = f.round() as i64;
                if f64::abs(i as f64 - f) < 1e-5 {
                    Value::Number(i.into())
                } else {
                    Value::Number(serde_json::Number::from_f64(f).unwrap())
                }
            } else {
                Value::Number(n)
            }
        }
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(k, v)| (k, canonicalize_number(v)))
                .collect(),
        ),
        Value::Array(arr) => Value::Array(arr.into_iter().map(canonicalize_number).collect()),
        other => other,
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

/// format constraints for string fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldFormat {
    Slug,
    IpAddress,
    Cidr,
    Prefix,
    Mac,
    Uuid,
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

fn parse_field_format(raw: &str) -> Result<FieldFormat, String> {
    match raw {
        "slug" => Ok(FieldFormat::Slug),
        "ip_address" => Ok(FieldFormat::IpAddress),
        "cidr" => Ok(FieldFormat::Cidr),
        "prefix" => Ok(FieldFormat::Prefix),
        "mac" => Ok(FieldFormat::Mac),
        "uuid" => Ok(FieldFormat::Uuid),
        _ => Err(format!("unknown field format {raw}")),
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
    pub format: Option<FieldFormat>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
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

        let bool_field = |key: &str| -> Result<bool, D::Error> {
            match map.get(key) {
                None => Ok(false),
                Some(value) => value.as_bool().ok_or_else(|| {
                    serde::de::Error::custom(format!("field schema `{key}` must be a boolean"))
                }),
            }
        };
        let str_field = |key: &str| -> Result<Option<String>, D::Error> {
            match map.get(key) {
                None => Ok(None),
                Some(value) => value.as_str().map(|s| Some(s.to_string())).ok_or_else(|| {
                    serde::de::Error::custom(format!("field schema `{key}` must be a string"))
                }),
            }
        };

        let required = bool_field("required")?;
        let nullable = bool_field("nullable")?;
        let description = str_field("description")?;
        let pattern = str_field("pattern")?;
        let format = str_field("format")?
            .map(|raw| parse_field_format(&raw).map_err(serde::de::Error::custom))
            .transpose()?;

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
            format,
            pattern,
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
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    #[serde(default)]
    pub types: BTreeMap<String, TypeSchema>,
}

/// object envelope for the ir.
#[derive(Debug, Clone, Serialize, Deserialize, Eq)]
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
        // source location is intentionally excluded from equality
        self.uid == other.uid
            && self.type_name == other.type_name
            && self.key == other.key
            && self.attrs == other.attrs
    }
}

impl Hash for Object {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.uid.hash(hasher);
        self.type_name.hash(hasher);
        self.key.hash(hasher);
        self.attrs.hash(hasher);
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

    /// set the source location for this object.
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
        let parsed: serde_json::Value = serde_json::from_str(&s).unwrap();
        let expected = serde_json::json!({"a": 1, "b": "s"});
        assert_eq!(parsed, expected);
    }

    #[test]
    fn field_schema_deserialization() {
        // simple type
        let json = serde_json::json!({ "type": "string" });
        let schema: FieldSchema = serde_json::from_value(json).unwrap();
        assert_eq!(schema.r#type, FieldType::String);

        // map type
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

        // enum type
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

        // complex nested
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
    fn field_schema_format_and_pattern() {
        let json = serde_json::json!({
            "type": "string",
            "format": "slug",
            "pattern": "^[a-z0-9-]+$"
        });
        let schema: FieldSchema = serde_json::from_value(json).unwrap();
        assert_eq!(schema.format, Some(FieldFormat::Slug));
        assert_eq!(schema.pattern.as_deref(), Some("^[a-z0-9-]+$"));
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
        assert!(schema.format.is_none());
        assert!(schema.pattern.is_none());
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

    #[test]
    fn object_with_empty_type_errors() {
        let mut k = BTreeMap::new();
        k.insert("slug".to_string(), serde_json::json!("x"));
        let result = Object::new(
            Uuid::from_u128(1),
            TypeName::new(""),
            Key::from(k),
            JsonMap::default(),
        );
        assert_eq!(result.unwrap_err(), ObjectError::MissingType);
    }

    #[test]
    fn object_with_whitespace_only_type_errors() {
        let mut k = BTreeMap::new();
        k.insert("slug".to_string(), serde_json::json!("x"));
        let result = Object::new(
            Uuid::from_u128(1),
            TypeName::new("   "),
            Key::from(k),
            JsonMap::default(),
        );
        assert_eq!(result.unwrap_err(), ObjectError::MissingType);
    }

    #[test]
    fn object_error_display() {
        assert_eq!(
            ObjectError::MissingType.to_string(),
            "object type must be set"
        );
        assert_eq!(
            ObjectError::MissingKey.to_string(),
            "object key must be set"
        );
    }

    #[test]
    fn object_with_source() {
        let mut k = BTreeMap::new();
        k.insert("slug".to_string(), serde_json::json!("x"));
        let obj = Object::new(
            Uuid::from_u128(1),
            TypeName::new("dcim.site"),
            Key::from(k),
            JsonMap::default(),
        )
        .unwrap()
        .with_source(SourceLocation::file_line("test.yaml", 42));
        assert_eq!(obj.source.as_ref().unwrap().line, Some(42));
    }

    #[test]
    fn object_equality_ignores_source() {
        let mut k = BTreeMap::new();
        k.insert("slug".to_string(), serde_json::json!("x"));
        let a = Object::new(
            Uuid::from_u128(1),
            TypeName::new("dcim.site"),
            Key::from(k.clone()),
            JsonMap::default(),
        )
        .unwrap()
        .with_source(SourceLocation::file("a.yaml"));
        let b = Object::new(
            Uuid::from_u128(1),
            TypeName::new("dcim.site"),
            Key::from(k),
            JsonMap::default(),
        )
        .unwrap()
        .with_source(SourceLocation::file("b.yaml"));
        assert_eq!(a, b);
    }

    #[test]
    fn object_deserialize_kind_alias() {
        let json = serde_json::json!({
            "uid": "00000000-0000-0000-0000-000000000001",
            "kind": "dcim.site",
            "key": {"slug": "x"}
        });
        let obj: Object = serde_json::from_value(json).unwrap();
        assert_eq!(obj.type_name.as_str(), "dcim.site");
    }

    #[test]
    fn object_source_not_serialized() {
        let mut k = BTreeMap::new();
        k.insert("slug".to_string(), serde_json::json!("x"));
        let obj = Object::new(
            Uuid::from_u128(1),
            TypeName::new("dcim.site"),
            Key::from(k),
            JsonMap::default(),
        )
        .unwrap()
        .with_source(SourceLocation::file_line("test.yaml", 10));
        let value = serde_json::to_value(&obj).unwrap();
        assert!(value.get("source").is_none());
    }

    #[test]
    fn source_location_display_file_only() {
        let loc = SourceLocation::file("test.yaml");
        assert_eq!(loc.to_string(), "test.yaml");
        assert!(loc.line.is_none());
        assert!(loc.column.is_none());
    }

    #[test]
    fn source_location_display_file_and_line() {
        let loc = SourceLocation::file_line("test.yaml", 42);
        assert_eq!(loc.to_string(), "test.yaml:42");
    }

    #[test]
    fn source_location_display_file_line_column() {
        let loc = SourceLocation {
            file: "test.yaml".into(),
            line: Some(42),
            column: Some(7),
        };
        assert_eq!(loc.to_string(), "test.yaml:42:7");
    }

    #[test]
    fn uid_v5_deterministic() {
        let a = uid_v5("dcim.site", "fra1");
        let b = uid_v5("dcim.site", "fra1");
        assert_eq!(a, b);
    }

    #[test]
    fn uid_v5_different_inputs() {
        let a = uid_v5("dcim.site", "fra1");
        let b = uid_v5("dcim.site", "fra2");
        let c = uid_v5("dcim.device", "fra1");
        assert_ne!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn json_map_serde_transparent() {
        let mut map = JsonMap::default();
        map.insert("k".to_string(), serde_json::json!("v"));
        let json = serde_json::to_value(&map).unwrap();
        assert_eq!(json, serde_json::json!({"k": "v"}));
        let back: JsonMap = serde_json::from_value(json).unwrap();
        assert_eq!(back, map);
    }

    #[test]
    fn key_serde_transparent() {
        let mut k = BTreeMap::new();
        k.insert("slug".to_string(), serde_json::json!("x"));
        let key = Key::from(k);
        let json = serde_json::to_value(&key).unwrap();
        assert_eq!(json, serde_json::json!({"slug": "x"}));
        let back: Key = serde_json::from_value(json).unwrap();
        assert_eq!(back, key);
    }

    #[test]
    fn type_name_serde_transparent() {
        let t = TypeName::new("dcim.site");
        let json = serde_json::to_value(&t).unwrap();
        assert_eq!(json, serde_json::json!("dcim.site"));
        let back: TypeName = serde_json::from_value(json).unwrap();
        assert_eq!(back, t);
    }

    #[test]
    fn field_type_roundtrip_all_complex_variants() {
        let cases = vec![
            FieldType::Text,
            FieldType::Float,
            FieldType::Uuid,
            FieldType::Date,
            FieldType::Datetime,
            FieldType::Time,
            FieldType::Json,
            FieldType::IpAddress,
            FieldType::Cidr,
            FieldType::Prefix,
            FieldType::Mac,
            FieldType::Slug,
            FieldType::Map {
                value: Box::new(FieldType::String),
            },
            FieldType::ListRef {
                target: "dcim.device".to_string(),
            },
            FieldType::Enum {
                values: vec!["active".to_string(), "planned".to_string()],
            },
            FieldType::List {
                item: Box::new(FieldType::List {
                    item: Box::new(FieldType::Int),
                }),
            },
        ];
        for case in cases {
            let json = serde_json::to_string(&case).unwrap();
            let back: FieldType = serde_json::from_str(&json).unwrap();
            assert_eq!(back, case, "roundtrip failed for {:?}", case);
        }
    }

    #[test]
    fn field_format_serde_roundtrip() {
        let formats = vec![
            FieldFormat::Slug,
            FieldFormat::IpAddress,
            FieldFormat::Cidr,
            FieldFormat::Prefix,
            FieldFormat::Mac,
            FieldFormat::Uuid,
        ];
        for fmt in formats {
            let json = serde_json::to_value(&fmt).unwrap();
            let back: FieldFormat = serde_json::from_value(json).unwrap();
            assert_eq!(back, fmt);
        }
    }

    #[test]
    fn field_schema_with_all_fields_set() {
        let json = serde_json::json!({
            "type": "string",
            "required": true,
            "nullable": true,
            "format": "slug",
            "pattern": "^[a-z]+$",
            "description": "a slug field"
        });
        let schema: FieldSchema = serde_json::from_value(json).unwrap();
        assert!(schema.required);
        assert!(schema.nullable);
        assert_eq!(schema.format, Some(FieldFormat::Slug));
        assert_eq!(schema.pattern.as_deref(), Some("^[a-z]+$"));
        assert_eq!(schema.description.as_deref(), Some("a slug field"));
    }

    #[test]
    fn field_schema_roundtrip() {
        let schema = FieldSchema {
            r#type: FieldType::Ref {
                target: "dcim.site".to_string(),
            },
            required: true,
            nullable: false,
            format: None,
            pattern: None,
            description: Some("site ref".to_string()),
        };
        let json = serde_json::to_value(&schema).unwrap();
        let back: FieldSchema = serde_json::from_value(json).unwrap();
        assert_eq!(back, schema);
    }

    #[test]
    fn field_schema_unknown_format_errors() {
        let json = serde_json::json!({
            "type": "string",
            "format": "nope"
        });
        let result: Result<FieldSchema, _> = serde_json::from_value(json);
        assert!(result.is_err());
    }

    #[test]
    fn field_schema_non_bool_flag_errors() {
        let required = serde_json::json!({ "type": "string", "required": "true" });
        assert!(serde_json::from_value::<FieldSchema>(required).is_err());
        let nullable = serde_json::json!({ "type": "string", "nullable": 1 });
        assert!(serde_json::from_value::<FieldSchema>(nullable).is_err());
    }

    #[test]
    fn field_schema_non_string_meta_errors() {
        let format = serde_json::json!({ "type": "string", "format": 1 });
        assert!(serde_json::from_value::<FieldSchema>(format).is_err());
        let pattern = serde_json::json!({ "type": "string", "pattern": true });
        assert!(serde_json::from_value::<FieldSchema>(pattern).is_err());
    }

    #[test]
    fn field_type_list_ref_missing_target_errors() {
        let json = serde_json::json!({ "type": "list_ref" });
        let result: Result<FieldSchema, _> = serde_json::from_value(json);
        assert!(result.is_err());
    }

    #[test]
    fn field_type_invalid_value_type_errors() {
        let result = parse_field_type_value(&serde_json::json!(42));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("string or map"));
    }

    #[test]
    fn field_type_object_simple_fallback() {
        let json = serde_json::json!({ "type": "int" });
        let schema: FieldSchema = serde_json::from_value(json).unwrap();
        assert_eq!(schema.r#type, FieldType::Int);
    }

    #[test]
    fn type_schema_roundtrip() {
        let json = serde_json::json!({
            "key": {
                "slug": { "type": "string" }
            },
            "fields": {
                "name": { "type": "string", "required": true },
                "status": { "type": "enum", "values": ["active", "planned"] }
            }
        });
        let schema: TypeSchema = serde_json::from_value(json.clone()).unwrap();
        assert!(schema.key.contains_key("slug"));
        assert!(schema.fields.contains_key("name"));
        assert!(schema.fields.contains_key("status"));
        let back = serde_json::to_value(&schema).unwrap();
        let back_schema: TypeSchema = serde_json::from_value(back).unwrap();
        assert_eq!(back_schema, schema);
    }

    #[test]
    fn inventory_roundtrip() {
        let json = serde_json::json!({
            "schema": {
                "types": {
                    "dcim.site": {
                        "key": { "slug": { "type": "string" } },
                        "fields": { "name": { "type": "string" } }
                    }
                }
            },
            "objects": [
                {
                    "uid": "00000000-0000-0000-0000-000000000001",
                    "type": "dcim.site",
                    "key": { "slug": "fra1" },
                    "attrs": { "name": "FRA1" }
                }
            ]
        });
        let inv: Inventory = serde_json::from_value(json).unwrap();
        assert_eq!(inv.schema.types.len(), 1);
        assert_eq!(inv.objects.len(), 1);
        assert_eq!(inv.objects[0].type_name.as_str(), "dcim.site");
        let back = serde_json::to_value(&inv).unwrap();
        let back_inv: Inventory = serde_json::from_value(back).unwrap();
        assert_eq!(back_inv, inv);
    }

    #[test]
    fn inventory_empty_objects_default() {
        let json = serde_json::json!({
            "schema": { "types": {} }
        });
        let inv: Inventory = serde_json::from_value(json).unwrap();
        assert!(inv.objects.is_empty());
    }

    #[test]
    fn key_string_empty() {
        let key = Key::default();
        let s = key_string(&key);
        assert_eq!(s, "{}");
    }

    #[test]
    fn key_string_canonical_form() {
        let mut k = BTreeMap::new();
        k.insert("a".to_string(), serde_json::json!(1.0000001)); // becomes int
        k.insert("b".to_string(), serde_json::json!(2.001)); // kept as float
        k.insert(
            "c".to_string(),
            serde_json::json!({"d".to_string(): 2.99999999999}), // becomes int
        );
        let key = Key::from(k);
        let s = key_string(&key);
        assert_eq!(s, "{\"a\":1,\"b\":2.001,\"c\":{\"d\":3}}");
    }
}
