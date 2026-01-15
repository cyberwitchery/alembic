//! canonical ir types for alembic.

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

/// stable object identifier (uuid).
pub type Uid = Uuid;

pub const ALEMBIC_UID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x45, 0x93, 0x1a, 0x5f, 0x6c, 0x2b, 0x49, 0x6a, 0x9b, 0x6f, 0x8f, 0x77, 0x7d, 0x4f, 0x3a, 0x1c,
]);

pub fn uid_v5(kind: &str, stable: &str) -> Uid {
    let name = format!("{kind}:{stable}");
    Uuid::new_v5(&ALEMBIC_UID_NAMESPACE, name.as_bytes())
}

/// canonical object kind.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Kind {
    DcimSite,
    DcimDevice,
    DcimInterface,
    IpamPrefix,
    IpamIpAddress,
    Custom(String),
}

impl Kind {
    /// return the canonical string form used in serialization.
    pub fn as_str(&self) -> &'static str {
        match self {
            Kind::DcimSite => "dcim.site",
            Kind::DcimDevice => "dcim.device",
            Kind::DcimInterface => "dcim.interface",
            Kind::IpamPrefix => "ipam.prefix",
            Kind::IpamIpAddress => "ipam.ip_address",
            Kind::Custom(_) => "custom",
        }
    }

    /// return the serialized string for this kind.
    pub fn as_string(&self) -> String {
        match self {
            Kind::Custom(value) => value.clone(),
            _ => self.as_str().to_string(),
        }
    }

    /// return true when this is a custom kind.
    pub fn is_custom(&self) -> bool {
        matches!(self, Kind::Custom(_))
    }

    /// parse a kind string into a canonical kind.
    pub fn parse(value: &str) -> Self {
        Kind::from_str(value).unwrap_or_else(|_| Kind::Custom(value.to_string()))
    }

    /// return true when kind string is empty.
    pub fn is_empty(&self) -> bool {
        matches!(self, Kind::Custom(value) if value.trim().is_empty())
    }
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.as_string())
    }
}

impl Serialize for Kind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.as_string())
    }
}

impl<'de> Deserialize<'de> for Kind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Ok(Kind::parse(&raw))
    }
}

impl FromStr for Kind {
    type Err = std::convert::Infallible;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(match value {
            "dcim.site" => Kind::DcimSite,
            "dcim.device" => Kind::DcimDevice,
            "dcim.interface" => Kind::DcimInterface,
            "ipam.prefix" => Kind::IpamPrefix,
            "ipam.ip_address" => Kind::IpamIpAddress,
            _ => Kind::Custom(value.to_string()),
        })
    }
}

impl Ord for Kind {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_string().cmp(&other.as_string())
    }
}

impl PartialOrd for Kind {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// attributes for `dcim.site`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SiteAttrs {
    /// site name.
    pub name: String,
    /// site slug (netbox identifier).
    pub slug: String,
    /// optional status string.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    /// optional description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// attributes for `dcim.device`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeviceAttrs {
    /// device name.
    pub name: String,
    /// uid of the site that owns this device.
    pub site: Uid,
    /// device role name.
    pub role: String,
    /// device type model name.
    pub device_type: String,
    /// optional status string.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

/// attributes for `dcim.interface`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InterfaceAttrs {
    /// interface name.
    pub name: String,
    /// uid of the parent device.
    pub device: Uid,
    /// optional interface type string.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub if_type: Option<String>,
    /// optional enabled flag.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// optional description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// attributes for `ipam.prefix`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrefixAttrs {
    /// cidr prefix string.
    pub prefix: String,
    /// optional owning site uid.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub site: Option<Uid>,
    /// optional description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// attributes for `ipam.ip_address`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IpAddressAttrs {
    /// cidr address string.
    pub address: String,
    /// optional assigned interface uid.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assigned_interface: Option<Uid>,
    /// optional description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// typed attributes for any object kind.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Attrs {
    Site(SiteAttrs),
    Device(DeviceAttrs),
    Interface(InterfaceAttrs),
    Prefix(PrefixAttrs),
    IpAddress(IpAddressAttrs),
    Generic(BTreeMap<String, Value>),
}

impl Attrs {
    /// return the kind implied by this attrs variant.
    pub fn kind(&self) -> Option<Kind> {
        match self {
            Attrs::Site(_) => Some(Kind::DcimSite),
            Attrs::Device(_) => Some(Kind::DcimDevice),
            Attrs::Interface(_) => Some(Kind::DcimInterface),
            Attrs::Prefix(_) => Some(Kind::IpamPrefix),
            Attrs::IpAddress(_) => Some(Kind::IpamIpAddress),
            Attrs::Generic(_) => None,
        }
    }
}

/// object envelope for the ir.
#[derive(Debug, Clone, PartialEq)]
pub struct Object {
    /// stable identifier for the object.
    pub uid: Uid,
    /// canonical kind for the object.
    pub kind: Kind,
    /// human key used for matching when state is missing.
    pub key: String,
    /// typed attributes for this kind.
    pub attrs: Attrs,
    /// namespaced extension fields.
    pub x: BTreeMap<String, Value>,
}

impl Object {
    /// create an object and infer its kind from attrs.
    pub fn new(uid: Uid, key: String, attrs: Attrs) -> Self {
        let kind = attrs
            .kind()
            .unwrap_or_else(|| panic!("generic attrs require explicit kind"));
        Self {
            uid,
            kind,
            key,
            attrs,
            x: BTreeMap::new(),
        }
    }

    /// create a generic object with an explicit kind.
    pub fn new_generic(uid: Uid, kind: Kind, key: String, attrs: BTreeMap<String, Value>) -> Self {
        Self {
            uid,
            kind,
            key,
            attrs: Attrs::Generic(attrs),
            x: BTreeMap::new(),
        }
    }
}

/// top-level inventory of objects.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Inventory {
    /// list of objects in this inventory.
    #[serde(default)]
    pub objects: Vec<Object>,
}

impl Serialize for Object {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;

        let mut state = serializer.serialize_struct("Object", 5)?;
        state.serialize_field("uid", &self.uid)?;
        state.serialize_field("kind", &self.kind)?;
        state.serialize_field("key", &self.key)?;
        match (&self.kind, &self.attrs) {
            (Kind::DcimSite, Attrs::Site(attrs)) => state.serialize_field("attrs", attrs)?,
            (Kind::DcimDevice, Attrs::Device(attrs)) => state.serialize_field("attrs", attrs)?,
            (Kind::DcimInterface, Attrs::Interface(attrs)) => {
                state.serialize_field("attrs", attrs)?
            }
            (Kind::IpamPrefix, Attrs::Prefix(attrs)) => state.serialize_field("attrs", attrs)?,
            (Kind::IpamIpAddress, Attrs::IpAddress(attrs)) => {
                state.serialize_field("attrs", attrs)?
            }
            (_, Attrs::Generic(attrs)) => state.serialize_field("attrs", attrs)?,
            _ => {
                return Err(serde::ser::Error::custom(
                    "object kind does not match attrs",
                ))
            }
        }
        if !self.x.is_empty() {
            state.serialize_field("x", &self.x)?;
        }
        state.end()
    }
}

impl<'de> Deserialize<'de> for Object {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawObject {
            uid: Uid,
            kind: Kind,
            key: String,
            attrs: Value,
            #[serde(default)]
            x: BTreeMap<String, Value>,
        }

        let raw = RawObject::deserialize(deserializer)?;
        let attrs = match raw.kind {
            Kind::DcimSite => match serde_json::from_value::<SiteAttrs>(raw.attrs.clone()) {
                Ok(parsed) => Attrs::Site(parsed),
                Err(_) => Attrs::Generic(to_object_map(raw.attrs)?),
            },
            Kind::DcimDevice => match serde_json::from_value::<DeviceAttrs>(raw.attrs.clone()) {
                Ok(parsed) => Attrs::Device(parsed),
                Err(_) => Attrs::Generic(to_object_map(raw.attrs)?),
            },
            Kind::DcimInterface => {
                match serde_json::from_value::<InterfaceAttrs>(raw.attrs.clone()) {
                    Ok(parsed) => Attrs::Interface(parsed),
                    Err(_) => Attrs::Generic(to_object_map(raw.attrs)?),
                }
            }
            Kind::IpamPrefix => match serde_json::from_value::<PrefixAttrs>(raw.attrs.clone()) {
                Ok(parsed) => Attrs::Prefix(parsed),
                Err(_) => Attrs::Generic(to_object_map(raw.attrs)?),
            },
            Kind::IpamIpAddress => {
                match serde_json::from_value::<IpAddressAttrs>(raw.attrs.clone()) {
                    Ok(parsed) => Attrs::IpAddress(parsed),
                    Err(_) => Attrs::Generic(to_object_map(raw.attrs)?),
                }
            }
            Kind::Custom(_) => Attrs::Generic(to_object_map(raw.attrs)?),
        };

        Ok(Object {
            uid: raw.uid,
            kind: raw.kind,
            key: raw.key,
            attrs,
            x: raw.x,
        })
    }
}

fn to_object_map<E>(value: Value) -> Result<BTreeMap<String, Value>, E>
where
    E: de::Error,
{
    match value {
        Value::Object(map) => Ok(map.into_iter().collect()),
        _ => Err(de::Error::custom("attrs must be an object")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_display_matches_str() {
        assert_eq!(Kind::DcimSite.to_string(), "dcim.site");
        assert_eq!(Kind::IpamIpAddress.to_string(), "ipam.ip_address");
    }

    #[test]
    fn object_roundtrip_json() {
        let object = Object::new(
            Uuid::from_u128(1),
            "site=fra1".to_string(),
            Attrs::Site(SiteAttrs {
                name: "FRA1".to_string(),
                slug: "fra1".to_string(),
                status: Some("active".to_string()),
                description: Some("test".to_string()),
            }),
        );

        let value = serde_json::to_value(&object).unwrap();
        let decoded: Object = serde_json::from_value(value).unwrap();
        assert_eq!(decoded.uid, object.uid);
        assert_eq!(decoded.kind, object.kind);
        assert_eq!(decoded.key, object.key);
        assert_eq!(decoded.attrs, object.attrs);
    }

    #[test]
    fn object_roundtrip_generic() {
        let mut attrs = BTreeMap::new();
        attrs.insert("peers".to_string(), serde_json::json!([{"name": "site1"}]));
        attrs.insert("pre_shared_key".to_string(), serde_json::json!("secret"));

        let object = Object::new_generic(
            Uuid::from_u128(10),
            Kind::Custom("services.vpn".to_string()),
            "vpn=corp".to_string(),
            attrs,
        );

        let value = serde_json::to_value(&object).unwrap();
        let decoded: Object = serde_json::from_value(value).unwrap();
        assert_eq!(decoded.uid, object.uid);
        assert_eq!(decoded.kind.to_string(), "services.vpn");
        assert_eq!(decoded.key, object.key);
        assert_eq!(decoded.attrs, object.attrs);
    }
    #[test]
    fn object_serialize_rejects_mismatched_kind() {
        let object = Object {
            uid: Uuid::from_u128(2),
            kind: Kind::DcimSite,
            key: "site=fra1".to_string(),
            attrs: Attrs::Device(DeviceAttrs {
                name: "leaf01".to_string(),
                site: Uuid::from_u128(3),
                role: "leaf".to_string(),
                device_type: "leaf-switch".to_string(),
                status: None,
            }),
            x: BTreeMap::new(),
        };

        let result = serde_json::to_value(&object);
        assert!(result.is_err());
    }

    #[test]
    fn attrs_kind_matches_variant() {
        let site = Attrs::Site(SiteAttrs {
            name: "FRA1".to_string(),
            slug: "fra1".to_string(),
            status: None,
            description: None,
        });
        let device = Attrs::Device(DeviceAttrs {
            name: "leaf01".to_string(),
            site: Uuid::from_u128(4),
            role: "leaf".to_string(),
            device_type: "leaf-switch".to_string(),
            status: None,
        });
        let iface = Attrs::Interface(InterfaceAttrs {
            name: "eth0".to_string(),
            device: Uuid::from_u128(5),
            if_type: None,
            enabled: None,
            description: None,
        });
        let prefix = Attrs::Prefix(PrefixAttrs {
            prefix: "10.0.0.0/24".to_string(),
            site: None,
            description: None,
        });
        let ip = Attrs::IpAddress(IpAddressAttrs {
            address: "10.0.0.10/24".to_string(),
            assigned_interface: None,
            description: None,
        });

        assert_eq!(site.kind(), Some(Kind::DcimSite));
        assert_eq!(device.kind(), Some(Kind::DcimDevice));
        assert_eq!(iface.kind(), Some(Kind::DcimInterface));
        assert_eq!(prefix.kind(), Some(Kind::IpamPrefix));
        assert_eq!(ip.kind(), Some(Kind::IpamIpAddress));
    }

    #[test]
    fn inventory_defaults_to_empty() {
        let inventory = Inventory { objects: vec![] };
        let value = serde_json::to_value(&inventory).unwrap();
        let decoded: Inventory = serde_json::from_value(value).unwrap();
        assert!(decoded.objects.is_empty());
    }

    #[test]
    fn object_with_extensions_roundtrip() {
        let mut object = Object::new(
            Uuid::from_u128(6),
            "site=fra1".to_string(),
            Attrs::Site(SiteAttrs {
                name: "FRA1".to_string(),
                slug: "fra1".to_string(),
                status: None,
                description: None,
            }),
        );
        object
            .x
            .insert("example.note".to_string(), Value::String("ok".to_string()));

        let value = serde_json::to_value(&object).unwrap();
        let decoded: Object = serde_json::from_value(value).unwrap();
        assert_eq!(decoded.x.get("example.note").unwrap(), "ok");
    }
}
