//! canonical ir types for alembic.

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;
use uuid::Uuid;

/// stable object identifier (uuid).
pub type Uid = Uuid;

/// canonical object kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Kind {
    #[serde(rename = "dcim.site")]
    DcimSite,
    #[serde(rename = "dcim.device")]
    DcimDevice,
    #[serde(rename = "dcim.interface")]
    DcimInterface,
    #[serde(rename = "ipam.prefix")]
    IpamPrefix,
    #[serde(rename = "ipam.ip_address")]
    IpamIpAddress,
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
        }
    }
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
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
}

impl Attrs {
    /// return the kind implied by this attrs variant.
    pub fn kind(&self) -> Kind {
        match self {
            Attrs::Site(_) => Kind::DcimSite,
            Attrs::Device(_) => Kind::DcimDevice,
            Attrs::Interface(_) => Kind::DcimInterface,
            Attrs::Prefix(_) => Kind::IpamPrefix,
            Attrs::IpAddress(_) => Kind::IpamIpAddress,
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
        let kind = attrs.kind();
        Self {
            uid,
            kind,
            key,
            attrs,
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
            Kind::DcimSite => {
                let parsed: SiteAttrs = serde_json::from_value(raw.attrs)
                    .map_err(|e| de::Error::custom(format!("site attrs: {e}")))?;
                Attrs::Site(parsed)
            }
            Kind::DcimDevice => {
                let parsed: DeviceAttrs = serde_json::from_value(raw.attrs)
                    .map_err(|e| de::Error::custom(format!("device attrs: {e}")))?;
                Attrs::Device(parsed)
            }
            Kind::DcimInterface => {
                let parsed: InterfaceAttrs = serde_json::from_value(raw.attrs)
                    .map_err(|e| de::Error::custom(format!("interface attrs: {e}")))?;
                Attrs::Interface(parsed)
            }
            Kind::IpamPrefix => {
                let parsed: PrefixAttrs = serde_json::from_value(raw.attrs)
                    .map_err(|e| de::Error::custom(format!("prefix attrs: {e}")))?;
                Attrs::Prefix(parsed)
            }
            Kind::IpamIpAddress => {
                let parsed: IpAddressAttrs = serde_json::from_value(raw.attrs)
                    .map_err(|e| de::Error::custom(format!("ip_address attrs: {e}")))?;
                Attrs::IpAddress(parsed)
            }
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
