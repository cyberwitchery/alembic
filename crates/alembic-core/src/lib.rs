//! core ir and validation primitives for alembic.

pub mod ir;
pub mod validation;

pub use ir::{
    uid_v5, Attrs, DeviceAttrs, InterfaceAttrs, Inventory, IpAddressAttrs, JsonMap, Kind, Object,
    PrefixAttrs, SiteAttrs, Uid, ALEMBIC_UID_NAMESPACE,
};
pub use validation::{validate_inventory, ValidationError, ValidationReport};
