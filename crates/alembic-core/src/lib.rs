//! core ir and validation primitives for alembic.

pub mod ir;
pub mod validation;

pub use ir::{
    Attrs, DeviceAttrs, InterfaceAttrs, Inventory, IpAddressAttrs, Kind, Object, PrefixAttrs,
    SiteAttrs, Uid,
};
pub use validation::{validate_inventory, ValidationError, ValidationReport};
