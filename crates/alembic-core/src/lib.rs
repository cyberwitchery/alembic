//! core ir and validation primitives for alembic.

pub mod ir;
pub mod validation;

pub use ir::{
    uid_v5, FieldSchema, FieldType, Inventory, JsonMap, Object, ObjectError, Schema, TypeName,
    TypeSchema, Uid, ALEMBIC_UID_NAMESPACE,
};
pub use validation::{validate_inventory, ValidationError, ValidationReport};
