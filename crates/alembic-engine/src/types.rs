//! core engine types and adapter contract.

use crate::projection::{BackendCapabilities, ProjectedObject, ProjectionData};
use crate::state::StateStore;
use alembic_core::{JsonMap, TypeName, Uid};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// field-level change for an update op.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldChange {
    /// field name within attrs.
    pub field: String,
    /// previous value from observed state.
    pub from: serde_json::Value,
    /// desired value from the ir.
    pub to: serde_json::Value,
}

/// plan operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Op {
    /// create a new backend object.
    Create {
        uid: Uid,
        type_name: TypeName,
        desired: ProjectedObject,
    },
    /// update an existing backend object.
    Update {
        uid: Uid,
        type_name: TypeName,
        desired: ProjectedObject,
        changes: Vec<FieldChange>,
        #[serde(skip_serializing_if = "Option::is_none")]
        backend_id: Option<u64>,
    },
    /// delete a backend object.
    Delete {
        uid: Uid,
        type_name: TypeName,
        key: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        backend_id: Option<u64>,
    },
}

/// full plan document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Plan {
    /// ordered list of operations.
    pub ops: Vec<Op>,
}

/// observed backend object representation.
#[derive(Debug, Clone)]
pub struct ObservedObject {
    /// object type.
    pub type_name: TypeName,
    /// human key for matching.
    pub key: String,
    /// observed attrs mapped to ir types.
    pub attrs: JsonMap,
    /// observed projection data.
    pub projection: ProjectionData,
    /// backend id when known.
    pub backend_id: Option<u64>,
}

/// observed backend state indexed by id and key.
#[derive(Debug, Default, Clone)]
pub struct ObservedState {
    /// observed objects keyed by backend id.
    pub by_backend_id: BTreeMap<(TypeName, u64), ObservedObject>,
    /// observed objects keyed by natural key.
    pub by_key: BTreeMap<String, ObservedObject>,
    /// backend capabilities (custom fields, tags).
    pub capabilities: BackendCapabilities,
}

impl ObservedState {
    /// insert an observed object into both indexes.
    pub fn insert(&mut self, object: ObservedObject) {
        if let Some(id) = object.backend_id {
            self.by_backend_id
                .insert((object.type_name.clone(), id), object.clone());
        }
        self.by_key.insert(object.key.clone(), object);
    }
}

/// result for a single applied operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppliedOp {
    /// ir uid for the operation.
    pub uid: Uid,
    /// type for the operation.
    pub type_name: TypeName,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// backend id returned by the adapter, if any.
    pub backend_id: Option<u64>,
}

/// aggregated apply report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyReport {
    /// list of operations applied by the adapter.
    pub applied: Vec<AppliedOp>,
}

/// adapter contract for backend-specific io.
#[async_trait]
pub trait Adapter: Send + Sync {
    async fn observe(&self, types: &[TypeName]) -> anyhow::Result<ObservedState>;
    async fn apply(&self, ops: &[Op]) -> anyhow::Result<ApplyReport>;
    fn update_state(&self, _state: &StateStore) {}
}
