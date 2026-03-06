//! core engine types and adapter contract.

use alembic_core::{key_string, JsonMap, Key, Object, Schema, TypeName, Uid};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

/// generic backend identifier (integer or string/uuid).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BackendId {
    Int(u64),
    String(String),
}

impl fmt::Display for BackendId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BackendId::Int(id) => write!(f, "{}", id),
            BackendId::String(id) => write!(f, "{}", id),
        }
    }
}

impl From<u64> for BackendId {
    fn from(id: u64) -> Self {
        BackendId::Int(id)
    }
}

impl From<String> for BackendId {
    fn from(id: String) -> Self {
        BackendId::String(id)
    }
}

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
        desired: Object,
    },
    /// update an existing backend object.
    Update {
        uid: Uid,
        type_name: TypeName,
        desired: Object,
        changes: Vec<FieldChange>,
        #[serde(skip_serializing_if = "Option::is_none")]
        backend_id: Option<BackendId>,
    },
    /// delete a backend object.
    Delete {
        uid: Uid,
        type_name: TypeName,
        key: Key,
        #[serde(skip_serializing_if = "Option::is_none")]
        backend_id: Option<BackendId>,
    },
}

impl Op {
    /// returns the ir uid for this operation.
    pub fn uid(&self) -> Uid {
        match self {
            Op::Create { uid, .. } => *uid,
            Op::Update { uid, .. } => *uid,
            Op::Delete { uid, .. } => *uid,
        }
    }

    /// returns the type name for this operation.
    pub fn type_name(&self) -> &TypeName {
        match self {
            Op::Create { type_name, .. } => type_name,
            Op::Update { type_name, .. } => type_name,
            Op::Delete { type_name, .. } => type_name,
        }
    }
}

/// full plan document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    /// schema definitions required for apply.
    pub schema: Schema,
    /// ordered list of operations.
    pub ops: Vec<Op>,
    /// high-level summary of the plan.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<PlanSummary>,
}

/// high-level summary of plan operations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PlanSummary {
    /// number of objects to create.
    pub create: usize,
    /// number of objects to update.
    pub update: usize,
    /// number of objects to delete.
    pub delete: usize,
}

impl Plan {
    /// build a summary for the current plan.
    pub fn summary(&self) -> PlanSummary {
        let mut summary = PlanSummary::default();
        for op in &self.ops {
            match op {
                Op::Create { .. } => summary.create += 1,
                Op::Update { .. } => summary.update += 1,
                Op::Delete { .. } => summary.delete += 1,
            }
        }
        summary
    }
}

/// observed backend object representation.
#[derive(Debug, Clone)]
pub struct ObservedObject {
    /// object type.
    pub type_name: TypeName,
    /// human key for matching.
    pub key: Key,
    /// observed attrs mapped to ir types.
    pub attrs: JsonMap,
    /// backend id when known.
    pub backend_id: Option<BackendId>,
}

/// observed backend state indexed by id and key.
#[derive(Debug, Default, Clone)]
pub struct ObservedState {
    /// observed objects keyed by backend id.
    pub by_backend_id: BTreeMap<(TypeName, BackendId), ObservedObject>,
    /// observed objects keyed by natural key.
    pub by_key: BTreeMap<(TypeName, String), ObservedObject>,
}

impl ObservedState {
    /// insert an observed object into both indexes.
    pub fn insert(&mut self, object: ObservedObject) {
        if let Some(id) = &object.backend_id {
            self.by_backend_id
                .insert((object.type_name.clone(), id.clone()), object.clone());
        }
        self.by_key
            .insert((object.type_name.clone(), key_string(&object.key)), object);
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
    pub backend_id: Option<BackendId>,
}

/// aggregated apply report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyReport {
    /// list of operations applied by the adapter.
    pub applied: Vec<AppliedOp>,
}

/// report from ensure_schema provisioning.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProvisionReport {
    /// custom fields created on the backend.
    pub created_fields: Vec<String>,
    /// tags created on the backend.
    pub created_tags: Vec<String>,
    /// custom object types created on the backend.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub created_object_types: Vec<String>,
    /// custom object fields created on the backend.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub created_object_fields: Vec<String>,
    /// object types deprecated on the backend.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deprecated_object_types: Vec<String>,
    /// object fields deprecated on the backend.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deprecated_object_fields: Vec<String>,
    /// object types deleted on the backend.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deleted_object_types: Vec<String>,
    /// object fields deleted on the backend.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deleted_object_fields: Vec<String>,
}

/// adapter contract for backend-specific io.
#[async_trait]
pub trait Adapter: Send + Sync {
    async fn read(
        &self,
        schema: &Schema,
        types: &[TypeName],
        state: &crate::state::StateStore,
    ) -> anyhow::Result<ObservedState>;
    async fn write(
        &self,
        schema: &Schema,
        ops: &[Op],
        state: &crate::state::StateStore,
    ) -> anyhow::Result<ApplyReport>;
    async fn ensure_schema(&self, _schema: &Schema) -> anyhow::Result<ProvisionReport> {
        Ok(ProvisionReport::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alembic_core::{Key, TypeName, Uid};

    #[test]
    fn backend_id_serialization() {
        let int_id = BackendId::Int(123);
        let json = serde_json::to_string(&int_id).unwrap();
        assert_eq!(json, "123");
        let back: BackendId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, int_id);

        let str_id = BackendId::String("uuid".to_string());
        let json = serde_json::to_string(&str_id).unwrap();
        assert_eq!(json, "\"uuid\"");
        let back: BackendId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, str_id);
    }

    #[test]
    fn op_helpers() {
        let uid = Uid::from_u128(1);
        let type_name = TypeName::new("test.type");
        let op = Op::Delete {
            uid,
            type_name: type_name.clone(),
            key: Key::default(),
            backend_id: None,
        };
        assert_eq!(op.uid(), uid);
        assert_eq!(op.type_name(), &type_name);
    }
}
