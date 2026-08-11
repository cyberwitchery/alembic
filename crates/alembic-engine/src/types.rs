//! core engine types and adapter contract.

use alembic_core::{key_string, JsonMap, Key, Object, Schema, TypeName, Uid};
use anyhow::{anyhow, Result};
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash, Eq)]
pub struct FieldChange {
    /// field name within attrs.
    pub field: String,
    /// previous value from observed state.
    pub from: serde_json::Value,
    /// desired value from the ir.
    pub to: serde_json::Value,
}

/// plan operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash, Eq)]
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

    pub fn hashed(&self) -> u64 {
        stable_json_hash(self)
    }
}

/// hash a value's json serialization via the same v5 uuid mechanism ir
/// identity is built on. journal identity (file names, per-op hashes) is
/// persisted to disk and compared across runs, so it must not depend on
/// `DefaultHasher`, whose algorithm is not stable across rust releases.
pub(crate) fn stable_json_hash<T: Serialize>(value: &T) -> u64 {
    // serializing engine types cannot fail: plain structs and enums whose only
    // maps are string-keyed.
    let bytes = serde_json::to_vec(value).expect("engine value serializes to json");
    uuid::Uuid::new_v5(&alembic_core::ALEMBIC_UID_NAMESPACE, &bytes)
        .as_u64_pair()
        .0
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
    /// read-only preview of the schema provisioning apply would perform, populated at
    /// plan time. `None` when the backend cannot preview schema (or was not asked).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_preview: Option<ProvisionReport>,
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
    /// disallows duplicate backend ids.
    pub fn insert(&mut self, object: ObservedObject) -> Result<()> {
        if let Some(id) = &object.backend_id {
            let key = (object.type_name.clone(), id.clone());
            if self.by_backend_id.contains_key(&key) {
                return Err(anyhow!(
                    "ObservedState already contains an object with backend id {} for type {}",
                    id,
                    object.type_name
                ));
            }
            self.by_backend_id.insert(key, object.clone());
        }

        let key = (object.type_name.clone(), key_string(&object.key));
        if self.by_key.contains_key(&key) {
            return Err(anyhow!(
                "ObservedState already contains an object with natural key {:?}",
                key
            ));
        }
        self.by_key.insert(key, object);

        Ok(())
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
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ApplyReport {
    /// list of operations applied by the adapter.
    pub applied: Vec<AppliedOp>,
    /// operations an interrupted run applied, recovered from its journal on resume,
    /// with the backend id each one returned.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resumed: Vec<AppliedOp>,
    /// number of previously applied operations, only set when apply is accompanied by a journal
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previously_applied_count: Option<usize>,
    /// provisioning report, merged from both passes: `ensure_schema` and `write`.
    #[serde(default)]
    pub provision: ProvisionReport,
}

/// report of what an apply provisioned, across `ensure_schema` and `write`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProvisionReport {
    /// custom fields created on the backend.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub created_fields: Vec<String>,
    /// custom fields the backend already had, converged onto their declared
    /// properties.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub updated_fields: Vec<String>,
    /// tags created on the backend.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub created_tags: Vec<String>,
    /// custom object types created on the backend.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub created_object_types: Vec<String>,
    /// custom object fields created on the backend.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub created_object_fields: Vec<String>,
    /// custom object fields the backend already had, converged onto their declared
    /// properties. its own category because creates and deletes are split
    /// native/object here too.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub updated_object_fields: Vec<String>,
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

/// how `named_changes` words a change: what a run did, or what a preview says
/// it would do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tense {
    Past,
    Would,
}

impl ProvisionReport {
    /// fold another report in. one apply provisions in two passes -- `ensure_schema`
    /// fills the schema categories, `write` fills the tags it creates from the ops --
    /// so both have to reach the same report.
    pub fn merge(&mut self, other: ProvisionReport) {
        // every reader of these categories destructures without `..`: a category
        // added later has to answer in each rather than be dropped in silence.
        let ProvisionReport {
            created_fields,
            updated_fields,
            created_tags,
            created_object_types,
            created_object_fields,
            updated_object_fields,
            deprecated_object_types,
            deprecated_object_fields,
            deleted_object_types,
            deleted_object_fields,
        } = other;
        self.created_fields.extend(created_fields);
        self.updated_fields.extend(updated_fields);
        self.created_tags.extend(created_tags);
        self.created_object_types.extend(created_object_types);
        self.created_object_fields.extend(created_object_fields);
        self.updated_object_fields.extend(updated_object_fields);
        self.deprecated_object_types.extend(deprecated_object_types);
        self.deprecated_object_fields
            .extend(deprecated_object_fields);
        self.deleted_object_types.extend(deleted_object_types);
        self.deleted_object_fields.extend(deleted_object_fields);
    }

    /// the changes a run made to schema it did not create, labelled for the
    /// operator. a create is new, so its count is the whole story; everything
    /// else reaches into state that was already there, so name what it wrote.
    pub fn named_changes(&self, tense: Tense) -> Vec<(&'static str, &str)> {
        // destructured without `..`, like the folds above: a category added later
        // has to answer whether it names what it touched.
        let ProvisionReport {
            created_fields: _,
            updated_fields,
            created_tags: _,
            created_object_types: _,
            created_object_fields: _,
            updated_object_fields,
            deprecated_object_types,
            deprecated_object_fields,
            deleted_object_types,
            deleted_object_fields,
        } = self;
        let (updated, deprecated, deleted) = match tense {
            Tense::Past => ("updated", "deprecated", "deleted"),
            Tense::Would => ("would update", "would deprecate", "would delete"),
        };
        [
            (updated, updated_fields),
            (updated, updated_object_fields),
            (deprecated, deprecated_object_types),
            (deprecated, deprecated_object_fields),
            (deleted, deleted_object_types),
            (deleted, deleted_object_fields),
        ]
        .into_iter()
        .flat_map(|(label, names)| names.iter().map(move |name| (label, name.as_str())))
        .collect()
    }

    pub fn is_empty(&self) -> bool {
        let ProvisionReport {
            created_fields,
            updated_fields,
            created_tags,
            created_object_types,
            created_object_fields,
            updated_object_fields,
            deprecated_object_types,
            deprecated_object_fields,
            deleted_object_types,
            deleted_object_fields,
        } = self;
        created_fields.is_empty()
            && updated_fields.is_empty()
            && created_tags.is_empty()
            && created_object_types.is_empty()
            && created_object_fields.is_empty()
            && updated_object_fields.is_empty()
            && deprecated_object_types.is_empty()
            && deprecated_object_fields.is_empty()
            && deleted_object_types.is_empty()
            && deleted_object_fields.is_empty()
    }
}

impl fmt::Display for ProvisionReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            return write!(f, "no schema changes");
        }

        let ProvisionReport {
            created_fields,
            updated_fields,
            created_tags,
            created_object_types,
            created_object_fields,
            updated_object_fields,
            deprecated_object_types,
            deprecated_object_fields,
            deleted_object_types,
            deleted_object_fields,
        } = self;

        let mut first = true;
        let sections: &[(&str, &[String])] = &[
            ("fields created", created_fields),
            ("fields updated", updated_fields),
            ("tags created", created_tags),
            ("object types created", created_object_types),
            ("object fields created", created_object_fields),
            ("object fields updated", updated_object_fields),
            ("object types deprecated", deprecated_object_types),
            ("object fields deprecated", deprecated_object_fields),
            ("object types deleted", deleted_object_types),
            ("object fields deleted", deleted_object_fields),
        ];

        for (label, items) in sections {
            if items.is_empty() {
                continue;
            }
            if !first {
                write!(f, ", ")?;
            }
            write!(f, "{} {label}", items.len())?;
            first = false;
        }

        Ok(())
    }
}

/// read capability: observe backend state.
#[async_trait]
pub trait Observer: Send + Sync {
    async fn read(
        &self,
        schema: &Schema,
        types: &[TypeName],
        state: &crate::state::StateStore,
    ) -> anyhow::Result<ObservedState>;
}

/// write capability: apply a plan's operations, and provision the schema they
/// need. provisioning is itself a write, so a write-only backend gets it too;
/// one that provisions nothing keeps the defaults below.
#[async_trait]
pub trait Emitter: Send + Sync {
    async fn write(
        &self,
        schema: &Schema,
        ops: &[Op],
        state: &crate::state::StateStore,
    ) -> anyhow::Result<ApplyReport>;

    async fn ensure_schema(&self, _schema: &Schema) -> anyhow::Result<ProvisionReport> {
        Ok(ProvisionReport::default())
    }

    /// read-only counterpart to [`Emitter::ensure_schema`]: report what provisioning
    /// apply would perform, writing nothing. `None` means this adapter cannot preview
    /// schema (reported honestly, never as "no schema changes"); `Some(report)` is the
    /// provisioning it would carry out, `Some(empty)` that there is nothing to provision.
    async fn preview_schema(&self, _schema: &Schema) -> anyhow::Result<Option<ProvisionReport>> {
        Ok(None)
    }
}

/// read+write capability tag, carrying no methods of its own: it marks a backend
/// that both observes and emits, so [`Backend::Adapter`] can box one value as both.
pub trait Adapter: Observer + Emitter {}

/// a constructed backend, tagged with its capability.
pub enum Backend {
    /// read-only backend (e.g. peeringdb).
    Observer(Box<dyn Observer>),
    /// write-only backend (e.g. django codegen).
    Emitter(Box<dyn Emitter>),
    /// read+write backend.
    Adapter(Box<dyn Adapter>),
}

/// every refusal of an observation over an emitter opens with these words, so
/// rewording one rewords all of them.
pub(crate) const CANNOT_OBSERVE: &str = "backend is write-only; it cannot observe state";

impl Backend {
    pub fn observer(&self) -> anyhow::Result<&dyn Observer> {
        match self {
            Backend::Observer(observer) => Ok(observer.as_ref()),
            Backend::Adapter(adapter) => Ok(adapter.as_ref()),
            Backend::Emitter(_) => Err(anyhow::anyhow!(CANNOT_OBSERVE)),
        }
    }

    pub fn emitter(&self) -> anyhow::Result<&dyn Emitter> {
        match self {
            Backend::Emitter(emitter) => Ok(emitter.as_ref()),
            Backend::Adapter(adapter) => Ok(adapter.as_ref()),
            Backend::Observer(_) => Err(anyhow::anyhow!(
                "backend is read-only; it cannot apply changes"
            )),
        }
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
    fn provision_report_defaults_omitted_lists() {
        // a non-Rust ensure_schema adapter that provisioned an object type but no
        // custom fields/tags naturally omits the empty lists; that must deserialize.
        let report: ProvisionReport =
            serde_json::from_value(serde_json::json!({"created_object_types": ["dcim.site"]}))
                .unwrap();
        assert!(report.created_fields.is_empty());
        assert!(report.created_tags.is_empty());
        assert_eq!(report.created_object_types, ["dcim.site"]);

        // the whole report deserializes from an empty object.
        assert!(serde_json::from_str::<ProvisionReport>("{}")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn named_changes_names_every_write_to_pre_existing_schema() {
        // one entry per category, so a category dropped from the classification
        // shows up as a missing pair rather than passing on a count.
        let report = ProvisionReport {
            created_fields: vec!["site.tier".to_string()],
            updated_fields: vec!["site.owner".to_string()],
            created_tags: vec!["managed".to_string()],
            created_object_types: vec!["dcim.widget".to_string()],
            created_object_fields: vec!["dcim.widget.size".to_string()],
            updated_object_fields: vec!["dcim.widget.color".to_string()],
            deprecated_object_types: vec!["dcim.gadget".to_string()],
            deprecated_object_fields: vec!["dcim.gadget.color".to_string()],
            deleted_object_types: vec!["dcim.relic".to_string()],
            deleted_object_fields: vec!["dcim.relic.age".to_string()],
        };

        // the four create categories are counted by Display and named nowhere.
        assert_eq!(
            report.named_changes(Tense::Past),
            [
                ("updated", "site.owner"),
                ("updated", "dcim.widget.color"),
                ("deprecated", "dcim.gadget"),
                ("deprecated", "dcim.gadget.color"),
                ("deleted", "dcim.relic"),
                ("deleted", "dcim.relic.age"),
            ]
        );
        assert_eq!(
            report.named_changes(Tense::Would),
            [
                ("would update", "site.owner"),
                ("would update", "dcim.widget.color"),
                ("would deprecate", "dcim.gadget"),
                ("would deprecate", "dcim.gadget.color"),
                ("would delete", "dcim.relic"),
                ("would delete", "dcim.relic.age"),
            ]
        );

        let creates_only = ProvisionReport {
            created_object_types: vec!["dcim.widget".to_string()],
            ..Default::default()
        };
        assert!(creates_only.named_changes(Tense::Past).is_empty());
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
