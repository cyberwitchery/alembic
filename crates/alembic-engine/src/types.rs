//! core engine types and adapter contract.

use alembic_adapter_sdk::{ApplyReport, BackendId, Op, ProvisionReport};
use alembic_core::{key_string, JsonMap, Key, Schema, TypeName, Uid};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// full plan document.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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

/// the raw observation: everything an adapter's read returned. objects are
/// held once and indexed uniquely by backend id (a read returning one id twice
/// is broken) and non-uniquely by natural key. key ambiguity is data here --
/// real backends hold legitimate same-key objects (netbox ships with duplicate
/// ips allowed) -- and only dereferencing an ambiguous key fails, at the site
/// that needs it: adoption, key matching, or import.
#[derive(Debug, Default, Clone)]
pub struct ObservedState {
    objects: Vec<ObservedObject>,
    by_backend_id: BTreeMap<(TypeName, BackendId), usize>,
    by_key: BTreeMap<(TypeName, String), Vec<usize>>,
}

impl ObservedState {
    /// insert an observed object. refuses a duplicate backend id; a duplicate
    /// key is recorded, not refused.
    pub fn insert(&mut self, object: ObservedObject) -> Result<()> {
        if let Some(id) = &object.backend_id {
            let slot = (object.type_name.clone(), id.clone());
            if self.by_backend_id.contains_key(&slot) {
                return Err(anyhow!(
                    "ObservedState already contains an object with backend id {} for type {}",
                    id,
                    object.type_name
                ));
            }
        }
        let index = self.objects.len();
        if let Some(id) = &object.backend_id {
            self.by_backend_id
                .insert((object.type_name.clone(), id.clone()), index);
        }
        self.by_key
            .entry((object.type_name.clone(), key_string(&object.key)))
            .or_default()
            .push(index);
        self.objects.push(object);
        Ok(())
    }

    /// every observed object, in insertion order.
    pub fn objects(&self) -> impl Iterator<Item = &ObservedObject> {
        self.objects.iter()
    }

    /// consume the observation into its objects.
    pub fn into_objects(self) -> Vec<ObservedObject> {
        self.objects
    }

    pub fn len(&self) -> usize {
        self.objects.len()
    }

    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }

    /// the object a backend id names, if observed. ids are unique, so this
    /// needs no ambiguity handling.
    pub fn by_backend_id(&self, type_name: &TypeName, id: &BackendId) -> Option<&ObservedObject> {
        self.by_backend_id
            .get(&(type_name.clone(), id.clone()))
            .map(|&index| &self.objects[index])
    }

    /// every id-bearing observed object with its id, in id order. the
    /// iteration deletion detection addresses objects through: an object
    /// without a backend id cannot be deleted.
    pub fn backend_indexed(
        &self,
    ) -> impl Iterator<Item = (&TypeName, &BackendId, &ObservedObject)> {
        self.by_backend_id
            .iter()
            .map(|((type_name, id), &index)| (type_name, id, &self.objects[index]))
    }

    /// dereference a key: `Ok(None)` when unobserved, the object when unique,
    /// and an error naming every candidate's backend id when ambiguous --
    /// alembic never picks among same-key objects.
    pub fn unique_by_key(
        &self,
        type_name: &TypeName,
        key: &str,
    ) -> Result<Option<&ObservedObject>> {
        let Some(indexes) = self.by_key.get(&(type_name.clone(), key.to_string())) else {
            return Ok(None);
        };
        match indexes.as_slice() {
            [] => Ok(None),
            [index] => Ok(Some(&self.objects[*index])),
            many => Err(anyhow!(
                "{} {} objects share the key {}: backend ids {}; alembic cannot tell them \
                 apart, so bind the intended one in state or key the type the way the \
                 backend scopes uniqueness",
                many.len(),
                type_name,
                key,
                many.iter()
                    .map(|&index| describe_backend_id(&self.objects[index].backend_id))
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        }
    }

    /// every key held by more than one observed object, with its holders.
    /// import fails on these: an inventory cannot represent two objects under
    /// one (type, key).
    pub fn ambiguities(&self) -> impl Iterator<Item = (&TypeName, &str, Vec<&ObservedObject>)> {
        self.by_key
            .iter()
            .filter(|(_, indexes)| indexes.len() > 1)
            .map(|((type_name, key), indexes)| {
                (
                    type_name,
                    key.as_str(),
                    indexes.iter().map(|&index| &self.objects[index]).collect(),
                )
            })
    }
}

/// an object the backend returned without an id still has to be nameable in an error.
fn describe_backend_id(id: &Option<BackendId>) -> String {
    match id {
        Some(id) => id.to_string(),
        None => "unknown".to_string(),
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

    /// read only objects already bound in `state`. the engine calls this when
    /// delete detection is disabled and key adoption is either disabled or no
    /// longer possible. an adapter that can address its backend by id may skip
    /// the full listing. returning more remains correct, so the default delegates
    /// to [`Observer::read`].
    async fn read_bound(
        &self,
        schema: &Schema,
        types: &[TypeName],
        state: &crate::state::StateStore,
    ) -> anyhow::Result<ObservedState> {
        self.read(schema, types, state).await
    }
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
    /// would perform, writing nothing. the default pairs with `ensure_schema`'s: nothing
    /// to provision. `None` means "cannot preview", and refuses to provision at all.
    async fn preview_schema(&self, _schema: &Schema) -> anyhow::Result<Option<ProvisionReport>> {
        Ok(Some(ProvisionReport::default()))
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

/// one key-match adoption: the run bound a declared uid to an existing
/// backend object because no state mapping answered for it. adoption writes
/// identity memory, so every adoption is reported.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Adoption {
    pub type_name: TypeName,
    pub uid: Uid,
    pub key: Key,
    pub backend_id: BackendId,
}

/// a backend id moving from one uid to another: the inventory claimed an
/// object another uid used to answer for.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupersededBinding {
    pub type_name: TypeName,
    pub backend_id: BackendId,
    pub superseded: Uid,
    pub by: Uid,
}

/// what bootstrapping state against an observation did to identity memory:
/// the adoptions it made and the bindings those superseded. a plan run may
/// persist these, so the cli surfaces them; silence would let a plan bind
/// identity that later authorizes an update or delete.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BootstrapReport {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub adoptions: Vec<Adoption>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub superseded: Vec<SupersededBinding>,
}

impl BootstrapReport {
    pub fn is_empty(&self) -> bool {
        self.adoptions.is_empty() && self.superseded.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use crate::{Plan, PlanSummary};

    #[test]
    fn a_misspelled_schema_preview_key_is_rejected() {
        // both optional plan keys default, so a typo'd preview reads as a plan that
        // carries none and apply's early --allow-delete gate never runs.
        let err = serde_json::from_str::<Plan>(
            r#"{"schema":{"types":{}},"ops":[],"schema_preveiw":{"deleted_object_types":["dcim.site"]}}"#,
        )
            .unwrap_err();
        assert!(err.to_string().contains("schema_preveiw"), "{err}");
    }

    #[test]
    fn a_plan_may_still_omit_its_summary_and_preview() {
        let plan: Plan = serde_json::from_str(r#"{"schema":{"types":{}},"ops":[]}"#).unwrap();
        assert!(plan.summary.is_none());
        assert!(plan.schema_preview.is_none());
    }

    #[test]
    fn a_misspelled_summary_key_is_rejected() {
        let err =
            serde_json::from_str::<PlanSummary>(r#"{"create":1,"update":0,"delete":0,"dlete":3}"#)
                .unwrap_err();
        assert!(err.to_string().contains("dlete"), "{err}");
    }
}
