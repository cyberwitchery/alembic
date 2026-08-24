//! engine orchestration: load, validate, plan, apply.

mod adapter_ops;
mod apply_retry;
mod drift;
mod endpoint;
mod errors;
pub mod external;
mod extract;
mod inflect;
pub mod journal;
mod loader;
pub mod mapping;
mod pipeline;
mod plan_view;
mod planner;
mod predicate;
mod pretty_printing;
mod refs;
mod render;
#[cfg(feature = "starlark")]
mod starlark_transforms;
mod state;
#[cfg(test)]
mod test_log;
mod transform;
mod types;
use alembic_core::{key_string, validate_inventory, Inventory, Object, ValidationReport};
use anyhow::{anyhow, Context, Result};

#[cfg(test)]
mod tests;

pub use adapter_ops::{
    backend_id_from_value, build_key_from_schema, build_request_body, collect_tag_names,
    normalize_attrs_refs, query_filters_from_key, resolve_nested_ref_uid,
    resolve_ref_keyed_identity, resolve_value_for_type, resolved_ids_from_state,
    resolved_ids_identity, state_mappings_by_id, RawNode, RefMappings, StateMappings,
};
pub use apply_retry::{
    apply_non_delete_journaled, apply_non_delete_with_retries, describe_missing_refs,
    is_missing_ref_error, JournalGuard, RetryApplyDriver, RetryApplyResult,
};
pub use drift::{ChangedEntry, DriftEntry, DriftReport};
pub use endpoint::normalize_endpoint;
pub use errors::AdapterApplyError;
pub use external::{
    run_external_adapter, ExternalAdapter, ExternalCapabilities, ExternalEnvelope,
    ExternalEnvelopeRef, ExternalObject, ExternalRequest, ExternalRequestRef, ExternalResponse,
    ExternalRole, EXTERNAL_PROTOCOL_VERSION,
};
pub use extract::{import_inventory, ImportReport};
pub use inflect::pluralize;
pub use journal::Journal;
pub use loader::{load_inventory, load_inventory_unvalidated};
pub use pipeline::{guard_drift_report, guard_schema_deletes, guard_schema_provisioning};
pub use plan_view::render_plan;
pub use planner::{plan, sort_ops_for_apply};
pub use pretty_printing::bullet_list;
pub use state::{BackendIdentity, PostgresTlsMode, StateData, StateFile, StateLock, StateStore};
pub use transform::{compile_map, eval_map_transform, load_map_spec, MapSpec, TransformsSpec};
pub use types::{
    Adapter, Adoption, AppliedOp, ApplyReport, Backend, BackendId, BootstrapReport, Emitter,
    FieldChange, ObservedObject, ObservedState, Observer, Op, Plan, PlanSummary, ProvisionReport,
    SupersededBinding, Tense,
};

/// validate an inventory and return the report.
pub fn validate(inventory: &Inventory) -> ValidationReport {
    validate_inventory(inventory)
}

/// helper to format a validation report into a Result.
pub fn report_to_result(report: ValidationReport) -> Result<()> {
    report_to_result_with_sources(report, &[])
}

/// helper to format a validation report with source locations into a Result.
pub fn report_to_result_with_sources(report: ValidationReport, objects: &[Object]) -> Result<()> {
    if report.is_ok() {
        return Ok(());
    }

    let located_errors = report.with_sources(objects);
    let mut message = String::from("validation failed:\n");
    for error in located_errors {
        message.push_str(&format!("- {error}\n"));
    }
    Err(anyhow!(message))
}

/// observe backend state and produce a deterministic plan, plus the report of
/// what bootstrapping wrote into identity memory. `adopt_by_key` gates
/// brownfield adoption: off, only state-known objects match and everything
/// else plans as a create.
pub async fn build_plan(
    adapter: &(dyn Observer + '_),
    inventory: &Inventory,
    state: &mut StateStore,
    allow_delete: bool,
    adopt_by_key: bool,
) -> Result<(Plan, types::BootstrapReport)> {
    let (observed, bootstrap) = pipeline::observe(adapter, inventory, state, adopt_by_key).await?;
    let plan = plan(
        &inventory.objects,
        &observed,
        state,
        &inventory.schema,
        allow_delete,
        adopt_by_key,
    )?;
    Ok((plan, bootstrap))
}

/// produce a plan for a write-only backend, which cannot report existing state.
/// the inventory is validated, then planned against an empty observation, so
/// every declared object becomes a create (and nothing is updated or deleted).
pub fn plan_write_only(inventory: &Inventory, state: &StateStore) -> Result<Plan> {
    report_to_result(validate(inventory))?;
    plan(
        &inventory.objects,
        &ObservedState::default(),
        state,
        &inventory.schema,
        false,
        true,
    )
}

/// adopt existing backend objects by matching declared keys against an
/// observation, reporting every binding written into identity memory. with
/// `adopt_by_key` off, state-known objects still settle but nothing new is
/// adopted, so unmatched declared objects plan as creates. adoption binds
/// identity, so a key several backend objects share is an error naming every
/// candidate, never a choice among them.
pub(crate) fn bootstrap_state_from_observed(
    state: &mut StateStore,
    desired: &[Object],
    observed: &ObservedState,
    adopt_by_key: bool,
) -> Result<types::BootstrapReport> {
    let mut report = types::BootstrapReport::default();
    for object in desired {
        if let Some(backend_id) = state.backend_id(object.type_name.clone(), object.uid) {
            // the desired set is the only place a superseded uid can be settled
            // with information rather than by uid ordering: it says which uid the
            // object answers to now.
            if let Some(displaced) = state
                .uid_for_backend_id(&object.type_name, &backend_id)
                .filter(|uid| *uid != object.uid)
            {
                report.superseded.push(types::SupersededBinding {
                    type_name: object.type_name.clone(),
                    backend_id: backend_id.clone(),
                    superseded: displaced,
                    by: object.uid,
                });
            }
            if state.uid_for_backend_id(&object.type_name, &backend_id) != Some(object.uid) {
                state.set_backend_id(object.type_name.clone(), object.uid, backend_id);
            }
            continue;
        }
        if !adopt_by_key {
            continue;
        }
        let candidate = observed
            .unique_by_key(&object.type_name, &key_string(&object.key))
            .with_context(|| {
                format!(
                    "cannot adopt {} {}",
                    object.type_name,
                    key_string(&object.key)
                )
            })?;
        if let Some(obs) = candidate {
            if let Some(backend_id) = &obs.backend_id {
                if let Some(displaced) = state
                    .uid_for_backend_id(&object.type_name, backend_id)
                    .filter(|uid| *uid != object.uid)
                {
                    report.superseded.push(types::SupersededBinding {
                        type_name: object.type_name.clone(),
                        backend_id: backend_id.clone(),
                        superseded: displaced,
                        by: object.uid,
                    });
                }
                state.set_backend_id(object.type_name.clone(), object.uid, backend_id.clone());
                report.adoptions.push(types::Adoption {
                    type_name: object.type_name.clone(),
                    uid: object.uid,
                    key: object.key.clone(),
                    backend_id: backend_id.clone(),
                });
            }
        }
    }
    Ok(report)
}

/// apply a plan and update the state store. full adapters provision schema
/// before writing; emitters only write.
pub async fn apply_plan(
    backend: &Backend,
    plan: &Plan,
    state: &mut StateStore,
    allow_delete: bool,
) -> Result<ApplyReport> {
    pipeline::apply(backend, plan, state, allow_delete).await
}
