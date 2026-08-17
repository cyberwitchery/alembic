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
use anyhow::{anyhow, Result};

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
pub use state::{PostgresTlsMode, StateData, StateLock, StateStore};
pub use transform::{compile_map, eval_map_transform, load_map_spec, MapSpec, TransformsSpec};
pub use types::{
    Adapter, AppliedOp, ApplyReport, Backend, BackendId, Emitter, FieldChange, ObservedObject,
    ObservedState, Observer, Op, Plan, PlanSummary, ProvisionReport, Tense,
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

/// observe backend state and produce a deterministic plan.
pub async fn build_plan(
    adapter: &(dyn Observer + '_),
    inventory: &Inventory,
    state: &mut StateStore,
    allow_delete: bool,
) -> Result<Plan> {
    let observed = pipeline::observe(adapter, inventory, state).await?;
    Ok(plan(
        &inventory.objects,
        &observed,
        state,
        &inventory.schema,
        allow_delete,
    ))
}

/// produce a plan for a write-only backend, which cannot report existing state.
/// the inventory is validated, then planned against an empty observation, so
/// every declared object becomes a create (and nothing is updated or deleted).
pub fn plan_write_only(inventory: &Inventory, state: &StateStore) -> Result<Plan> {
    report_to_result(validate(inventory))?;
    Ok(plan(
        &inventory.objects,
        &ObservedState::default(),
        state,
        &inventory.schema,
        false,
    ))
}

/// adopt existing backend objects by matching declared keys against an
/// observation.
pub(crate) fn bootstrap_state_from_observed(
    state: &mut StateStore,
    desired: &[Object],
    observed: &ObservedState,
) {
    for object in desired {
        if state
            .backend_id(object.type_name.clone(), object.uid)
            .is_some()
        {
            continue;
        }
        if let Some(obs) = observed
            .by_key
            .get(&(object.type_name.clone(), key_string(&object.key)))
        {
            if let Some(backend_id) = &obs.backend_id {
                state.set_backend_id(object.type_name.clone(), object.uid, backend_id.clone());
            }
        }
    }
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
