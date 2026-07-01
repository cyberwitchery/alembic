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
mod planner;
mod predicate;
mod pretty_printing;
mod render;
#[cfg(feature = "starlark")]
mod starlark_transforms;
mod state;
mod transform;
mod types;
use alembic_core::{key_string, validate_inventory, Inventory, Object, ValidationReport};
use anyhow::{anyhow, Result};

#[cfg(test)]
mod tests;

pub use adapter_ops::{
    backend_id_from_value, build_key_from_schema, build_request_body, normalize_attrs_refs,
    normalize_ref_value, normalize_value_for_type, query_filters_from_key, resolve_value_for_type,
    resolved_ids_from_state, resolved_ids_identity, state_mappings_by_id, StateMappings,
};
pub use apply_retry::{
    apply_non_delete_with_retries, describe_missing_refs, is_missing_ref_error, RetryApplyDriver,
    RetryApplyResult,
};
pub use drift::{ChangedEntry, DriftEntry, DriftReport};
pub use endpoint::normalize_endpoint;
pub use errors::AdapterApplyError;
pub use external::{
    run_external_adapter, ExternalAdapter, ExternalEnvelope, ExternalObject, ExternalRequest,
    ExternalResponse, EXTERNAL_PROTOCOL_VERSION,
};
pub use extract::{import_inventory, ImportReport};
pub use inflect::pluralize;
pub use loader::load_inventory;
pub use planner::{plan, sort_ops_for_apply};
pub use state::{PostgresTlsMode, StateData, StateStore};
pub use transform::{compile_map, eval_map_transform, load_map_spec, MapSpec, TransformsSpec};
pub use types::{
    Adapter, AppliedOp, ApplyReport, Backend, BackendId, Emitter, FieldChange, ObservedObject,
    ObservedState, Observer, Op, Plan, PlanSummary, ProvisionReport,
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

pub(crate) fn bootstrap_state_from_observed(
    state: &mut StateStore,
    desired: &[Object],
    observed: &ObservedState,
) -> bool {
    let mut updated = false;
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
            if obs.type_name != object.type_name {
                continue;
            }
            if let Some(backend_id) = &obs.backend_id {
                state.set_backend_id(object.type_name.clone(), object.uid, backend_id.clone());
                updated = true;
            }
        }
    }
    updated
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
