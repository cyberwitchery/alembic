//! engine orchestration: load, validate, plan, apply.

mod adapter_ops;
mod apply_retry;
mod drift;
mod errors;
pub mod external;
mod extract;
mod loader;
pub mod mapping;
mod pipeline;
mod planner;
mod predicate;
mod render;
mod state;
mod transform;
mod types;

use alembic_core::{key_string, validate_inventory, Inventory, Object, ValidationReport};
use anyhow::{anyhow, Result};

#[cfg(test)]
mod tests;

pub use adapter_ops::{
    build_key_from_schema, build_request_body, query_filters_from_key, resolve_value_for_type,
};
pub use apply_retry::{apply_non_delete_with_retries, RetryApplyDriver, RetryApplyResult};
pub use drift::{ChangedEntry, DriftEntry, DriftReport};
pub use errors::AdapterApplyError;
pub use external::{
    run_external_adapter, ExternalAdapter, ExternalEnvelope, ExternalObject, ExternalRequest,
    ExternalResponse, EXTERNAL_PROTOCOL_VERSION,
};
pub use extract::{import_inventory, ImportReport};
pub use loader::load_inventory;
pub use planner::{plan, sort_ops_for_apply};
pub use state::{PostgresTlsMode, StateData, StateStore};
pub use transform::{compile_map, load_map_spec, MapSpec};
pub use types::{
    Adapter, AppliedOp, ApplyReport, BackendId, FieldChange, ObservedObject, ObservedState, Op,
    Plan, PlanSummary, ProvisionReport,
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
    adapter: &(dyn Adapter + '_),
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
        let key = key_string(&object.key);
        let lookup_key = (object.type_name.clone(), key.clone());
        if observed.duplicate_keys.contains(&lookup_key) {
            tracing::warn!(
                "skipping uid bootstrap for type {} (key {key}): the observed backend state has a \
                 duplicate natural key, so the matching backend object is ambiguous; leaving the \
                 uid unbound for the operator to disambiguate",
                object.type_name
            );
            continue;
        }
        if let Some(obs) = observed.by_key.get(&lookup_key) {
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

/// apply a plan and update the state store.
pub async fn apply_plan(
    adapter: &(dyn Adapter + '_),
    plan: &Plan,
    state: &mut StateStore,
    allow_delete: bool,
) -> Result<ApplyReport> {
    pipeline::apply(adapter, plan, state, allow_delete).await
}
