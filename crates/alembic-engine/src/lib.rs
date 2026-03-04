//! engine orchestration: load, validate, plan, apply.

mod adapter_ops;
mod apply_retry;
mod django;
mod errors;
mod extract;
mod lint;
mod loader;
mod pipeline;
mod planner;
mod projection;
mod retort;
mod state;
mod types;

use alembic_core::{key_string, validate_inventory, Inventory, Object, ValidationReport};
use anyhow::{anyhow, Result};

#[cfg(test)]
mod tests;

pub use adapter_ops::{
    build_key_from_schema, build_request_body, query_filters_from_key, resolve_value_for_type,
};
pub use apply_retry::{apply_non_delete_with_retries, RetryApplyDriver, RetryApplyResult};
pub use django::{emit_django_app, DjangoEmitOptions};
pub use errors::AdapterApplyError;
pub use extract::{extract_inventory, ExtractReport};
pub use lint::{lint_specs, LintReport};
pub use loader::load_brew;
use pipeline::{ApplyContext, LoadContext};
pub use planner::{plan, sort_ops_for_apply};
pub use projection::{
    apply_projection, load_projection, missing_custom_fields, missing_tags, project_default,
    validate_projection_strict, BackendCapabilities, MissingCustomField, MissingTag,
    ProjectedInventory, ProjectedObject, ProjectionData, ProjectionSpec,
};
pub use retort::{compile_retort, is_brew_format, load_raw_yaml, load_retort, Retort};
pub use state::{PostgresTlsMode, StateData, StateStore};
pub use types::{
    Adapter, AppliedOp, ApplyAdapter, ApplyReport, BackendId, FieldChange, ObserveAdapter,
    ObservedObject, ObservedState, Op, Plan, PlanningAdapter, ProjectionProvisioner,
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
    adapter: &(impl ObserveAdapter + ?Sized),
    inventory: &Inventory,
    state: &mut StateStore,
    allow_delete: bool,
) -> Result<Plan> {
    build_plan_with_projection(adapter, inventory, state, allow_delete, None, true).await
}

pub async fn build_plan_with_projection(
    adapter: &(impl ObserveAdapter + ?Sized),
    inventory: &Inventory,
    state: &mut StateStore,
    allow_delete: bool,
    projection: Option<&ProjectionSpec>,
    projection_strict: bool,
) -> Result<Plan> {
    let observed = LoadContext::from_ref(inventory)?
        .project(projection)?
        .observe(adapter, state, projection_strict, false)
        .await?;
    Ok(observed.plan(state, allow_delete))
}

pub async fn build_plan_with_projection_observe_schema(
    adapter: &(impl ObserveAdapter + ?Sized),
    inventory: &Inventory,
    state: &mut StateStore,
    allow_delete: bool,
    projection: Option<&ProjectionSpec>,
    projection_strict: bool,
) -> Result<Plan> {
    let observed = LoadContext::from_ref(inventory)?
        .project(projection)?
        .observe(adapter, state, projection_strict, true)
        .await?;
    Ok(observed.plan(state, allow_delete))
}

pub async fn project_and_observe(
    adapter: &(impl ObserveAdapter + ?Sized),
    inventory: &Inventory,
    state: &mut StateStore,
    projection: Option<&ProjectionSpec>,
) -> Result<(ProjectedInventory, ObservedState)> {
    let observed = LoadContext::from_ref(inventory)?
        .project(projection)?
        .observe(adapter, state, false, false)
        .await?;
    Ok((observed.projected, observed.observed))
}

pub(crate) fn bootstrap_state_from_observed(
    state: &mut StateStore,
    desired: &ProjectedInventory,
    observed: &ObservedState,
) -> bool {
    let mut updated = false;
    for object in &desired.objects {
        if state
            .backend_id(object.base.type_name.clone(), object.base.uid)
            .is_some()
        {
            continue;
        }
        if let Some(obs) = observed
            .by_key
            .get(&(object.base.type_name.clone(), key_string(&object.base.key)))
        {
            if obs.type_name != object.base.type_name {
                continue;
            }
            if let Some(backend_id) = &obs.backend_id {
                state.set_backend_id(
                    object.base.type_name.clone(),
                    object.base.uid,
                    backend_id.clone(),
                );
                updated = true;
            }
        }
    }
    updated
}

/// apply a plan and update the state store.
pub async fn apply_plan(
    adapter: &(impl ApplyAdapter + ?Sized),
    plan: &Plan,
    state: &mut StateStore,
    allow_delete: bool,
) -> Result<ApplyReport> {
    ApplyContext::new(plan, allow_delete)?
        .apply(adapter, state)
        .await
}
