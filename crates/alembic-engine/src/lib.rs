//! engine orchestration: load, validate, plan, apply.

mod django;
mod extract;
mod lint;
mod loader;
mod pipeline;
mod planner;
mod projection;
mod retort;
mod state;
mod types;

use alembic_core::{key_string, validate_inventory, Inventory};
use anyhow::{anyhow, Result};

#[cfg(test)]
mod tests;

pub use django::{emit_django_app, DjangoEmitOptions};
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
pub use state::StateStore;
pub use types::{
    Adapter, AppliedOp, ApplyReport, BackendId, FieldChange, ObservedObject, ObservedState, Op,
    Plan,
};

/// validate an inventory and return an aggregated error on failure.
pub fn validate(inventory: &Inventory) -> Result<()> {
    let report = validate_inventory(inventory);
    if report.is_ok() {
        return Ok(());
    }

    let mut message = String::from("validation failed:\n");
    for error in report.errors {
        message.push_str(&format!("- {error}\n"));
    }
    Err(anyhow!(message))
}

/// observe backend state and produce a deterministic plan.
pub async fn build_plan(
    adapter: &dyn Adapter,
    inventory: &Inventory,
    state: &mut StateStore,
    allow_delete: bool,
) -> Result<Plan> {
    build_plan_with_projection(adapter, inventory, state, allow_delete, None, true).await
}

pub async fn build_plan_with_projection(
    adapter: &dyn Adapter,
    inventory: &Inventory,
    state: &mut StateStore,
    allow_delete: bool,
    projection: Option<&ProjectionSpec>,
    projection_strict: bool,
) -> Result<Plan> {
    let projected = LoadContext::from_ref(inventory)?
        .project(projection)?
        .observe(adapter, state, projection_strict, allow_delete)
        .await?;
    Ok(projected.plan(state))
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
    adapter: &dyn Adapter,
    plan: &Plan,
    state: &mut StateStore,
    allow_delete: bool,
) -> Result<ApplyReport> {
    ApplyContext::new(plan, allow_delete)?
        .apply(adapter, state)
        .await
}
