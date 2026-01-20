//! engine orchestration: load, validate, plan, apply.

mod django;
mod extract;
mod lint;
mod loader;
mod planner;
mod projection;
mod retort;
mod state;
mod types;

use alembic_core::{validate_inventory, Inventory};
use anyhow::{anyhow, Result};

#[cfg(test)]
mod tests;

pub use django::{emit_django_app, DjangoEmitOptions};
pub use extract::{extract_inventory, ExtractReport};
pub use lint::{lint_specs, LintReport};
pub use loader::load_brew;
pub use planner::{plan, sort_ops_for_apply};
pub use projection::{
    apply_projection, load_projection, missing_custom_fields, missing_tags, project_default,
    validate_projection_strict, BackendCapabilities, MissingCustomField, MissingTag,
    ProjectedInventory, ProjectedObject, ProjectionData, ProjectionSpec,
};
pub use retort::{compile_retort, is_brew_format, load_raw_yaml, load_retort, Retort};
pub use state::StateStore;
pub use types::{
    Adapter, AppliedOp, ApplyReport, FieldChange, ObservedObject, ObservedState, Op, Plan,
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
    validate(inventory)?;
    let projected = if let Some(spec) = projection {
        apply_projection(spec, &inventory.objects)?
    } else {
        let objects = inventory
            .objects
            .iter()
            .cloned()
            .map(|base| ProjectedObject {
                base,
                projection: ProjectionData::default(),
            })
            .collect();
        ProjectedInventory { objects }
    };

    let kinds: Vec<_> = projected
        .objects
        .iter()
        .map(|o| o.base.kind.clone())
        .collect();
    let mut observed = adapter.observe(&kinds).await?;
    let bootstrapped = bootstrap_state_from_observed(state, &projected, &observed);
    if bootstrapped {
        adapter.update_state(state);
        observed = adapter.observe(&kinds).await?;
    }
    if projection_strict {
        if let Some(spec) = projection {
            validate_projection_strict(spec, &inventory.objects, &observed.capabilities)?;
        }
    }
    Ok(plan(&projected, &observed, state, allow_delete))
}

fn bootstrap_state_from_observed(
    state: &mut StateStore,
    desired: &ProjectedInventory,
    observed: &ObservedState,
) -> bool {
    let mut updated = false;
    for object in &desired.objects {
        if state
            .backend_id(object.base.kind.clone(), object.base.uid)
            .is_some()
        {
            continue;
        }
        if let Some(obs) = observed.by_key.get(&object.base.key) {
            if obs.kind != object.base.kind {
                continue;
            }
            if let Some(backend_id) = obs.backend_id {
                state.set_backend_id(object.base.kind.clone(), object.base.uid, backend_id);
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
    if !allow_delete {
        let has_delete = plan.ops.iter().any(|op| matches!(op, Op::Delete { .. }));
        if has_delete {
            return Err(anyhow!(
                "plan contains delete operations; re-run with --allow-delete"
            ));
        }
    }

    let ordered = sort_ops_for_apply(&plan.ops);
    let report = adapter.apply(&ordered).await?;

    for applied in &report.applied {
        if let Some(backend_id) = applied.backend_id {
            state.set_backend_id(applied.kind.clone(), applied.uid, backend_id);
        } else {
            state.remove_backend_id(applied.kind.clone(), applied.uid);
        }
    }

    Ok(report)
}
