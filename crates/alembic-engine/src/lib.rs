//! engine orchestration: load, validate, plan, apply.

mod loader;
mod planner;
mod state;
mod types;

use alembic_core::{validate_inventory, Inventory};
use anyhow::{anyhow, Result};

#[cfg(test)]
mod tests;

pub use loader::load_brew;
pub use planner::{plan, sort_ops_for_apply};
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
    state: &StateStore,
    allow_delete: bool,
) -> Result<Plan> {
    validate(inventory)?;
    let kinds: Vec<_> = inventory.objects.iter().map(|o| o.kind).collect();
    let observed = adapter.observe(&kinds).await?;
    Ok(plan(inventory, &observed, state, allow_delete))
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
            state.set_backend_id(applied.kind, applied.uid, backend_id);
        } else {
            state.remove_backend_id(applied.kind, applied.uid);
        }
    }

    Ok(report)
}
