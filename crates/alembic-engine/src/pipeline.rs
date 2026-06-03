use crate::sort_ops_for_apply;
use crate::types::{Adapter, ApplyReport, ObservedState, Plan};
use crate::StateStore;
use alembic_core::{Inventory, TypeName};
use anyhow::{anyhow, Result};
use std::collections::BTreeSet;

pub(crate) async fn observe(
    adapter: &(dyn Adapter + '_),
    inventory: &Inventory,
    state: &mut StateStore,
) -> Result<ObservedState> {
    crate::report_to_result(crate::validate(inventory))?;

    let mut types: BTreeSet<TypeName> = inventory
        .objects
        .iter()
        .map(|o| o.type_name.clone())
        .collect();
    for type_name in inventory.schema.types.keys() {
        types.insert(TypeName::new(type_name));
    }
    let types_vec: Vec<_> = types.into_iter().collect();

    let observed = adapter.read(&inventory.schema, &types_vec, state).await?;
    crate::bootstrap_state_from_observed(state, &inventory.objects, &observed);
    Ok(observed)
}

pub(crate) async fn apply(
    adapter: &(dyn Adapter + '_),
    plan: &Plan,
    state: &mut StateStore,
    allow_delete: bool,
) -> Result<ApplyReport> {
    if !allow_delete {
        let has_delete = plan
            .ops
            .iter()
            .any(|op| matches!(op, crate::Op::Delete { .. }));
        if has_delete {
            return Err(anyhow!(
                "plan contains delete operations; re-run with --allow-delete"
            ));
        }
    }

    let provision = adapter.ensure_schema(&plan.schema).await?;

    let ordered = sort_ops_for_apply(&plan.ops, &plan.schema);
    let mut report = adapter.write(&plan.schema, &ordered, state).await?;
    report.provision = provision;

    for applied in &report.applied {
        if let Some(backend_id) = &applied.backend_id {
            state.set_backend_id(applied.type_name.clone(), applied.uid, backend_id.clone());
        } else {
            state.remove_backend_id(applied.type_name.clone(), applied.uid);
        }
    }

    Ok(report)
}
