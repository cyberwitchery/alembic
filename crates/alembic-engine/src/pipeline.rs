use crate::pretty_printing::{bullet_list, comma_separated};
use crate::types::{ApplyReport, Backend, Emitter, ObservedState, Observer, Plan, ProvisionReport};
use crate::StateStore;
use crate::{sort_ops_for_apply, BackendId};
use alembic_core::{key_string, Inventory, TypeName};
use anyhow::{anyhow, Result};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) async fn observe(
    adapter: &(dyn Observer + '_),
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
    detect_key_collisions(&observed)?;

    crate::bootstrap_state_from_observed(state, &inventory.objects, &observed);
    Ok(observed)
}

/// checks if any of the observed objects share the same keys
fn detect_key_collisions(observed: &ObservedState) -> Result<()> {
    let mut keys = BTreeMap::<_, Vec<BackendId>>::new();
    for ((type_name, backend_id), object) in &observed.by_backend_id {
        let key = (type_name.clone(), key_string(&object.key));
        keys.entry(key)
            .and_modify(|ids| ids.push(backend_id.clone()))
            .or_insert(vec![backend_id.clone()]);
    }

    let collisions = keys
        .iter()
        .filter(|(_, ids)| ids.len() > 1)
        .map(|((key_typename, key_string), ids)| {
            format!(
                "objects with ids {} all share the key ('{}, {}')",
                comma_separated(ids),
                key_typename,
                key_string
            )
        })
        .collect::<Vec<_>>();

    if !collisions.is_empty() {
        return Err(anyhow!("colliding keys:\n{}", bullet_list(&collisions)));
    }

    Ok(())
}

pub(crate) async fn apply(
    backend: &Backend,
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

    let (emitter, provision): (&dyn Emitter, ProvisionReport) = match backend {
        Backend::Adapter(adapter) => (adapter.as_ref(), adapter.ensure_schema(&plan.schema).await?),
        Backend::Emitter(emitter) => (emitter.as_ref(), ProvisionReport::default()),
        Backend::Observer(_) => {
            return Err(anyhow!("backend is read-only; it cannot apply changes"))
        }
    };

    let ordered = sort_ops_for_apply(&plan.ops, &plan.schema);
    let mut report = emitter.write(&plan.schema, &ordered, state).await?;
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
