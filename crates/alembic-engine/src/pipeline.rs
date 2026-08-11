use crate::pretty_printing::{bullet_list, comma_separated};
use crate::types::{
    ApplyReport, Backend, ObservedState, Observer, Plan, ProvisionReport, CANNOT_OBSERVE,
};
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

/// refuse destructive schema provisioning (deleting custom object types/fields
/// the inventory no longer declares) unless `allow_delete` is set. these deletes
/// cascade to their objects on the backend, so they are gated behind the same
/// flag as object deletes. `preview` is the read-only schema preview both `plan`
/// (before provisioning) and `apply` compute.
pub fn guard_schema_deletes(preview: &ProvisionReport, allow_delete: bool) -> Result<()> {
    if allow_delete {
        return Ok(());
    }
    // destructured without `..`, like the folds on the type: a category added
    // later has to be classified as destructive or not, rather than default to
    // not and provision past this gate in silence.
    let ProvisionReport {
        created_fields: _,
        // not destructive: an update only writes properties the schema declares,
        // and never blanks one it does not.
        updated_fields: _,
        created_tags: _,
        created_object_types: _,
        created_object_fields: _,
        updated_object_fields: _,
        deprecated_object_types: _,
        deprecated_object_fields: _,
        deleted_object_types,
        deleted_object_fields,
    } = preview;
    // named, not counted: a delete cascades, and the count alone leaves no way
    // to find out what it took short of running it.
    let doomed: Vec<String> = deleted_object_types
        .iter()
        .map(|name| format!("type {name}"))
        .chain(
            deleted_object_fields
                .iter()
                .map(|name| format!("field {name}")),
        )
        .collect();
    if !doomed.is_empty() {
        return Err(anyhow!(
            "provisioning would delete schema; re-run with --allow-delete:\n{}",
            bullet_list(&doomed)
        ));
    }
    Ok(())
}

/// refuse a drift report over a backend that cannot observe. the report asserts
/// what the backend holds, and a write-only backend is planned against an empty
/// observation, so every declared object would be reported absent from a backend
/// nothing ever read. `import` refuses the same situation for the same reason.
pub fn guard_drift_report(backend: &Backend) -> Result<()> {
    match backend {
        Backend::Observer(_) | Backend::Adapter(_) => Ok(()),
        Backend::Emitter(_) => Err(anyhow!(
            "{CANNOT_OBSERVE}, so there is no drift to report. run `alembic plan` \
             without --report to see what apply would emit"
        )),
    }
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
    // schema provisioning can delete custom object types/fields the inventory no
    // longer declares, cascading to their objects on the backend; gate it behind
    // the same flag as object deletes. the plan's schema_preview is only a cheap
    // early gate when a caller populated it: planner::plan hard-codes None and the
    // interactive/library apply paths rebuild with None, so it cannot be the
    // authoritative gate. the self-preview below is.
    if let Some(preview) = &plan.schema_preview {
        guard_schema_deletes(preview, allow_delete)?;
    }

    // errors for an observer; every backend that can write can also provision,
    // so read+write and write-only take the same path from here.
    let emitter = backend.emitter()?;
    // authoritative gate: self-preview at the chokepoint before ensure_schema, so
    // no caller can forget (mirrors `plan --provision`). preview_schema defaults
    // to Ok(None), leaving backends that cannot preview unaffected; an Err fails
    // closed rather than provision blind.
    if !allow_delete {
        if let Some(preview) = emitter.preview_schema(&plan.schema).await? {
            guard_schema_deletes(&preview, allow_delete)?;
        }
    }
    let provision = emitter.ensure_schema(&plan.schema).await?;

    let ordered = sort_ops_for_apply(&plan.ops, &plan.schema);
    let mut report = emitter.write(&plan.schema, &ordered, state).await?;
    // write provisions too: tags come from the plan's ops, so only the write pass
    // knows them. merging keeps both passes instead of the schema pass winning.
    report.provision.merge(provision);

    // ops an interrupted run applied: their mappings exist nowhere else, since it
    // never reached a state save. only ever set one, never clear on a `None` -- a
    // journal written before ids were recorded has `None` throughout.
    for applied in &report.resumed {
        if let Some(backend_id) = &applied.backend_id {
            state.set_backend_id(applied.type_name.clone(), applied.uid, backend_id.clone());
        }
    }

    for applied in &report.applied {
        if let Some(backend_id) = &applied.backend_id {
            state.set_backend_id(applied.type_name.clone(), applied.uid, backend_id.clone());
        } else {
            state.remove_backend_id(applied.type_name.clone(), applied.uid);
        }
    }

    Ok(report)
}
