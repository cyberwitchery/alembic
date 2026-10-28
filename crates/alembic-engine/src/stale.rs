//! apply's check that the backend still holds what a plan was diffed against.

use crate::state::StateStore;
use crate::types::{Backend, Plan};
use alembic_adapter_sdk::{FieldChange, Op, StateData};
use alembic_core::{key_string, Inventory, Object, TypeName, Uid};
use anyhow::Result;
use std::collections::BTreeMap;
use std::fmt;
use thiserror::Error;

/// the backend moved since the plan was made: applying it would overwrite or
/// collide with what changed. each line names an object and what moved.
#[derive(Debug, Error)]
pub struct StalePlan {
    pub changed: Vec<String>,
}

impl fmt::Display for StalePlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "the backend changed since the plan was made; re-run `alembic plan` and review it again:"
        )?;
        write!(f, "{}", crate::bullet_list(&self.changed))
    }
}

/// refuse a plan the backend has moved away from. the plan's objects are
/// diffed again against the backend as it is now, with identity memory copied
/// so nothing is bound and key adoption as the plan had it; every op that diff
/// needs has to be one the plan holds,
/// with the same `from` values. an op the diff no longer needs was applied
/// already (an interrupted run, or the same edit made by hand), which is not a
/// conflict. deletes are not checked: an object already gone is the delete
/// applied. a write-only backend observes nothing, so it is not checked.
pub async fn guard_stale_plan(backend: &Backend, plan: &Plan, state: &StateStore) -> Result<()> {
    let Ok(observer) = backend.observer() else {
        return Ok(());
    };
    let planned = planned_objects(&plan.ops);
    if planned.is_empty() {
        return Ok(());
    }
    let objects: Vec<Object> = planned.values().map(|&object| object.clone()).collect();
    let inventory = Inventory {
        schema: plan.schema.clone(),
        scope: None,
        objects,
    };
    let mut scratch = StateStore::new(
        None,
        StateData {
            mappings: state.all_mappings().clone(),
        },
    );
    // as the plan was made: a `--no-adopt` plan's create of a same-key twin is
    // what it asked for. a plan from before the field adopted by key.
    let adopt_by_key = plan.adopt_by_key.unwrap_or(true);
    let (observed, bootstrap) =
        crate::pipeline::observe_validated(observer, &inventory, &mut scratch, false, adopt_by_key)
            .await?;
    let now = crate::planner::plan_with_bootstrap(
        &inventory.objects,
        &observed,
        &scratch,
        &inventory.schema,
        None,
        false,
        adopt_by_key,
        &bootstrap,
    )?;

    let changed: Vec<String> = now
        .ops
        .iter()
        .filter_map(|needed| moved(needed, &plan.ops))
        .collect();
    if changed.is_empty() {
        Ok(())
    } else {
        Err(StalePlan { changed }.into())
    }
}

/// the object each create or update writes, by uid and type. a uid created and
/// then updated (a deferred ref) writes the update's, which carries every field.
fn planned_objects(ops: &[Op]) -> BTreeMap<(Uid, TypeName), &Object> {
    let mut objects = BTreeMap::new();
    for op in ops {
        match op {
            Op::Create {
                uid,
                type_name,
                desired,
            } => {
                objects.entry((*uid, type_name.clone())).or_insert(desired);
            }
            Op::Update {
                uid,
                type_name,
                desired,
                ..
            } => {
                objects.insert((*uid, type_name.clone()), desired);
            }
            Op::Delete { .. } => {}
        }
    }
    objects
}

/// what moved, when the plan holds no op covering `needed`.
fn moved(needed: &Op, planned: &[Op]) -> Option<String> {
    let same_object = |op: &&Op| op.uid() == needed.uid() && op.type_name() == needed.type_name();
    let create = planned
        .iter()
        .filter(same_object)
        .find(|op| matches!(op, Op::Create { .. }));
    let update = planned.iter().filter(same_object).find_map(|op| match op {
        Op::Update {
            changes,
            backend_id,
            ..
        } => Some((changes, backend_id)),
        _ => None,
    });
    match needed {
        Op::Create {
            type_name, desired, ..
        } => match create {
            Some(_) => None,
            None => Some(format!(
                "{type_name} {} no longer exists on the backend",
                key_string(&desired.key)
            )),
        },
        Op::Update {
            type_name,
            desired,
            changes,
            backend_id,
            ..
        } => {
            let object = format!("{type_name} {}", key_string(&desired.key));
            let Some((planned_changes, planned_id)) = update else {
                return Some(match create {
                    Some(_) => format!("{object} did not exist when planned and now does"),
                    None => format!("{object} changed on the backend"),
                });
            };
            if planned_id != backend_id {
                return Some(format!("{object} is now a different backend object"));
            }
            let fields: Vec<String> = changes
                .iter()
                .filter(|change| !planned_changes.contains(change))
                .map(|change| describe_field(change, planned_changes))
                .collect();
            (!fields.is_empty()).then(|| format!("{object}: {}", fields.join(", ")))
        }
        // deletes are never re-planned here
        Op::Delete { .. } => None,
    }
}

fn describe_field(now: &FieldChange, planned: &[FieldChange]) -> String {
    let was = planned
        .iter()
        .find(|change| change.field == now.field)
        .map_or(&now.to, |change| &change.from);
    format!("`{}` is {}, planned against {}", now.field, now.from, was)
}
