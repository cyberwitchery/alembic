use crate::projection::{apply_projection, validate_projection_strict};
use crate::types::{Adapter, ApplyReport, Plan};
use crate::{plan, sort_ops_for_apply, ProjectedInventory, ProjectedObject, ProjectionData};
use crate::{ObservedState, ProjectionSpec, StateStore};
use alembic_core::Inventory;
use anyhow::{anyhow, Result};
use std::collections::BTreeSet;

pub(crate) struct LoadContext {
    inventory: Inventory,
}

impl LoadContext {
    pub(crate) fn from_ref(inventory: &Inventory) -> Result<Self> {
        crate::validate(inventory)?;
        Ok(Self {
            inventory: inventory.clone(),
        })
    }

    pub(crate) fn project<'a>(
        self,
        spec: Option<&'a ProjectionSpec>,
    ) -> Result<ProjectionContext<'a>> {
        let projected = if let Some(spec) = spec {
            apply_projection(spec, &self.inventory.objects)?
        } else {
            let objects = self
                .inventory
                .objects
                .iter()
                .cloned()
                .map(|base| ProjectedObject {
                    base,
                    projection: ProjectionData::default(),
                    projection_inputs: BTreeSet::new(),
                })
                .collect();
            ProjectedInventory { objects }
        };

        Ok(ProjectionContext {
            inventory: self.inventory,
            projection: spec,
            projected,
        })
    }
}

pub(crate) struct ProjectionContext<'a> {
    inventory: Inventory,
    projection: Option<&'a ProjectionSpec>,
    projected: ProjectedInventory,
}

impl<'a> ProjectionContext<'a> {
    pub(crate) async fn observe(
        self,
        adapter: &dyn Adapter,
        state: &mut StateStore,
        projection_strict: bool,
        allow_delete: bool,
    ) -> Result<PlanContext> {
        let types: Vec<_> = self
            .projected
            .objects
            .iter()
            .map(|o| o.base.type_name.clone())
            .collect();
        let mut observed = adapter.observe(&types).await?;
        let bootstrapped = crate::bootstrap_state_from_observed(state, &self.projected, &observed);
        if bootstrapped {
            adapter.update_state(state);
            observed = adapter.observe(&types).await?;
        }
        if projection_strict {
            if let Some(spec) = self.projection {
                validate_projection_strict(spec, &self.inventory.objects, &observed.capabilities)?;
            }
        }

        Ok(PlanContext {
            projected: self.projected,
            observed,
            allow_delete,
        })
    }
}

pub(crate) struct PlanContext {
    projected: ProjectedInventory,
    observed: ObservedState,
    allow_delete: bool,
}

impl PlanContext {
    pub(crate) fn plan(self, state: &StateStore) -> Plan {
        plan(&self.projected, &self.observed, state, self.allow_delete)
    }
}

pub(crate) struct ApplyContext<'a> {
    plan: &'a Plan,
}

impl<'a> ApplyContext<'a> {
    pub(crate) fn new(plan: &'a Plan, allow_delete: bool) -> Result<Self> {
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

        Ok(Self { plan })
    }

    pub(crate) async fn apply(
        self,
        adapter: &dyn Adapter,
        state: &mut StateStore,
    ) -> Result<ApplyReport> {
        let ordered = sort_ops_for_apply(&self.plan.ops);
        let report = adapter.apply(&ordered).await?;

        for applied in &report.applied {
            if let Some(backend_id) = applied.backend_id {
                state.set_backend_id(applied.type_name.clone(), applied.uid, backend_id);
            } else {
                state.remove_backend_id(applied.type_name.clone(), applied.uid);
            }
        }

        Ok(report)
    }
}
