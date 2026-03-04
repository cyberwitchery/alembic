use crate::projection::{apply_projection, validate_projection_strict};
use crate::types::{ApplyAdapter, ApplyReport, ObserveAdapter, Plan};
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
        crate::report_to_result(crate::validate(inventory))?;
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

pub(crate) struct ObservedContext {
    pub(crate) projected: ProjectedInventory,
    pub(crate) observed: ObservedState,
    pub(crate) schema: alembic_core::Schema,
}

impl<'a> ProjectionContext<'a> {
    pub(crate) async fn observe(
        self,
        adapter: &(impl ObserveAdapter + ?Sized),
        state: &mut StateStore,
        projection_strict: bool,
        include_schema_types: bool,
    ) -> Result<ObservedContext> {
        let mut types: BTreeSet<_> = self
            .projected
            .objects
            .iter()
            .map(|o| o.base.type_name.clone())
            .collect();
        if include_schema_types {
            for type_name in self.inventory.schema.types.keys() {
                types.insert(alembic_core::TypeName::new(type_name));
            }
        }
        let types_vec: Vec<_> = types.into_iter().collect();
        let mut observed = adapter
            .observe(&self.inventory.schema, &types_vec, state)
            .await?;
        let bootstrapped = crate::bootstrap_state_from_observed(state, &self.projected, &observed);
        if bootstrapped {
            observed = adapter
                .observe(&self.inventory.schema, &types_vec, state)
                .await?;
        }
        if projection_strict {
            if let Some(spec) = self.projection {
                validate_projection_strict(spec, &self.inventory.objects, &observed.capabilities)?;
            }
        }

        Ok(ObservedContext {
            projected: self.projected,
            observed,
            schema: self.inventory.schema,
        })
    }
}

impl ObservedContext {
    pub(crate) fn plan(self, state: &StateStore, allow_delete: bool) -> Plan {
        plan(
            &self.projected,
            &self.observed,
            state,
            &self.schema,
            allow_delete,
        )
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
        adapter: &(impl ApplyAdapter + ?Sized),
        state: &mut StateStore,
    ) -> Result<ApplyReport> {
        let ordered = sort_ops_for_apply(&self.plan.ops);
        let report = adapter.apply(&self.plan.schema, &ordered, state).await?;

        for applied in &report.applied {
            if let Some(backend_id) = &applied.backend_id {
                state.set_backend_id(applied.type_name.clone(), applied.uid, backend_id.clone());
            } else {
                state.remove_backend_id(applied.type_name.clone(), applied.uid);
            }
        }

        Ok(report)
    }
}
