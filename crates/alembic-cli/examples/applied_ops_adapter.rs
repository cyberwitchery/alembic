use alembic_core::{Schema, TypeName};
use alembic_engine::{
    alembic_external_main, AppliedOp, ApplyReport, ExternalAdapter, ExternalObject, Op, StateData,
};
use anyhow::Result;

alembic_external_main!(AppliedOpsAdapter::default());

#[derive(Debug, Default)]
pub struct AppliedOpsAdapter {}

impl ExternalAdapter for AppliedOpsAdapter {
    fn setup(&mut self, _configuration: &serde_yaml::Value) -> Result<()> {
        Ok(())
    }

    fn read(
        &mut self,
        _schema: &Schema,
        _types: &[TypeName],
        _state: &StateData,
    ) -> Result<Vec<ExternalObject>> {
        Ok(vec![])
    }

    // reports back every op it was handed, so `applied N operations` counts what
    // actually reached the backend.
    fn write(&mut self, _schema: &Schema, ops: &[Op], _state: &StateData) -> Result<ApplyReport> {
        Ok(ApplyReport {
            applied: ops
                .iter()
                .map(|op| AppliedOp {
                    uid: op.uid(),
                    type_name: op.type_name().clone(),
                    backend_id: None,
                })
                .collect(),
            ..ApplyReport::default()
        })
    }
}
