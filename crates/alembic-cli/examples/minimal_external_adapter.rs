use alembic_core::{Schema, TypeName};
use alembic_engine::{
    alembic_external_main, ApplyReport, ExternalAdapter, ExternalObject, Op, StateData,
};
use anyhow::Result;

// this will define the main function of the crate
alembic_external_main!(MinimalAdapter::default());

#[derive(Debug, Default)]
pub struct MinimalAdapter {}

impl MinimalAdapter {}

impl ExternalAdapter for MinimalAdapter {
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

    fn write(&mut self, _schema: &Schema, _ops: &[Op], _state: &StateData) -> Result<ApplyReport> {
        Ok(ApplyReport::default())
    }
}
