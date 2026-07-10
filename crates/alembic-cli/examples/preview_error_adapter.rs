use alembic_core::{Schema, TypeName};
use alembic_engine::{
    alembic_external_main, ApplyReport, ExternalAdapter, ExternalObject, Op, ProvisionReport,
    StateData,
};
use anyhow::Result;

// this will define the main function of the crate
alembic_external_main!(PreviewErrorAdapter::default());

#[derive(Debug, Default)]
pub struct PreviewErrorAdapter {}

impl PreviewErrorAdapter {}

impl ExternalAdapter for PreviewErrorAdapter {
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

    // override just the preview to fail, so the cli's provision guard sees an
    // Err from preview_schema; ensure_schema stays defaulted (Ok), which is what
    // makes the pre-fix code provision blind.
    fn preview_schema(&mut self, _schema: &Schema) -> Result<Option<ProvisionReport>> {
        Err(anyhow::anyhow!("preview failed for test"))
    }
}
