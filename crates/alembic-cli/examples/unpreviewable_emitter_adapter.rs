use alembic_core::{Schema, TypeName};
use alembic_engine::{
    alembic_external_main, ApplyReport, ExternalAdapter, ExternalCapabilities, ExternalObject,
    ExternalRole, Op, ProvisionReport, StateData,
};
use anyhow::Result;

alembic_external_main!(UnpreviewableEmitterAdapter::default());

// an adapter that deletes schema and declares it cannot preview: the gate has
// nothing to gate on, so provisioning is refused rather than run blind.
#[derive(Debug, Default)]
pub struct UnpreviewableEmitterAdapter {}

impl ExternalAdapter for UnpreviewableEmitterAdapter {
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

    fn ensure_schema(&mut self, _schema: &Schema) -> Result<ProvisionReport> {
        Ok(ProvisionReport {
            deleted_object_types: vec!["dcim.fossil".to_string()],
            deleted_object_fields: vec!["dcim.fossil.age".to_string()],
            ..Default::default()
        })
    }

    fn preview_schema(&mut self, _schema: &Schema) -> Result<Option<ProvisionReport>> {
        Ok(None)
    }

    fn capabilities(&mut self) -> ExternalCapabilities {
        ExternalCapabilities {
            role: ExternalRole::Emitter,
        }
    }
}
