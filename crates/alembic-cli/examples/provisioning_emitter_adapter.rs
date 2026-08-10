use alembic_core::{Schema, TypeName};
use alembic_engine::{
    alembic_external_main, ApplyReport, ExternalAdapter, ExternalCapabilities, ExternalObject,
    ExternalRole, Op, ProvisionReport, StateData,
};
use anyhow::Result;

alembic_external_main!(ProvisioningEmitterAdapter::default());

// an emit-only adapter that provisions: the declared role governs read vs write,
// not whether the host may ask it for schema.
#[derive(Debug, Default)]
pub struct ProvisioningEmitterAdapter {}

impl ExternalAdapter for ProvisioningEmitterAdapter {
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

    fn ensure_schema(&mut self, schema: &Schema) -> Result<ProvisionReport> {
        Ok(ProvisionReport {
            created_object_types: schema.types.keys().map(|ty| ty.to_string()).collect(),
            ..Default::default()
        })
    }

    fn preview_schema(&mut self, schema: &Schema) -> Result<Option<ProvisionReport>> {
        Ok(Some(ProvisionReport {
            created_object_types: schema.types.keys().map(|ty| ty.to_string()).collect(),
            ..Default::default()
        }))
    }

    fn capabilities(&mut self) -> ExternalCapabilities {
        ExternalCapabilities {
            role: ExternalRole::Emitter,
        }
    }
}
