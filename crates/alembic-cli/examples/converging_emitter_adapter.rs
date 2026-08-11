use alembic_core::{Schema, TypeName};
use alembic_engine::{
    alembic_external_main, ApplyReport, ExternalAdapter, ExternalCapabilities, ExternalObject,
    ExternalRole, Op, ProvisionReport, StateData,
};
use anyhow::Result;

alembic_external_main!(ConvergingEmitterAdapter::default());

// an adapter that provisions schema it did not create: one create plus an update,
// a deprecation and two deletes, which is the set the cli names rather than counts.
#[derive(Debug, Default)]
pub struct ConvergingEmitterAdapter {}

fn converged() -> ProvisionReport {
    ProvisionReport {
        created_object_types: vec!["dcim.widget".to_string()],
        updated_object_fields: vec!["dcim.gadget.color".to_string()],
        deprecated_object_types: vec!["dcim.relic".to_string()],
        deleted_object_types: vec!["dcim.fossil".to_string()],
        deleted_object_fields: vec!["dcim.fossil.age".to_string()],
        ..Default::default()
    }
}

impl ExternalAdapter for ConvergingEmitterAdapter {
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
        Ok(converged())
    }

    fn preview_schema(&mut self, _schema: &Schema) -> Result<Option<ProvisionReport>> {
        Ok(Some(converged()))
    }

    fn capabilities(&mut self) -> ExternalCapabilities {
        ExternalCapabilities {
            role: ExternalRole::Emitter,
        }
    }
}
