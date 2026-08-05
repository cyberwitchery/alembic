use alembic_core::{Schema, TypeName};
use alembic_engine::{
    alembic_external_main, ApplyReport, ExternalAdapter, ExternalCapabilities, ExternalObject,
    ExternalRole, Op, StateData,
};
use anyhow::Result;

alembic_external_main!(EmitterRoleAdapter::default());

#[derive(Debug, Default)]
pub struct EmitterRoleAdapter {}

impl ExternalAdapter for EmitterRoleAdapter {
    fn setup(&mut self, _configuration: &serde_yaml::Value) -> Result<()> {
        Ok(())
    }

    // read is stubbed to succeed with nothing, so a host that ignores the
    // declared role observes an empty backend instead of failing.
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

    fn capabilities(&mut self) -> ExternalCapabilities {
        ExternalCapabilities {
            role: ExternalRole::Emitter,
        }
    }
}
