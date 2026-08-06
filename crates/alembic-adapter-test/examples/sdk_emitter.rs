//! an sdk-based emitter, the shape every emit-only adapter in the org ships.
//! the suite must stay green against it: `run_external_adapter` rejects an
//! unsupported version before setup and before method dispatch, so the version
//! probe is answered whichever method it rides.

use alembic_core::{Schema, TypeName};
use alembic_engine::{
    alembic_external_main, ApplyReport, ExternalAdapter, ExternalCapabilities, ExternalObject,
    ExternalRole, Op, StateData,
};
use anyhow::Result;

alembic_external_main!(SdkEmitter::default());

#[derive(Debug, Default)]
pub struct SdkEmitter {}

impl ExternalAdapter for SdkEmitter {
    fn setup(&mut self, _configuration: &serde_yaml::Value) -> Result<()> {
        Ok(())
    }

    fn read(
        &mut self,
        _schema: &Schema,
        _types: &[TypeName],
        _state: &StateData,
    ) -> Result<Vec<ExternalObject>> {
        anyhow::bail!("this adapter is write-only; it cannot observe state")
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
