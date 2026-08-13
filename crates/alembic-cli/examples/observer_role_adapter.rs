use alembic_core::{Schema, TypeName};
use alembic_engine::{
    alembic_external_main, ApplyReport, ExternalAdapter, ExternalCapabilities, ExternalObject,
    ExternalRole, Op, ProvisionReport, StateData,
};
use anyhow::Result;

alembic_external_main!(ObserverRoleAdapter::default());

#[derive(Debug, Default)]
pub struct ObserverRoleAdapter {}

/// appends each method the host sends to `$OBSERVER_ROLE_ADAPTER_LOG`, so a test
/// can assert which ones arrive; a no-op when the variable is unset.
fn record(method: &str) {
    use std::io::Write;
    let Ok(path) = std::env::var("OBSERVER_ROLE_ADAPTER_LOG") else {
        return;
    };
    if let Ok(mut log) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        let _ = writeln!(log, "{method}");
    }
}

impl ExternalAdapter for ObserverRoleAdapter {
    fn setup(&mut self, _configuration: &serde_yaml::Value) -> Result<()> {
        Ok(())
    }

    fn read(
        &mut self,
        _schema: &Schema,
        _types: &[TypeName],
        _state: &StateData,
    ) -> Result<Vec<ExternalObject>> {
        record("read");
        Ok(vec![])
    }

    // write is stubbed to succeed, so a host that ignores the declared role
    // applies instead of failing.
    fn write(&mut self, _schema: &Schema, _ops: &[Op], _state: &StateData) -> Result<ApplyReport> {
        record("write");
        Ok(ApplyReport::default())
    }

    // the provisioning methods answer exactly what the trait defaults answer and
    // only record: an observer is never asked for schema, and the log proves it.
    fn ensure_schema(&mut self, _schema: &Schema) -> Result<ProvisionReport> {
        record("ensure_schema");
        Ok(ProvisionReport::default())
    }

    fn preview_schema(&mut self, _schema: &Schema) -> Result<Option<ProvisionReport>> {
        record("preview_schema");
        Ok(None)
    }

    fn capabilities(&mut self) -> ExternalCapabilities {
        record("capabilities");
        ExternalCapabilities {
            role: ExternalRole::Observer,
        }
    }
}
