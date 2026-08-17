use alembic_core::{JsonMap, Schema, TypeName};
use alembic_engine::{
    alembic_external_main, build_key_from_schema, ApplyReport, BackendId, ExternalAdapter,
    ExternalCapabilities, ExternalObject, ExternalRole, Op, StateData,
};
use anyhow::{anyhow, Result};
use serde_json::json;

alembic_external_main!(RawRefAdapter::default());

/// the backend `ref_chain_adapter` reads, read without resolving: every
/// ref-typed field comes back holding the backend's own id.
#[derive(Debug, Default)]
pub struct RawRefAdapter {}

fn rows() -> Vec<(&'static str, u64, serde_json::Value)> {
    vec![
        ("dcim.site", 1, json!({ "slug": "fra1", "name": "FRA1" })),
        ("dcim.device", 2, json!({ "site": 1, "name": "leaf01" })),
        ("dcim.interface", 3, json!({ "device": 2, "name": "eth0" })),
    ]
}

fn attrs_of(value: serde_json::Value) -> JsonMap {
    let serde_json::Value::Object(map) = value else {
        unreachable!("rows are objects");
    };
    map.into_iter()
        .collect::<std::collections::BTreeMap<_, _>>()
        .into()
}

impl ExternalAdapter for RawRefAdapter {
    fn setup(&mut self, _configuration: &serde_yaml::Value) -> Result<()> {
        Ok(())
    }

    fn read(
        &mut self,
        schema: &Schema,
        _types: &[TypeName],
        _state: &StateData,
    ) -> Result<Vec<ExternalObject>> {
        rows()
            .into_iter()
            .map(|(type_name, backend_id, attrs)| {
                let type_schema = schema
                    .types
                    .get(type_name)
                    .ok_or_else(|| anyhow!("{type_name} is not declared"))?;
                let attrs = attrs_of(attrs);
                Ok(ExternalObject {
                    type_name: TypeName::new(type_name),
                    key: build_key_from_schema(type_schema, &attrs)?,
                    attrs,
                    backend_id: Some(BackendId::Int(backend_id)),
                })
            })
            .collect()
    }

    fn write(&mut self, _schema: &Schema, _ops: &[Op], _state: &StateData) -> Result<ApplyReport> {
        Err(anyhow!("read-only"))
    }

    fn capabilities(&mut self) -> ExternalCapabilities {
        ExternalCapabilities {
            role: ExternalRole::Observer,
        }
    }
}
