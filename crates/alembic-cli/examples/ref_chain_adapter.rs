use alembic_core::{JsonMap, Schema, TypeName};
use alembic_engine::{
    alembic_external_main, build_key_from_schema, normalize_attrs_refs, resolve_ref_keyed_identity,
    ApplyReport, ExternalAdapter, ExternalCapabilities, ExternalObject, ExternalRole, Op, RawNode,
    StateData, StateMappings,
};
use anyhow::{anyhow, Result};
use serde_json::json;

alembic_external_main!(RefChainAdapter::default());

/// a backend holding a site, a device keyed on a ref to it, and an interface
/// keyed on a ref to the device, with every ref stored as a backend id. reads
/// resolve those ids through the engine's `resolve_ref_keyed_identity`, like
/// the built-in adapters do.
#[derive(Debug, Default)]
pub struct RefChainAdapter {}

/// appends each read to `$REF_CHAIN_ADAPTER_LOG`, so a test can count them; a
/// no-op when the variable is unset.
fn record(method: &str) {
    use std::io::Write;
    let Ok(path) = std::env::var("REF_CHAIN_ADAPTER_LOG") else {
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

impl ExternalAdapter for RefChainAdapter {
    fn setup(&mut self, _configuration: &serde_yaml::Value) -> Result<()> {
        Ok(())
    }

    fn read(
        &mut self,
        schema: &Schema,
        _types: &[TypeName],
        state: &StateData,
    ) -> Result<Vec<ExternalObject>> {
        record("read");
        let mut mappings = StateMappings::default();
        for (type_name, mapping) in &state.mappings {
            for (uid, backend_id) in mapping {
                mappings.insert(type_name.as_str(), backend_id.clone(), *uid);
            }
        }

        let raw: Vec<RawNode> = rows()
            .into_iter()
            .map(|(type_name, backend_id, attrs)| RawNode {
                type_name: TypeName::new(type_name),
                backend_id: alembic_engine::BackendId::Int(backend_id),
                attrs: attrs_of(attrs),
            })
            .collect();

        Ok(resolve_ref_keyed_identity(
            &raw,
            schema,
            &mut mappings,
            |node, type_schema, mappings| normalize_attrs_refs(&node.attrs, type_schema, mappings),
            |node, type_schema, attrs| {
                build_key_from_schema(type_schema, attrs)
                    .map_err(|err| anyhow!("build key for {}: {err}", node.type_name))
            },
        )?
        .into_iter()
        .map(|object| ExternalObject {
            type_name: object.type_name,
            key: object.key,
            attrs: object.attrs,
            backend_id: object.backend_id,
        })
        .collect())
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
