//! peeringdb adapter for alembic.
//!
//! uses the peeringdb-rs crate to fetch data from PeeringDB.
//! set the `PEERINGDB_API_KEY` environment variable to authenticate.

use alembic_core::{JsonMap, Schema, TypeName};
use alembic_engine::{build_key_from_schema, BackendId, ObservedObject, ObservedState, Observer};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

/// supported PeeringDB types.
const SUPPORTED_TYPES: &[&str] = &[
    "peeringdb.ix",
    "peeringdb.net",
    "peeringdb.org",
    "peeringdb.netixlan",
];

pub struct PeeringDBAdapter;

impl PeeringDBAdapter {
    /// create a new PeeringDB adapter.
    ///
    /// authentication is handled via the `PEERINGDB_API_KEY` environment variable.
    pub fn new() -> Self {
        Self
    }
}

impl Default for PeeringDBAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Observer for PeeringDBAdapter {
    async fn read(
        &self,
        schema: &Schema,
        types: &[TypeName],
        _state: &alembic_engine::StateStore,
    ) -> Result<ObservedState> {
        let requested: BTreeSet<TypeName> = if types.is_empty() {
            // empty means every schema-declared supported type; skip types the schema omits.
            SUPPORTED_TYPES
                .iter()
                .map(|s| TypeName::new(*s))
                .filter(|tn| schema.types.contains_key(tn.as_str()))
                .collect()
        } else {
            types.iter().cloned().collect()
        };

        let mut state = ObservedState::default();

        for type_name in requested {
            let type_schema = schema
                .types
                .get(type_name.as_str())
                .ok_or_else(|| anyhow!("missing schema for {}", type_name))?
                .clone();

            let objects = match type_name.as_str() {
                "peeringdb.ix" => {
                    let data = tokio::task::spawn_blocking(peeringdb_rs::load_peeringdb_ix)
                        .await?
                        .map_err(|e| anyhow!("failed to load ix data: {}", e))?;
                    to_observed_objects(&type_name, &type_schema, data)?
                }
                "peeringdb.net" => {
                    let data = tokio::task::spawn_blocking(peeringdb_rs::load_peeringdb_net)
                        .await?
                        .map_err(|e| anyhow!("failed to load net data: {}", e))?;
                    to_observed_objects(&type_name, &type_schema, data)?
                }
                "peeringdb.org" => {
                    let data = tokio::task::spawn_blocking(peeringdb_rs::load_peeringdb_org)
                        .await?
                        .map_err(|e| anyhow!("failed to load org data: {}", e))?;
                    to_observed_objects(&type_name, &type_schema, data)?
                }
                "peeringdb.netixlan" => {
                    let data = tokio::task::spawn_blocking(peeringdb_rs::load_peeringdb_netixlan)
                        .await?
                        .map_err(|e| anyhow!("failed to load netixlan data: {}", e))?;
                    to_observed_objects(&type_name, &type_schema, data)?
                }
                _ => continue, // skip unsupported types
            };

            for object in objects {
                state.insert(object)?;
            }
        }

        Ok(state)
    }
}

/// trait for PeeringDB objects that have an id field.
trait HasId {
    fn id(&self) -> u32;
}

impl HasId for peeringdb_rs::PeeringdbIx {
    fn id(&self) -> u32 {
        self.id
    }
}

impl HasId for peeringdb_rs::PeeringdbNet {
    fn id(&self) -> u32 {
        self.id
    }
}

impl HasId for peeringdb_rs::PeeringdbOrg {
    fn id(&self) -> u32 {
        self.id
    }
}

impl HasId for peeringdb_rs::PeeringdbNetixlan {
    fn id(&self) -> u32 {
        self.id
    }
}

fn to_observed_objects<T: Serialize + HasId>(
    type_name: &TypeName,
    type_schema: &alembic_core::TypeSchema,
    items: Vec<T>,
) -> Result<Vec<ObservedObject>> {
    let mut objects = Vec::new();

    for item in items {
        let id = item.id();
        let value = serde_json::to_value(&item)?;
        let attrs: JsonMap = match value {
            serde_json::Value::Object(map) => map.into_iter().collect::<BTreeMap<_, _>>().into(),
            _ => return Err(anyhow!("expected object from serialization")),
        };

        let key = build_key_from_schema(type_schema, &attrs)?;

        objects.push(ObservedObject {
            type_name: type_name.clone(),
            key,
            attrs,
            backend_id: Some(BackendId::Int(id as u64)),
        });
    }

    Ok(objects)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alembic_core::{FieldSchema, FieldType, TypeSchema};

    fn ix_schema() -> TypeSchema {
        TypeSchema {
            key: BTreeMap::from([(
                "name".to_string(),
                FieldSchema {
                    r#type: FieldType::String,
                    required: true,
                    nullable: false,
                    description: None,
                    format: None,
                    pattern: None,
                },
            )]),
            fields: BTreeMap::new(),
        }
    }

    #[test]
    fn build_key_extracts_fields() {
        let schema = ix_schema();
        let attrs: JsonMap = BTreeMap::from([
            ("id".to_string(), serde_json::json!(1)),
            ("name".to_string(), serde_json::json!("DE-CIX Frankfurt")),
        ])
        .into();
        let key = build_key_from_schema(&schema, &attrs).unwrap();
        assert_eq!(
            key.get("name"),
            Some(&serde_json::json!("DE-CIX Frankfurt"))
        );
    }

    #[test]
    fn build_key_errors_on_missing_field() {
        let schema = ix_schema();
        let attrs: JsonMap = BTreeMap::from([("id".to_string(), serde_json::json!(1))]).into();
        let err = build_key_from_schema(&schema, &attrs).unwrap_err();
        assert!(err.to_string().contains("missing key field name"));
    }

    fn sample_ix(id: u32, name: &str) -> peeringdb_rs::PeeringdbIx {
        peeringdb_rs::PeeringdbIx {
            id,
            org_id_id: None,
            name: Some(name.to_string()),
            aka: None,
            name_long: None,
            city: None,
            country: None,
            region_continent: None,
            media: None,
            notes: None,
            proto_unicast: None,
            proto_multicast: None,
            proto_ipv6: None,
            website: None,
            url_stats: None,
            status_dashboard: None,
            tech_email: None,
            tech_phone: None,
            policy_email: None,
            policy_phone: None,
            sales_email: None,
            sales_phone: None,
            ixf_net_count: None,
            ixf_last_import: None,
            ixf_import_request: None,
            ixf_import_request_status: None,
            net_count: None,
            fac_count: None,
            service_level: None,
            terms: None,
            created: None,
            updated: None,
            status: None,
        }
    }

    #[test]
    fn to_observed_objects_maps_id_key_and_attrs() {
        let type_name = TypeName::new("peeringdb.ix");
        let schema = ix_schema();
        let objects =
            to_observed_objects(&type_name, &schema, vec![sample_ix(42, "DE-CIX Frankfurt")])
                .unwrap();

        assert_eq!(objects.len(), 1);
        let object = &objects[0];
        assert_eq!(object.type_name, type_name);
        assert_eq!(object.backend_id, Some(BackendId::Int(42)));
        assert_eq!(
            object.key.get("name"),
            Some(&serde_json::json!("DE-CIX Frankfurt"))
        );
        assert_eq!(
            object.attrs.get("name"),
            Some(&serde_json::json!("DE-CIX Frankfurt"))
        );
    }

    #[tokio::test]
    async fn observe_errors_on_missing_schema() {
        let adapter = PeeringDBAdapter::new();
        let schema = Schema {
            types: BTreeMap::new(),
        };
        let state = alembic_engine::StateStore::new(None, alembic_engine::StateData::default());
        let err = adapter
            .read(&schema, &[TypeName::new("peeringdb.ix")], &state)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("missing schema"));
    }

    #[tokio::test]
    async fn observe_empty_types_skips_undeclared_types() {
        // empty types + a schema that declares none of them must skip (not error);
        // the explicit-type path still errors (observe_errors_on_missing_schema).
        let adapter = PeeringDBAdapter::new();
        let schema = Schema {
            types: BTreeMap::new(),
        };
        let state = alembic_engine::StateStore::new(None, alembic_engine::StateData::default());
        let observed = adapter.read(&schema, &[], &state).await.unwrap();
        assert!(observed.by_key.is_empty());
    }

    #[tokio::test]
    async fn observe_skips_unsupported_types() {
        let adapter = PeeringDBAdapter::new();
        let schema = Schema {
            types: BTreeMap::from([("peeringdb.unsupported".to_string(), ix_schema())]),
        };
        let state_store =
            alembic_engine::StateStore::new(None, alembic_engine::StateData::default());
        let state = adapter
            .read(
                &schema,
                &[TypeName::new("peeringdb.unsupported")],
                &state_store,
            )
            .await
            .unwrap();

        assert_eq!(state.by_key.len(), 0);
    }
}
