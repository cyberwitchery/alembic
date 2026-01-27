//! peeringdb adapter for alembic.
//!
//! Uses the peeringdb-rs crate to fetch data from PeeringDB.
//! Set the `PEERINGDB_API_KEY` environment variable to authenticate.

use alembic_core::{JsonMap, Key, Schema, TypeName};
use alembic_engine::{
    Adapter, ApplyReport, BackendId, ObservedObject, ObservedState, Op, ProjectionData,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

/// Supported PeeringDB types.
const SUPPORTED_TYPES: &[&str] = &[
    "peeringdb.ix",
    "peeringdb.net",
    "peeringdb.org",
    "peeringdb.netixlan",
];

pub struct PeeringDBAdapter;

impl PeeringDBAdapter {
    /// Create a new PeeringDB adapter.
    ///
    /// Authentication is handled via the `PEERINGDB_API_KEY` environment variable.
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
impl Adapter for PeeringDBAdapter {
    async fn observe(&self, schema: &Schema, types: &[TypeName]) -> Result<ObservedState> {
        let requested: BTreeSet<TypeName> = if types.is_empty() {
            SUPPORTED_TYPES.iter().map(|s| TypeName::new(*s)).collect()
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
                _ => continue, // Skip unsupported types
            };

            for object in objects {
                state.insert(object);
            }
        }

        Ok(state)
    }

    async fn apply(&self, _schema: &Schema, _ops: &[Op]) -> Result<ApplyReport> {
        Err(anyhow!("PeeringDB adapter is read-only"))
    }
}

/// Trait for PeeringDB objects that have an id field.
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
            projection: ProjectionData::default(),
            backend_id: Some(BackendId::Int(id as u64)),
        });
    }

    Ok(objects)
}

fn build_key_from_schema(type_schema: &alembic_core::TypeSchema, attrs: &JsonMap) -> Result<Key> {
    let mut map = BTreeMap::new();
    for field in type_schema.key.keys() {
        let Some(value) = attrs.get(field) else {
            return Err(anyhow!("missing key field {field}"));
        };
        map.insert(field.clone(), value.clone());
    }
    Ok(Key::from(map))
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

    fn test_schema() -> Schema {
        Schema {
            types: BTreeMap::from([("peeringdb.ix".to_string(), ix_schema())]),
        }
    }

    #[test]
    fn new_creates_adapter() {
        let _adapter = PeeringDBAdapter::new();
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

    #[tokio::test]
    async fn apply_returns_read_only_error() {
        let adapter = PeeringDBAdapter::new();
        let schema = test_schema();
        let err = adapter.apply(&schema, &[]).await.unwrap_err();
        assert!(err.to_string().contains("read-only"));
    }

    #[tokio::test]
    async fn observe_errors_on_missing_schema() {
        let adapter = PeeringDBAdapter::new();
        let schema = Schema {
            types: BTreeMap::new(),
        };
        let err = adapter
            .observe(&schema, &[TypeName::new("peeringdb.ix")])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("missing schema"));
    }

    #[tokio::test]
    async fn observe_skips_unsupported_types() {
        let adapter = PeeringDBAdapter::new();
        let schema = Schema {
            types: BTreeMap::from([("peeringdb.unsupported".to_string(), ix_schema())]),
        };
        let state = adapter
            .observe(&schema, &[TypeName::new("peeringdb.unsupported")])
            .await
            .unwrap();

        assert_eq!(state.by_key.len(), 0);
    }
}
