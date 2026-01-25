//! peeringdb adapter for alembic.

use alembic_core::{JsonMap, Key, Schema, TypeName};
use alembic_engine::{
    Adapter, ApplyReport, BackendId, ObservedObject, ObservedState, Op, ProjectionData,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub struct PeeringDBAdapter {
    client: reqwest::Client,
    base_url: String,
}

impl PeeringDBAdapter {
    pub fn new(api_key: Option<&str>) -> Result<Self> {
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(key) = api_key {
            let mut val = reqwest::header::HeaderValue::from_str(&format!("Api-Key {}", key))?;
            val.set_sensitive(true);
            headers.insert("Authorization", val);
        }

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;

        Ok(Self {
            client,
            base_url: "https://www.peeringdb.com/api".to_string(),
        })
    }
}

#[async_trait]
impl Adapter for PeeringDBAdapter {
    async fn observe(&self, schema: &Schema, types: &[TypeName]) -> Result<ObservedState> {
        let mut state = ObservedState::default();
        let requested: BTreeSet<TypeName> = if types.is_empty() {
            vec![
                TypeName::new("peeringdb.ix"),
                TypeName::new("peeringdb.net"),
            ]
            .into_iter()
            .collect()
        } else {
            types.iter().cloned().collect()
        };

        let mut tasks = Vec::new();
        for type_name in requested {
            let type_schema = schema
                .types
                .get(type_name.as_str())
                .ok_or_else(|| anyhow!("missing schema for {}", type_name))?
                .clone();

            let client = self.client.clone();
            let url = match type_name.as_str() {
                "peeringdb.ix" => format!("{}/ix", self.base_url),
                "peeringdb.net" => format!("{}/net", self.base_url),
                _ => continue, // Skip types we don't support
            };

            tasks.push(tokio::spawn(async move {
                let resp = client.get(&url).send().await?.error_for_status()?;
                let body: Value = resp.json().await?;

                let results = body["data"]
                    .as_array()
                    .ok_or_else(|| anyhow!("expected data array in peeringdb response"))?;

                let mut observed = Vec::new();
                for item in results {
                    let id = item["id"]
                        .as_u64()
                        .ok_or_else(|| anyhow!("missing id in peeringdb object"))?;

                    let attrs: JsonMap = match item {
                        Value::Object(map) => {
                            map.clone().into_iter().collect::<BTreeMap<_, _>>().into()
                        }
                        _ => continue,
                    };

                    let key = build_key_from_schema(&type_schema, &attrs)?;

                    observed.push(ObservedObject {
                        type_name: type_name.clone(),
                        key,
                        attrs,
                        projection: ProjectionData::default(),
                        backend_id: Some(BackendId::Int(id)),
                    });
                }
                Ok::<Vec<ObservedObject>, anyhow::Error>(observed)
            }));
        }

        let results = futures::future::join_all(tasks).await;
        for result in results {
            let objects = result??;
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
    use alembic_engine::Adapter;
    use httpmock::prelude::*;
    use serde_json::json;

    fn ix_schema() -> TypeSchema {
        TypeSchema {
            key: BTreeMap::from([(
                "name".to_string(),
                FieldSchema {
                    r#type: FieldType::String,
                    required: true,
                    nullable: false,
                    description: None,
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
    fn new_creates_adapter_without_key() {
        let adapter = PeeringDBAdapter::new(None);
        assert!(adapter.is_ok());
    }

    #[test]
    fn new_creates_adapter_with_key() {
        let adapter = PeeringDBAdapter::new(Some("test-api-key"));
        assert!(adapter.is_ok());
    }

    #[test]
    fn build_key_extracts_fields() {
        let schema = ix_schema();
        let attrs: JsonMap = BTreeMap::from([
            ("id".to_string(), json!(1)),
            ("name".to_string(), json!("DE-CIX Frankfurt")),
        ])
        .into();
        let key = build_key_from_schema(&schema, &attrs).unwrap();
        assert_eq!(key.get("name"), Some(&json!("DE-CIX Frankfurt")));
    }

    #[test]
    fn build_key_errors_on_missing_field() {
        let schema = ix_schema();
        let attrs: JsonMap = BTreeMap::from([("id".to_string(), json!(1))]).into();
        let err = build_key_from_schema(&schema, &attrs).unwrap_err();
        assert!(err.to_string().contains("missing key field name"));
    }

    #[tokio::test]
    async fn apply_returns_read_only_error() {
        let adapter = PeeringDBAdapter::new(None).unwrap();
        let schema = test_schema();
        let err = adapter.apply(&schema, &[]).await.unwrap_err();
        assert!(err.to_string().contains("read-only"));
    }

    #[tokio::test]
    async fn observe_fetches_ix_data() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method(GET).path("/ix");
            then.status(200).json_body(json!({
                "data": [
                    {"id": 1, "name": "DE-CIX Frankfurt"},
                    {"id": 2, "name": "AMS-IX"}
                ]
            }));
        });

        let mut adapter = PeeringDBAdapter::new(None).unwrap();
        adapter.base_url = server.base_url();

        let schema = test_schema();
        let state = adapter
            .observe(&schema, &[TypeName::new("peeringdb.ix")])
            .await
            .unwrap();

        assert_eq!(state.by_key.len(), 2);
    }

    #[tokio::test]
    async fn observe_errors_on_missing_schema() {
        let adapter = PeeringDBAdapter::new(None).unwrap();
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
    async fn observe_errors_on_invalid_response() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method(GET).path("/ix");
            then.status(200).json_body(json!({"invalid": "response"}));
        });

        let mut adapter = PeeringDBAdapter::new(None).unwrap();
        adapter.base_url = server.base_url();

        let schema = test_schema();
        let err = adapter
            .observe(&schema, &[TypeName::new("peeringdb.ix")])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("expected data array"));
    }

    #[tokio::test]
    async fn observe_errors_on_missing_id() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method(GET).path("/ix");
            then.status(200).json_body(json!({
                "data": [{"name": "DE-CIX Frankfurt"}]
            }));
        });

        let mut adapter = PeeringDBAdapter::new(None).unwrap();
        adapter.base_url = server.base_url();

        let schema = test_schema();
        let err = adapter
            .observe(&schema, &[TypeName::new("peeringdb.ix")])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("missing id"));
    }

    #[tokio::test]
    async fn observe_skips_unsupported_types() {
        let server = MockServer::start();

        // No mock needed - unsupported types are skipped
        let mut adapter = PeeringDBAdapter::new(None).unwrap();
        adapter.base_url = server.base_url();

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
