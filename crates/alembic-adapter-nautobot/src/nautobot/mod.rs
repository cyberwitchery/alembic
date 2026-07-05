//! nautobot adapter implementation.

mod client;
mod mapping;
mod ops;
mod registry;
mod state;

use anyhow::Result;
use std::sync::Arc;

use client::NautobotClient;

/// nautobot adapter that maps ir objects to nautobot api calls.
pub struct NautobotAdapter {
    client: Arc<NautobotClient>,
}

impl NautobotAdapter {
    /// create a new adapter with url, token, and state store.
    pub fn new(url: &str, token: &str) -> Result<Self> {
        let client = Arc::new(NautobotClient::new(url, token)?);
        Ok(Self { client })
    }
}

#[cfg(test)]
mod tests {
    use super::NautobotAdapter;
    use alembic_core::{
        FieldSchema, FieldType, JsonMap, Key, Object, Schema, TypeName, TypeSchema, Uid,
    };
    use alembic_engine::{Adapter, BackendId, Emitter, FieldChange, Observer, Op, StateStore};
    use httpmock::Method::{DELETE, GET, PATCH, POST};
    use httpmock::MockServer;
    use serde_json::json;
    use std::collections::BTreeMap;
    use tempfile::tempdir;
    use uuid::Uuid;

    fn uid(value: u128) -> Uid {
        Uuid::from_u128(value)
    }

    // an optional string field; key fields reuse this shape.
    fn field(field_type: FieldType) -> FieldSchema {
        FieldSchema {
            r#type: field_type,
            required: false,
            nullable: true,
            description: None,
            format: None,
            pattern: None,
        }
    }

    fn attrs(value: serde_json::Value) -> JsonMap {
        let serde_json::Value::Object(map) = value else {
            panic!("attrs must be a json object");
        };
        map.into_iter().collect::<BTreeMap<_, _>>().into()
    }

    fn key(field: &str, value: serde_json::Value) -> Key {
        Key::from(BTreeMap::from([(field.to_string(), value)]))
    }

    fn page(results: serde_json::Value) -> serde_json::Value {
        json!({
            "count": results.as_array().map(|a| a.len()).unwrap_or(0),
            "next": null,
            "previous": null,
            "results": results,
        })
    }

    // a single dcim.site type, keyed on name, with name + slug fields.
    fn site_schema() -> Schema {
        Schema {
            types: BTreeMap::from([(
                "dcim.site".to_string(),
                TypeSchema {
                    key: BTreeMap::from([("name".to_string(), field(FieldType::String))]),
                    fields: BTreeMap::from([
                        ("name".to_string(), field(FieldType::String)),
                        ("slug".to_string(), field(FieldType::String)),
                    ]),
                },
            )]),
        }
    }

    // content-types drives the endpoint registry: "dcim"/"site" -> dcim/sites/.
    fn mock_content_types(server: &MockServer) {
        let _m = server.mock(|when, then| {
            when.method(GET).path("/api/extras/content-types/");
            then.status(200)
                .json_body(page(json!([{ "app_label": "dcim", "model": "site" }])));
        });
    }

    fn mock_empty_custom_fields(server: &MockServer) {
        let _m = server.mock(|when, then| {
            when.method(GET).path("/api/extras/custom-fields/");
            then.status(200).json_body(page(json!([])));
        });
    }

    fn state(dir: &std::path::Path) -> StateStore {
        StateStore::load(dir.join("state.json")).unwrap()
    }

    #[tokio::test]
    async fn observe_reads_objects_and_maps_backend_id() {
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        mock_content_types(&server);
        let site_id = "11111111-1111-1111-1111-111111111111";
        let _sites = server.mock(|when, then| {
            when.method(GET).path("/api/dcim/sites/");
            then.status(200).json_body(page(json!([{
                "id": site_id,
                "url": "https://nautobot.example.com/api/dcim/sites/11111111-1111-1111-1111-111111111111/",
                "name": "FRA1",
                "slug": "fra1",
            }])));
        });

        let adapter = NautobotAdapter::new(&server.base_url(), "token").unwrap();
        let observed = adapter
            .read(
                &site_schema(),
                &[TypeName::new("dcim.site")],
                &state(dir.path()),
            )
            .await
            .unwrap();

        assert_eq!(observed.by_key.len(), 1);
        let object = observed.by_key.values().next().unwrap();
        assert_eq!(object.attrs.get("name"), Some(&json!("FRA1")));
        // nautobot backend ids are uuid strings, not ints.
        assert_eq!(
            object.backend_id,
            Some(BackendId::String(site_id.to_string()))
        );
    }

    #[tokio::test]
    async fn apply_creates_object() {
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        mock_content_types(&server);
        mock_empty_custom_fields(&server);
        let site_id = "11111111-1111-1111-1111-111111111111";
        let _create = server.mock(|when, then| {
            when.method(POST).path("/api/dcim/sites/");
            then.status(201)
                .json_body(json!({ "id": site_id, "name": "FRA1", "slug": "fra1" }));
        });

        let ops = vec![Op::Create {
            uid: uid(1),
            type_name: TypeName::new("dcim.site"),
            desired: Object::new(
                uid(1),
                TypeName::new("dcim.site"),
                key("name", json!("FRA1")),
                attrs(json!({ "name": "FRA1", "slug": "fra1" })),
            )
            .unwrap(),
        }];

        let adapter = NautobotAdapter::new(&server.base_url(), "token").unwrap();
        let report = adapter
            .write(&site_schema(), &ops, &state(dir.path()))
            .await
            .unwrap();

        assert_eq!(report.applied.len(), 1);
        assert_eq!(
            report.applied[0].backend_id,
            Some(BackendId::String(site_id.to_string()))
        );
    }

    #[tokio::test]
    async fn apply_updates_object() {
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        mock_content_types(&server);
        mock_empty_custom_fields(&server);
        let site_id = "22222222-2222-2222-2222-222222222222";
        let _patch = server.mock(|when, then| {
            when.method(PATCH)
                .path(format!("/api/dcim/sites/{site_id}/"));
            then.status(200)
                .json_body(json!({ "id": site_id, "name": "FRA1-Updated" }));
        });

        let ops = vec![Op::Update {
            uid: uid(1),
            type_name: TypeName::new("dcim.site"),
            backend_id: Some(BackendId::String(site_id.to_string())),
            desired: Object::new(
                uid(1),
                TypeName::new("dcim.site"),
                key("name", json!("FRA1-Updated")),
                attrs(json!({ "name": "FRA1-Updated" })),
            )
            .unwrap(),
            changes: vec![FieldChange {
                field: "name".to_string(),
                from: json!("FRA1"),
                to: json!("FRA1-Updated"),
            }],
        }];

        let adapter = NautobotAdapter::new(&server.base_url(), "token").unwrap();
        let report = adapter
            .write(&site_schema(), &ops, &state(dir.path()))
            .await
            .unwrap();

        assert_eq!(report.applied.len(), 1);
    }

    #[tokio::test]
    async fn apply_deletes_object() {
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        mock_content_types(&server);
        mock_empty_custom_fields(&server);
        let site_id = "33333333-3333-3333-3333-333333333333";
        let _delete = server.mock(|when, then| {
            when.method(DELETE)
                .path(format!("/api/dcim/sites/{site_id}/"));
            then.status(204);
        });

        let ops = vec![Op::Delete {
            uid: uid(1),
            type_name: TypeName::new("dcim.site"),
            key: key("name", json!("FRA1")),
            backend_id: Some(BackendId::String(site_id.to_string())),
        }];

        let adapter = NautobotAdapter::new(&server.base_url(), "token").unwrap();
        let report = adapter
            .write(&site_schema(), &ops, &state(dir.path()))
            .await
            .unwrap();

        assert_eq!(report.applied.len(), 1);
    }

    #[tokio::test]
    async fn ensure_schema_creates_custom_fields() {
        let server = MockServer::start();
        mock_content_types(&server);
        mock_empty_custom_fields(&server);
        // native-field probe: one object, none present.
        let _probe = server.mock(|when, then| {
            when.method(GET).path("/api/dcim/sites/");
            then.status(200).json_body(page(json!([])));
        });
        let _cf_create = server.mock(|when, then| {
            when.method(POST).path("/api/extras/custom-fields/");
            then.status(201).json_body(json!({
                "id": "44444444-4444-4444-4444-444444444444",
                "label": "asset_tag",
                "content_types": ["dcim.site"],
                "type": {},
            }));
        });

        // asset_tag is neither a key field nor in the native allowlist, so it
        // must be provisioned as a custom field.
        let schema = Schema {
            types: BTreeMap::from([(
                "dcim.site".to_string(),
                TypeSchema {
                    key: BTreeMap::from([("name".to_string(), field(FieldType::String))]),
                    fields: BTreeMap::from([("asset_tag".to_string(), field(FieldType::String))]),
                },
            )]),
        };

        let adapter = NautobotAdapter::new(&server.base_url(), "token").unwrap();
        let report = adapter.ensure_schema(&schema).await.unwrap();

        assert_eq!(
            report.created_fields,
            vec!["dcim.site.asset_tag".to_string()]
        );
    }

    #[tokio::test]
    async fn preview_schema_reports_without_creating() {
        let server = MockServer::start();
        mock_content_types(&server);
        mock_empty_custom_fields(&server);
        // native-field probe: one object, none present.
        let _probe = server.mock(|when, then| {
            when.method(GET).path("/api/dcim/sites/");
            then.status(200).json_body(page(json!([])));
        });
        // preview must never touch the custom-field create endpoint.
        let cf_create = server.mock(|when, then| {
            when.method(POST).path("/api/extras/custom-fields/");
            then.status(201).json_body(json!({
                "id": "44444444-4444-4444-4444-444444444444",
                "label": "asset_tag",
                "content_types": ["dcim.site"],
                "type": {},
            }));
        });

        let schema = Schema {
            types: BTreeMap::from([(
                "dcim.site".to_string(),
                TypeSchema {
                    key: BTreeMap::from([("name".to_string(), field(FieldType::String))]),
                    fields: BTreeMap::from([("asset_tag".to_string(), field(FieldType::String))]),
                },
            )]),
        };

        let adapter = NautobotAdapter::new(&server.base_url(), "token").unwrap();
        let report = adapter.preview_schema(&schema).await.unwrap().unwrap();

        // same missing field ensure_schema would create, computed from the same reads.
        assert_eq!(
            report.created_fields,
            vec!["dcim.site.asset_tag".to_string()]
        );
        // read-only: the custom-field create endpoint saw zero writes.
        cf_create.assert_calls(0);
    }
}
