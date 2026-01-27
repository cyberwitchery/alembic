//! netbox adapter implementation.

mod client;
mod mapping;
mod ops;
mod registry;
mod state;

use alembic_engine::{MissingCustomField, StateStore};
use anyhow::{anyhow, Result};
use std::collections::BTreeSet;
use std::sync::MutexGuard;

#[cfg(test)]
use alembic_engine::Adapter;
use client::NetBoxClient;
use mapping::*;

/// netbox adapter that maps ir objects to netbox api calls.
pub struct NetBoxAdapter {
    client: NetBoxClient,
    state: std::sync::Mutex<StateStore>,
}

impl NetBoxAdapter {
    /// create a new adapter with url, token, and state store.
    pub fn new(url: &str, token: &str, state: StateStore) -> Result<Self> {
        let client = NetBoxClient::new(url, token)?;
        Ok(Self {
            client,
            state: std::sync::Mutex::new(state),
        })
    }

    pub async fn create_custom_fields(&self, missing: &[MissingCustomField]) -> Result<()> {
        let grouped = group_custom_fields(missing);
        for (field, entry) in grouped {
            let request = netbox::extras::CreateCustomFieldRequest {
                object_types: entry.object_types.into_iter().collect(),
                r#type: entry.field_type,
                name: field.clone(),
                related_object_type: None,
                label: Some(field),
                group_name: None,
                description: None,
                required: None,
                unique: None,
                search_weight: None,
                filter_logic: None,
                ui_visible: None,
                ui_editable: None,
                is_cloneable: None,
                default: None,
                related_object_filter: None,
                weight: None,
                validation_minimum: None,
                validation_maximum: None,
                validation_regex: None,
                choice_set: None,
                comments: None,
            };
            let _ = self
                .client
                .extras()
                .custom_fields()
                .create(&request)
                .await?;
        }
        Ok(())
    }

    pub async fn create_tags(&self, tags: &[String]) -> Result<()> {
        let unique: BTreeSet<String> = tags.iter().cloned().collect();
        for tag in unique {
            let request = netbox::extras::CreateTagRequest {
                name: tag.clone(),
                slug: slugify(&tag),
                color: None,
                description: None,
                weight: None,
                object_types: None,
            };
            let _ = self.client.extras().tags().create(&request).await?;
        }
        Ok(())
    }

    fn state_guard(&self) -> Result<MutexGuard<'_, StateStore>> {
        self.state
            .lock()
            .map_err(|_| anyhow!("state lock poisoned"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alembic_core::{JsonMap, Key, TypeName, Uid};
    use alembic_engine::{Op, ProjectedObject, ProjectionData};
    use httpmock::Method::{GET, POST};
    use httpmock::{Mock, MockServer};
    use serde_json::json;
    use std::collections::BTreeSet;
    use tempfile::tempdir;
    use uuid::Uuid;

    fn uid(value: u128) -> Uid {
        Uuid::from_u128(value)
    }

    fn attrs_map(value: serde_json::Value) -> JsonMap {
        let serde_json::Value::Object(map) = value else {
            panic!("attrs must be a json object");
        };
        map.into_iter()
            .collect::<std::collections::BTreeMap<_, _>>()
            .into()
    }

    fn key(field: &str, value: serde_json::Value) -> Key {
        let mut map = std::collections::BTreeMap::new();
        map.insert(field.to_string(), value);
        Key::from(map)
    }

    fn obj(uid: Uid, type_name: &str, key: Key, attrs: serde_json::Value) -> alembic_core::Object {
        alembic_core::Object::new(uid, TypeName::new(type_name), key, attrs_map(attrs)).unwrap()
    }

    fn projected(base: alembic_core::Object) -> ProjectedObject {
        ProjectedObject {
            base,
            projection: ProjectionData::default(),
            projection_inputs: BTreeSet::new(),
        }
    }

    fn page(results: serde_json::Value) -> serde_json::Value {
        json!({
            "count": results.as_array().map(|a| a.len()).unwrap_or(0),
            "next": null,
            "previous": null,
            "results": results
        })
    }

    fn state_with_mappings(path: &std::path::Path) -> StateStore {
        let mut store = StateStore::load(path).unwrap();
        store.set_backend_id(
            TypeName::new("dcim.site"),
            uid(1),
            alembic_engine::BackendId::Int(1),
        );
        store
    }

    fn mock_list<'a>(
        server: &'a MockServer,
        path: &'a str,
        payload: serde_json::Value,
    ) -> Mock<'a> {
        server.mock(|when, then| {
            when.method(GET)
                .path(path)
                .query_param("limit", "200")
                .query_param("offset", "0");
            then.status(200).json_body(page(payload));
        })
    }

    #[tokio::test]
    async fn observe_maps_nested_refs_to_uids() {
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        let state = state_with_mappings(&dir.path().join("state.json"));
        let adapter = NetBoxAdapter::new(&server.base_url(), "token", state).unwrap();

        let _object_types = mock_list(
            &server,
            "/api/core/object-types/",
            json!([
                {
                    "app_label": "dcim",
                    "model": "device",
                    "rest_api_endpoint": "/api/dcim/devices/",
                    "features": ["custom-fields", "tags"]
                },
                {
                    "app_label": "dcim",
                    "model": "site",
                    "rest_api_endpoint": "/api/dcim/sites/",
                    "features": ["custom-fields", "tags"]
                }
            ]),
        );
        let _devices = mock_list(
            &server,
            "/api/dcim/devices/",
            json!([
                {
                    "id": 2,
                    "name": "leaf01",
                    "site": {
                        "id": 1,
                        "url": "https://netbox.example.com/api/dcim/sites/1/",
                        "name": "FRA1",
                        "slug": "fra1"
                    }
                }
            ]),
        );
        let _custom_fields = server.mock(|when, then| {
            when.method(GET).path("/api/extras/custom-fields/");
            then.status(200).json_body(page(json!([])));
        });
        let _tags = server.mock(|when, then| {
            when.method(GET)
                .path("/api/extras/tags/")
                .query_param("limit", "200")
                .query_param("offset", "0");
            then.status(200)
                .json_body(page(json!([{"id": 1, "name": "fabric", "slug": "fabric"}])));
        });

        let schema = alembic_core::Schema {
            types: std::collections::BTreeMap::from([
                (
                    "dcim.device".to_string(),
                    alembic_core::TypeSchema {
                        key: std::collections::BTreeMap::from([(
                            "name".to_string(),
                            alembic_core::FieldSchema {
                                r#type: alembic_core::FieldType::String,
                                required: true,
                                nullable: false,
                                description: None,
                            },
                        )]),
                        fields: std::collections::BTreeMap::new(),
                    },
                ),
                (
                    "dcim.site".to_string(),
                    alembic_core::TypeSchema {
                        key: std::collections::BTreeMap::from([(
                            "name".to_string(),
                            alembic_core::FieldSchema {
                                r#type: alembic_core::FieldType::String,
                                required: true,
                                nullable: false,
                                description: None,
                            },
                        )]),
                        fields: std::collections::BTreeMap::new(),
                    },
                ),
            ]),
        };
        let observed = adapter
            .observe(&schema, &[TypeName::new("dcim.device")])
            .await
            .unwrap();

        let device = observed
            .by_key
            .get(&(TypeName::new("dcim.device"), "name=leaf01".to_string()))
            .unwrap();
        let site_uid = uid(1).to_string();
        assert_eq!(
            device.attrs.get("name").and_then(|v| v.as_str()),
            Some("leaf01")
        );
        assert_eq!(
            device.attrs.get("site").and_then(|v| v.as_str()),
            Some(site_uid.as_str())
        );
        assert!(observed.capabilities.tags.contains("fabric"));
    }

    #[tokio::test]
    async fn apply_orders_creates_by_dependency() {
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        let state = StateStore::load(dir.path().join("state.json")).unwrap();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token", state).unwrap();

        let _object_types = mock_list(
            &server,
            "/api/core/object-types/",
            json!([
                {
                    "app_label": "dcim",
                    "model": "site",
                    "rest_api_endpoint": "/api/dcim/sites/",
                    "features": ["custom-fields", "tags"]
                },
                {
                    "app_label": "dcim",
                    "model": "device",
                    "rest_api_endpoint": "/api/dcim/devices/",
                    "features": ["custom-fields", "tags"]
                }
            ]),
        );
        let _site_create = server.mock(|when, then| {
            when.method(POST)
                .path("/api/dcim/sites/")
                .json_body(json!({ "name": "FRA1", "slug": "fra1" }));
            then.status(201)
                .json_body(json!({ "id": 1, "name": "FRA1", "slug": "fra1" }));
        });
        let _device_create = server.mock(|when, then| {
            when.method(POST)
                .path("/api/dcim/devices/")
                .json_body(json!({ "name": "leaf01", "site": 1 }));
            then.status(201)
                .json_body(json!({ "id": 2, "name": "leaf01" }));
        });

        let ops = vec![
            Op::Create {
                uid: uid(2),
                type_name: TypeName::new("dcim.device"),
                desired: projected(obj(
                    uid(2),
                    "dcim.device",
                    key("name", json!("leaf01")),
                    json!({
                        "name": "leaf01",
                        "site": uid(1).to_string()
                    }),
                )),
            },
            Op::Create {
                uid: uid(1),
                type_name: TypeName::new("dcim.site"),
                desired: projected(obj(
                    uid(1),
                    "dcim.site",
                    key("name", json!("fra1")),
                    json!({ "name": "FRA1", "slug": "fra1" }),
                )),
            },
        ];

        let schema = alembic_core::Schema {
            types: std::collections::BTreeMap::from([
                (
                    "dcim.device".to_string(),
                    alembic_core::TypeSchema {
                        key: std::collections::BTreeMap::from([(
                            "name".to_string(),
                            alembic_core::FieldSchema {
                                r#type: alembic_core::FieldType::String,
                                required: true,
                                nullable: false,
                                description: None,
                            },
                        )]),
                        fields: std::collections::BTreeMap::from([
                            (
                                "name".to_string(),
                                alembic_core::FieldSchema {
                                    r#type: alembic_core::FieldType::String,
                                    required: true,
                                    nullable: false,
                                    description: None,
                                },
                            ),
                            (
                                "site".to_string(),
                                alembic_core::FieldSchema {
                                    r#type: alembic_core::FieldType::Ref {
                                        target: "dcim.site".to_string(),
                                    },
                                    required: true,
                                    nullable: false,
                                    description: None,
                                },
                            ),
                        ]),
                    },
                ),
                (
                    "dcim.site".to_string(),
                    alembic_core::TypeSchema {
                        key: std::collections::BTreeMap::from([(
                            "name".to_string(),
                            alembic_core::FieldSchema {
                                r#type: alembic_core::FieldType::String,
                                required: true,
                                nullable: false,
                                description: None,
                            },
                        )]),
                        fields: std::collections::BTreeMap::from([
                            (
                                "name".to_string(),
                                alembic_core::FieldSchema {
                                    r#type: alembic_core::FieldType::String,
                                    required: true,
                                    nullable: false,
                                    description: None,
                                },
                            ),
                            (
                                "slug".to_string(),
                                alembic_core::FieldSchema {
                                    r#type: alembic_core::FieldType::String,
                                    required: true,
                                    nullable: false,
                                    description: None,
                                },
                            ),
                        ]),
                    },
                ),
            ]),
        };
        let report = adapter.apply(&schema, &ops).await.unwrap();
        assert_eq!(report.applied.len(), 2);
    }

    #[tokio::test]
    async fn create_tags_posts_unique_names() {
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        let state = StateStore::load(dir.path().join("state.json")).unwrap();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token", state).unwrap();

        let _tags = server.mock(|when, then| {
            when.method(POST).path("/api/extras/tags/");
            then.status(201)
                .json_body(json!({"id": 1, "name": "fabric", "slug": "fabric"}));
        });

        adapter
            .create_tags(&["fabric".to_string(), "fabric".to_string()])
            .await
            .unwrap();
    }

    #[test]
    fn slugify_normalizes_value() {
        assert_eq!(slugify("EVPN Fabric"), "evpn-fabric");
        assert_eq!(slugify("edge--core"), "edge-core");
    }

    #[test]
    fn build_tag_inputs_uses_slugify() {
        let tags = vec!["EVPN Fabric".to_string()];
        let inputs = build_tag_inputs(&tags);
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].name, "EVPN Fabric");
        assert_eq!(inputs[0].slug, "evpn-fabric");
    }

    #[tokio::test]
    async fn apply_handles_update_operation() {
        use alembic_engine::FieldChange;
        use httpmock::Method::PATCH;

        let server = MockServer::start();
        let dir = tempdir().unwrap();
        let mut state = StateStore::load(dir.path().join("state.json")).unwrap();
        state.set_backend_id(
            TypeName::new("dcim.site"),
            uid(1),
            alembic_engine::BackendId::Int(1),
        );
        let adapter = NetBoxAdapter::new(&server.base_url(), "token", state).unwrap();

        let _object_types = mock_list(
            &server,
            "/api/core/object-types/",
            json!([{
                "app_label": "dcim",
                "model": "site",
                "rest_api_endpoint": "/api/dcim/sites/",
                "features": ["custom-fields", "tags"]
            }]),
        );
        let _site_update = server.mock(|when, then| {
            when.method(PATCH).path("/api/dcim/sites/1/");
            then.status(200)
                .json_body(json!({ "id": 1, "name": "FRA1-Updated", "slug": "fra1" }));
        });

        let mut key = std::collections::BTreeMap::new();
        key.insert("slug".to_string(), json!("fra1"));
        let mut attrs = std::collections::BTreeMap::new();
        attrs.insert("name".to_string(), json!("FRA1-Updated"));

        let ops = vec![Op::Update {
            uid: uid(1),
            type_name: TypeName::new("dcim.site"),
            backend_id: Some(alembic_engine::BackendId::Int(1)),
            desired: alembic_engine::ProjectedObject {
                base: alembic_core::Object {
                    uid: uid(1),
                    type_name: TypeName::new("dcim.site"),
                    key: alembic_core::Key::from(key),
                    attrs: alembic_core::JsonMap::from(attrs),
                    source: None,
                },
                projection: alembic_engine::ProjectionData::default(),
                projection_inputs: std::collections::BTreeSet::new(),
            },
            changes: vec![FieldChange {
                field: "name".to_string(),
                from: json!("FRA1"),
                to: json!("FRA1-Updated"),
            }],
        }];

        let schema = alembic_core::Schema {
            types: std::collections::BTreeMap::from([(
                "dcim.site".to_string(),
                alembic_core::TypeSchema {
                    key: std::collections::BTreeMap::from([(
                        "slug".to_string(),
                        alembic_core::FieldSchema {
                            r#type: alembic_core::FieldType::String,
                            required: true,
                            nullable: false,
                            description: None,
                        },
                    )]),
                    fields: std::collections::BTreeMap::from([(
                        "name".to_string(),
                        alembic_core::FieldSchema {
                            r#type: alembic_core::FieldType::String,
                            required: true,
                            nullable: false,
                            description: None,
                        },
                    )]),
                },
            )]),
        };
        let report = adapter.apply(&schema, &ops).await.unwrap();
        assert_eq!(report.applied.len(), 1);
    }

    #[tokio::test]
    async fn apply_handles_delete_operation() {
        use httpmock::Method::DELETE;

        let server = MockServer::start();
        let dir = tempdir().unwrap();
        let mut state = StateStore::load(dir.path().join("state.json")).unwrap();
        state.set_backend_id(
            TypeName::new("dcim.site"),
            uid(1),
            alembic_engine::BackendId::Int(1),
        );
        let adapter = NetBoxAdapter::new(&server.base_url(), "token", state).unwrap();

        let _object_types = mock_list(
            &server,
            "/api/core/object-types/",
            json!([{
                "app_label": "dcim",
                "model": "site",
                "rest_api_endpoint": "/api/dcim/sites/",
                "features": ["custom-fields", "tags"]
            }]),
        );
        let _site_delete = server.mock(|when, then| {
            when.method(DELETE).path("/api/dcim/sites/");
            then.status(204);
        });

        let ops = vec![Op::Delete {
            uid: uid(1),
            type_name: TypeName::new("dcim.site"),
            key: key("slug", json!("fra1")),
            backend_id: Some(alembic_engine::BackendId::Int(1)),
        }];

        let schema = alembic_core::Schema {
            types: std::collections::BTreeMap::from([(
                "dcim.site".to_string(),
                alembic_core::TypeSchema {
                    key: std::collections::BTreeMap::from([(
                        "slug".to_string(),
                        alembic_core::FieldSchema {
                            r#type: alembic_core::FieldType::String,
                            required: true,
                            nullable: false,
                            description: None,
                        },
                    )]),
                    fields: std::collections::BTreeMap::new(),
                },
            )]),
        };
        let report = adapter.apply(&schema, &ops).await.unwrap();
        assert_eq!(report.applied.len(), 1);
    }

    #[tokio::test]
    async fn observe_handles_empty_types_list() {
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        let state = StateStore::load(dir.path().join("state.json")).unwrap();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token", state).unwrap();

        let _object_types = mock_list(
            &server,
            "/api/core/object-types/",
            json!([{
                "app_label": "dcim",
                "model": "site",
                "rest_api_endpoint": "/api/dcim/sites/",
                "features": ["custom-fields", "tags"]
            }]),
        );
        let _sites = mock_list(&server, "/api/dcim/sites/", json!([]));
        let _custom_fields = server.mock(|when, then| {
            when.method(GET).path("/api/extras/custom-fields/");
            then.status(200).json_body(page(json!([])));
        });
        let _tags = server.mock(|when, then| {
            when.method(GET)
                .path("/api/extras/tags/")
                .query_param("limit", "200")
                .query_param("offset", "0");
            then.status(200).json_body(page(json!([])));
        });

        let schema = alembic_core::Schema {
            types: std::collections::BTreeMap::from([(
                "dcim.site".to_string(),
                alembic_core::TypeSchema {
                    key: std::collections::BTreeMap::from([(
                        "slug".to_string(),
                        alembic_core::FieldSchema {
                            r#type: alembic_core::FieldType::String,
                            required: true,
                            nullable: false,
                            description: None,
                        },
                    )]),
                    fields: std::collections::BTreeMap::new(),
                },
            )]),
        };
        // Empty types list should observe all types from registry
        let observed = adapter.observe(&schema, &[]).await.unwrap();
        assert!(observed.by_key.is_empty());
    }

    #[tokio::test]
    async fn create_custom_fields_works() {
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        let state = StateStore::load(dir.path().join("state.json")).unwrap();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token", state).unwrap();

        let _cf_create = server.mock(|when, then| {
            when.method(POST).path("/api/extras/custom-fields/");
            then.status(201).json_body(json!({
                "id": 1,
                "url": "http://localhost/api/extras/custom-fields/1/",
                "display_url": "http://localhost/extras/custom-fields/1/",
                "display": "alembic_test",
                "name": "alembic_test",
                "label": "alembic_test",
                "object_types": ["dcim.site"],
                "type": {"value": "text", "label": "Text"},
                "related_object_type": null,
                "group_name": "",
                "description": "",
                "required": false,
                "unique": false,
                "search_weight": 1000,
                "filter_logic": {"value": "loose", "label": "Loose"},
                "ui_visible": {"value": "always", "label": "Always"},
                "ui_editable": {"value": "yes", "label": "Yes"},
                "is_cloneable": true,
                "default": null,
                "related_object_filter": null,
                "weight": 100,
                "validation_minimum": null,
                "validation_maximum": null,
                "validation_regex": "",
                "choice_set": null,
                "comments": "",
                "created": "2024-01-01T00:00:00Z",
                "last_updated": "2024-01-01T00:00:00Z"
            }));
        });

        let missing = vec![alembic_engine::MissingCustomField {
            rule: "test".to_string(),
            type_name: "dcim.site".to_string(),
            attr_key: "cf_test".to_string(),
            field: "alembic_test".to_string(),
            sample: json!("example"),
        }];

        adapter.create_custom_fields(&missing).await.unwrap();
    }
}
