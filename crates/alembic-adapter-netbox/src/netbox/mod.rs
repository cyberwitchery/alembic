//! netbox adapter implementation.

mod client;
mod mapping;
mod ops;
mod registry;
mod state;

use anyhow::Result;

#[cfg(test)]
use alembic_engine::StateStore;
#[cfg(test)]
use alembic_engine::{Adapter, Emitter, Observer};
use client::NetBoxClient;

/// netbox adapter that maps ir objects to netbox api calls.
pub struct NetBoxAdapter {
    client: NetBoxClient,
}

impl NetBoxAdapter {
    /// create a new adapter with url, token, and state store.
    pub fn new(url: &str, token: &str) -> Result<Self> {
        let client = NetBoxClient::new(url, token)?;
        Ok(Self { client })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::netbox::mapping::{build_tag_inputs, slugify};
    use alembic_core::{key_string, JsonMap, Key, TypeName, Uid};
    use alembic_engine::journal::Journal;
    use alembic_engine::Op;
    use httpmock::Method::{GET, POST};
    use httpmock::{Mock, MockServer};
    use serde_json::json;
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
        let adapter = NetBoxAdapter::new(&server.base_url(), "token").unwrap();

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
                                format: None,
                                pattern: None,
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
                                format: None,
                                pattern: None,
                            },
                        )]),
                        fields: std::collections::BTreeMap::new(),
                    },
                ),
            ]),
        };
        let observed = adapter
            .read(&schema, &[TypeName::new("dcim.device")], &state)
            .await
            .unwrap();

        let device = observed
            .by_key
            .get(&(
                TypeName::new("dcim.device"),
                key_string(&key("name", json!("leaf01"))),
            ))
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
    }

    #[tokio::test]
    async fn apply_orders_creates_by_dependency() {
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        let state = StateStore::load(dir.path().join("state.json")).unwrap();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token").unwrap();

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
        let _custom_fields = server.mock(|when, then| {
            when.method(GET).path("/api/extras/custom-fields/");
            then.status(200).json_body(page(json!([])));
        });
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
                desired: obj(
                    uid(2),
                    "dcim.device",
                    key("name", json!("leaf01")),
                    json!({
                        "name": "leaf01",
                        "site": uid(1).to_string()
                    }),
                ),
            },
            Op::Create {
                uid: uid(1),
                type_name: TypeName::new("dcim.site"),
                desired: obj(
                    uid(1),
                    "dcim.site",
                    key("name", json!("fra1")),
                    json!({ "name": "FRA1", "slug": "fra1" }),
                ),
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
                                format: None,
                                pattern: None,
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
                                    format: None,
                                    pattern: None,
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
                                    format: None,
                                    pattern: None,
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
                                format: None,
                                pattern: None,
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
                                    format: None,
                                    pattern: None,
                                },
                            ),
                            (
                                "slug".to_string(),
                                alembic_core::FieldSchema {
                                    r#type: alembic_core::FieldType::String,
                                    required: true,
                                    nullable: false,
                                    description: None,
                                    format: None,
                                    pattern: None,
                                },
                            ),
                        ]),
                    },
                ),
            ]),
        };
        let mut journal = Journal::new_ephemeral(&ops);
        let report = adapter
            .write(&schema, &ops, &mut journal, &state)
            .await
            .unwrap();
        assert_eq!(report.applied.len(), 2);
    }

    #[tokio::test]
    async fn cables() {
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        let state = StateStore::load(dir.path().join("state.json")).unwrap();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token").unwrap();

        let _object_types = mock_list(
            &server,
            "/api/core/object-types/",
            json!([
                {
                    "app_label": "dcim",
                    "model": "cable",
                    "rest_api_endpoint": "/api/dcim/cables/",
                    "features": ["custom-fields", "tags"]
                },
                {
                    "app_label": "dcim",
                    "model": "interface",
                    "rest_api_endpoint": "/api/dcim/interfaces/",
                    "features": ["custom-fields", "tags"]
                }
            ]),
        );
        let _custom_fields = server.mock(|when, then| {
            when.method(GET).path("/api/extras/custom-fields/");
            then.status(200).json_body(page(json!([])));
        });
        let _interface_1_create = server.mock(|when, then| {
            when.method(POST)
                .path("/api/dcim/interfaces/")
                .json_body(json!({ "name": "eth01" }));
            then.status(201)
                .json_body(json!({ "id": 1, "name": "eth01" }));
        });
        let _interface_2_create = server.mock(|when, then| {
            when.method(POST)
                .path("/api/dcim/interfaces/")
                .json_body(json!({ "name": "eth02" }));
            then.status(201)
                .json_body(json!({ "id": 2, "name": "eth02" }));
        });
        let _cable_create = server.mock(|when, then| {
            when.method(POST)
                .path("/api/dcim/cables/")
                .json_body(json!({
                    "label": "cable01",
                    "a_terminations": [{ "object_id": 1, "object_type": "dcim.interface" }],
                    "b_terminations": [{ "object_id": 2, "object_type": "dcim.interface" }],
                }));
            then.status(201).json_body(json!({
                "id": 3,
                "label": "cable01",
            }));
        });

        let ops = vec![
            Op::Create {
                uid: uid(1),
                type_name: TypeName::new("dcim.interface"),
                desired: obj(
                    uid(1),
                    "dcim.interface",
                    key("name", json!("eth01")),
                    json!({
                        "name": "eth01",
                    }),
                ),
            },
            Op::Create {
                uid: uid(2),
                type_name: TypeName::new("dcim.interface"),
                desired: obj(
                    uid(2),
                    "dcim.interface",
                    key("name", json!("eth02")),
                    json!({
                        "name": "eth02",
                    }),
                ),
            },
            Op::Create {
                uid: uid(3),
                type_name: TypeName::new("dcim.cable"),
                desired: obj(
                    uid(3),
                    "dcim.cable",
                    key("label", json!("cable01")),
                    json!({
                        "label": "cable01",
                        "a_terminations": json!([uid(1)]),
                        "b_terminations": json!([uid(2)]),
                    }),
                ),
            },
        ];

        let schema = alembic_core::Schema {
            types: std::collections::BTreeMap::from([
                (
                    "dcim.cable".to_string(),
                    alembic_core::TypeSchema {
                        key: std::collections::BTreeMap::from([(
                            "label".to_string(),
                            alembic_core::FieldSchema {
                                r#type: alembic_core::FieldType::Slug,
                                required: true,
                                nullable: false,
                                description: None,
                                format: None,
                                pattern: None,
                            },
                        )]),
                        fields: std::collections::BTreeMap::from([
                            (
                                "label".to_string(),
                                alembic_core::FieldSchema {
                                    r#type: alembic_core::FieldType::Slug,
                                    required: true,
                                    nullable: false,
                                    description: None,
                                    format: None,
                                    pattern: None,
                                },
                            ),
                            (
                                "a_terminations".to_string(),
                                alembic_core::FieldSchema {
                                    r#type: alembic_core::FieldType::ListRef {
                                        target: "dcim.interface".to_string(),
                                    },
                                    required: true,
                                    nullable: false,
                                    description: None,
                                    format: None,
                                    pattern: None,
                                },
                            ),
                            (
                                "b_terminations".to_string(),
                                alembic_core::FieldSchema {
                                    r#type: alembic_core::FieldType::ListRef {
                                        target: "dcim.interface".to_string(),
                                    },
                                    required: true,
                                    nullable: false,
                                    description: None,
                                    format: None,
                                    pattern: None,
                                },
                            ),
                        ]),
                    },
                ),
                (
                    "dcim.interface".to_string(),
                    alembic_core::TypeSchema {
                        key: std::collections::BTreeMap::from([(
                            "name".to_string(),
                            alembic_core::FieldSchema {
                                r#type: alembic_core::FieldType::String,
                                required: true,
                                nullable: false,
                                description: None,
                                format: None,
                                pattern: None,
                            },
                        )]),
                        fields: std::collections::BTreeMap::from([(
                            "name".to_string(),
                            alembic_core::FieldSchema {
                                r#type: alembic_core::FieldType::String,
                                required: true,
                                nullable: false,
                                description: None,
                                format: None,
                                pattern: None,
                            },
                        )]),
                    },
                ),
            ]),
        };
        let mut journal = Journal::new_ephemeral(&ops);
        let report = adapter
            .write(&schema, &ops, &mut journal, &state)
            .await
            .unwrap();
        assert_eq!(report.applied.len(), 3);
    }

    #[tokio::test]
    async fn apply_creates_missing_tags() {
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        let state = StateStore::load(dir.path().join("state.json")).unwrap();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token").unwrap();

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
        let _tag_create = server.mock(|when, then| {
            when.method(POST).path("/api/extras/tags/");
            then.status(201)
                .json_body(json!({"id": 1, "name": "fabric", "slug": "fabric"}));
        });
        let _site_create = server.mock(|when, then| {
            when.method(POST).path("/api/dcim/sites/");
            then.status(201)
                .json_body(json!({ "id": 1, "name": "FRA1", "slug": "fra1" }));
        });

        let mut attrs = std::collections::BTreeMap::new();
        attrs.insert("name".to_string(), json!("FRA1"));
        attrs.insert("slug".to_string(), json!("fra1"));
        attrs.insert("tags".to_string(), json!(["fabric"]));

        let ops = vec![Op::Create {
            uid: uid(1),
            type_name: TypeName::new("dcim.site"),
            desired: alembic_core::Object {
                uid: uid(1),
                type_name: TypeName::new("dcim.site"),
                key: Key::default(),
                attrs: alembic_core::JsonMap::from(attrs),
                source: None,
            },
        }];

        let schema = alembic_core::Schema {
            types: std::collections::BTreeMap::from([(
                "dcim.site".to_string(),
                alembic_core::TypeSchema {
                    key: std::collections::BTreeMap::new(),
                    fields: std::collections::BTreeMap::from([
                        (
                            "name".to_string(),
                            alembic_core::FieldSchema {
                                r#type: alembic_core::FieldType::String,
                                required: true,
                                nullable: false,
                                description: None,
                                format: None,
                                pattern: None,
                            },
                        ),
                        (
                            "slug".to_string(),
                            alembic_core::FieldSchema {
                                r#type: alembic_core::FieldType::String,
                                required: true,
                                nullable: false,
                                description: None,
                                format: None,
                                pattern: None,
                            },
                        ),
                        (
                            "tags".to_string(),
                            alembic_core::FieldSchema {
                                r#type: alembic_core::FieldType::List {
                                    item: Box::new(alembic_core::FieldType::String),
                                },
                                required: false,
                                nullable: true,
                                description: None,
                                format: None,
                                pattern: None,
                            },
                        ),
                    ]),
                },
            )]),
        };

        let mut journal = Journal::new_ephemeral(&ops);
        let report = adapter
            .write(&schema, &ops, &mut journal, &state)
            .await
            .unwrap();
        assert_eq!(report.applied.len(), 1);
    }

    #[tokio::test]
    async fn fetch_tags_paginates_across_all_pages() {
        let server = MockServer::start();
        let client = NetBoxClient::new(&server.base_url(), "token").unwrap();

        // 250 unique tags spread across two pages exceeds the per-request
        // limit of 200, so fetch_tags must follow pagination to see them all.
        let total = 250usize;
        let limit = 200usize;
        let tag = |i: usize| json!({ "id": i, "name": format!("tag-{i:04}"), "slug": format!("tag-{i:04}") });
        let first: Vec<serde_json::Value> = (0..limit).map(tag).collect();
        let second: Vec<serde_json::Value> = (limit..total).map(tag).collect();

        let _page_one = server.mock(|when, then| {
            when.method(GET)
                .path("/api/extras/tags/")
                .query_param("limit", "200")
                .query_param("offset", "0");
            then.status(200).json_body(json!({
                "count": total,
                "next": null,
                "previous": null,
                "results": first,
            }));
        });
        let _page_two = server.mock(|when, then| {
            when.method(GET)
                .path("/api/extras/tags/")
                .query_param("limit", "200")
                .query_param("offset", "200");
            then.status(200).json_body(json!({
                "count": total,
                "next": null,
                "previous": null,
                "results": second,
            }));
        });

        let tags = client.fetch_tags().await.unwrap();

        assert_eq!(tags.len(), total);
        // first/last of each page, proving both requests were made rather than
        // stopping after the first page.
        assert!(tags.contains("tag-0000"));
        assert!(tags.contains("tag-0199"));
        assert!(tags.contains("tag-0200"));
        assert!(tags.contains("tag-0249"));
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
        let adapter = NetBoxAdapter::new(&server.base_url(), "token").unwrap();

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
        let _custom_fields = server.mock(|when, then| {
            when.method(GET).path("/api/extras/custom-fields/");
            then.status(200).json_body(page(json!([])));
        });
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
            desired: alembic_core::Object {
                uid: uid(1),
                type_name: TypeName::new("dcim.site"),
                key: alembic_core::Key::from(key),
                attrs: alembic_core::JsonMap::from(attrs),
                source: None,
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
                            format: None,
                            pattern: None,
                        },
                    )]),
                    fields: std::collections::BTreeMap::from([(
                        "name".to_string(),
                        alembic_core::FieldSchema {
                            r#type: alembic_core::FieldType::String,
                            required: true,
                            nullable: false,
                            description: None,
                            format: None,
                            pattern: None,
                        },
                    )]),
                },
            )]),
        };
        let mut journal = Journal::new_ephemeral(&ops);
        let report = adapter
            .write(&schema, &ops, &mut journal, &state)
            .await
            .unwrap();
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
        let adapter = NetBoxAdapter::new(&server.base_url(), "token").unwrap();

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
        let _custom_fields = server.mock(|when, then| {
            when.method(GET).path("/api/extras/custom-fields/");
            then.status(200).json_body(page(json!([])));
        });
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
                            format: None,
                            pattern: None,
                        },
                    )]),
                    fields: std::collections::BTreeMap::new(),
                },
            )]),
        };
        let mut journal = Journal::new_ephemeral(&ops);
        let report = adapter
            .write(&schema, &ops, &mut journal, &state)
            .await
            .unwrap();
        assert_eq!(report.applied.len(), 1);
    }

    #[tokio::test]
    async fn observe_handles_empty_types_list() {
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        let state = StateStore::load(dir.path().join("state.json")).unwrap();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token").unwrap();

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
                            format: None,
                            pattern: None,
                        },
                    )]),
                    fields: std::collections::BTreeMap::new(),
                },
            )]),
        };
        // empty types list should observe all types from registry
        let observed = adapter.read(&schema, &[], &state).await.unwrap();
        assert!(observed.by_key.is_empty());
    }

    #[tokio::test]
    async fn ensure_schema_creates_custom_fields() {
        let server = MockServer::start();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token").unwrap();

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
        let _custom_fields = server.mock(|when, then| {
            when.method(GET).path("/api/extras/custom-fields/");
            then.status(200).json_body(page(json!([])));
        });
        let _sites = server.mock(|when, then| {
            when.method(GET)
                .path("/api/dcim/sites/")
                .query_param("limit", "1");
            then.status(200).json_body(page(json!([])));
        });
        let _cf_create = server.mock(|when, then| {
            when.method(POST).path("/api/extras/custom-fields/");
            then.status(201).json_body(json!({
                "id": 1,
                "name": "cf_test",
                "object_types": ["dcim.site"],
                "type": {"value": "text", "label": "Text"}
            }));
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
                            format: None,
                            pattern: None,
                        },
                    )]),
                    fields: std::collections::BTreeMap::from([(
                        "cf_test".to_string(),
                        alembic_core::FieldSchema {
                            r#type: alembic_core::FieldType::String,
                            required: false,
                            nullable: true,
                            description: None,
                            format: None,
                            pattern: None,
                        },
                    )]),
                },
            )]),
        };

        let report = adapter.ensure_schema(&schema).await.unwrap();
        assert_eq!(report.created_fields, vec!["dcim.site.cf_test".to_string()]);
    }

    #[tokio::test]
    async fn ensure_schema_creates_custom_object_types() {
        let server = MockServer::start();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token").unwrap();

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
        let _custom_fields = server.mock(|when, then| {
            when.method(GET).path("/api/extras/custom-fields/");
            then.status(200).json_body(page(json!([])));
        });
        let _custom_object_types = server.mock(|when, then| {
            when.method(GET)
                .path("/api/plugins/custom-objects/custom-object-types/")
                .query_param("limit", "200")
                .query_param("offset", "0");
            then.status(200).json_body(page(json!([])));
        });
        let _custom_object_fields = server.mock(|when, then| {
            when.method(GET)
                .path("/api/plugins/custom-objects/custom-object-type-fields/")
                .query_param("limit", "200")
                .query_param("offset", "0");
            then.status(200).json_body(page(json!([])));
        });
        let _custom_object_type_create = server.mock(|when, then| {
            when.method(POST)
                .path("/api/plugins/custom-objects/custom-object-types/");
            then.status(201).json_body(json!({
                "id": 42,
                "name": "custom-asset"
            }));
        });
        let _custom_object_field_create = server.mock(|when, then| {
            when.method(POST)
                .path("/api/plugins/custom-objects/custom-object-type-fields/");
            then.status(201).json_body(json!({
                "id": 100
            }));
        });

        let schema = alembic_core::Schema {
            types: std::collections::BTreeMap::from([(
                "custom.asset".to_string(),
                alembic_core::TypeSchema {
                    key: std::collections::BTreeMap::from([(
                        "name".to_string(),
                        alembic_core::FieldSchema {
                            r#type: alembic_core::FieldType::String,
                            required: true,
                            nullable: false,
                            description: None,
                            format: None,
                            pattern: None,
                        },
                    )]),
                    fields: std::collections::BTreeMap::from([(
                        "owner".to_string(),
                        alembic_core::FieldSchema {
                            r#type: alembic_core::FieldType::String,
                            required: false,
                            nullable: true,
                            description: None,
                            format: None,
                            pattern: None,
                        },
                    )]),
                },
            )]),
        };

        let report = adapter.ensure_schema(&schema).await.unwrap();
        assert_eq!(
            report.created_object_types,
            vec!["custom.asset".to_string()]
        );
        assert!(report
            .created_object_fields
            .contains(&"custom.asset.name".to_string()));
        assert!(report
            .created_object_fields
            .contains(&"custom.asset.owner".to_string()));
    }
}
