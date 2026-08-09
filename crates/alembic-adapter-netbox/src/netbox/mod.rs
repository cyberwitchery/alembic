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
    use alembic_core::{key_string, JsonMap, Key, TypeName, Uid};
    use alembic_engine::Op;
    use httpmock::Method::{GET, PATCH, POST};
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

    /// the schema the import tests share: a device keyed on `name` referencing a
    /// site, and a site keyed on `site_key`.
    fn import_schema(site_key: &str) -> alembic_core::Schema {
        alembic_core::Schema {
            types: std::collections::BTreeMap::from([
                (
                    "dcim.device".to_string(),
                    alembic_core::TypeSchema {
                        key: std::collections::BTreeMap::from([(
                            "name".to_string(),
                            string_field(true),
                        )]),
                        fields: std::collections::BTreeMap::from([
                            ("name".to_string(), string_field(true)),
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
                            site_key.to_string(),
                            string_field(true),
                        )]),
                        fields: std::collections::BTreeMap::from([
                            ("name".to_string(), string_field(true)),
                            ("slug".to_string(), string_field(false)),
                        ]),
                    },
                ),
            ]),
        }
    }

    /// one site (id 1) and one device (id 2) whose nested `site` is `site_brief`.
    fn mock_import_backend<'a>(
        server: &'a MockServer,
        site_brief: serde_json::Value,
    ) -> Vec<Mock<'a>> {
        vec![
            mock_list(
                server,
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
            ),
            mock_list(
                server,
                "/api/dcim/devices/",
                json!([{ "id": 2, "name": "leaf01", "site": site_brief }]),
            ),
            mock_list(
                server,
                "/api/dcim/sites/",
                json!([{ "id": 1, "name": "FRA1", "slug": "fra1" }]),
            ),
            server.mock(|when, then| {
                when.method(GET).path("/api/extras/custom-fields/");
                then.status(200).json_body(page(json!([])));
            }),
            server.mock(|when, then| {
                when.method(GET)
                    .path("/api/extras/tags/")
                    .query_param("limit", "200")
                    .query_param("offset", "0");
                then.status(200).json_body(page(json!([])));
            }),
        ]
    }

    /// import through the adapter and assert the device's `site` is the site's
    /// canonical uid, the same one the imported site object carries.
    async fn assert_import_resolves_site(
        adapter: &NetBoxAdapter,
        schema: &alembic_core::Schema,
        site_key: Key,
    ) {
        let report = alembic_engine::import_inventory(adapter, schema, &[])
            .await
            .unwrap();

        let validation = alembic_core::validate_inventory(&report.inventory);
        assert!(
            validation.errors.is_empty(),
            "imported inventory must validate: {:?}",
            validation.errors
        );

        let device = report
            .inventory
            .objects
            .iter()
            .find(|object| object.type_name.as_str() == "dcim.device")
            .unwrap();
        let canonical_site = alembic_core::uid_v5("dcim.site", &key_string(&site_key)).to_string();
        assert_eq!(
            device.attrs.get("site").and_then(|v| v.as_str()),
            Some(canonical_site.as_str())
        );
    }

    #[tokio::test]
    async fn import_resolves_refs_in_canonical_uid_space() {
        // observe alone would state-map the ref (see observe_maps_nested_refs_to_uids)
        // and the inventory would dangle.
        let server = MockServer::start();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token").unwrap();
        let _mocks = mock_import_backend(
            &server,
            json!({
                "id": 1,
                "url": "https://netbox.example.com/api/dcim/sites/1/",
                "name": "FRA1",
                "slug": "fra1"
            }),
        );

        assert_import_resolves_site(&adapter, &import_schema("name"), key("name", json!("FRA1")))
            .await;
    }

    #[tokio::test]
    async fn import_resolves_a_ref_whose_brief_omits_a_key_field() {
        // `resolve_nested_ref_uid` needs the target's whole declared key for its
        // canonical fallback, so a brief missing one field leaves the ref a bare
        // backend id once state is out of the picture. the index bootstrapped
        // from the observation resolves it anyway.
        let server = MockServer::start();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token").unwrap();
        let _mocks = mock_import_backend(
            &server,
            json!({
                "id": 1,
                "url": "https://netbox.example.com/api/dcim/sites/1/",
                "name": "FRA1"
            }),
        );

        assert_import_resolves_site(&adapter, &import_schema("slug"), key("slug", json!("fra1")))
            .await;
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
        let report = adapter.write(&schema, &ops, &state).await.unwrap();
        assert_eq!(report.applied.len(), 2);
    }

    #[tokio::test]
    async fn create_conflict_surfaces_lookup_failure() {
        // a create conflict whose recovery lookup itself fails (here the list
        // returns 500) must surface the lookup failure, not mask it as the
        // conflict, so the operator chases the real blocker.
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
                }
            ]),
        );
        let _custom_fields = server.mock(|when, then| {
            when.method(GET).path("/api/extras/custom-fields/");
            then.status(200).json_body(page(json!([])));
        });
        let _site_create = server.mock(|when, then| {
            when.method(POST).path("/api/dcim/sites/");
            then.status(409)
                .json_body(json!({ "detail": "site with this name already exists." }));
        });
        let _site_lookup = server.mock(|when, then| {
            when.method(GET).path("/api/dcim/sites/");
            then.status(500).body("boom");
        });

        let ops = vec![Op::Create {
            uid: uid(1),
            type_name: TypeName::new("dcim.site"),
            desired: obj(
                uid(1),
                "dcim.site",
                key("name", json!("FRA1")),
                json!({ "name": "FRA1", "slug": "fra1" }),
            ),
        }];

        let schema = alembic_core::Schema {
            types: std::collections::BTreeMap::from([(
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
            )]),
        };

        let err = adapter.write(&schema, &ops, &state).await.unwrap_err();
        let chain = format!("{err:#}");
        assert!(
            chain.contains("500"),
            "expected the lookup (list) failure to surface, got: {chain}"
        );
        assert!(
            !chain.contains("already exists"),
            "the conflict must not mask the lookup failure, got: {chain}"
        );
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
        let report = adapter.write(&schema, &ops, &state).await.unwrap();
        assert_eq!(report.applied.len(), 3);
    }

    fn tagged_site_schema() -> alembic_core::Schema {
        let string_field = |required: bool| alembic_core::FieldSchema {
            r#type: alembic_core::FieldType::String,
            required,
            nullable: !required,
            description: None,
            format: None,
            pattern: None,
        };
        alembic_core::Schema {
            types: std::collections::BTreeMap::from([(
                "dcim.site".to_string(),
                alembic_core::TypeSchema {
                    key: std::collections::BTreeMap::new(),
                    fields: std::collections::BTreeMap::from([
                        ("name".to_string(), string_field(true)),
                        ("slug".to_string(), string_field(true)),
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
        }
    }

    fn tagged_site_ops(tags: serde_json::Value) -> Vec<Op> {
        let mut attrs = std::collections::BTreeMap::new();
        attrs.insert("name".to_string(), json!("FRA1"));
        attrs.insert("slug".to_string(), json!("fra1"));
        attrs.insert("tags".to_string(), tags);
        vec![Op::Create {
            uid: uid(1),
            type_name: TypeName::new("dcim.site"),
            desired: alembic_core::Object {
                uid: uid(1),
                type_name: TypeName::new("dcim.site"),
                key: Key::default(),
                attrs: alembic_core::JsonMap::from(attrs),
                source: None,
            },
        }]
    }

    /// object types, custom fields and the site create every tag test needs.
    fn mock_tagged_site(server: &MockServer) {
        mock_list(
            server,
            "/api/core/object-types/",
            json!([{
                "app_label": "dcim",
                "model": "site",
                "rest_api_endpoint": "/api/dcim/sites/",
                "features": ["custom-fields", "tags"]
            }]),
        );
        server.mock(|when, then| {
            when.method(GET).path("/api/extras/custom-fields/");
            then.status(200).json_body(page(json!([])));
        });
        server.mock(|when, then| {
            when.method(POST).path("/api/dcim/sites/");
            then.status(201)
                .json_body(json!({ "id": 1, "name": "FRA1", "slug": "fra1" }));
        });
    }

    #[tokio::test]
    async fn apply_creates_missing_tags() {
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        let state = StateStore::load(dir.path().join("state.json")).unwrap();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token").unwrap();

        mock_tagged_site(&server);
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

        let report = adapter
            .write(
                &tagged_site_schema(),
                &tagged_site_ops(json!(["fabric"])),
                &state,
            )
            .await
            .unwrap();
        assert_eq!(report.applied.len(), 1);
        assert_eq!(report.provision.created_tags, vec!["fabric".to_string()]);
    }

    #[tokio::test]
    async fn apply_does_not_report_tags_the_backend_already_has() {
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        let state = StateStore::load(dir.path().join("state.json")).unwrap();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token").unwrap();

        mock_tagged_site(&server);
        let _tags = server.mock(|when, then| {
            when.method(GET)
                .path("/api/extras/tags/")
                .query_param("limit", "200")
                .query_param("offset", "0");
            then.status(200)
                .json_body(page(json!([{"id": 1, "name": "fabric", "slug": "fabric"}])));
        });
        let _tag_create = server.mock(|when, then| {
            when.method(POST).path("/api/extras/tags/");
            then.status(201)
                .json_body(json!({"id": 2, "name": "edge", "slug": "edge"}));
        });

        let report = adapter
            .write(
                &tagged_site_schema(),
                &tagged_site_ops(json!(["fabric", "edge"])),
                &state,
            )
            .await
            .unwrap();
        // only the one this run actually posted.
        assert_eq!(report.provision.created_tags, vec!["edge".to_string()]);
    }

    #[tokio::test]
    async fn apply_does_not_report_a_tag_that_lost_the_create_race() {
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        let state = StateStore::load(dir.path().join("state.json")).unwrap();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token").unwrap();

        mock_tagged_site(&server);
        // the first fetch sees no tags, so `fabric` is planned; the re-fetch after the
        // create fails sees it, i.e. someone else created it in between.
        let first_fetch = std::sync::atomic::AtomicBool::new(true);
        let _tags_empty = server.mock(|when, then| {
            when.method(GET)
                .path("/api/extras/tags/")
                .is_true(move |_| first_fetch.swap(false, std::sync::atomic::Ordering::SeqCst));
            then.status(200).json_body(page(json!([])));
        });
        let _tags_present = server.mock(|when, then| {
            when.method(GET).path("/api/extras/tags/");
            then.status(200)
                .json_body(page(json!([{"id": 1, "name": "fabric", "slug": "fabric"}])));
        });
        let _tag_create = server.mock(|when, then| {
            when.method(POST).path("/api/extras/tags/");
            then.status(400)
                .json_body(json!({"name": ["tag with this name already exists."]}));
        });

        let report = adapter
            .write(
                &tagged_site_schema(),
                &tagged_site_ops(json!(["fabric"])),
                &state,
            )
            .await
            .unwrap();
        assert_eq!(report.applied.len(), 1);
        assert!(report.provision.created_tags.is_empty());
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

    #[tokio::test]
    async fn fetch_custom_fields_paginates_across_all_pages() {
        let server = MockServer::start();
        let client = NetBoxClient::new(&server.base_url(), "token").unwrap();

        // 250 custom fields spread across two pages exceeds the per-request
        // limit of 200, so fetch_custom_fields must follow pagination.
        let total = 250usize;
        let limit = 200usize;
        let field = |i: usize| json!({ "object_types": ["dcim.device"], "type": {}, "name": format!("cf-{i:04}") });
        let first: Vec<serde_json::Value> = (0..limit).map(field).collect();
        let second: Vec<serde_json::Value> = (limit..total).map(field).collect();

        let _page_one = server.mock(|when, then| {
            when.method(GET)
                .path("/api/extras/custom-fields/")
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
                .path("/api/extras/custom-fields/")
                .query_param("limit", "200")
                .query_param("offset", "200");
            then.status(200).json_body(json!({
                "count": total,
                "next": null,
                "previous": null,
                "results": second,
            }));
        });

        let by_type = client.fetch_custom_fields().await.unwrap();
        let fields = by_type
            .get("dcim.device")
            .expect("dcim.device custom fields");

        assert_eq!(fields.len(), total);
        // first/last of each page, proving both requests were made rather than
        // stopping after the first page.
        assert!(fields.contains("cf-0000"));
        assert!(fields.contains("cf-0199"));
        assert!(fields.contains("cf-0200"));
        assert!(fields.contains("cf-0249"));
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
        let report = adapter.write(&schema, &ops, &state).await.unwrap();
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
        let report = adapter.write(&schema, &ops, &state).await.unwrap();
        assert_eq!(report.applied.len(), 1);
    }

    #[tokio::test]
    async fn apply_tolerates_already_deleted_404() {
        // re-issued deletes must tolerate an already-gone object (404) as a no-op.
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
            then.status(404);
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
        let report = adapter.write(&schema, &ops, &state).await.unwrap();
        assert_eq!(report.applied.len(), 1);
        assert_eq!(report.applied[0].backend_id, None);
    }

    #[tokio::test]
    async fn a_failed_delete_phase_leaves_a_journal_the_re_run_skips_the_creates_from() {
        // deletes run after the creates and are not journaled. the journal used to be
        // unlinked the moment the last create landed, so a delete-phase failure left
        // nothing on disk and the re-run posted every create again, straight into the
        // backend's uniqueness constraint.
        use httpmock::Method::DELETE;

        let dir = tempdir().unwrap();
        let state = StateStore::load(dir.path().join("state.json"))
            .unwrap()
            .with_journal_dir(dir.path().to_path_buf());

        let site_schema = alembic_core::Schema {
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
        let ops = vec![
            Op::Create {
                uid: uid(1),
                type_name: TypeName::new("dcim.site"),
                desired: obj(
                    uid(1),
                    "dcim.site",
                    key("slug", json!("fra1")),
                    json!({ "name": "FRA1" }),
                ),
            },
            Op::Delete {
                uid: uid(2),
                type_name: TypeName::new("dcim.site"),
                key: key("slug", json!("ber1")),
                backend_id: Some(alembic_engine::BackendId::Int(2)),
            },
        ];
        let creates: Vec<Op> = ops
            .iter()
            .filter(|op| !matches!(op, Op::Delete { .. }))
            .cloned()
            .collect();
        let journal_path =
            alembic_engine::Journal::stable_file_name(dir.path(), "netbox", &creates);

        // run 1: the create lands, the backend refuses the delete.
        let first = MockServer::start();
        let _types = mock_list(
            &first,
            "/api/core/object-types/",
            json!([{
                "app_label": "dcim",
                "model": "site",
                "rest_api_endpoint": "/api/dcim/sites/",
                "features": ["custom-fields", "tags"]
            }]),
        );
        let _fields = first.mock(|when, then| {
            when.method(GET).path("/api/extras/custom-fields/");
            then.status(200).json_body(page(json!([])));
        });
        let first_create = first.mock(|when, then| {
            when.method(POST).path("/api/dcim/sites/");
            then.status(201)
                .json_body(json!({ "id": 1, "slug": "fra1" }));
        });
        let first_delete = first.mock(|when, then| {
            when.method(DELETE).path("/api/dcim/sites/");
            then.status(500).json_body(json!({ "detail": "boom" }));
        });

        let adapter = NetBoxAdapter::new(&first.base_url(), "token").unwrap();
        adapter
            .write(&site_schema, &ops, &state)
            .await
            .expect_err("the delete phase must fail");
        first_create.assert_calls(1);
        first_delete.assert_calls(1);

        assert!(
            journal_path.exists(),
            "the journal has to outlive the delete phase"
        );
        let journal =
            alembic_engine::Journal::load_or_create(dir.path(), "netbox", &creates).unwrap();
        assert!(
            journal.is_completed(),
            "every create is recorded done before the deletes run"
        );
        assert_eq!(
            journal
                .done_applied_ops()
                .iter()
                .map(|op| (op.uid, op.backend_id.clone()))
                .collect::<Vec<_>>(),
            vec![(uid(1), Some(alembic_engine::BackendId::Int(1)))],
        );
        drop(journal);

        // run 2 against a backend that now accepts the delete: the site is not created
        // a second time.
        let second = MockServer::start();
        let _types = mock_list(
            &second,
            "/api/core/object-types/",
            json!([{
                "app_label": "dcim",
                "model": "site",
                "rest_api_endpoint": "/api/dcim/sites/",
                "features": ["custom-fields", "tags"]
            }]),
        );
        let _fields = second.mock(|when, then| {
            when.method(GET).path("/api/extras/custom-fields/");
            then.status(200).json_body(page(json!([])));
        });
        let second_create = second.mock(|when, then| {
            when.method(POST).path("/api/dcim/sites/");
            then.status(201)
                .json_body(json!({ "id": 9, "slug": "fra1" }));
        });
        let second_delete = second.mock(|when, then| {
            when.method(DELETE).path("/api/dcim/sites/");
            then.status(204);
        });

        let adapter = NetBoxAdapter::new(&second.base_url(), "token").unwrap();
        let report = adapter.write(&site_schema, &ops, &state).await.unwrap();

        second_create.assert_calls(0);
        second_delete.assert_calls(1);
        assert_eq!(report.previously_applied_count, Some(1));
        assert_eq!(
            report.resumed.iter().map(|op| op.uid).collect::<Vec<_>>(),
            vec![uid(1)],
        );
        assert_eq!(
            report.applied.iter().map(|op| op.uid).collect::<Vec<_>>(),
            vec![uid(2)],
            "only the delete is left to apply"
        );
        assert!(
            !journal_path.exists(),
            "the finished apply drops the journal"
        );
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
        // dcim.device is in the registry but not the schema, so it is skipped, not an error.
        let observed = adapter.read(&schema, &[], &state).await.unwrap();
        assert!(observed.by_key.is_empty());
    }

    const EXISTING_FIELD_ID: u64 = 7;

    /// the object-types read every schema provision starts from: one `dcim.site`.
    fn mock_site_object_type(server: &MockServer) -> Mock<'_> {
        mock_list(
            server,
            "/api/core/object-types/",
            json!([{
                "app_label": "dcim",
                "model": "site",
                "rest_api_endpoint": "/api/dcim/sites/",
                "features": ["custom-fields", "tags"]
            }]),
        )
    }

    /// the custom-fields list, holding an `asset_tag` on `dcim.site` whose
    /// converged properties are `current`.
    fn mock_existing_custom_field(server: &MockServer, current: serde_json::Value) {
        let mut field = json!({
            "id": EXISTING_FIELD_ID,
            "name": "asset_tag",
            "object_types": ["dcim.site"],
            "type": {"value": "text", "label": "Text"},
        });
        let (Some(field), Some(current)) = (field.as_object_mut(), current.as_object()) else {
            unreachable!("both are json objects")
        };
        field.extend(current.clone());
        let body = page(json!([field]));
        let _m = server.mock(|when, then| {
            when.method(GET).path("/api/extras/custom-fields/");
            then.status(200).json_body(body);
        });
        let _probe = server.mock(|when, then| {
            when.method(GET)
                .path("/api/dcim/sites/")
                .query_param("limit", "1");
            then.status(200).json_body(page(json!([])));
        });
    }

    /// `dcim.site` declaring a single `asset_tag` custom field.
    fn declaring_schema(
        pattern: Option<&str>,
        description: Option<&str>,
        required: bool,
    ) -> alembic_core::Schema {
        let key_field = alembic_core::FieldSchema {
            r#type: alembic_core::FieldType::String,
            required: true,
            nullable: false,
            description: None,
            format: None,
            pattern: None,
        };
        alembic_core::Schema {
            types: std::collections::BTreeMap::from([(
                "dcim.site".to_string(),
                alembic_core::TypeSchema {
                    key: std::collections::BTreeMap::from([("slug".to_string(), key_field)]),
                    fields: std::collections::BTreeMap::from([(
                        "asset_tag".to_string(),
                        alembic_core::FieldSchema {
                            r#type: alembic_core::FieldType::String,
                            required,
                            nullable: !required,
                            description: description.map(str::to_string),
                            format: None,
                            pattern: pattern.map(str::to_string),
                        },
                    )]),
                },
            )]),
        }
    }

    // provisioning converges an existing field onto the properties the schema
    // declares. the three below are exactly what a create sends beyond identity
    // and type, and all three sit on netbox's patch body.
    #[tokio::test]
    async fn ensure_schema_converges_an_existing_field() {
        let server = MockServer::start();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token").unwrap();
        let _object_types = mock_site_object_type(&server);
        mock_existing_custom_field(
            &server,
            json!({"required": false, "description": "", "validation_regex": ""}),
        );
        let cf_create = server.mock(|when, then| {
            when.method(POST).path("/api/extras/custom-fields/");
            then.status(201).json_body(json!({}));
        });
        let cf_patch = server.mock(|when, then| {
            when.method(PATCH)
                .path(format!("/api/extras/custom-fields/{EXISTING_FIELD_ID}/"))
                .json_body(json!({
                    "required": true,
                    "description": "asset tag",
                    "validation_regex": "^SITE-",
                }));
            then.status(200).json_body(json!({}));
        });

        let report = adapter
            .ensure_schema(&declaring_schema(Some("^SITE-"), Some("asset tag"), true))
            .await
            .unwrap();

        assert_eq!(
            report.updated_fields,
            vec!["dcim.site.asset_tag".to_string()]
        );
        assert!(report.created_fields.is_empty());
        cf_patch.assert_calls(1);
        // the field is there: nothing is created.
        cf_create.assert_calls(0);
    }

    // a field that already agrees is not written at all: no patch, nothing reported.
    #[tokio::test]
    async fn ensure_schema_leaves_an_agreeing_field_alone() {
        let server = MockServer::start();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token").unwrap();
        let _object_types = mock_site_object_type(&server);
        mock_existing_custom_field(
            &server,
            json!({
                "required": true,
                "description": "asset tag",
                "validation_regex": "^SITE-",
            }),
        );
        let cf_patch = server.mock(|when, then| {
            when.method(PATCH)
                .path(format!("/api/extras/custom-fields/{EXISTING_FIELD_ID}/"));
            then.status(200).json_body(json!({}));
        });

        let report = adapter
            .ensure_schema(&declaring_schema(Some("^SITE-"), Some("asset tag"), true))
            .await
            .unwrap();

        assert!(report.updated_fields.is_empty());
        cf_patch.assert_calls(0);
    }

    // additive-only, one level up: a property the schema does not declare keeps
    // whatever the backend holds. the patch matcher is an exact body, so a
    // description or required key sneaking in fails to match and the test goes red.
    #[tokio::test]
    async fn ensure_schema_does_not_blank_an_undeclared_property() {
        let server = MockServer::start();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token").unwrap();
        let _object_types = mock_site_object_type(&server);
        mock_existing_custom_field(
            &server,
            json!({
                "required": true,
                "description": "written by an operator",
                "validation_regex": "",
            }),
        );
        let cf_patch = server.mock(|when, then| {
            when.method(PATCH)
                .path(format!("/api/extras/custom-fields/{EXISTING_FIELD_ID}/"))
                .json_body(json!({"validation_regex": "^SITE-"}));
            then.status(200).json_body(json!({}));
        });

        let report = adapter
            .ensure_schema(&declaring_schema(Some("^SITE-"), None, false))
            .await
            .unwrap();

        assert_eq!(
            report.updated_fields,
            vec!["dcim.site.asset_tag".to_string()]
        );
        cf_patch.assert_calls(1);
    }

    // preview and ensure make the same decision, and preview writes nothing.
    #[tokio::test]
    async fn preview_schema_reports_the_update_without_writing() {
        let server = MockServer::start();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token").unwrap();
        let _object_types = mock_site_object_type(&server);
        mock_existing_custom_field(
            &server,
            json!({"required": false, "description": "", "validation_regex": ""}),
        );
        let cf_patch = server.mock(|when, then| {
            when.method(PATCH)
                .path(format!("/api/extras/custom-fields/{EXISTING_FIELD_ID}/"));
            then.status(200).json_body(json!({}));
        });

        let report = adapter
            .preview_schema(&declaring_schema(Some("^SITE-"), Some("asset tag"), true))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            report.updated_fields,
            vec!["dcim.site.asset_tag".to_string()]
        );
        cf_patch.assert_calls(0);
    }

    /// the object-types read, plus the sample-object probe each native type needs:
    /// `dcim.site` and `dcim.device`, both custom-field capable.
    fn mock_two_object_types(server: &MockServer) -> Mock<'_> {
        for path in ["/api/dcim/sites/", "/api/dcim/devices/"] {
            let _probe = server.mock(|when, then| {
                when.method(GET).path(path).query_param("limit", "1");
                then.status(200).json_body(page(json!([])));
            });
        }
        mock_list(
            server,
            "/api/core/object-types/",
            json!([
                {
                    "app_label": "dcim", "model": "site",
                    "rest_api_endpoint": "/api/dcim/sites/",
                    "features": ["custom-fields", "tags"]
                },
                {
                    "app_label": "dcim", "model": "device",
                    "rest_api_endpoint": "/api/dcim/devices/",
                    "features": ["custom-fields", "tags"]
                }
            ]),
        )
    }

    /// one `asset_tag` field carrying *both* content types, which is how both
    /// vendors model a field attached to more than one type.
    fn mock_shared_custom_field(server: &MockServer, current: serde_json::Value) -> Mock<'_> {
        let mut field = json!({
            "id": EXISTING_FIELD_ID,
            "name": "asset_tag",
            "object_types": ["dcim.site", "dcim.device"],
            "type": {"value": "text", "label": "Text"},
        });
        let (Some(field), Some(current)) = (field.as_object_mut(), current.as_object()) else {
            unreachable!("both are json objects")
        };
        field.extend(current.clone());
        let body = page(json!([field]));
        server.mock(|when, then| {
            when.method(GET).path("/api/extras/custom-fields/");
            then.status(200).json_body(body);
        })
    }

    /// `asset_tag` declared on both types, each with its own pattern.
    fn schema_declaring_on_both(site_pattern: &str, device_pattern: &str) -> alembic_core::Schema {
        let field = |pattern: &str| alembic_core::FieldSchema {
            r#type: alembic_core::FieldType::String,
            required: true,
            nullable: false,
            description: Some("asset tag".to_string()),
            format: None,
            pattern: Some(pattern.to_string()),
        };
        let key_field = alembic_core::FieldSchema {
            r#type: alembic_core::FieldType::String,
            required: true,
            nullable: false,
            description: None,
            format: None,
            pattern: None,
        };
        let type_schema = |pattern: &str| alembic_core::TypeSchema {
            key: std::collections::BTreeMap::from([("slug".to_string(), key_field.clone())]),
            fields: std::collections::BTreeMap::from([("asset_tag".to_string(), field(pattern))]),
        };
        alembic_core::Schema {
            types: std::collections::BTreeMap::from([
                ("dcim.site".to_string(), type_schema(site_pattern)),
                ("dcim.device".to_string(), type_schema(device_pattern)),
            ]),
        }
    }

    // one backend field serving two content types is patched once, not once per
    // type, and the run reports both declarations it answered.
    #[tokio::test]
    async fn ensure_schema_patches_a_shared_field_once() {
        let server = MockServer::start();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token").unwrap();
        let _object_types = mock_two_object_types(&server);
        let _fields = mock_shared_custom_field(
            &server,
            json!({"required": false, "description": "", "validation_regex": ""}),
        );
        let cf_patch = server.mock(|when, then| {
            when.method(PATCH)
                .path(format!("/api/extras/custom-fields/{EXISTING_FIELD_ID}/"))
                .json_body(json!({
                    "required": true,
                    "description": "asset tag",
                    "validation_regex": "^ASSET-",
                }));
            then.status(200).json_body(json!({}));
        });

        let report = adapter
            .ensure_schema(&schema_declaring_on_both("^ASSET-", "^ASSET-"))
            .await
            .unwrap();

        assert_eq!(
            report.updated_fields,
            vec![
                "dcim.device.asset_tag".to_string(),
                "dcim.site.asset_tag".to_string()
            ]
        );
        cf_patch.assert_calls(1);
    }

    // the fixed point: a second provision against the backend the first one
    // produced plans nothing and writes nothing.
    #[tokio::test]
    async fn a_shared_field_converges_in_one_run() {
        let server = MockServer::start();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token").unwrap();
        let _object_types = mock_two_object_types(&server);
        // what the first run's patch left behind.
        let _fields = mock_shared_custom_field(
            &server,
            json!({
                "required": true,
                "description": "asset tag",
                "validation_regex": "^ASSET-",
            }),
        );
        let cf_patch = server.mock(|when, then| {
            when.method(PATCH)
                .path(format!("/api/extras/custom-fields/{EXISTING_FIELD_ID}/"));
            then.status(200).json_body(json!({}));
        });

        let schema = schema_declaring_on_both("^ASSET-", "^ASSET-");
        let preview = adapter.preview_schema(&schema).await.unwrap().unwrap();
        let report = adapter.ensure_schema(&schema).await.unwrap();

        assert!(preview.updated_fields.is_empty());
        assert!(report.updated_fields.is_empty());
        cf_patch.assert_calls(0);
    }

    // two types declaring different values for one shared backend field cannot
    // both be honoured, so the run says so rather than writing whichever comes
    // last. the create path already fails loudly on the same inventory.
    #[tokio::test]
    async fn ensure_schema_refuses_conflicting_declarations_on_a_shared_field() {
        let server = MockServer::start();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token").unwrap();
        let _object_types = mock_two_object_types(&server);
        let _fields = mock_shared_custom_field(
            &server,
            json!({"required": true, "description": "asset tag", "validation_regex": ""}),
        );
        let cf_patch = server.mock(|when, then| {
            when.method(PATCH)
                .path(format!("/api/extras/custom-fields/{EXISTING_FIELD_ID}/"));
            then.status(200).json_body(json!({}));
        });

        let schema = schema_declaring_on_both("^SITE-", "^DEV-");
        let err = adapter
            .ensure_schema(&schema)
            .await
            .unwrap_err()
            .to_string();

        assert!(err.contains("asset_tag"), "{err}");
        assert!(err.contains("validation_regex"), "{err}");
        assert!(err.contains("dcim.site.asset_tag"), "{err}");
        assert!(err.contains("dcim.device.asset_tag"), "{err}");
        // the preview refuses the same inventory, so a plan cannot pass what an
        // apply would reject.
        assert!(adapter.preview_schema(&schema).await.is_err());
        cf_patch.assert_calls(0);
    }

    // `required` is outside that guard: the create payload omits a declared
    // `false`, so two types disagreeing about it is a union rather than a
    // conflict, and the field is tightened for both.
    #[tokio::test]
    async fn a_shared_field_takes_the_union_of_required() {
        let server = MockServer::start();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token").unwrap();
        let _object_types = mock_two_object_types(&server);
        let _fields = mock_shared_custom_field(
            &server,
            json!({
                "required": false,
                "description": "asset tag",
                "validation_regex": "^ASSET-",
            }),
        );
        let cf_patch = server.mock(|when, then| {
            when.method(PATCH)
                .path(format!("/api/extras/custom-fields/{EXISTING_FIELD_ID}/"))
                .json_body(json!({"required": true}));
            then.status(200).json_body(json!({}));
        });

        let mut schema = schema_declaring_on_both("^ASSET-", "^ASSET-");
        schema
            .types
            .get_mut("dcim.device")
            .and_then(|type_schema| type_schema.fields.get_mut("asset_tag"))
            .unwrap()
            .required = false;

        let report = adapter.ensure_schema(&schema).await.unwrap();

        // `dcim.device` asked for the opposite and is still listed as converged.
        assert_eq!(
            report.updated_fields,
            vec![
                "dcim.device.asset_tag".to_string(),
                "dcim.site.asset_tag".to_string()
            ]
        );
        cf_patch.assert_calls(1);
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

    // ip_address is a multi-word model: its endpoint-form type name is
    // `ipam.ip_address`, but its django content type (what netbox keys a custom
    // field's `object_types` by, and returns them under) is `ipam.ipaddress`.
    // the tests below guard custom-field lookup and `object_types` posting
    // against mixing the endpoint form into the content-type form. `fields`
    // holds only the non-key fields under test.
    fn ip_address_schema(fields: &[&str]) -> alembic_core::Schema {
        let string_field = || alembic_core::FieldSchema {
            r#type: alembic_core::FieldType::String,
            required: false,
            nullable: true,
            description: None,
            format: None,
            pattern: None,
        };
        let field_map: std::collections::BTreeMap<String, alembic_core::FieldSchema> = fields
            .iter()
            .map(|name| (name.to_string(), string_field()))
            .collect();
        alembic_core::Schema {
            types: std::collections::BTreeMap::from([(
                "ipam.ip_address".to_string(),
                alembic_core::TypeSchema {
                    key: std::collections::BTreeMap::from([(
                        "address".to_string(),
                        string_field(),
                    )]),
                    fields: field_map,
                },
            )]),
        }
    }

    // write: a custom field on a multi-word model must be nested under
    // `custom_fields`, not sent as a top-level body field (which netbox silently
    // drops, so the field never converges and every plan re-updates it).
    #[tokio::test]
    async fn write_nests_custom_field_on_multi_word_model() {
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        let state = StateStore::load(dir.path().join("state.json")).unwrap();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token").unwrap();

        let _object_types = mock_list(
            &server,
            "/api/core/object-types/",
            json!([{
                "app_label": "ipam",
                "model": "ipaddress",
                "rest_api_endpoint": "/api/ipam/ip-addresses/",
                "features": ["custom-fields", "tags"]
            }]),
        );
        // criticality is a custom field on the content type `ipam.ipaddress`.
        let _custom_fields = server.mock(|when, then| {
            when.method(GET).path("/api/extras/custom-fields/");
            then.status(200).json_body(page(json!([{
                "name": "criticality",
                "object_types": ["ipam.ipaddress"],
                "type": {}
            }])));
        });
        // the create must nest criticality under `custom_fields`.
        let create = server.mock(|when, then| {
            when.method(POST)
                .path("/api/ipam/ip-addresses/")
                .json_body_includes(r#"{"custom_fields": {"criticality": "high"}}"#);
            then.status(201)
                .json_body(json!({ "id": 5, "address": "10.0.0.1/32" }));
        });

        let ops = vec![Op::Create {
            uid: uid(1),
            type_name: TypeName::new("ipam.ip_address"),
            desired: obj(
                uid(1),
                "ipam.ip_address",
                key("address", json!("10.0.0.1/32")),
                json!({ "address": "10.0.0.1/32", "criticality": "high" }),
            ),
        }];

        let report = adapter
            .write(
                &ip_address_schema(&["address", "criticality"]),
                &ops,
                &state,
            )
            .await
            .unwrap();
        assert_eq!(report.applied.len(), 1);
        create.assert_calls(1);
    }

    // provision: creating a custom field on a multi-word model must post its
    // object_types in the django content-type form (`ipam.ipaddress`), not the
    // endpoint form (`ipam.ip_address`), which netbox rejects.
    #[tokio::test]
    async fn ensure_schema_posts_content_type_for_multi_word_model() {
        let server = MockServer::start();
        let adapter = NetBoxAdapter::new(&server.base_url(), "token").unwrap();

        let _object_types = mock_list(
            &server,
            "/api/core/object-types/",
            json!([{
                "app_label": "ipam",
                "model": "ipaddress",
                "rest_api_endpoint": "/api/ipam/ip-addresses/",
                "features": ["custom-fields", "tags"]
            }]),
        );
        let _custom_fields = server.mock(|when, then| {
            when.method(GET).path("/api/extras/custom-fields/");
            then.status(200).json_body(page(json!([])));
        });
        // native-field probe reads one sample object.
        let _probe = server.mock(|when, then| {
            when.method(GET)
                .path("/api/ipam/ip-addresses/")
                .query_param("limit", "1");
            then.status(200).json_body(page(json!([])));
        });
        // the create must carry object_types in the content-type form.
        let cf_create = server.mock(|when, then| {
            when.method(POST)
                .path("/api/extras/custom-fields/")
                .json_body_includes(r#"{"object_types": ["ipam.ipaddress"]}"#);
            then.status(201).json_body(json!({
                "id": 1,
                "name": "criticality",
                "object_types": ["ipam.ipaddress"],
                "type": {"value": "text", "label": "Text"}
            }));
        });

        let report = adapter
            .ensure_schema(&ip_address_schema(&["criticality"]))
            .await
            .unwrap();
        assert_eq!(
            report.created_fields,
            vec!["ipam.ip_address.criticality".to_string()]
        );
        cf_create.assert_calls(1);
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

    #[tokio::test]
    async fn preview_schema_reports_created_fields_without_writing() {
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
        let cf_create = server.mock(|when, then| {
            when.method(POST).path("/api/extras/custom-fields/");
            then.status(201).json_body(json!({ "id": 1 }));
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

        let report = adapter.preview_schema(&schema).await.unwrap().unwrap();
        assert_eq!(report.created_fields, vec!["dcim.site.cf_test".to_string()]);
        assert_eq!(cf_create.calls(), 0, "preview must not write custom fields");
    }

    #[tokio::test]
    async fn preview_schema_reports_custom_object_type_without_writing() {
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
        let type_create = server.mock(|when, then| {
            when.method(POST)
                .path("/api/plugins/custom-objects/custom-object-types/");
            then.status(201)
                .json_body(json!({ "id": 42, "name": "custom-asset" }));
        });
        let field_create = server.mock(|when, then| {
            when.method(POST)
                .path("/api/plugins/custom-objects/custom-object-type-fields/");
            then.status(201).json_body(json!({ "id": 100 }));
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

        let report = adapter.preview_schema(&schema).await.unwrap().unwrap();
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
        assert_eq!(type_create.calls(), 0, "preview must not create types");
        assert_eq!(field_create.calls(), 0, "preview must not create fields");
    }

    #[tokio::test]
    async fn preview_schema_rejects_an_invalid_custom_object_field_name() {
        // netbox rejects a custom-object field name outside [A-Za-z0-9_] at
        // provision time, so the preview must error on it exactly as
        // `ensure_schema` would, not report a create that apply then refuses.
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
        let type_create = server.mock(|when, then| {
            when.method(POST)
                .path("/api/plugins/custom-objects/custom-object-types/");
            then.status(201)
                .json_body(json!({ "id": 42, "name": "custom-asset" }));
        });
        let field_create = server.mock(|when, then| {
            when.method(POST)
                .path("/api/plugins/custom-objects/custom-object-type-fields/");
            then.status(201).json_body(json!({ "id": 100 }));
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
                        "mgmt-vlan".to_string(),
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

        let err = adapter
            .preview_schema(&schema)
            .await
            .expect_err("preview must reject a field name apply would reject");
        assert!(
            err.to_string().contains("invalid custom object field name"),
            "unexpected error: {err}"
        );
        assert_eq!(type_create.calls(), 0, "preview must not create types");
        assert_eq!(field_create.calls(), 0, "preview must not create fields");
    }

    fn string_field(required: bool) -> alembic_core::FieldSchema {
        alembic_core::FieldSchema {
            r#type: alembic_core::FieldType::String,
            required,
            nullable: !required,
            description: None,
            format: None,
            pattern: None,
        }
    }

    fn simple_type_schema(key: &[&str], fields: &[&str]) -> alembic_core::TypeSchema {
        alembic_core::TypeSchema {
            key: key
                .iter()
                .map(|name| (name.to_string(), string_field(true)))
                .collect(),
            fields: fields
                .iter()
                .map(|name| (name.to_string(), string_field(false)))
                .collect(),
        }
    }

    // against a mock backend with no TOCTOU race, the report `preview_schema`
    // renders must equal the one `ensure_schema` produces (see ProvisionPlan).
    #[tokio::test]
    async fn preview_and_ensure_report_the_same_plan() {
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
        let _cf_create = server.mock(|when, then| {
            when.method(POST).path("/api/extras/custom-fields/");
            then.status(201).json_body(json!({
                "id": 1,
                "name": "cf_test",
                "object_types": ["dcim.site"],
                "type": {"value": "text", "label": "Text"}
            }));
        });
        let _type_create = server.mock(|when, then| {
            when.method(POST)
                .path("/api/plugins/custom-objects/custom-object-types/");
            then.status(201)
                .json_body(json!({ "id": 42, "name": "custom-asset" }));
        });
        let _field_create = server.mock(|when, then| {
            when.method(POST)
                .path("/api/plugins/custom-objects/custom-object-type-fields/");
            then.status(201).json_body(json!({ "id": 100 }));
        });

        let schema = alembic_core::Schema {
            types: std::collections::BTreeMap::from([
                (
                    "dcim.site".to_string(),
                    simple_type_schema(&["slug"], &["cf_test"]),
                ),
                (
                    "custom.asset".to_string(),
                    simple_type_schema(&["name"], &["owner"]),
                ),
            ]),
        };

        let preview = adapter.preview_schema(&schema).await.unwrap().unwrap();
        let ensure = adapter.ensure_schema(&schema).await.unwrap();

        // serde equality pins every report field without ProvisionReport: PartialEq.
        assert_eq!(
            serde_json::to_value(&preview).unwrap(),
            serde_json::to_value(&ensure).unwrap(),
            "preview must report exactly what ensure provisions"
        );
        assert_eq!(
            preview.created_fields,
            vec!["dcim.site.cf_test".to_string()]
        );
        assert_eq!(
            preview.created_object_types,
            vec!["custom.asset".to_string()]
        );
        assert!(preview
            .created_object_fields
            .contains(&"custom.asset.name".to_string()));
        assert!(preview
            .created_object_fields
            .contains(&"custom.asset.owner".to_string()));
    }

    // the delete branch (a still-present alembic-owned custom type/field the
    // schema no longer declares) is exercised by neither the create tests nor the
    // originals; preview and ensure must agree on it too.
    #[tokio::test]
    async fn preview_and_ensure_report_the_same_deletes() {
        use httpmock::Method::DELETE;

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
            then.status(200).json_body(page(json!([{
                "id": 42,
                "name": "custom_legacy",
                "description": "alembic custom object for custom.legacy"
            }])));
        });
        let _custom_object_fields = server.mock(|when, then| {
            when.method(GET)
                .path("/api/plugins/custom-objects/custom-object-type-fields/")
                .query_param("limit", "200")
                .query_param("offset", "0");
            then.status(200).json_body(page(json!([{
                "id": 100,
                "custom_object_type": 42,
                "name": "old_field"
            }])));
        });
        let field_delete = server.mock(|when, then| {
            when.method(DELETE)
                .path("/api/plugins/custom-objects/custom-object-type-fields/100/");
            then.status(204);
        });
        let type_delete = server.mock(|when, then| {
            when.method(DELETE)
                .path("/api/plugins/custom-objects/custom-object-types/42/");
            then.status(204);
        });

        // the schema declares nothing, so the alembic-owned custom.legacy type and
        // its field are both stale.
        let schema = alembic_core::Schema {
            types: std::collections::BTreeMap::new(),
        };

        let preview = adapter.preview_schema(&schema).await.unwrap().unwrap();
        assert_eq!(
            field_delete.calls(),
            0,
            "preview must not delete custom object fields"
        );
        assert_eq!(
            type_delete.calls(),
            0,
            "preview must not delete custom object types"
        );

        let ensure = adapter.ensure_schema(&schema).await.unwrap();
        assert_eq!(field_delete.calls(), 1, "ensure deletes the stale field");
        assert_eq!(type_delete.calls(), 1, "ensure deletes the stale type");

        assert_eq!(
            serde_json::to_value(&preview).unwrap(),
            serde_json::to_value(&ensure).unwrap(),
            "preview must report exactly what ensure deletes"
        );
        assert_eq!(
            preview.deleted_object_fields,
            vec!["custom.legacy.old_field".to_string()]
        );
        assert_eq!(
            preview.deleted_object_types,
            vec!["custom.legacy".to_string()]
        );
    }
}
