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

    const EXISTING_FIELD_ID: &str = "44444444-4444-4444-4444-444444444444";

    /// the custom-fields list, holding an `asset_tag` on `dcim.site` whose
    /// converged properties are `current`.
    fn mock_existing_custom_field(server: &MockServer, current: serde_json::Value) {
        let mut field = json!({
            "id": EXISTING_FIELD_ID,
            "key": "asset_tag",
            "label": "asset_tag",
            "content_types": ["dcim.site"],
            "type": {},
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
    }

    // content-types for both dcim.site and dcim.device, plus the sample-object
    // probe each native type needs.
    fn mock_two_content_types(server: &MockServer) {
        for path in ["/api/dcim/sites/", "/api/dcim/devices/"] {
            let _probe = server.mock(|when, then| {
                when.method(GET).path(path);
                then.status(200).json_body(page(json!([])));
            });
        }
        let _m = server.mock(|when, then| {
            when.method(GET).path("/api/extras/content-types/");
            then.status(200).json_body(page(json!([
                { "app_label": "dcim", "model": "site" },
                { "app_label": "dcim", "model": "device" }
            ])));
        });
    }

    /// one `asset_tag` field carrying *both* content types, which is how both
    /// vendors model a field attached to more than one type.
    fn mock_shared_custom_field(server: &MockServer, current: serde_json::Value) {
        let mut field = json!({
            "id": EXISTING_FIELD_ID,
            "key": "asset_tag",
            "label": "asset_tag",
            "content_types": ["dcim.site", "dcim.device"],
            "type": {},
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
    }

    /// `asset_tag` declared on both types, each with its own pattern.
    fn schema_declaring_on_both(site_pattern: &str, device_pattern: &str) -> Schema {
        let asset_tag = |pattern: &str| FieldSchema {
            r#type: FieldType::String,
            required: true,
            nullable: false,
            description: Some("asset tag".to_string()),
            format: None,
            pattern: Some(pattern.to_string()),
        };
        let type_schema = |pattern: &str| TypeSchema {
            key: BTreeMap::from([("name".to_string(), field(FieldType::String))]),
            fields: BTreeMap::from([("asset_tag".to_string(), asset_tag(pattern))]),
        };
        Schema {
            types: BTreeMap::from([
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
        let adapter = NautobotAdapter::new(&server.base_url(), "token").unwrap();
        mock_two_content_types(&server);
        mock_shared_custom_field(
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
        let adapter = NautobotAdapter::new(&server.base_url(), "token").unwrap();
        mock_two_content_types(&server);
        // what the first run's patch left behind.
        mock_shared_custom_field(
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
    // last.
    #[tokio::test]
    async fn ensure_schema_refuses_conflicting_declarations_on_a_shared_field() {
        let server = MockServer::start();
        let adapter = NautobotAdapter::new(&server.base_url(), "token").unwrap();
        mock_two_content_types(&server);
        mock_shared_custom_field(
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

    // a provision never writes the type, but one field has one type, so two
    // declarations disagreeing about it is the disagreement that cannot be
    // honoured for both. it is refused rather than accepted in silence and
    // reported as converged.
    #[tokio::test]
    async fn ensure_schema_refuses_a_type_disagreement_on_a_shared_field() {
        let server = MockServer::start();
        let adapter = NautobotAdapter::new(&server.base_url(), "token").unwrap();
        mock_two_content_types(&server);
        mock_shared_custom_field(
            &server,
            json!({"required": true, "description": "asset tag", "validation_regex": ""}),
        );
        let cf_patch = server.mock(|when, then| {
            when.method(PATCH)
                .path(format!("/api/extras/custom-fields/{EXISTING_FIELD_ID}/"));
            then.status(200).json_body(json!({}));
        });

        // `dcim.site` a string held to a pattern, `dcim.device` an integer:
        // nautobot `text` against `integer`, and the pattern is the string
        // side's alone, so nothing but the type disagrees.
        let mut schema = schema_declaring_on_both("^SITE-", "^SITE-");
        let device = schema
            .types
            .get_mut("dcim.device")
            .and_then(|type_schema| type_schema.fields.get_mut("asset_tag"))
            .unwrap();
        device.r#type = FieldType::Int;
        device.pattern = None;

        let err = adapter
            .ensure_schema(&schema)
            .await
            .unwrap_err()
            .to_string();

        assert!(err.contains("asset_tag"), "{err}");
        assert!(err.contains("different type"), "{err}");
        assert!(err.contains("dcim.site.asset_tag"), "{err}");
        assert!(err.contains("dcim.device.asset_tag"), "{err}");
        assert!(adapter.preview_schema(&schema).await.is_err());
        cf_patch.assert_calls(0);
    }

    // `required` is outside that guard: the create payload omits a declared
    // `false`, so two types disagreeing about it is a union rather than a
    // conflict, and the field is tightened for both.
    #[tokio::test]
    async fn a_shared_field_takes_the_union_of_required() {
        let server = MockServer::start();
        let adapter = NautobotAdapter::new(&server.base_url(), "token").unwrap();
        mock_two_content_types(&server);
        mock_shared_custom_field(
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

    // `description` and `validation_regex` are omitted from the create payload
    // the same way when a declaration does not carry them, so the guard needs
    // both sides to have declared one: a silent declaration takes its sibling's.
    #[tokio::test]
    async fn a_silent_declaration_takes_a_shared_field_constraint() {
        let server = MockServer::start();
        let adapter = NautobotAdapter::new(&server.base_url(), "token").unwrap();
        mock_two_content_types(&server);
        mock_shared_custom_field(
            &server,
            json!({"required": true, "description": "asset tag", "validation_regex": ""}),
        );
        let mac_regex = alembic_core::format_regex(&alembic_core::FieldFormat::Mac);
        let cf_patch = server.mock(|when, then| {
            when.method(PATCH)
                .path(format!("/api/extras/custom-fields/{EXISTING_FIELD_ID}/"))
                .json_body(json!({"validation_regex": mac_regex}));
            then.status(200).json_body(json!({}));
        });

        // `dcim.site` declares a mac, `dcim.device` a plain string constrained
        // by nothing at all. both map to nautobot `text`.
        let mut schema = schema_declaring_on_both("^ASSET-", "^ASSET-");
        for (type_name, format) in [
            ("dcim.site", Some(alembic_core::FieldFormat::Mac)),
            ("dcim.device", None),
        ] {
            let field = schema
                .types
                .get_mut(type_name)
                .and_then(|type_schema| type_schema.fields.get_mut("asset_tag"))
                .unwrap();
            field.pattern = None;
            field.format = format;
        }

        let report = adapter.ensure_schema(&schema).await.unwrap();

        assert_eq!(
            report.updated_fields,
            vec![
                "dcim.device.asset_tag".to_string(),
                "dcim.site.asset_tag".to_string()
            ]
        );
        cf_patch.assert_calls(1);
    }

    /// `dcim.site` declaring a single `asset_tag` custom field.
    fn declaring_schema(
        pattern: Option<&str>,
        description: Option<&str>,
        required: bool,
    ) -> Schema {
        Schema {
            types: BTreeMap::from([(
                "dcim.site".to_string(),
                TypeSchema {
                    key: BTreeMap::from([("name".to_string(), field(FieldType::String))]),
                    fields: BTreeMap::from([(
                        "asset_tag".to_string(),
                        FieldSchema {
                            r#type: FieldType::String,
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

    fn mock_empty_custom_fields(server: &MockServer) {
        let _m = server.mock(|when, then| {
            when.method(GET).path("/api/extras/custom-fields/");
            then.status(200).json_body(page(json!([])));
        });
    }

    fn state(dir: &std::path::Path) -> StateStore {
        StateStore::load(dir.join("state.json")).unwrap()
    }

    // site_schema plus the tags field the tag tests apply.
    fn tagged_site_schema() -> Schema {
        let mut schema = site_schema();
        schema.types.get_mut("dcim.site").unwrap().fields.insert(
            "tags".to_string(),
            field(FieldType::List {
                item: Box::new(FieldType::String),
            }),
        );
        schema
    }

    fn tagged_site_ops(tags: serde_json::Value) -> Vec<Op> {
        vec![Op::Create {
            uid: uid(1),
            type_name: TypeName::new("dcim.site"),
            desired: Object::new(
                uid(1),
                TypeName::new("dcim.site"),
                key("name", json!("FRA1")),
                attrs(json!({ "name": "FRA1", "slug": "fra1", "tags": tags })),
            )
            .unwrap(),
        }]
    }

    // nautobot's tag serializer requires content_types.
    fn tag_body(id: &str, name: &str) -> serde_json::Value {
        json!({ "id": id, "name": name, "slug": name, "content_types": [] })
    }

    fn mock_site_create(server: &MockServer) {
        let _m = server.mock(|when, then| {
            when.method(POST).path("/api/dcim/sites/");
            then.status(201).json_body(json!({
                "id": "11111111-1111-1111-1111-111111111111",
                "name": "FRA1",
                "slug": "fra1"
            }));
        });
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
    async fn observe_handles_empty_types_list() {
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        // dcim.device is in the registry but not the schema, so it is skipped, not an error.
        let _content_types = server.mock(|when, then| {
            when.method(GET).path("/api/extras/content-types/");
            then.status(200).json_body(page(json!([
                { "app_label": "dcim", "model": "site" },
                { "app_label": "dcim", "model": "device" },
            ])));
        });
        let _sites = server.mock(|when, then| {
            when.method(GET).path("/api/dcim/sites/");
            then.status(200).json_body(page(json!([])));
        });

        let adapter = NautobotAdapter::new(&server.base_url(), "token").unwrap();
        let observed = adapter
            .read(&site_schema(), &[], &state(dir.path()))
            .await
            .unwrap();
        assert!(observed.by_key.is_empty());
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
    async fn create_conflict_surfaces_lookup_failure() {
        // a create conflict whose recovery lookup itself fails (here the list
        // returns 500) must surface the lookup failure, not mask it as the
        // conflict, so the operator chases the real blocker.
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        mock_content_types(&server);
        mock_empty_custom_fields(&server);
        let _create = server.mock(|when, then| {
            when.method(POST).path("/api/dcim/sites/");
            then.status(409)
                .json_body(json!({ "detail": "site with this name already exists." }));
        });
        let _lookup = server.mock(|when, then| {
            when.method(GET).path("/api/dcim/sites/");
            then.status(500).body("boom");
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
        let err = adapter
            .write(&site_schema(), &ops, &state(dir.path()))
            .await
            .unwrap_err();
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
    async fn apply_tolerates_already_deleted_404() {
        // re-issued deletes must tolerate an already-gone object (404) as a no-op.
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        mock_content_types(&server);
        mock_empty_custom_fields(&server);
        let site_id = "33333333-3333-3333-3333-333333333333";
        let _delete = server.mock(|when, then| {
            when.method(DELETE)
                .path(format!("/api/dcim/sites/{site_id}/"));
            then.status(404);
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
        assert_eq!(report.applied[0].backend_id, None);
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

    // a non-slug custom field name must be created with `key` = the field name
    // so the read/detect/write paths (which key on `field.key`) match it.
    // nautobot has no writable `name`; sending `name` lets it derive `key` by
    // slugifying `label`, so `assetTag` becomes `assettag` and never matches.
    #[tokio::test]
    async fn ensure_schema_creates_non_slug_custom_field_with_key() {
        let server = MockServer::start();
        mock_content_types(&server);
        mock_empty_custom_fields(&server);
        // native-field probe: one object, none present.
        let _probe = server.mock(|when, then| {
            when.method(GET).path("/api/dcim/sites/");
            then.status(200).json_body(page(json!([])));
        });
        // the create must carry `key` (= the field name), not `name`: this mock
        // only matches when the body includes `"key": "assetTag"`.
        let cf_create = server.mock(|when, then| {
            when.method(POST)
                .path("/api/extras/custom-fields/")
                .json_body_includes(r#"{"key": "assetTag", "label": "assetTag"}"#);
            then.status(201).json_body(json!({
                "id": "44444444-4444-4444-4444-444444444444",
                "key": "assetTag",
                "label": "assetTag",
                "content_types": ["dcim.site"],
                "type": {},
            }));
        });

        let schema = Schema {
            types: BTreeMap::from([(
                "dcim.site".to_string(),
                TypeSchema {
                    key: BTreeMap::from([("name".to_string(), field(FieldType::String))]),
                    fields: BTreeMap::from([("assetTag".to_string(), field(FieldType::String))]),
                },
            )]),
        };

        let adapter = NautobotAdapter::new(&server.base_url(), "token").unwrap();
        let report = adapter.ensure_schema(&schema).await.unwrap();

        assert_eq!(
            report.created_fields,
            vec!["dcim.site.assetTag".to_string()]
        );
        cf_create.assert_calls(1);
    }

    // a declared enum is provisioned as a `select` carrying its values as
    // choices, weighted in declaration order.
    #[tokio::test]
    async fn ensure_schema_creates_a_select_with_its_choices() {
        let server = MockServer::start();
        mock_content_types(&server);
        mock_empty_custom_fields(&server);
        let _probe = server.mock(|when, then| {
            when.method(GET).path("/api/dcim/sites/");
            then.status(200).json_body(page(json!([])));
        });
        let field_id = "44444444-4444-4444-4444-444444444444";
        let cf_create = server.mock(|when, then| {
            when.method(POST)
                .path("/api/extras/custom-fields/")
                .json_body_includes(r#"{"type": "select"}"#);
            then.status(201).json_body(json!({
                "id": field_id,
                "key": "tier",
                "label": "tier",
                "content_types": ["dcim.site"],
                "type": {},
            }));
        });
        // one mock per declared value: each matches only its own value, so the
        // weights prove the choices went out in declaration order.
        let choices: Vec<_> = ["core", "agg", "edge"]
            .iter()
            .enumerate()
            .map(|(index, value)| {
                let body = json!({
                    "value": value,
                    "weight": (index + 1) * 100,
                    "custom_field": field_id,
                })
                .to_string();
                server.mock(|when, then| {
                    when.method(POST)
                        .path("/api/extras/custom-field-choices/")
                        .json_body_includes(&body);
                    then.status(201)
                        .json_body(json!({ "id": field_id, "value": value }));
                })
            })
            .collect();

        let mut schema = site_schema();
        schema.types.get_mut("dcim.site").unwrap().fields.insert(
            "tier".to_string(),
            field(FieldType::Enum {
                values: vec!["core".to_string(), "agg".to_string(), "edge".to_string()],
            }),
        );

        let adapter = NautobotAdapter::new(&server.base_url(), "token").unwrap();
        let report = adapter.ensure_schema(&schema).await.unwrap();

        assert_eq!(report.created_fields, vec!["dcim.site.tier".to_string()]);
        cf_create.assert_calls(1);
        for choice in choices {
            choice.assert_calls(1);
        }
    }

    // a nautobot select reads back as a plain string (multi-select as an array
    // of them), which is exactly what core's enum check wants.
    #[tokio::test]
    async fn observe_round_trips_an_enum_valued_object() {
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        mock_content_types(&server);
        let _sites = server.mock(|when, then| {
            when.method(GET).path("/api/dcim/sites/");
            then.status(200).json_body(page(json!([{
                "id": "11111111-1111-1111-1111-111111111111",
                "name": "FRA1",
                "slug": "fra1",
                "_custom_field_data": { "tier": "core", "roles": ["core", "edge"] },
            }])));
        });

        let values = vec!["core".to_string(), "agg".to_string(), "edge".to_string()];
        let mut schema = site_schema();
        let site = schema.types.get_mut("dcim.site").unwrap();
        // the shared `field()` helper is nullable; a key field may not be.
        let name_key = site.key.get_mut("name").unwrap();
        name_key.nullable = false;
        name_key.required = true;
        let fields = &mut site.fields;
        fields.insert(
            "tier".to_string(),
            field(FieldType::Enum {
                values: values.clone(),
            }),
        );
        fields.insert(
            "roles".to_string(),
            field(FieldType::List {
                item: Box::new(FieldType::Enum { values }),
            }),
        );

        let adapter = NautobotAdapter::new(&server.base_url(), "token").unwrap();
        let observed = adapter
            .read(&schema, &[TypeName::new("dcim.site")], &state(dir.path()))
            .await
            .unwrap();

        let object = observed.by_key.values().next().unwrap();
        assert_eq!(object.attrs.get("tier"), Some(&json!("core")));
        assert_eq!(object.attrs.get("roles"), Some(&json!(["core", "edge"])));

        // and core accepts what came back against the schema that declared it.
        let inventory = alembic_core::Inventory {
            schema,
            objects: vec![Object::new(
                uid(1),
                object.type_name.clone(),
                object.key.clone(),
                object.attrs.clone(),
            )
            .unwrap()],
        };
        let report = alembic_core::validate_inventory(&inventory);
        assert!(report.errors.is_empty(), "{:?}", report.errors);
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

    // provisioning converges an existing field onto the properties the schema
    // declares. the three below are exactly what a create sends beyond identity
    // and type, and all three sit on nautobot's patch body.
    #[tokio::test]
    async fn ensure_schema_converges_an_existing_field() {
        let server = MockServer::start();
        mock_content_types(&server);
        mock_existing_custom_field(
            &server,
            json!({"required": false, "description": "", "validation_regex": ""}),
        );
        let _probe = server.mock(|when, then| {
            when.method(GET).path("/api/dcim/sites/");
            then.status(200).json_body(page(json!([])));
        });
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

        let adapter = NautobotAdapter::new(&server.base_url(), "token").unwrap();
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
        mock_content_types(&server);
        mock_existing_custom_field(
            &server,
            json!({
                "required": true,
                "description": "asset tag",
                "validation_regex": "^SITE-",
            }),
        );
        let _probe = server.mock(|when, then| {
            when.method(GET).path("/api/dcim/sites/");
            then.status(200).json_body(page(json!([])));
        });
        let cf_patch = server.mock(|when, then| {
            when.method(PATCH)
                .path(format!("/api/extras/custom-fields/{EXISTING_FIELD_ID}/"));
            then.status(200).json_body(json!({}));
        });

        let adapter = NautobotAdapter::new(&server.base_url(), "token").unwrap();
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
        mock_content_types(&server);
        mock_existing_custom_field(
            &server,
            json!({
                "required": true,
                "description": "written by an operator",
                "validation_regex": "",
            }),
        );
        let _probe = server.mock(|when, then| {
            when.method(GET).path("/api/dcim/sites/");
            then.status(200).json_body(page(json!([])));
        });
        let cf_patch = server.mock(|when, then| {
            when.method(PATCH)
                .path(format!("/api/extras/custom-fields/{EXISTING_FIELD_ID}/"))
                .json_body(json!({"validation_regex": "^SITE-"}));
            then.status(200).json_body(json!({}));
        });

        let adapter = NautobotAdapter::new(&server.base_url(), "token").unwrap();
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
        mock_content_types(&server);
        mock_existing_custom_field(
            &server,
            json!({"required": false, "description": "", "validation_regex": ""}),
        );
        let _probe = server.mock(|when, then| {
            when.method(GET).path("/api/dcim/sites/");
            then.status(200).json_body(page(json!([])));
        });
        let cf_patch = server.mock(|when, then| {
            when.method(PATCH)
                .path(format!("/api/extras/custom-fields/{EXISTING_FIELD_ID}/"));
            then.status(200).json_body(json!({}));
        });

        let adapter = NautobotAdapter::new(&server.base_url(), "token").unwrap();
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

    #[tokio::test]
    async fn apply_creates_missing_tags() {
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        mock_content_types(&server);
        mock_empty_custom_fields(&server);
        mock_site_create(&server);
        let _tags = server.mock(|when, then| {
            when.method(GET).path("/api/extras/tags/");
            then.status(200).json_body(page(json!([])));
        });
        let _tag_create = server.mock(|when, then| {
            when.method(POST).path("/api/extras/tags/");
            then.status(201)
                .json_body(tag_body("22222222-2222-2222-2222-222222222222", "fabric"));
        });

        let adapter = NautobotAdapter::new(&server.base_url(), "token").unwrap();
        let report = adapter
            .write(
                &tagged_site_schema(),
                &tagged_site_ops(json!(["fabric"])),
                &state(dir.path()),
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
        mock_content_types(&server);
        mock_empty_custom_fields(&server);
        mock_site_create(&server);
        let _tags = server.mock(|when, then| {
            when.method(GET).path("/api/extras/tags/");
            then.status(200).json_body(page(json!([tag_body(
                "22222222-2222-2222-2222-222222222222",
                "fabric"
            )])));
        });
        let _tag_create = server.mock(|when, then| {
            when.method(POST).path("/api/extras/tags/");
            then.status(201)
                .json_body(tag_body("33333333-3333-3333-3333-333333333333", "edge"));
        });

        let adapter = NautobotAdapter::new(&server.base_url(), "token").unwrap();
        let report = adapter
            .write(
                &tagged_site_schema(),
                &tagged_site_ops(json!(["fabric", "edge"])),
                &state(dir.path()),
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
        mock_content_types(&server);
        mock_empty_custom_fields(&server);
        mock_site_create(&server);
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
            then.status(200).json_body(page(json!([tag_body(
                "22222222-2222-2222-2222-222222222222",
                "fabric"
            )])));
        });
        let _tag_create = server.mock(|when, then| {
            when.method(POST).path("/api/extras/tags/");
            then.status(400)
                .json_body(json!({"name": ["tag with this name already exists."]}));
        });

        let adapter = NautobotAdapter::new(&server.base_url(), "token").unwrap();
        let report = adapter
            .write(
                &tagged_site_schema(),
                &tagged_site_ops(json!(["fabric"])),
                &state(dir.path()),
            )
            .await
            .unwrap();

        assert_eq!(report.applied.len(), 1);
        assert!(report.provision.created_tags.is_empty());
    }
}
