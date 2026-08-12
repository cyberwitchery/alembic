use super::*;
use alembic_core::{FieldSchema, FieldType, Key, TypeSchema};
use alembic_engine::{StateData, StateStore};
use httpmock::prelude::*;
use httpmock::Method::PATCH;

fn new_state_store() -> StateStore {
    StateStore::new(None, StateData::default())
}

fn test_config(base_url: &str) -> GenericConfig {
    let mut types = BTreeMap::new();
    types.insert(
        "device".to_string(),
        EndpointConfig {
            path: "/api/devices".to_string(),
            results_path: Some("results".to_string()),
            next_path: None,
            id_path: "id".to_string(),
            delete_strategy: DeleteStrategy::Standard,
            update_method: "PATCH".to_string(),
        },
    );
    types.insert(
        "site".to_string(),
        EndpointConfig {
            path: "/api/sites".to_string(),
            results_path: None,
            next_path: None,
            id_path: "id".to_string(),
            delete_strategy: DeleteStrategy::None,
            update_method: "PUT".to_string(),
        },
    );
    GenericConfig {
        base_url: base_url.to_string(),
        headers: BTreeMap::new(),
        types,
    }
}

fn test_schema() -> Schema {
    let mut types = BTreeMap::new();

    let mut device_fields = BTreeMap::new();
    device_fields.insert(
        "name".to_string(),
        FieldSchema {
            r#type: FieldType::String,
            required: true,
            nullable: false,
            description: None,
            format: None,
            pattern: None,
        },
    );
    device_fields.insert(
        "site".to_string(),
        FieldSchema {
            r#type: FieldType::Ref {
                target: "site".to_string(),
            },
            required: true,
            nullable: false,
            description: None,
            format: None,
            pattern: None,
        },
    );

    let mut device_key = BTreeMap::new();
    device_key.insert(
        "name".to_string(),
        FieldSchema {
            r#type: FieldType::String,
            required: true,
            nullable: false,
            description: None,
            format: None,
            pattern: None,
        },
    );
    types.insert(
        "device".to_string(),
        TypeSchema {
            key: device_key,
            fields: device_fields,
        },
    );

    let mut site_fields = BTreeMap::new();
    site_fields.insert(
        "name".to_string(),
        FieldSchema {
            r#type: FieldType::String,
            required: true,
            nullable: false,
            description: None,
            format: None,
            pattern: None,
        },
    );

    let mut site_key = BTreeMap::new();
    site_key.insert(
        "name".to_string(),
        FieldSchema {
            r#type: FieldType::String,
            required: true,
            nullable: false,
            description: None,
            format: None,
            pattern: None,
        },
    );
    types.insert(
        "site".to_string(),
        TypeSchema {
            key: site_key,
            fields: site_fields,
        },
    );

    Schema { types }
}

fn empty_schema() -> Schema {
    Schema {
        types: BTreeMap::new(),
    }
}

fn field_schema(r#type: FieldType) -> FieldSchema {
    FieldSchema {
        r#type,
        required: false,
        nullable: false,
        description: None,
        format: None,
        pattern: None,
    }
}

// tests for resolve_path
#[test]
fn test_resolve_path_simple() {
    let value = serde_json::json!({"id": 42, "name": "test"});
    let result = resolve_path(&value, "id").unwrap();
    assert_eq!(result, serde_json::json!(42));
}

#[test]
fn test_resolve_path_nested() {
    let value = serde_json::json!({"data": {"results": [1, 2, 3]}});
    let result = resolve_path(&value, "data.results").unwrap();
    assert_eq!(result, serde_json::json!([1, 2, 3]));
}

#[test]
fn test_resolve_path_empty() {
    let value = serde_json::json!({"id": 42});
    let result = resolve_path(&value, "").unwrap();
    assert_eq!(result, serde_json::json!({"id": 42}));
}

#[test]
fn test_resolve_path_not_found() {
    let value = serde_json::json!({"id": 42});
    let err = resolve_path(&value, "missing").unwrap_err();
    assert!(err.to_string().contains("path segment not found"));
}

// tests for parse_backend_id
#[test]
fn test_parse_backend_id_number() {
    assert_eq!(
        parse_backend_id(serde_json::json!(42)).unwrap(),
        BackendId::Int(42)
    );
}

#[test]
fn test_parse_backend_id_string() {
    assert_eq!(
        parse_backend_id(serde_json::json!("abc-123")).unwrap(),
        BackendId::String("abc-123".to_string())
    );
}

#[test]
fn test_parse_backend_id_negative_rejected() {
    let err = parse_backend_id(serde_json::json!(-1)).unwrap_err();
    assert!(err.to_string().contains("invalid integer id"));
}

#[test]
fn test_parse_backend_id_non_scalar_rejected() {
    let err = parse_backend_id(serde_json::json!(true)).unwrap_err();
    assert!(err.to_string().contains("id must be number or string"));
}

// tests for resolved_ids_identity
#[test]
fn test_resolved_ids_identity_empty() {
    let state = new_state_store();
    let resolved = resolved_ids_identity(&state);
    assert!(resolved.is_empty());
}

#[test]
fn test_resolved_ids_identity_with_mappings() {
    let mut state = new_state_store();
    let uid = Uid::new_v4();
    state.set_backend_id(TypeName::new("device".to_string()), uid, BackendId::Int(42));
    let resolved = resolved_ids_identity(&state);
    assert_eq!(resolved.get(&uid), Some(&BackendId::Int(42)));
}

// these drive resolve_attrs (which delegates to the shared engine
// build_request_body) with a one-field schema, exercising the generic encode
// closure for int/string backend ids, the ref/list-ref resolution and MissingRef
// path surfaced by the shared helper, and the null passthrough that clears a
// nullable field.
fn body_field(
    field_type: FieldType,
    nullable: bool,
    value: serde_json::Value,
    resolved: &BTreeMap<Uid, BackendId>,
) -> Result<serde_json::Value> {
    let mut fields = BTreeMap::new();
    fields.insert(
        "f".to_string(),
        FieldSchema {
            r#type: field_type,
            required: false,
            nullable,
            description: None,
            format: None,
            pattern: None,
        },
    );
    let type_schema = TypeSchema {
        key: BTreeMap::new(),
        fields,
    };
    let attrs: JsonMap = serde_json::json!({ "f": value })
        .as_object()
        .unwrap()
        .clone()
        .into_iter()
        .collect::<BTreeMap<_, _>>()
        .into();
    resolve_attrs(&attrs, &type_schema, resolved)
}

#[test]
fn test_resolve_attrs_encodes_ref_int_backend_id() {
    let mut resolved = BTreeMap::new();
    let uid = Uid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    resolved.insert(uid, BackendId::Int(123));

    let body = body_field(
        FieldType::Ref {
            target: "site".to_string(),
        },
        false,
        serde_json::json!("550e8400-e29b-41d4-a716-446655440000"),
        &resolved,
    )
    .unwrap();
    assert_eq!(body, serde_json::json!({ "f": 123 }));
}

#[test]
fn test_resolve_attrs_encodes_list_ref() {
    let mut resolved = BTreeMap::new();
    let uid1 = Uid::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap();
    let uid2 = Uid::parse_str("550e8400-e29b-41d4-a716-446655440002").unwrap();
    resolved.insert(uid1, BackendId::Int(1));
    resolved.insert(uid2, BackendId::String("abc".to_string()));

    let body = body_field(
        FieldType::ListRef {
            target: "tag".to_string(),
        },
        false,
        serde_json::json!([
            "550e8400-e29b-41d4-a716-446655440001",
            "550e8400-e29b-41d4-a716-446655440002"
        ]),
        &resolved,
    )
    .unwrap();
    assert_eq!(body, serde_json::json!({ "f": [1, "abc"] }));
}

// exercises the encode closure for a string backend id.
#[test]
fn test_resolve_attrs_encodes_ref_string_backend_id() {
    let mut resolved = BTreeMap::new();
    let uid = Uid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    resolved.insert(uid, BackendId::String("abc-123".to_string()));

    let body = body_field(
        FieldType::Ref {
            target: "site".to_string(),
        },
        false,
        serde_json::json!("550e8400-e29b-41d4-a716-446655440000"),
        &resolved,
    )
    .unwrap();
    assert_eq!(body, serde_json::json!({ "f": "abc-123" }));
}

// an unresolved ref uid still surfaces the shared MissingRef error.
#[test]
fn test_resolve_attrs_ref_missing_uid_errors() {
    let resolved = BTreeMap::new();
    let err = body_field(
        FieldType::Ref {
            target: "site".to_string(),
        },
        false,
        serde_json::json!("550e8400-e29b-41d4-a716-446655440000"),
        &resolved,
    )
    .unwrap_err();
    assert!(err.to_string().contains("missing referenced uid"));
}

// a null value for a nullable ref must pass straight through as json null (to
// clear the field on the backend), not reach the ref encoder and error with
// "ref value must be a uuid string".
#[test]
fn test_resolve_attrs_passes_null_ref_through() {
    let resolved = BTreeMap::new();
    let body = body_field(
        FieldType::Ref {
            target: "site".to_string(),
        },
        true,
        serde_json::Value::Null,
        &resolved,
    )
    .unwrap();
    assert_eq!(body, serde_json::json!({ "f": null }));
}

// tests for resolve_attrs
#[test]
fn test_resolve_attrs_success() {
    let schema = test_schema();
    let type_schema = schema.types.get("site").unwrap();
    let attrs: JsonMap = serde_json::json!({"name": "site1"})
        .as_object()
        .unwrap()
        .clone()
        .into_iter()
        .collect::<BTreeMap<_, _>>()
        .into();
    let resolved = BTreeMap::new();
    let result = resolve_attrs(&attrs, type_schema, &resolved).unwrap();
    assert_eq!(result, serde_json::json!({"name": "site1"}));
}

#[test]
fn test_resolve_attrs_missing_schema() {
    let schema = test_schema();
    let type_schema = schema.types.get("site").unwrap();
    let attrs: JsonMap = serde_json::json!({"unknown_field": "value"})
        .as_object()
        .unwrap()
        .clone()
        .into_iter()
        .collect::<BTreeMap<_, _>>()
        .into();
    let resolved = BTreeMap::new();
    let err = resolve_attrs(&attrs, type_schema, &resolved).unwrap_err();
    assert!(err.to_string().contains("missing schema for field"));
}

// the shared `resolve_value_for_type` resolves refs nested inside `List` and
// `Map` fields when building the request body, matching netbox/nautobot.
#[test]
fn test_resolve_attrs_resolves_refs_nested_in_list() {
    let type_schema = TypeSchema {
        key: BTreeMap::new(),
        fields: BTreeMap::from([(
            "members".to_string(),
            field_schema(FieldType::List {
                item: Box::new(FieldType::Ref {
                    target: "site".to_string(),
                }),
            }),
        )]),
    };
    let uid = Uid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let mut resolved = BTreeMap::new();
    resolved.insert(uid, BackendId::Int(123));

    let attrs: JsonMap = serde_json::json!({
        "members": ["550e8400-e29b-41d4-a716-446655440000"]
    })
    .as_object()
    .unwrap()
    .clone()
    .into_iter()
    .collect::<BTreeMap<_, _>>()
    .into();

    let body = resolve_attrs(&attrs, &type_schema, &resolved).unwrap();
    assert_eq!(body, serde_json::json!({"members": [123]}));
}

#[test]
fn test_resolve_attrs_resolves_refs_nested_in_map() {
    let type_schema = TypeSchema {
        key: BTreeMap::new(),
        fields: BTreeMap::from([(
            "links".to_string(),
            field_schema(FieldType::Map {
                value: Box::new(FieldType::Ref {
                    target: "site".to_string(),
                }),
            }),
        )]),
    };
    let uid = Uid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let mut resolved = BTreeMap::new();
    resolved.insert(uid, BackendId::String("dev-9".to_string()));

    let attrs: JsonMap = serde_json::json!({
        "links": {"primary": "550e8400-e29b-41d4-a716-446655440000"}
    })
    .as_object()
    .unwrap()
    .clone()
    .into_iter()
    .collect::<BTreeMap<_, _>>()
    .into();

    let body = resolve_attrs(&attrs, &type_schema, &resolved).unwrap();
    assert_eq!(body, serde_json::json!({"links": {"primary": "dev-9"}}));
}

#[test]
fn test_resolve_attrs_unresolved_nested_ref_surfaces_missing_ref() {
    let type_schema = TypeSchema {
        key: BTreeMap::new(),
        fields: BTreeMap::from([(
            "members".to_string(),
            field_schema(FieldType::List {
                item: Box::new(FieldType::Ref {
                    target: "site".to_string(),
                }),
            }),
        )]),
    };
    // empty `resolved` -> the nested uid cannot be resolved.
    let resolved = BTreeMap::new();

    let attrs: JsonMap = serde_json::json!({
        "members": ["550e8400-e29b-41d4-a716-446655440000"]
    })
    .as_object()
    .unwrap()
    .clone()
    .into_iter()
    .collect::<BTreeMap<_, _>>()
    .into();

    let err = resolve_attrs(&attrs, &type_schema, &resolved).unwrap_err();
    assert!(
        is_missing_ref_error(&err),
        "nested unresolved ref must surface MissingRef for the retry loop, got: {err}"
    );
}

#[test]
fn test_normalize_attrs_refs_resolves_refs_nested_in_list() {
    let type_schema = TypeSchema {
        key: BTreeMap::new(),
        fields: BTreeMap::from([(
            "members".to_string(),
            field_schema(FieldType::List {
                item: Box::new(FieldType::Ref {
                    target: "site".to_string(),
                }),
            }),
        )]),
    };
    let site_uid = Uid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let mut state = new_state_store();
    state.set_backend_id(
        TypeName::new("site".to_string()),
        site_uid,
        BackendId::Int(7),
    );
    let mappings = StateMappings::from_state(&state);

    let attrs: JsonMap = serde_json::json!({ "members": [7] })
        .as_object()
        .unwrap()
        .clone()
        .into_iter()
        .collect::<BTreeMap<_, _>>()
        .into();

    let normalized = normalize_attrs_refs(&attrs, &type_schema, &mappings);
    assert_eq!(
        normalized.get("members"),
        Some(&serde_json::json!([site_uid.to_string()]))
    );
}

#[test]
fn test_normalize_attrs_refs_resolves_refs_nested_in_map() {
    let type_schema = TypeSchema {
        key: BTreeMap::new(),
        fields: BTreeMap::from([(
            "links".to_string(),
            field_schema(FieldType::Map {
                value: Box::new(FieldType::Ref {
                    target: "site".to_string(),
                }),
            }),
        )]),
    };
    let site_uid = Uid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let mut state = new_state_store();
    state.set_backend_id(
        TypeName::new("site".to_string()),
        site_uid,
        BackendId::Int(7),
    );
    let mappings = StateMappings::from_state(&state);

    let attrs: JsonMap = serde_json::json!({ "links": {"primary": 7} })
        .as_object()
        .unwrap()
        .clone()
        .into_iter()
        .collect::<BTreeMap<_, _>>()
        .into();

    let normalized = normalize_attrs_refs(&attrs, &type_schema, &mappings);
    assert_eq!(
        normalized.get("links"),
        Some(&serde_json::json!({"primary": site_uid.to_string()}))
    );
}

#[test]
fn test_normalize_attrs_refs_resolves_list_ref() {
    let type_schema = TypeSchema {
        key: BTreeMap::new(),
        fields: BTreeMap::from([(
            "peers".to_string(),
            field_schema(FieldType::ListRef {
                target: "site".to_string(),
            }),
        )]),
    };
    let site_uid = Uid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let mut state = new_state_store();
    state.set_backend_id(
        TypeName::new("site".to_string()),
        site_uid,
        BackendId::Int(7),
    );
    let mappings = StateMappings::from_state(&state);

    let attrs: JsonMap = serde_json::json!({ "peers": [7] })
        .as_object()
        .unwrap()
        .clone()
        .into_iter()
        .collect::<BTreeMap<_, _>>()
        .into();

    let normalized = normalize_attrs_refs(&attrs, &type_schema, &mappings);
    assert_eq!(
        normalized.get("peers"),
        Some(&serde_json::json!([site_uid.to_string()]))
    );
}

#[test]
fn test_normalize_attrs_refs_resolves_object_shaped_ref() {
    let type_schema = TypeSchema {
        key: BTreeMap::new(),
        fields: BTreeMap::from([(
            "site".to_string(),
            field_schema(FieldType::Ref {
                target: "site".to_string(),
            }),
        )]),
    };
    let site_uid = Uid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let mut state = new_state_store();
    state.set_backend_id(
        TypeName::new("site".to_string()),
        site_uid,
        BackendId::Int(7),
    );
    let mappings = StateMappings::from_state(&state);

    let attrs: JsonMap = serde_json::json!({ "site": {"id": 7} })
        .as_object()
        .unwrap()
        .clone()
        .into_iter()
        .collect::<BTreeMap<_, _>>()
        .into();

    let normalized = normalize_attrs_refs(&attrs, &type_schema, &mappings);
    assert_eq!(
        normalized.get("site"),
        Some(&serde_json::json!(site_uid.to_string()))
    );
}

// tests for GenericAdapter::new
#[test]
fn test_generic_adapter_new_success() {
    let config = test_config("http://example.com");
    let adapter = GenericAdapter::new(config);
    assert!(adapter.is_ok());
}

#[test]
fn test_generic_adapter_new_with_headers() {
    let mut config = test_config("http://example.com");
    config
        .headers
        .insert("Authorization".to_string(), "Bearer token".to_string());
    config
        .headers
        .insert("Content-Type".to_string(), "application/json".to_string());
    let adapter = GenericAdapter::new(config);
    assert!(adapter.is_ok());
}

#[test]
fn test_generic_adapter_new_invalid_header_name() {
    let mut config = test_config("http://example.com");
    config
        .headers
        .insert("invalid\nheader".to_string(), "value".to_string());
    let adapter = GenericAdapter::new(config);
    assert!(adapter.is_err());
}

// tests for backend_id_to_url
#[test]
fn test_backend_id_to_url_int() {
    let config = test_config("http://example.com/");
    let adapter = GenericAdapter::new(config).unwrap();
    let endpoint = adapter.config.types.get("device").unwrap();
    let url = adapter.backend_id_to_url(endpoint, &BackendId::Int(42));
    assert_eq!(url, "http://example.com/api/devices/42");
}

#[test]
fn test_backend_id_to_url_string() {
    let config = test_config("http://example.com");
    let adapter = GenericAdapter::new(config).unwrap();
    let endpoint = adapter.config.types.get("device").unwrap();
    let url = adapter.backend_id_to_url(endpoint, &BackendId::String("abc-123".to_string()));
    assert_eq!(url, "http://example.com/api/devices/abc-123");
}

// tests for observe with mocked server
#[tokio::test]
async fn test_observe_with_results_path() {
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(GET).path("/api/devices");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({
                "results": [
                    {"id": 1, "name": "device1"},
                    {"id": 2, "name": "device2"}
                ]
            }));
    });

    let config = test_config(&server.base_url());
    let adapter = GenericAdapter::new(config).unwrap();
    let schema = test_schema();
    let state_store = new_state_store();

    let state = adapter
        .read(
            &schema,
            &[TypeName::new("device".to_string())],
            &state_store,
        )
        .await
        .unwrap();

    mock.assert();
    assert_eq!(state.by_key.len(), 2);
}

#[tokio::test]
async fn test_observe_resolves_ref_ids_to_uids() {
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(GET).path("/api/devices");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({
                "results": [
                    {"id": 1, "name": "device1", "site": 7}
                ]
            }));
    });

    let mut state = new_state_store();
    let site_uid = Uid::new_v4();
    state.set_backend_id(
        TypeName::new("site".to_string()),
        site_uid,
        BackendId::Int(7),
    );

    let config = test_config(&server.base_url());
    let adapter = GenericAdapter::new(config).unwrap();
    let schema = test_schema();

    let observed = adapter
        .read(&schema, &[TypeName::new("device".to_string())], &state)
        .await
        .unwrap();

    mock.assert();
    let device = observed
        .by_key
        .values()
        .next()
        .expect("expected observed device");
    assert_eq!(
        device.attrs.get("site"),
        Some(&serde_json::Value::String(site_uid.to_string()))
    );
}

#[tokio::test]
async fn test_import_resolves_bare_ref_ids_from_the_observation() {
    // generic refs are bare backend ids with no key material behind them, so
    // with no state nothing but an index bootstrapped from the observation puts
    // the device's `site` in the uid space the imported site object lives in.
    // this is the second import of an already-adopted backend.
    let server = MockServer::start();
    let _devices = server.mock(|when, then| {
        when.method(GET).path("/api/devices");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({
                "results": [{"id": 2, "name": "leaf01", "site": 1}]
            }));
    });
    let _sites = server.mock(|when, then| {
        when.method(GET).path("/api/sites");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!([{"id": 1, "name": "FRA1"}]));
    });

    let adapter = GenericAdapter::new(test_config(&server.base_url())).unwrap();
    let schema = test_schema();

    let report = alembic_engine::import_inventory(&adapter, &schema, &[])
        .await
        .unwrap();

    let validation = alembic_core::validate_inventory(&report.inventory);
    assert!(
        validation.errors.is_empty(),
        "imported inventory must validate: {:?}",
        validation.errors
    );

    let object_of = |type_name: &str| {
        report
            .inventory
            .objects
            .iter()
            .find(|object| object.type_name.as_str() == type_name)
            .unwrap_or_else(|| panic!("no {type_name} in the imported inventory"))
    };
    assert_eq!(
        object_of("device")
            .attrs
            .get("site")
            .and_then(|v| v.as_str()),
        Some(object_of("site").uid.to_string().as_str())
    );
}

#[tokio::test]
async fn test_observe_without_results_path() {
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(GET).path("/api/sites");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!([
                {"id": 1, "name": "site1"},
                {"id": 2, "name": "site2"}
            ]));
    });

    let config = test_config(&server.base_url());
    let adapter = GenericAdapter::new(config).unwrap();
    let schema = test_schema();
    let state_store = new_state_store();

    let state = adapter
        .read(&schema, &[TypeName::new("site".to_string())], &state_store)
        .await
        .unwrap();

    mock.assert();
    assert_eq!(state.by_key.len(), 2);
}

#[tokio::test]
async fn test_observe_all_types() {
    let server = MockServer::start();
    let device_mock = server.mock(|when, then| {
        when.method(GET).path("/api/devices");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({"results": [{"id": 1, "name": "device1"}]}));
    });
    let site_mock = server.mock(|when, then| {
        when.method(GET).path("/api/sites");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!([{"id": 1, "name": "site1"}]));
    });

    let config = test_config(&server.base_url());
    let adapter = GenericAdapter::new(config).unwrap();
    let schema = test_schema();
    let state_store = new_state_store();

    let state = adapter.read(&schema, &[], &state_store).await.unwrap();

    device_mock.assert();
    site_mock.assert();
    assert_eq!(state.by_key.len(), 2);
}

#[tokio::test]
async fn test_observe_string_id() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(GET).path("/api/sites");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!([{"id": "uuid-123", "name": "site1"}]));
    });

    let config = test_config(&server.base_url());
    let adapter = GenericAdapter::new(config).unwrap();
    let schema = test_schema();
    let state_store = new_state_store();

    let state = adapter
        .read(&schema, &[TypeName::new("site".to_string())], &state_store)
        .await
        .unwrap();

    assert_eq!(state.by_key.len(), 1);
    let obj = state.by_key.values().next().unwrap();
    assert_eq!(
        obj.backend_id,
        Some(BackendId::String("uuid-123".to_string()))
    );
}

#[tokio::test]
async fn test_observe_unknown_type() {
    let config = test_config("http://example.com");
    let adapter = GenericAdapter::new(config).unwrap();
    let schema = test_schema();
    let state_store = new_state_store();

    let err = adapter
        .read(
            &schema,
            &[TypeName::new("unknown".to_string())],
            &state_store,
        )
        .await
        .unwrap_err();

    assert!(err.to_string().contains("no generic config for type"));
}

#[tokio::test]
async fn test_observe_missing_schema() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(GET).path("/api/devices");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({"results": []}));
    });

    let config = test_config(&server.base_url());
    let adapter = GenericAdapter::new(config).unwrap();
    let empty_schema = empty_schema();
    let state_store = new_state_store();

    let err = adapter
        .read(
            &empty_schema,
            &[TypeName::new("device".to_string())],
            &state_store,
        )
        .await
        .unwrap_err();

    assert!(err.to_string().contains("missing schema for"));
}

/// site keyed on `slug`, which is declared in `fields:` only when `declared`.
fn slug_keyed_site_schema(declared: bool) -> Schema {
    let mut fields = BTreeMap::new();
    fields.insert("name".to_string(), field_schema(FieldType::String));
    if declared {
        fields.insert("slug".to_string(), field_schema(FieldType::Slug));
    }
    let mut key = BTreeMap::new();
    key.insert("slug".to_string(), field_schema(FieldType::Slug));

    let mut types = BTreeMap::new();
    types.insert("site".to_string(), TypeSchema { key, fields });
    Schema { types }
}

#[tokio::test]
async fn test_observe_undeclared_key_field_names_the_type_and_the_field() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(GET).path("/api/sites");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!([{"id": 1, "name": "FRA1"}]));
    });

    let config = test_config(&server.base_url());
    let adapter = GenericAdapter::new(config).unwrap();

    let err = adapter
        .read(
            &slug_keyed_site_schema(false),
            &[TypeName::new("site".to_string())],
            &new_state_store(),
        )
        .await
        .unwrap_err();

    let msg = format!("{err:#}");
    assert!(msg.contains("build key for site at /api/sites/1"), "{msg}");
    assert!(msg.contains("`fields:`"), "{msg}");
    assert!(msg.contains("missing key field slug"), "{msg}");
}

#[tokio::test]
async fn test_apply_create_then_observe_round_trips_the_key() {
    let server = MockServer::start();
    let create = server.mock(|when, then| {
        when.method(POST)
            .path("/api/sites")
            .json_body(serde_json::json!({"name": "FRA1", "slug": "fra1"}));
        then.status(201)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({"id": 1, "name": "FRA1", "slug": "fra1"}));
    });
    let list = server.mock(|when, then| {
        when.method(GET).path("/api/sites");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!([{"id": 1, "name": "FRA1", "slug": "fra1"}]));
    });

    let config = test_config(&server.base_url());
    let adapter = GenericAdapter::new(config).unwrap();
    let schema = slug_keyed_site_schema(true);
    let state_store = new_state_store();

    let uid = Uid::new_v4();
    let mut key = BTreeMap::new();
    key.insert("slug".to_string(), serde_json::json!("fra1"));
    let key = Key::from(key);
    let mut attrs = BTreeMap::new();
    attrs.insert("name".to_string(), serde_json::json!("FRA1"));
    attrs.insert("slug".to_string(), serde_json::json!("fra1"));

    let ops = vec![Op::Create {
        uid,
        type_name: TypeName::new("site".to_string()),
        desired: alembic_core::Object {
            uid,
            type_name: TypeName::new("site".to_string()),
            key: key.clone(),
            attrs: attrs.into(),
            source: None,
        },
    }];

    adapter.write(&schema, &ops, &state_store).await.unwrap();
    create.assert();

    let observed = adapter
        .read(&schema, &[TypeName::new("site".to_string())], &state_store)
        .await
        .unwrap();
    list.assert();

    let obj = observed.by_key.values().next().unwrap();
    assert_eq!(obj.key, key);
    assert_eq!(obj.backend_id, Some(BackendId::Int(1)));
}

// tests for apply with mocked server
#[tokio::test]
async fn test_apply_create() {
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(POST).path("/api/sites");
        then.status(201)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({"id": 42, "name": "new-site"}));
    });

    let config = test_config(&server.base_url());
    let adapter = GenericAdapter::new(config).unwrap();
    let schema = test_schema();

    let uid = Uid::new_v4();
    let mut key = BTreeMap::new();
    key.insert("name".to_string(), serde_json::json!("new-site"));
    let mut attrs = BTreeMap::new();
    attrs.insert("name".to_string(), serde_json::json!("new-site"));

    let ops = vec![Op::Create {
        uid,
        type_name: TypeName::new("site".to_string()),
        desired: alembic_core::Object {
            uid,
            type_name: TypeName::new("site".to_string()),
            key: Key::from(key),
            attrs: attrs.into(),
            source: None,
        },
    }];

    let state = new_state_store();
    let report = adapter.write(&schema, &ops, &state).await.unwrap();
    mock.assert();
    assert_eq!(report.applied.len(), 1);
    assert_eq!(report.applied[0].backend_id, Some(BackendId::Int(42)));
}

#[tokio::test]
async fn test_apply_create_retries_out_of_order_dependencies() {
    let server = MockServer::start();
    let create_site = server.mock(|when, then| {
        when.method(POST).path("/api/sites");
        then.status(201)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({"id": 11, "name": "fra1"}));
    });
    let create_device = server.mock(|when, then| {
        when.method(POST)
            .path("/api/devices")
            .json_body(serde_json::json!({"name": "leaf01", "site": 11}));
        then.status(201)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({"id": 22, "name": "leaf01"}));
    });

    let config = test_config(&server.base_url());
    let adapter = GenericAdapter::new(config).unwrap();
    let schema = test_schema();

    let site_uid = Uid::new_v4();
    let device_uid = Uid::new_v4();

    let mut site_key = BTreeMap::new();
    site_key.insert("name".to_string(), serde_json::json!("fra1"));
    let mut site_attrs = BTreeMap::new();
    site_attrs.insert("name".to_string(), serde_json::json!("fra1"));

    let mut device_key = BTreeMap::new();
    device_key.insert("name".to_string(), serde_json::json!("leaf01"));
    let mut device_attrs = BTreeMap::new();
    device_attrs.insert("name".to_string(), serde_json::json!("leaf01"));
    device_attrs.insert("site".to_string(), serde_json::json!(site_uid.to_string()));

    // intentionally place the dependent object first to assert retry behavior.
    let ops = vec![
        Op::Create {
            uid: device_uid,
            type_name: TypeName::new("device".to_string()),
            desired: alembic_core::Object {
                uid: device_uid,
                type_name: TypeName::new("device".to_string()),
                key: Key::from(device_key),
                attrs: device_attrs.into(),
                source: None,
            },
        },
        Op::Create {
            uid: site_uid,
            type_name: TypeName::new("site".to_string()),
            desired: alembic_core::Object {
                uid: site_uid,
                type_name: TypeName::new("site".to_string()),
                key: Key::from(site_key),
                attrs: site_attrs.into(),
                source: None,
            },
        },
    ];

    let state = new_state_store();
    let report = adapter.write(&schema, &ops, &state).await.unwrap();
    create_site.assert();
    create_device.assert();
    assert_eq!(report.applied.len(), 2);
}

#[tokio::test]
async fn test_apply_update_patch() {
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(PATCH).path("/api/devices/42");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({"id": 42, "name": "updated"}));
    });

    let mut state = new_state_store();
    let uid = Uid::new_v4();
    state.set_backend_id(TypeName::new("device".to_string()), uid, BackendId::Int(42));

    // add a site reference that will be resolved
    let site_uid = Uid::new_v4();
    state.set_backend_id(
        TypeName::new("site".to_string()),
        site_uid,
        BackendId::Int(1),
    );

    let config = test_config(&server.base_url());
    let adapter = GenericAdapter::new(config).unwrap();
    let schema = test_schema();

    let mut key = BTreeMap::new();
    key.insert("name".to_string(), serde_json::json!("updated"));
    let mut attrs = BTreeMap::new();
    attrs.insert("name".to_string(), serde_json::json!("updated"));
    attrs.insert("site".to_string(), serde_json::json!(site_uid.to_string()));

    let ops = vec![Op::Update {
        uid,
        type_name: TypeName::new("device".to_string()),
        desired: alembic_core::Object {
            uid,
            type_name: TypeName::new("device".to_string()),
            key: Key::from(key),
            attrs: attrs.into(),
            source: None,
        },
        backend_id: Some(BackendId::Int(42)),
        changes: vec![],
    }];

    let report = adapter.write(&schema, &ops, &state).await.unwrap();
    mock.assert();
    assert_eq!(report.applied.len(), 1);
}

#[tokio::test]
async fn test_apply_update_put() {
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(PUT).path("/api/sites/42");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({"id": 42, "name": "updated"}));
    });

    let mut state = new_state_store();
    let uid = Uid::new_v4();
    state.set_backend_id(TypeName::new("site".to_string()), uid, BackendId::Int(42));

    let config = test_config(&server.base_url());
    let adapter = GenericAdapter::new(config).unwrap();
    let schema = test_schema();

    let mut key = BTreeMap::new();
    key.insert("name".to_string(), serde_json::json!("updated"));
    let mut attrs = BTreeMap::new();
    attrs.insert("name".to_string(), serde_json::json!("updated"));

    let ops = vec![Op::Update {
        uid,
        type_name: TypeName::new("site".to_string()),
        desired: alembic_core::Object {
            uid,
            type_name: TypeName::new("site".to_string()),
            key: Key::from(key),
            attrs: attrs.into(),
            source: None,
        },
        backend_id: Some(BackendId::Int(42)),
        changes: vec![],
    }];

    let report = adapter.write(&schema, &ops, &state).await.unwrap();
    mock.assert();
    assert_eq!(report.applied.len(), 1);
}

#[tokio::test]
async fn test_apply_delete_standard() {
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(DELETE).path("/api/devices/42");
        then.status(204);
    });

    let config = test_config(&server.base_url());
    let adapter = GenericAdapter::new(config).unwrap();
    let schema = test_schema();

    let uid = Uid::new_v4();
    let mut key = BTreeMap::new();
    key.insert("name".to_string(), serde_json::json!("to-delete"));

    let ops = vec![Op::Delete {
        uid,
        type_name: TypeName::new("device".to_string()),
        key: Key::from(key),
        backend_id: Some(BackendId::Int(42)),
    }];

    let state = new_state_store();
    let report = adapter.write(&schema, &ops, &state).await.unwrap();
    mock.assert();
    assert_eq!(report.applied.len(), 1);
    assert_eq!(report.applied[0].backend_id, None);
}

#[tokio::test]
async fn test_apply_delete_none_strategy() {
    // site has DeleteStrategy::None, so delete should fail with an explicit error
    let config = test_config("http://example.com");
    let adapter = GenericAdapter::new(config).unwrap();
    let schema = test_schema();

    let uid = Uid::new_v4();
    let mut key = BTreeMap::new();
    key.insert("name".to_string(), serde_json::json!("to-delete"));

    let ops = vec![Op::Delete {
        uid,
        type_name: TypeName::new("site".to_string()),
        key: Key::from(key),
        backend_id: Some(BackendId::Int(42)),
    }];

    let state = new_state_store();
    let err = adapter.write(&schema, &ops, &state).await.unwrap_err();
    assert!(
        err.to_string().contains("delete not supported"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_apply_delete_missing_backend_id() {
    let config = test_config("http://example.com");
    let adapter = GenericAdapter::new(config).unwrap();
    let schema = test_schema();

    let uid = Uid::new_v4();
    let mut key = BTreeMap::new();
    key.insert("name".to_string(), serde_json::json!("to-delete"));

    let ops = vec![Op::Delete {
        uid,
        type_name: TypeName::new("device".to_string()),
        key: Key::from(key),
        backend_id: None,
    }];

    let state = new_state_store();
    let err = adapter.write(&schema, &ops, &state).await.unwrap_err();
    assert!(err.to_string().contains("delete requires backend id"));
}

#[tokio::test]
async fn test_apply_update_missing_backend_id() {
    let config = test_config("http://example.com");
    let adapter = GenericAdapter::new(config).unwrap();
    let schema = test_schema();

    let uid = Uid::new_v4();
    let mut key = BTreeMap::new();
    key.insert("name".to_string(), serde_json::json!("test"));
    let mut attrs = BTreeMap::new();
    attrs.insert("name".to_string(), serde_json::json!("test"));

    let ops = vec![Op::Update {
        uid,
        type_name: TypeName::new("site".to_string()),
        desired: alembic_core::Object {
            uid,
            type_name: TypeName::new("site".to_string()),
            key: Key::from(key),
            attrs: attrs.into(),
            source: None,
        },
        backend_id: None,
        changes: vec![],
    }];

    let state = new_state_store();
    let err = adapter.write(&schema, &ops, &state).await.unwrap_err();
    assert!(err.to_string().contains("update requires backend id"));
}

#[tokio::test]
async fn test_apply_create_string_id() {
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(POST).path("/api/sites");
        then.status(201)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({"id": "uuid-abc-123", "name": "new-site"}));
    });

    let config = test_config(&server.base_url());
    let adapter = GenericAdapter::new(config).unwrap();
    let schema = test_schema();

    let uid = Uid::new_v4();
    let mut key = BTreeMap::new();
    key.insert("name".to_string(), serde_json::json!("new-site"));
    let mut attrs = BTreeMap::new();
    attrs.insert("name".to_string(), serde_json::json!("new-site"));

    let ops = vec![Op::Create {
        uid,
        type_name: TypeName::new("site".to_string()),
        desired: alembic_core::Object {
            uid,
            type_name: TypeName::new("site".to_string()),
            key: Key::from(key),
            attrs: attrs.into(),
            source: None,
        },
    }];

    let state = new_state_store();
    let report = adapter.write(&schema, &ops, &state).await.unwrap();
    mock.assert();
    assert_eq!(
        report.applied[0].backend_id,
        Some(BackendId::String("uuid-abc-123".to_string()))
    );
}

#[tokio::test]
async fn test_apply_unknown_type() {
    let config = test_config("http://example.com");
    let adapter = GenericAdapter::new(config).unwrap();
    let schema = test_schema();

    let uid = Uid::new_v4();
    let mut key = BTreeMap::new();
    key.insert("name".to_string(), serde_json::json!("test"));
    let mut attrs = BTreeMap::new();
    attrs.insert("name".to_string(), serde_json::json!("test"));

    let ops = vec![Op::Create {
        uid,
        type_name: TypeName::new("unknown".to_string()),
        desired: alembic_core::Object {
            uid,
            type_name: TypeName::new("unknown".to_string()),
            key: Key::from(key),
            attrs: attrs.into(),
            source: None,
        },
    }];

    let state = new_state_store();
    let err = adapter.write(&schema, &ops, &state).await.unwrap_err();
    assert!(err.to_string().contains("no config for unknown"));
}

#[tokio::test]
async fn test_observe_invalid_id_type() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(GET).path("/api/sites");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!([{"id": {"nested": "object"}, "name": "site1"}]));
    });

    let config = test_config(&server.base_url());
    let adapter = GenericAdapter::new(config).unwrap();
    let schema = test_schema();
    let state_store = new_state_store();

    let err = adapter
        .read(&schema, &[TypeName::new("site".to_string())], &state_store)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("id must be number or string"));
}

#[tokio::test]
async fn test_observe_non_object_in_results() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(GET).path("/api/sites");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!(["string_item", "another"]));
    });

    let config = test_config(&server.base_url());
    let adapter = GenericAdapter::new(config).unwrap();
    let schema = test_schema();
    let state_store = new_state_store();

    let err = adapter
        .read(&schema, &[TypeName::new("site".to_string())], &state_store)
        .await
        .unwrap_err();

    // the error could be about missing id path since strings don't have "id"
    let err_str = err.to_string();
    assert!(
        err_str.contains("expected object in results")
            || err_str.contains("path segment not found"),
        "unexpected error: {}",
        err_str
    );
}

#[tokio::test]
async fn test_observe_non_array_response() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(GET).path("/api/sites");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({"not": "an_array"}));
    });

    let config = test_config(&server.base_url());
    let adapter = GenericAdapter::new(config).unwrap();
    let schema = test_schema();
    let state_store = new_state_store();

    let err = adapter
        .read(&schema, &[TypeName::new("site".to_string())], &state_store)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("expected array in list response"));
}

#[tokio::test]
async fn test_apply_create_conflict_reuses_existing() {
    // a create that conflicts (409, e.g. a prior run already created it) is
    // recovered by looking the object up by key and reusing its id.
    let server = MockServer::start();
    let create = server.mock(|when, then| {
        when.method(POST).path("/api/sites");
        then.status(409);
    });
    let lookup = server.mock(|when, then| {
        when.method(GET).path("/api/sites");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!([{"id": 7, "name": "fra1"}]));
    });

    let config = test_config(&server.base_url());
    let adapter = GenericAdapter::new(config).unwrap();
    let schema = test_schema();

    let uid = Uid::new_v4();
    let mut key = BTreeMap::new();
    key.insert("name".to_string(), serde_json::json!("fra1"));
    let mut attrs = BTreeMap::new();
    attrs.insert("name".to_string(), serde_json::json!("fra1"));

    let ops = vec![Op::Create {
        uid,
        type_name: TypeName::new("site".to_string()),
        desired: alembic_core::Object {
            uid,
            type_name: TypeName::new("site".to_string()),
            key: Key::from(key),
            attrs: attrs.into(),
            source: None,
        },
    }];

    let state = new_state_store();
    let report = adapter.write(&schema, &ops, &state).await.unwrap();
    create.assert();
    lookup.assert();
    assert_eq!(report.applied.len(), 1);
    assert_eq!(report.applied[0].backend_id, Some(BackendId::Int(7)));
}

#[tokio::test]
async fn test_apply_create_conflict_recovers_through_results_path() {
    // recovery must extract the list identically to observation, including the
    // `results_path`-wrapped shape: device wraps its list in "results" (unlike
    // the bare-array site above), so this drives the shared extraction's other
    // branch on the 409-recovery path.
    let server = MockServer::start();
    let create = server.mock(|when, then| {
        when.method(POST).path("/api/devices");
        then.status(409);
    });
    let lookup = server.mock(|when, then| {
        when.method(GET).path("/api/devices");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({"results": [{"id": 5, "name": "leaf01", "site": 1}]}));
    });

    // device carries a required site ref; seed it so the create body resolves.
    let mut state = new_state_store();
    let site_uid = Uid::new_v4();
    state.set_backend_id(
        TypeName::new("site".to_string()),
        site_uid,
        BackendId::Int(1),
    );

    let config = test_config(&server.base_url());
    let adapter = GenericAdapter::new(config).unwrap();
    let schema = test_schema();

    let uid = Uid::new_v4();
    let mut key = BTreeMap::new();
    key.insert("name".to_string(), serde_json::json!("leaf01"));
    let mut attrs = BTreeMap::new();
    attrs.insert("name".to_string(), serde_json::json!("leaf01"));
    attrs.insert("site".to_string(), serde_json::json!(site_uid.to_string()));

    let ops = vec![Op::Create {
        uid,
        type_name: TypeName::new("device".to_string()),
        desired: alembic_core::Object {
            uid,
            type_name: TypeName::new("device".to_string()),
            key: Key::from(key),
            attrs: attrs.into(),
            source: None,
        },
    }];

    let report = adapter.write(&schema, &ops, &state).await.unwrap();
    create.assert();
    lookup.assert();
    assert_eq!(report.applied.len(), 1);
    assert_eq!(report.applied[0].backend_id, Some(BackendId::Int(5)));
}

#[tokio::test]
async fn test_apply_delete_tolerates_not_found() {
    // deletes are re-issued on every run (they are not journaled), so deleting an
    // already-gone object (404) must be a no-op rather than an error.
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(DELETE).path("/api/devices/42");
        then.status(404);
    });

    let config = test_config(&server.base_url());
    let adapter = GenericAdapter::new(config).unwrap();
    let schema = test_schema();

    let uid = Uid::new_v4();
    let mut key = BTreeMap::new();
    key.insert("name".to_string(), serde_json::json!("to-delete"));

    let ops = vec![Op::Delete {
        uid,
        type_name: TypeName::new("device".to_string()),
        key: Key::from(key),
        backend_id: Some(BackendId::Int(42)),
    }];

    let state = new_state_store();
    let report = adapter.write(&schema, &ops, &state).await.unwrap();
    mock.assert();
    assert_eq!(report.applied.len(), 1);
    assert_eq!(report.applied[0].backend_id, None);
}

#[tokio::test]
async fn test_preview_schema_reports_nothing_to_provision() {
    // the generic adapter never provisions schema, so preview_schema must
    // honestly report Some(empty) ("nothing to provision"), not None ("cannot
    // preview") -- the latter the cli surfaces as "preview unavailable for this
    // backend". this mirrors its no-op ensure_schema.
    let adapter = GenericAdapter::new(test_config("http://localhost")).unwrap();
    let schema = test_schema();

    let preview = adapter.preview_schema(&schema).await.unwrap();
    let report = preview.expect("generic previews nothing-to-provision, never None");
    assert!(report.is_empty());

    // and it matches what ensure_schema actually does (a no-op empty report).
    assert!(adapter.ensure_schema(&schema).await.unwrap().is_empty());
}

#[test]
fn config_rejects_an_unknown_key() {
    // a typo'd `headers` used to default to empty, so every request went out
    // unauthenticated and the backend's 401 read as a credentials problem.
    let err = serde_json::from_str::<GenericConfig>(
        r#"{"base_url": "http://localhost", "header": {"Authorization": "Token t"}, "types": {}}"#,
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("header"), "{err}");
}

#[test]
fn endpoint_config_rejects_an_unknown_key() {
    for typo in [
        "results_pathh",
        "next_pathh",
        "delete_strategyy",
        "id_pathh",
        "update_methd",
    ] {
        let err = serde_json::from_str::<EndpointConfig>(&format!(
            r#"{{"path": "/api/sites", "{typo}": "results"}}"#
        ))
        .unwrap_err()
        .to_string();
        assert!(err.contains(typo), "{err}");
    }
}

#[test]
fn config_still_accepts_every_documented_key() {
    let config: GenericConfig = serde_json::from_str(
        r#"{
            "base_url": "http://localhost",
            "headers": {"Authorization": "Token t"},
            "types": {
                "site": {
                    "path": "/api/sites",
                    "results_path": "results",
                    "next_path": "next",
                    "id_path": "pk",
                    "delete_strategy": "standard",
                    "update_method": "PUT"
                }
            }
        }"#,
    )
    .unwrap();
    let site = &config.types["site"];
    assert_eq!(site.results_path.as_deref(), Some("results"));
    assert_eq!(site.next_path.as_deref(), Some("next"));
    assert_eq!(site.id_path, "pk");
    assert_eq!(site.update_method, "PUT");
}

// pagination: `next_path` set makes the shared list walk every page. without it
// the adapter observed only page one, so plan reported every later-page object
// as a create and apply duplicated it.

fn paged_config(base_url: &str, next_path: &str) -> GenericConfig {
    let mut config = test_config(base_url);
    config
        .types
        .get_mut("device")
        .expect("device endpoint")
        .next_path = Some(next_path.to_string());
    config
}

fn observed_names(state: &ObservedState) -> Vec<String> {
    let mut names: Vec<String> = state
        .by_key
        .values()
        .filter_map(|object| object.attrs.get("name")?.as_str().map(str::to_string))
        .collect();
    names.sort();
    names
}

async fn read_devices(config: GenericConfig) -> Result<ObservedState> {
    let adapter = GenericAdapter::new(config)?;
    adapter
        .read(
            &test_schema(),
            &[TypeName::new("device".to_string())],
            &new_state_store(),
        )
        .await
}

#[tokio::test]
async fn test_observe_follows_an_absolute_next_across_two_pages() {
    let server = MockServer::start();
    let first = server.mock(|when, then| {
        when.method(GET)
            .path("/api/devices")
            .query_param_missing("page");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({
                "next": format!("{}/api/devices?page=2", server.base_url()),
                "results": [{"id": 1, "name": "leaf01"}]
            }));
    });
    let second = server.mock(|when, then| {
        when.method(GET)
            .path("/api/devices")
            .query_param("page", "2");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({
                "next": serde_json::Value::Null,
                "results": [{"id": 2, "name": "leaf02"}]
            }));
    });

    let state = read_devices(paged_config(&server.base_url(), "next"))
        .await
        .unwrap();

    first.assert();
    second.assert();
    assert_eq!(observed_names(&state), vec!["leaf01", "leaf02"]);
}

#[tokio::test]
async fn test_observe_walks_three_pages() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(GET)
            .path("/api/devices")
            .query_param_missing("page");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({
                "next": "/api/devices?page=2",
                "results": [{"id": 1, "name": "leaf01"}]
            }));
    });
    server.mock(|when, then| {
        when.method(GET)
            .path("/api/devices")
            .query_param("page", "2");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({
                "next": "/api/devices?page=3",
                "results": [{"id": 2, "name": "leaf02"}]
            }));
    });
    let last = server.mock(|when, then| {
        when.method(GET)
            .path("/api/devices")
            .query_param("page", "3");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({"results": [{"id": 3, "name": "leaf03"}]}));
    });

    let state = read_devices(paged_config(&server.base_url(), "next"))
        .await
        .unwrap();

    last.assert();
    assert_eq!(observed_names(&state), vec!["leaf01", "leaf02", "leaf03"]);
}

#[tokio::test]
async fn test_observe_follows_a_query_only_relative_next() {
    // a bare `?page=2` resolves against the page that returned it, not against
    // `base_url`, which would drop the endpoint path.
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(GET)
            .path("/api/devices")
            .query_param_missing("page");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({
                "next": "?page=2",
                "results": [{"id": 1, "name": "leaf01"}]
            }));
    });
    let second = server.mock(|when, then| {
        when.method(GET)
            .path("/api/devices")
            .query_param("page", "2");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({"next": null, "results": [{"id": 2, "name": "leaf02"}]}));
    });

    let state = read_devices(paged_config(&server.base_url(), "next"))
        .await
        .unwrap();

    second.assert();
    assert_eq!(observed_names(&state), vec!["leaf01", "leaf02"]);
}

#[tokio::test]
async fn test_observe_reads_next_through_a_dotted_path() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(GET)
            .path("/api/devices")
            .query_param_missing("page");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({
                "links": {"next": "/api/devices?page=2"},
                "results": [{"id": 1, "name": "leaf01"}]
            }));
    });
    server.mock(|when, then| {
        when.method(GET)
            .path("/api/devices")
            .query_param("page", "2");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({"results": [{"id": 2, "name": "leaf02"}]}));
    });

    let state = read_devices(paged_config(&server.base_url(), "links.next"))
        .await
        .unwrap();

    assert_eq!(observed_names(&state), vec!["leaf01", "leaf02"]);
}

#[tokio::test]
async fn test_observe_stops_on_null_absent_and_empty_next() {
    // all three spellings of "no more pages" end the walk after one request.
    for last_page in [
        serde_json::json!({"next": null, "results": [{"id": 1, "name": "leaf01"}]}),
        serde_json::json!({"results": [{"id": 1, "name": "leaf01"}]}),
        serde_json::json!({"next": "", "results": [{"id": 1, "name": "leaf01"}]}),
    ] {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(GET).path("/api/devices");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(last_page.clone());
        });

        let state = read_devices(paged_config(&server.base_url(), "next"))
            .await
            .unwrap();

        assert_eq!(mock.calls(), 1, "{last_page}");
        assert_eq!(observed_names(&state), vec!["leaf01"], "{last_page}");
    }
}

#[tokio::test]
async fn test_observe_errors_on_a_next_that_is_neither_string_nor_null() {
    // stopping silently on a shape we did not expect is the failure this closes.
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(GET).path("/api/devices");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({
                "next": {"href": "/api/devices?page=2"},
                "results": [{"id": 1, "name": "leaf01"}]
            }));
    });

    let err = read_devices(paged_config(&server.base_url(), "next"))
        .await
        .unwrap_err()
        .to_string();

    assert!(
        err.contains("expected string or null at next_path next"),
        "{err}"
    );
    assert!(err.contains("got object"), "{err}");
}

#[tokio::test]
async fn test_observe_refuses_an_off_host_next() {
    // the client attaches the operator's auth headers to every request, so a
    // next url on another origin would send that token to a third party.
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(GET).path("/api/devices");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({
                "next": "http://attacker.example/api/devices?page=2",
                "results": [{"id": 1, "name": "leaf01"}]
            }));
    });

    let err = read_devices(paged_config(&server.base_url(), "next"))
        .await
        .unwrap_err()
        .to_string();

    assert!(err.contains("leaves the configured origin"), "{err}");
    assert!(err.contains("attacker.example"), "{err}");
    assert!(err.contains(&server.base_url()), "{err}");
}

#[tokio::test]
async fn test_observe_refuses_a_self_referential_next() {
    // an api echoing its own url as `next` would otherwise spin forever.
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(GET).path("/api/devices");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({
                "next": "/api/devices",
                "results": [{"id": 1, "name": "leaf01"}]
            }));
    });

    let err = read_devices(paged_config(&server.base_url(), "next"))
        .await
        .unwrap_err()
        .to_string();

    assert!(err.contains("pagination loop for device"), "{err}");
}

#[tokio::test]
async fn test_observe_keeps_paging_past_an_empty_page() {
    // an empty page that still advertises a next is legal; the objects are on
    // the page after it.
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(GET)
            .path("/api/devices")
            .query_param_missing("page");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({"next": "/api/devices?page=2", "results": []}));
    });
    let second = server.mock(|when, then| {
        when.method(GET)
            .path("/api/devices")
            .query_param("page", "2");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({"results": [{"id": 1, "name": "leaf01"}]}));
    });

    let state = read_devices(paged_config(&server.base_url(), "next"))
        .await
        .unwrap();

    second.assert();
    assert_eq!(observed_names(&state), vec!["leaf01"]);
}

#[tokio::test]
async fn test_observe_names_the_page_whose_results_path_is_missing() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(GET)
            .path("/api/devices")
            .query_param_missing("page");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({"next": "/api/devices?page=2", "results": []}));
    });
    server.mock(|when, then| {
        when.method(GET)
            .path("/api/devices")
            .query_param("page", "2");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({"items": []}));
    });

    let err = read_devices(paged_config(&server.base_url(), "next"))
        .await
        .unwrap_err()
        .to_string();

    assert!(err.contains("path segment not found: results"), "{err}");
    assert!(err.contains("page=2"), "{err}");
}

#[tokio::test]
async fn test_observe_without_next_path_issues_exactly_one_request() {
    // the backward-compatibility pin: an unset `next_path` must not start paging,
    // whatever the response happens to carry.
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(GET).path("/api/devices");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({
                "next": "/api/devices?page=2",
                "results": [{"id": 1, "name": "leaf01"}]
            }));
    });

    let state = read_devices(test_config(&server.base_url())).await.unwrap();

    assert_eq!(mock.calls(), 1);
    assert_eq!(observed_names(&state), vec!["leaf01"]);
}

#[tokio::test]
async fn test_apply_create_conflict_recovers_across_a_page_boundary() {
    // 409-recovery re-lists through the same shared walk, so an object that lives
    // on page two must still be found; before pagination it was reported as a
    // hard conflict.
    let server = MockServer::start();
    let create = server.mock(|when, then| {
        when.method(POST).path("/api/devices");
        then.status(409);
    });
    server.mock(|when, then| {
        when.method(GET)
            .path("/api/devices")
            .query_param_missing("page");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({
                "next": "/api/devices?page=2",
                "results": [{"id": 4, "name": "leaf01", "site": 1}]
            }));
    });
    let second = server.mock(|when, then| {
        when.method(GET)
            .path("/api/devices")
            .query_param("page", "2");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({
                "next": null,
                "results": [{"id": 9, "name": "leaf02", "site": 1}]
            }));
    });

    let mut state = new_state_store();
    let site_uid = Uid::new_v4();
    state.set_backend_id(
        TypeName::new("site".to_string()),
        site_uid,
        BackendId::Int(1),
    );

    let adapter = GenericAdapter::new(paged_config(&server.base_url(), "next")).unwrap();

    let uid = Uid::new_v4();
    let mut key = BTreeMap::new();
    key.insert("name".to_string(), serde_json::json!("leaf02"));
    let mut attrs = BTreeMap::new();
    attrs.insert("name".to_string(), serde_json::json!("leaf02"));
    attrs.insert("site".to_string(), serde_json::json!(site_uid.to_string()));

    let ops = vec![Op::Create {
        uid,
        type_name: TypeName::new("device".to_string()),
        desired: alembic_core::Object {
            uid,
            type_name: TypeName::new("device".to_string()),
            key: Key::from(key),
            attrs: attrs.into(),
            source: None,
        },
    }];

    let report = adapter.write(&test_schema(), &ops, &state).await.unwrap();

    create.assert();
    second.assert();
    assert_eq!(report.applied.len(), 1);
    assert_eq!(report.applied[0].backend_id, Some(BackendId::Int(9)));
}

// a mistyped `next_path` value: an absent final key ends the chain, since apis
// omit it on the last page, but a missing segment above it means the path does
// not describe this response, which is what `results_path` already reports.

#[tokio::test]
async fn test_observe_errors_on_a_missing_intermediate_next_path_segment() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(GET).path("/api/devices");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({
                "next": "/api/devices?page=2",
                "results": [{"id": 1, "name": "leaf01"}]
            }));
    });

    let err = read_devices(paged_config(&server.base_url(), "links.next"))
        .await
        .unwrap_err()
        .to_string();

    assert!(
        err.contains("path segment not found: links at next_path links.next"),
        "{err}"
    );
    assert!(err.contains("for device at"), "{err}");
}

#[tokio::test]
async fn test_observe_treats_an_absent_final_next_key_as_the_last_page() {
    let server = MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(GET).path("/api/devices");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({
                "links": {"previous": null},
                "results": [{"id": 1, "name": "leaf01"}]
            }));
    });

    let state = read_devices(paged_config(&server.base_url(), "links.next"))
        .await
        .unwrap();

    assert_eq!(mock.calls(), 1);
    assert_eq!(observed_names(&state), vec!["leaf01"]);
}

#[test]
fn test_a_next_path_without_segments_is_rejected_at_construction() {
    // every segment is skipped, so the walk would land on the response root and
    // blame the payload for a config error on every read.
    for next_path in ["", ".", ".."] {
        let err = match GenericAdapter::new(paged_config("http://example.com", next_path)) {
            Ok(_) => panic!("{next_path:?} was accepted"),
            Err(err) => err.to_string(),
        };

        assert!(err.contains("invalid next_path"), "{next_path:?}: {err}");
        assert!(err.contains("for type device"), "{next_path:?}: {err}");
    }
}

// redirects: `headers` rides on every request the client makes and reqwest
// strips only its own sensitive names across hosts, so a token under any other
// name leaves the origin with no `next` involved.

fn keyed_config(base_url: &str) -> GenericConfig {
    let mut config = test_config(base_url);
    config
        .headers
        .insert("x-api-key".to_string(), "generic_secret".to_string());
    config
}

#[tokio::test]
async fn test_a_cross_host_redirect_is_refused_before_the_token_leaves() {
    let other = MockServer::start();
    let leaked = other.mock(|when, then| {
        when.method(GET).header("x-api-key", "generic_secret");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({"results": []}));
    });
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(GET).path("/api/devices");
        then.status(302)
            .header("location", format!("{}/api/devices", other.base_url()));
    });

    let result = read_devices(keyed_config(&server.base_url())).await;

    assert_eq!(leaked.calls(), 0, "the token reached the redirect target");
    let err = format!("{:#}", result.unwrap_err());
    assert!(err.contains("leaves the origin"), "{err}");
    assert!(err.contains(&other.base_url()), "{err}");
    assert!(err.contains(&server.base_url()), "{err}");
}

#[tokio::test]
async fn test_a_same_origin_redirect_is_followed() {
    // a trailing-slash 301 is what django's APPEND_SLASH answers, so refusing
    // every redirect would break an ordinary drf endpoint.
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(GET).path("/api/devices");
        then.status(301).header("location", "/api/devices/");
    });
    let target = server.mock(|when, then| {
        when.method(GET)
            .path("/api/devices/")
            .header("x-api-key", "generic_secret");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({"results": [{"id": 1, "name": "leaf01"}]}));
    });

    let state = read_devices(keyed_config(&server.base_url()))
        .await
        .unwrap();

    assert_eq!(target.calls(), 1);
    assert_eq!(observed_names(&state), vec!["leaf01"]);
}
