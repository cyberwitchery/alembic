use super::*;
use alembic_core::{
    FieldSchema, FieldType, Inventory, JsonMap, Key, Object, Schema, TypeName, TypeSchema, Uid,
};
use serde_json::json;
use std::collections::BTreeMap;
use tempfile::tempdir;
use uuid::Uuid;

fn uid(n: u128) -> Uid {
    Uuid::from_u128(n)
}

fn t(name: &str) -> TypeName {
    TypeName::new(name)
}

fn attrs_map(value: serde_json::Value) -> JsonMap {
    let serde_json::Value::Object(map) = value else {
        panic!("attrs must be a json object");
    };
    map.into_iter().collect::<BTreeMap<_, _>>().into()
}

fn key_str(raw: &str) -> Key {
    let mut map = BTreeMap::new();
    for segment in raw.split(';') {
        let (field, value) = segment
            .split_once('=')
            .unwrap_or_else(|| panic!("invalid key segment: {segment}"));
        map.insert(
            field.to_string(),
            serde_json::Value::String(value.to_string()),
        );
    }
    Key::from(map)
}

fn obj(uid: Uid, type_name: &str, key: &str, attrs: serde_json::Value) -> Object {
    Object::new(uid, t(type_name), key_str(key), attrs_map(attrs)).unwrap()
}

fn inv(objects: Vec<Object>) -> Inventory {
    let schema = schema_for(&objects);
    Inventory { schema, objects }
}

fn schema_for(objects: &[Object]) -> Schema {
    let mut types: BTreeMap<String, TypeSchema> = BTreeMap::new();
    for object in objects {
        let entry = types
            .entry(object.type_name.as_str().to_string())
            .or_insert_with(|| TypeSchema {
                key: BTreeMap::new(),
                fields: BTreeMap::new(),
            });
        for field in object.key.keys() {
            entry.key.entry(field.clone()).or_insert(FieldSchema {
                r#type: FieldType::Json,
                required: true,
                nullable: false,
                description: None,
                format: None,
                pattern: None,
            });
        }
        for field in object.attrs.keys() {
            entry.fields.entry(field.clone()).or_insert(FieldSchema {
                r#type: FieldType::Json,
                required: false,
                nullable: true,
                description: None,
                format: None,
                pattern: None,
            });
        }
    }
    Schema { types }
}

#[test]
fn load_includes_combines_objects() {
    let dir = tempdir().unwrap();
    let root = dir.path();

    let base = root.join("base.yaml");
    let child = root.join("child.yaml");

    std::fs::write(
        &child,
        r#"objects:
  - uid: "00000000-0000-0000-0000-000000000001"
    type: dcim.site
    key:
      site: "child"
    attrs:
      name: "Child"
      slug: "child"
"#,
    )
    .unwrap();

    std::fs::write(
        &base,
        format!(
            r#"include:
  - {}
schema:
  types:
    dcim.site:
      key:
        site:
          type: slug
      fields:
        name:
          type: string
        slug:
          type: slug
objects:
  - uid: "00000000-0000-0000-0000-000000000002"
    type: dcim.site
    key:
      site: "base"
    attrs:
      name: "Base"
      slug: "base"
"#,
            child.file_name().unwrap().to_str().unwrap()
        ),
    )
    .unwrap();

    let inventory = load_inventory(&base).unwrap();
    assert_eq!(inventory.objects.len(), 2);
}

#[test]
fn load_json_inventory() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("inventory.json");
    std::fs::write(
        &path,
        r#"{ "schema": { "types": { "dcim.site": { "key": { "site": { "type": "slug" } }, "fields": { "name": { "type": "string" }, "slug": { "type": "slug" } } } } }, "objects": [ { "uid": "00000000-0000-0000-0000-000000000010", "type": "dcim.site", "key": { "site": "fra1" }, "attrs": { "name": "FRA1", "slug": "fra1" } } ] }"#,
    )
    .unwrap();

    let inventory = load_inventory(&path).unwrap();
    assert_eq!(inventory.objects.len(), 1);
}

#[test]
fn load_generic_kind_as_generic_attrs() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("generic.yaml");
    std::fs::write(
        &path,
        r#"schema:
  types:
    services.vpn:
      key:
        vpn:
          type: slug
      fields:
        peers:
          type: json
        pre_shared_key:
          type: string
    dcim.site:
      key:
        site:
          type: slug
      fields:
        name:
          type: string
        slug:
          type: slug
objects:
  - uid: "00000000-0000-0000-0000-000000000010"
    type: services.vpn
    key:
      vpn: "corp"
    attrs:
      peers:
        - name: site1
          ip: 10.0.0.1
      pre_shared_key: "secret"
  - uid: "00000000-0000-0000-0000-000000000011"
    type: dcim.site
    key:
      site: "fra1"
    attrs:
      name: "FRA1"
      slug: "fra1"
"#,
    )
    .unwrap();

    let inventory = load_inventory(&path).unwrap();
    let generic = &inventory.objects[0];
    assert_eq!(generic.type_name.as_str(), "services.vpn");
    assert!(generic.attrs.contains_key("pre_shared_key"));
    let typed = &inventory.objects[1];
    assert_eq!(typed.type_name.as_str(), "dcim.site");
}
#[test]
fn load_with_imports_merges_objects() {
    let dir = tempdir().unwrap();
    let root = dir.path();

    let a = root.join("a.yaml");
    let b = root.join("b.yaml");
    std::fs::write(
        &a,
        r#"schema:
  types:
    dcim.site:
      key:
        site:
          type: slug
      fields:
        name:
          type: string
        slug:
          type: slug
objects:
  - uid: "00000000-0000-0000-0000-000000000020"
    type: dcim.site
    key:
      site: "a"
    attrs:
      name: "A"
      slug: "a"
"#,
    )
    .unwrap();
    std::fs::write(
        &b,
        format!(
            r#"imports:
  - {}
objects:
  - uid: "00000000-0000-0000-0000-000000000021"
    type: dcim.site
    key:
      site: "b"
    attrs:
      name: "B"
      slug: "b"
"#,
            a.file_name().unwrap().to_str().unwrap()
        ),
    )
    .unwrap();

    let inventory = load_inventory(&b).unwrap();
    assert_eq!(inventory.objects.len(), 2);
}

#[test]
fn load_is_idempotent_with_cycles() {
    let dir = tempdir().unwrap();
    let root = dir.path();

    let a = root.join("a.yaml");
    let b = root.join("b.yaml");

    std::fs::write(
        &a,
        format!(
            r#"include:
  - {}
schema:
  types:
    dcim.site:
      key:
        site:
          type: slug
      fields:
        name:
          type: string
        slug:
          type: slug
objects:
  - uid: "00000000-0000-0000-0000-000000000030"
    type: dcim.site
    key:
      site: "a"
    attrs:
      name: "A"
      slug: "a"
"#,
            b.file_name().unwrap().to_str().unwrap()
        ),
    )
    .unwrap();
    std::fs::write(
        &b,
        format!(
            r#"include:
  - {}
objects:
  - uid: "00000000-0000-0000-0000-000000000031"
    type: dcim.site
    key:
      site: "b"
    attrs:
      name: "B"
      slug: "b"
"#,
            a.file_name().unwrap().to_str().unwrap()
        ),
    )
    .unwrap();

    let inventory = load_inventory(&a).unwrap();
    assert_eq!(inventory.objects.len(), 2);
}

#[test]
fn load_errors_on_missing_include() {
    let dir = tempdir().unwrap();
    let root = dir.path();
    let base = root.join("base.yaml");
    std::fs::write(
        &base,
        r#"include:
  - missing.yaml
objects: []
"#,
    )
    .unwrap();

    let err = load_inventory(&base).unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("load inventory") || message.contains("read inventory"),
        "unexpected error: {message}"
    );
}

#[test]
fn load_errors_on_invalid_yaml() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("invalid.yaml");
    std::fs::write(&path, "objects: [").unwrap();

    let err = load_inventory(&path).unwrap_err();
    assert!(err.to_string().contains("parse yaml"));
}

#[test]
fn detects_duplicate_uids() {
    let objects = vec![
        obj(
            uid(1),
            "dcim.site",
            "site=a",
            json!({ "name": "A", "slug": "a" }),
        ),
        obj(
            uid(1),
            "dcim.site",
            "site=b",
            json!({ "name": "B", "slug": "b" }),
        ),
    ];
    let inventory = inv(objects);
    let result = validate(&inventory);
    assert!(result.is_err());
}

#[test]
fn detects_missing_references() {
    let objects = vec![obj(
        uid(2),
        "dcim.interface",
        "device=leaf01;interface=eth0",
        json!({
            "name": "eth0",
            "device": uid(3).to_string(),
            "if_type": "1000base-t",
            "enabled": true
        }),
    )];
    let inventory = Inventory {
        schema: Schema {
            types: BTreeMap::from([(
                "dcim.interface".to_string(),
                TypeSchema {
                    key: BTreeMap::from([
                        (
                            "device".to_string(),
                            FieldSchema {
                                r#type: FieldType::Json,
                                required: true,
                                nullable: false,
                                description: None,
                                format: None,
                                pattern: None,
                            },
                        ),
                        (
                            "interface".to_string(),
                            FieldSchema {
                                r#type: FieldType::Json,
                                required: true,
                                nullable: false,
                                description: None,
                                format: None,
                                pattern: None,
                            },
                        ),
                    ]),
                    fields: BTreeMap::from([(
                        "device".to_string(),
                        FieldSchema {
                            r#type: FieldType::Ref {
                                target: "dcim.device".to_string(),
                            },
                            required: true,
                            nullable: false,
                            description: None,
                            format: None,
                            pattern: None,
                        },
                    )]),
                },
            )]),
        },
        objects,
    };
    let result = validate(&inventory);
    assert!(result.is_err());
}

#[test]
fn plans_in_stable_order() {
    let site_uid = uid(10);
    let device_uid = uid(11);
    let objects = vec![
        obj(
            device_uid,
            "dcim.device",
            "site=fra1;device=leaf01",
            json!({
                "name": "leaf01",
                "site": site_uid.to_string(),
                "role": "leaf",
                "device_type": "leaf-switch"
            }),
        ),
        obj(
            site_uid,
            "dcim.site",
            "site=fra1",
            json!({ "name": "FRA1", "slug": "fra1" }),
        ),
    ];

    let inventory = inv(objects);
    let observed = ObservedState::default();
    let state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = plan(
        &inventory.objects,
        &observed,
        &state,
        &inventory.schema,
        false,
    );

    assert_eq!(plan.ops.len(), 2);
    let kinds: Vec<TypeName> = plan
        .ops
        .iter()
        .map(|op| match op {
            Op::Create { type_name, .. } => type_name.clone(),
            _ => panic!("unexpected op"),
        })
        .collect();

    assert_eq!(kinds, vec![t("dcim.device"), t("dcim.site")]);
}

#[test]
fn detects_attribute_diff() {
    let uid = uid(20);
    let desired = inv(vec![obj(
        uid,
        "dcim.site",
        "site=fra1",
        json!({ "name": "FRA1", "slug": "fra1" }),
    )]);

    let mut observed = ObservedState::default();
    observed
        .insert(ObservedObject {
            type_name: t("dcim.site"),
            key: key_str("site=fra1"),
            attrs: attrs_map(json!({ "name": "OLD", "slug": "fra1" })),
            backend_id: Some(BackendId::Int(100)),
        })
        .unwrap();

    let state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = plan(&desired.objects, &observed, &state, &desired.schema, false);

    assert_eq!(plan.ops.len(), 1);
    match &plan.ops[0] {
        Op::Update { changes, .. } => {
            assert!(changes.iter().any(|c| c.field == "name"));
        }
        _ => panic!("expected update"),
    }
}

#[test]
fn detects_generic_payload_diff() {
    let uid = uid(40);
    let mut from = BTreeMap::new();
    from.insert("a".to_string(), serde_json::json!(1));
    let mut to = BTreeMap::new();
    to.insert("a".to_string(), serde_json::json!(2));
    to.insert("b".to_string(), serde_json::json!({"nested": true}));

    let desired = inv(vec![Object::new(
        uid,
        t("services.vpn"),
        key_str("vpn=corp"),
        JsonMap::from(to),
    )
    .unwrap()]);

    let mut observed = ObservedState::default();
    observed
        .insert(ObservedObject {
            type_name: t("services.vpn"),
            key: key_str("vpn=corp"),
            attrs: from.into(),
            backend_id: Some(BackendId::Int(10)),
        })
        .unwrap();

    let state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = plan(&desired.objects, &observed, &state, &desired.schema, false);

    assert_eq!(plan.ops.len(), 1);
    match &plan.ops[0] {
        Op::Update { changes, .. } => {
            assert_eq!(changes.len(), 2);
            let mut fields: Vec<&str> =
                changes.iter().map(|change| change.field.as_str()).collect();
            fields.sort();
            assert_eq!(fields, vec!["a", "b"]);
        }
        _ => panic!("expected update"),
    }
}

#[test]
fn planner_ignores_optional_nulls() {
    let desired = obj(
        uid(80),
        "dcim.site",
        "site=fra1",
        json!({ "name": "FRA1", "slug": "fra1" }),
    );
    let schema = schema_for(std::slice::from_ref(&desired));

    let mut observed = ObservedState::default();
    observed
        .insert(ObservedObject {
            type_name: t("dcim.site"),
            key: key_str("site=fra1"),
            attrs: attrs_map(json!({
                "name": "FRA1",
                "slug": "fra1",
                "status": "active",
                "description": ""
            })),
            backend_id: Some(BackendId::Int(1)),
        })
        .unwrap();

    let state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = plan(
        std::slice::from_ref(&desired),
        &observed,
        &state,
        &schema,
        false,
    );
    assert!(plan.ops.is_empty());
}

#[test]
fn planner_matches_backend_id_by_kind() {
    let desired = obj(
        uid(82),
        "dcim.device",
        "site=fra1;device=leaf01",
        json!({
            "name": "leaf01",
            "site": uid(1).to_string(),
            "role": "leaf",
            "device_type": "leaf"
        }),
    );
    let schema = schema_for(std::slice::from_ref(&desired));

    let mut observed = ObservedState::default();
    observed
        .insert(ObservedObject {
            type_name: t("dcim.device"),
            key: key_str("site=fra1/device=leaf01"),
            attrs: desired.attrs.clone(),
            backend_id: Some(BackendId::Int(1)),
        })
        .unwrap();
    observed
        .insert(ObservedObject {
            type_name: t("dcim.interface"),
            key: key_str("device=leaf01;interface=eth0"),
            attrs: attrs_map(json!({
                "name": "eth0",
                "device": uid(82).to_string()
            })),
            backend_id: Some(BackendId::Int(1)),
        })
        .unwrap();

    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    state.set_backend_id(t("dcim.device"), desired.uid, BackendId::Int(1));
    let plan = plan(
        std::slice::from_ref(&desired),
        &observed,
        &state,
        &schema,
        false,
    );
    assert!(plan.ops.is_empty());
}

#[test]
fn planner_includes_prefix_site_diff() {
    let desired = obj(
        uid(83),
        "ipam.prefix",
        "prefix=10.0.0.0/24",
        json!({
            "prefix": "10.0.0.0/24",
            "site": uid(1).to_string()
        }),
    );
    let schema = schema_for(std::slice::from_ref(&desired));

    let mut observed = ObservedState::default();
    observed
        .insert(ObservedObject {
            type_name: t("ipam.prefix"),
            key: key_str("prefix=10.0.0.0/24"),
            attrs: attrs_map(json!({ "prefix": "10.0.0.0/24" })),
            backend_id: Some(BackendId::Int(1)),
        })
        .unwrap();

    let state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = plan(
        std::slice::from_ref(&desired),
        &observed,
        &state,
        &schema,
        false,
    );
    assert_eq!(plan.ops.len(), 1);
    match &plan.ops[0] {
        Op::Update { changes, .. } => {
            assert!(changes.iter().any(|change| change.field == "site"));
        }
        _ => panic!("expected update"),
    }
}

#[test]
fn plan_write_only_is_all_creates_even_with_state() {
    let inventory = inv(vec![
        obj(
            uid(1),
            "dcim.site",
            "site=fra1",
            serde_json::json!({"name": "FRA1"}),
        ),
        obj(
            uid(2),
            "dcim.site",
            "site=ber1",
            serde_json::json!({"name": "BER1"}),
        ),
    ]);
    // even though state already maps uid(1) to a backend id, a write-only backend
    // reports no observation, so every declared object is still a create.
    let mut state = StateStore::new(None, StateData::default());
    state.set_backend_id(t("dcim.site"), uid(1), BackendId::Int(5));
    let plan = plan_write_only(&inventory, &state).unwrap();
    assert_eq!(plan.ops.len(), 2);
    assert!(plan.ops.iter().all(|op| matches!(op, Op::Create { .. })));
}

#[test]
fn state_store_roundtrip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("state.json");
    let mut store = StateStore::load(&path).unwrap();
    store.set_backend_id(t("dcim.site"), uid(99), BackendId::Int(123));
    futures::executor::block_on(store.save_async()).unwrap();
    // a real reload happens in a separate process after the first exits; drop the
    // first store so its exclusive state lock is released before reloading.
    drop(store);

    let reloaded = StateStore::load(&path).unwrap();
    assert_eq!(
        reloaded.backend_id(t("dcim.site"), uid(99)),
        Some(BackendId::Int(123))
    );
    assert!(reloaded.all_mappings().contains_key(&t("dcim.site")));

    let mut reloaded = reloaded;
    reloaded.remove_backend_id(t("dcim.site"), uid(99));
    assert_eq!(reloaded.backend_id(t("dcim.site"), uid(99)), None);
}

#[test]
fn state_store_load_is_exclusive_across_concurrent_holders() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("state.json");
    let _held = StateStore::load(&path).unwrap();
    // a second concurrent load of the same state file is refused, so two runs
    // cannot load-modify-save it and clobber each other's mappings.
    let err = StateStore::load(&path).unwrap_err();
    assert!(err.to_string().contains("another alembic run"), "{err}");
}

#[test]
fn state_store_lock_releases_on_drop() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("state.json");
    {
        let _held = StateStore::load(&path).unwrap();
    }
    // the previous holder dropped, so a fresh load acquires the lock cleanly.
    StateStore::load(&path).unwrap();
}

#[test]
fn state_store_creates_parent_dir() {
    let dir = tempdir().unwrap();
    let path = dir.path().join(".alembic/state.json");
    let store = StateStore::load(&path).unwrap();
    futures::executor::block_on(store.save_async()).unwrap();
    assert!(path.exists());
}

#[test]
fn state_store_load_missing_is_empty() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("missing.json");
    let store = StateStore::load(&path).unwrap();
    assert!(store.all_mappings().is_empty());
}

#[test]
fn state_store_load_errors_on_invalid_json() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("state.json");
    std::fs::write(&path, "not-json").unwrap();
    let err = StateStore::load(&path).unwrap_err();
    assert!(err.to_string().contains("parse state"));
}

#[test]
fn state_store_load_errors_on_bad_parent() {
    let dir = tempdir().unwrap();
    let blocking_parent = dir.path().join("state.json");
    std::fs::write(&blocking_parent, "file").unwrap();
    let path = blocking_parent.join("child.json");
    // load now creates the state dir (for the lock file), so a parent that cannot
    // be a directory fails here rather than being deferred to save.
    let err = StateStore::load(&path).unwrap_err();
    assert!(err.to_string().contains("create state dir"), "{err}");
}

#[tokio::test]
async fn state_store_async_roundtrip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("async_state.json");
    let mut store = StateStore::load(&path).unwrap();
    store.set_backend_id(t("dcim.site"), uid(100), BackendId::Int(456));
    store.save_async().await.unwrap();
    drop(store); // release the state lock before reloading (models a fresh run).

    let mut reloaded = StateStore::load(&path).unwrap();
    reloaded.load_async().await.unwrap();
    assert_eq!(
        reloaded.backend_id(t("dcim.site"), uid(100)),
        Some(BackendId::Int(456))
    );
}

#[tokio::test]
async fn state_store_postgres_roundtrip_when_configured() {
    let Ok(url) = std::env::var("ALEMBIC_TEST_POSTGRES_URL") else {
        return;
    };
    let key = format!("alembic-test-{}", Uuid::new_v4());

    let mut store = StateStore::load_postgres(url.clone(), key.clone(), PostgresTlsMode::Disable)
        .await
        .unwrap();
    store.set_backend_id(t("dcim.site"), uid(777), BackendId::Int(12345));
    store.save_async().await.unwrap();

    let mut reloaded = StateStore::load_postgres(url, key, PostgresTlsMode::Disable)
        .await
        .unwrap();
    reloaded.load_async().await.unwrap();
    assert_eq!(
        reloaded.backend_id(t("dcim.site"), uid(777)),
        Some(BackendId::Int(12345))
    );
}

#[tokio::test]
async fn state_store_postgres_tls_roundtrip_when_configured() {
    let Ok(url) = std::env::var("ALEMBIC_TEST_POSTGRES_TLS_URL") else {
        return;
    };
    let key = format!("alembic-test-tls-{}", Uuid::new_v4());

    let mut store = StateStore::load_postgres(url.clone(), key.clone(), PostgresTlsMode::Require)
        .await
        .unwrap();
    store.set_backend_id(
        t("dcim.device"),
        uid(778),
        BackendId::String("abc".to_string()),
    );
    store.save_async().await.unwrap();

    let mut reloaded = StateStore::load_postgres(url, key, PostgresTlsMode::Require)
        .await
        .unwrap();
    reloaded.load_async().await.unwrap();
    assert_eq!(
        reloaded.backend_id(t("dcim.device"), uid(778)),
        Some(BackendId::String("abc".to_string()))
    );
}

#[tokio::test]
async fn state_store_postgres_consecutive_saves_when_configured() {
    let Ok(url) = std::env::var("ALEMBIC_TEST_POSTGRES_URL") else {
        return;
    };
    let key = format!("alembic-test-{}", Uuid::new_v4());

    let store = StateStore::load_postgres(url.clone(), key.clone(), PostgresTlsMode::Disable)
        .await
        .unwrap();
    store.save_async().await.unwrap();
    store.save_async().await.unwrap();
    store.save_async().await.unwrap();
}

#[tokio::test]
async fn state_store_postgres_prevent_race_condition_when_configured() {
    let Ok(url) = std::env::var("ALEMBIC_TEST_POSTGRES_URL") else {
        return;
    };
    let key = format!("alembic-test-{}", Uuid::new_v4());

    let mut store_a = StateStore::load_postgres(url.clone(), key.clone(), PostgresTlsMode::Disable)
        .await
        .unwrap();
    store_a.save_async().await.unwrap(); // ensures a "1" (or higher) in the version column

    // another client connects to the database
    let mut store_b = StateStore::load_postgres(url.clone(), key.clone(), PostgresTlsMode::Disable)
        .await
        .unwrap();

    store_a.load_async().await.unwrap(); // preparing to save
    store_b.load_async().await.unwrap(); // preparing to save

    store_a.save_async().await.unwrap();
    store_b.save_async().await.expect_err("race condition");
}

#[tokio::test]
async fn state_store_load_async_no_backend() {
    let mut store = StateStore::new(None, StateData::default());
    // should succeed without error even with no backend
    store.load_async().await.unwrap();
}

#[tokio::test]
async fn state_store_save_async_no_backend() {
    let store = StateStore::new(None, StateData::default());
    // should succeed without error even with no backend
    store.save_async().await.unwrap();
}

#[test]
fn plan_generates_deletes_when_enabled() {
    let desired = inv(vec![]);
    let mut observed = ObservedState::default();
    observed
        .insert(ObservedObject {
            type_name: t("dcim.site"),
            key: key_str("site=orphan"),
            attrs: attrs_map(json!({ "name": "orphan", "slug": "orphan" })),
            backend_id: Some(BackendId::Int(10)),
        })
        .unwrap();

    let state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = plan(&desired.objects, &observed, &state, &desired.schema, true);
    assert!(plan.ops.iter().any(|op| matches!(op, Op::Delete { .. })));
}

#[test]
fn apply_order_puts_deletes_last() {
    let ops = vec![
        Op::Delete {
            uid: uid(1),
            type_name: t("dcim.device"),
            key: key_str("site=fra1/device=leaf01"),
            backend_id: Some(BackendId::Int(2)),
        },
        Op::Create {
            uid: uid(2),
            type_name: t("dcim.site"),
            desired: obj(
                uid(2),
                "dcim.site",
                "site=fra1",
                json!({ "name": "FRA1", "slug": "fra1" }),
            ),
        },
    ];

    let ordered = sort_ops_for_apply(&ops, &Schema::default());
    assert!(matches!(ordered.first().unwrap(), Op::Create { .. }));
    assert!(matches!(ordered.last().unwrap(), Op::Delete { .. }));
}

#[derive(Clone)]
struct TestAdapter {
    observed: ObservedState,
    report: ApplyReport,
}

#[async_trait::async_trait]
impl Observer for TestAdapter {
    async fn read(
        &self,
        _schema: &alembic_core::Schema,
        _types: &[TypeName],
        _state: &StateStore,
    ) -> anyhow::Result<ObservedState> {
        Ok(self.observed.clone())
    }
}

#[async_trait::async_trait]
impl Emitter for TestAdapter {
    async fn write(
        &self,
        _schema: &alembic_core::Schema,
        _ops: &[Op],
        _state: &StateStore,
    ) -> anyhow::Result<ApplyReport> {
        Ok(self.report.clone())
    }
}

#[async_trait::async_trait]
impl Adapter for TestAdapter {}

#[test]
fn build_plan_creates_ops() {
    let inventory = inv(vec![obj(
        uid(1),
        "dcim.site",
        "site=fra1",
        json!({ "name": "FRA1", "slug": "fra1" }),
    )]);
    let adapter = TestAdapter {
        observed: ObservedState::default(),
        report: ApplyReport::default(),
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan =
        futures::executor::block_on(build_plan(&adapter, &inventory, &mut state, false)).unwrap();
    assert_eq!(plan.ops.len(), 1);
}

#[test]
fn build_plan_with_duplicated_uids() {
    let inventory = inv(vec![
        obj(
            uid(1),
            "dcim.site",
            "site=fra1",
            json!({ "name": "FRA1", "slug": "fra1" }),
        ),
        obj(
            uid(1),
            "dcim.site",
            "site=fra2",
            json!({ "name": "FRA2", "slug": "fra2" }),
        ),
    ]);
    let adapter = TestAdapter {
        observed: ObservedState::default(),
        report: ApplyReport::default(),
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = futures::executor::block_on(build_plan(&adapter, &inventory, &mut state, false));
    plan.expect_err("failed to detect duplicated uid");
}

#[test]
fn build_plan_with_duplicated_keys() {
    let inventory = inv(vec![
        obj(
            uid(1),
            "dcim.site",
            "site=fra1",
            json!({ "name": "FRA1", "slug": "fra1" }),
        ),
        obj(
            uid(2),
            "dcim.site",
            "site=fra1",
            json!({ "name": "FRA2", "slug": "fra2" }),
        ),
    ]);
    let adapter = TestAdapter {
        observed: ObservedState::default(),
        report: ApplyReport::default(),
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = futures::executor::block_on(build_plan(&adapter, &inventory, &mut state, false));
    plan.expect_err("failed to detect duplicated keys");
}

#[test]
fn build_plan_bootstraps_state_by_key() {
    let inventory = inv(vec![obj(
        uid(1),
        "dcim.site",
        "site=fra1",
        json!({ "name": "FRA1", "slug": "fra1" }),
    )]);
    let mut observed = ObservedState::default();
    observed
        .insert(ObservedObject {
            type_name: t("dcim.site"),
            key: key_str("site=fra1"),
            attrs: attrs_map(json!({ "name": "FRA1", "slug": "fra1" })),
            backend_id: Some(BackendId::Int(10)),
        })
        .unwrap();
    let adapter = TestAdapter {
        observed,
        report: ApplyReport::default(),
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan =
        futures::executor::block_on(build_plan(&adapter, &inventory, &mut state, false)).unwrap();
    assert!(plan.ops.is_empty());
    assert_eq!(
        state.backend_id(t("dcim.site"), uid(1)),
        Some(BackendId::Int(10))
    );
}

#[test]
fn insert_with_duplicate_backend_id() {
    let id = 123;
    let mut observed = ObservedState::default();
    observed
        .insert(ObservedObject {
            type_name: t("dcim.site"),
            key: key_str("site=fra1"),
            attrs: attrs_map(json!({ "name": "FRA1", "slug": "fra1" })),
            backend_id: Some(BackendId::Int(id)),
        })
        .unwrap();

    observed
        .insert(ObservedObject {
            type_name: t("dcim.site"),
            key: key_str("site=fra2"),
            attrs: attrs_map(json!({ "name": "FRA2", "slug": "fra2" })),
            backend_id: Some(BackendId::Int(id)),
        })
        .expect_err("duplicate backend id not detected");
}

#[test]
fn insert_with_duplicate_natural_key() {
    // same (type, key) with no backend id: the by_key guard must reject the
    // second insert. insert_with_duplicate_backend_id (different keys) returns
    // at the backend-id branch and never reaches this guard.
    let mut observed = ObservedState::default();
    let mk = |name: &str| ObservedObject {
        type_name: t("dcim.site"),
        key: key_str("site=fra1"),
        attrs: attrs_map(json!({ "name": name, "slug": "fra1" })),
        backend_id: None,
    };
    observed.insert(mk("FRA1")).unwrap();
    observed
        .insert(mk("FRA1-dup"))
        .expect_err("duplicate natural key not detected");
}

#[test]
fn build_plan_reobserves_after_bootstrap() {
    #[derive(Clone)]
    struct ReobserveAdapter {
        states: std::sync::Arc<std::sync::Mutex<Vec<ObservedState>>>,
    }

    #[async_trait::async_trait]
    impl Observer for ReobserveAdapter {
        async fn read(
            &self,
            _schema: &alembic_core::Schema,
            _types: &[TypeName],
            _state: &StateStore,
        ) -> anyhow::Result<ObservedState> {
            let mut states = self.states.lock().unwrap();
            Ok(states.remove(0))
        }
    }

    let inventory = inv(vec![obj(
        uid(1),
        "dcim.site",
        "site=fra1",
        json!({ "name": "FRA1", "slug": "fra1" }),
    )]);
    let mut first = ObservedState::default();
    first
        .insert(ObservedObject {
            type_name: t("dcim.site"),
            key: key_str("site=fra1"),
            attrs: attrs_map(json!({ "name": "FRA1", "slug": "fra1" })),
            backend_id: Some(BackendId::Int(1)),
        })
        .unwrap();
    let second = first.clone();

    let adapter = ReobserveAdapter {
        states: std::sync::Arc::new(std::sync::Mutex::new(vec![first, second])),
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan =
        futures::executor::block_on(build_plan(&adapter, &inventory, &mut state, false)).unwrap();
    assert!(plan.ops.is_empty());
}

#[test]
fn build_plan_observes_all_schema_types() {
    #[derive(Clone)]
    struct ScopeAdapter {
        seen: std::sync::Arc<std::sync::Mutex<Vec<TypeName>>>,
    }

    #[async_trait::async_trait]
    impl Observer for ScopeAdapter {
        async fn read(
            &self,
            _schema: &alembic_core::Schema,
            types: &[TypeName],
            _state: &StateStore,
        ) -> anyhow::Result<ObservedState> {
            *self.seen.lock().unwrap() = types.to_vec();
            Ok(ObservedState::default())
        }
    }

    let mut inventory = inv(vec![obj(
        uid(1),
        "dcim.site",
        "site=fra1",
        json!({ "name": "FRA1", "slug": "fra1" }),
    )]);
    inventory.schema.types.insert(
        "extra.type".to_string(),
        TypeSchema {
            key: BTreeMap::new(),
            fields: BTreeMap::new(),
        },
    );

    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let adapter = ScopeAdapter {
        seen: std::sync::Arc::clone(&seen),
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan =
        futures::executor::block_on(build_plan(&adapter, &inventory, &mut state, false)).unwrap();
    assert_eq!(plan.ops.len(), 1);
    assert!(seen.lock().unwrap().contains(&t("extra.type")));
}

#[test]
fn apply_plan_blocks_deletes_without_flag() {
    let adapter = TestAdapter {
        observed: ObservedState::default(),
        report: ApplyReport::default(),
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = Plan {
        schema: Schema {
            types: BTreeMap::new(),
        },
        ops: vec![Op::Delete {
            uid: uid(1),
            type_name: t("dcim.site"),
            key: key_str("site=fra1"),
            backend_id: Some(BackendId::Int(1)),
        }],
        summary: None,
        schema_preview: None,
    };
    let backend = Backend::Adapter(Box::new(adapter));
    let result = futures::executor::block_on(apply_plan(&backend, &plan, &mut state, false));
    assert!(result.is_err());
}

#[test]
fn guard_schema_deletes_gate() {
    let clean = ProvisionReport {
        created_object_types: vec!["dcim.widget".to_string()],
        ..Default::default()
    };
    // creates only: allowed regardless of the flag.
    guard_schema_deletes(&clean, false).unwrap();

    let with_deletes = ProvisionReport {
        deleted_object_types: vec!["dcim.widget".to_string()],
        deleted_object_fields: vec!["dcim.gadget.color".to_string()],
        ..Default::default()
    };
    // deletes without the flag: refused, and the message points at the flag.
    let err = guard_schema_deletes(&with_deletes, false).unwrap_err();
    assert!(err.to_string().contains("--allow-delete"), "{err}");
    // deletes with the flag: allowed.
    guard_schema_deletes(&with_deletes, true).unwrap();
}

#[test]
fn apply_plan_blocks_schema_deletes_without_flag() {
    let adapter = TestAdapter {
        observed: ObservedState::default(),
        report: ApplyReport::default(),
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = Plan {
        schema: Schema {
            types: BTreeMap::new(),
        },
        ops: vec![],
        summary: None,
        schema_preview: Some(ProvisionReport {
            deleted_object_types: vec!["dcim.widget".to_string()],
            ..Default::default()
        }),
    };
    let backend = Backend::Adapter(Box::new(adapter));
    let result = futures::executor::block_on(apply_plan(&backend, &plan, &mut state, false));
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("--allow-delete"));
}

#[test]
fn apply_plan_allows_schema_deletes_with_flag() {
    let adapter = TestAdapter {
        observed: ObservedState::default(),
        report: ApplyReport::default(),
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = Plan {
        schema: Schema {
            types: BTreeMap::new(),
        },
        ops: vec![],
        summary: None,
        schema_preview: Some(ProvisionReport {
            deleted_object_fields: vec!["dcim.widget.color".to_string()],
            ..Default::default()
        }),
    };
    let backend = Backend::Adapter(Box::new(adapter));
    futures::executor::block_on(apply_plan(&backend, &plan, &mut state, true)).unwrap();
}

/// adapter whose read-only schema preview reports pending deletions, exercising
/// apply's self-preview gate on a plan that carries no precomputed schema_preview.
struct PreviewAdapter {
    preview: ProvisionReport,
}

#[async_trait::async_trait]
impl Observer for PreviewAdapter {
    async fn read(
        &self,
        _schema: &alembic_core::Schema,
        _types: &[TypeName],
        _state: &StateStore,
    ) -> anyhow::Result<ObservedState> {
        Ok(ObservedState::default())
    }
}

#[async_trait::async_trait]
impl Emitter for PreviewAdapter {
    async fn write(
        &self,
        _schema: &alembic_core::Schema,
        _ops: &[Op],
        _state: &StateStore,
    ) -> anyhow::Result<ApplyReport> {
        Ok(ApplyReport::default())
    }
}

#[async_trait::async_trait]
impl Adapter for PreviewAdapter {
    async fn preview_schema(
        &self,
        _schema: &alembic_core::Schema,
    ) -> anyhow::Result<Option<ProvisionReport>> {
        Ok(Some(self.preview.clone()))
    }
}

#[test]
fn apply_plan_self_previews_and_blocks_schema_deletes() {
    // the plan carries no precomputed preview, as the interactive-apply and
    // library plan()+apply_plan() paths produce; the gate must still fire by
    // previewing the adapter's schema at apply time.
    let adapter = PreviewAdapter {
        preview: ProvisionReport {
            deleted_object_types: vec!["dcim.widget".to_string()],
            ..Default::default()
        },
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = Plan {
        schema: Schema {
            types: BTreeMap::new(),
        },
        ops: vec![],
        summary: None,
        schema_preview: None,
    };
    let backend = Backend::Adapter(Box::new(adapter));
    let result = futures::executor::block_on(apply_plan(&backend, &plan, &mut state, false));
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("--allow-delete"));
}

#[test]
fn apply_plan_self_preview_allows_schema_deletes_with_flag() {
    let adapter = PreviewAdapter {
        preview: ProvisionReport {
            deleted_object_fields: vec!["dcim.widget.color".to_string()],
            ..Default::default()
        },
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = Plan {
        schema: Schema {
            types: BTreeMap::new(),
        },
        ops: vec![],
        summary: None,
        schema_preview: None,
    };
    let backend = Backend::Adapter(Box::new(adapter));
    futures::executor::block_on(apply_plan(&backend, &plan, &mut state, true)).unwrap();
}

#[test]
fn apply_plan_updates_state() {
    let adapter = TestAdapter {
        observed: ObservedState::default(),
        report: ApplyReport {
            applied: vec![AppliedOp {
                uid: uid(1),
                type_name: t("dcim.site"),
                backend_id: Some(BackendId::Int(55)),
            }],
            ..Default::default()
        },
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = Plan {
        schema: Schema {
            types: BTreeMap::new(),
        },
        ops: vec![],
        summary: None,
        schema_preview: None,
    };
    let backend = Backend::Adapter(Box::new(adapter));
    futures::executor::block_on(apply_plan(&backend, &plan, &mut state, true)).unwrap();
    assert_eq!(
        state.backend_id(t("dcim.site"), uid(1)),
        Some(BackendId::Int(55))
    );
}

#[test]
fn apply_plan_clears_state_for_deleted_op() {
    // a delete surfaces as an AppliedOp with no backend id; apply must drop the
    // object's state mapping so the next plan no longer tracks it.
    let adapter = TestAdapter {
        observed: ObservedState::default(),
        report: ApplyReport {
            applied: vec![AppliedOp {
                uid: uid(1),
                type_name: t("dcim.site"),
                backend_id: None,
            }],
            ..Default::default()
        },
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    state.set_backend_id(t("dcim.site"), uid(1), BackendId::Int(55));
    let plan = Plan {
        schema: Schema {
            types: BTreeMap::new(),
        },
        ops: vec![],
        summary: None,
        schema_preview: None,
    };
    let backend = Backend::Adapter(Box::new(adapter));
    futures::executor::block_on(apply_plan(&backend, &plan, &mut state, true)).unwrap();
    assert_eq!(state.backend_id(t("dcim.site"), uid(1)), None);
}

#[test]
fn apply_plan_recovers_state_mappings_for_resumed_ops() {
    // the interrupted run never reached a state save, so the ops it applied are in
    // the journal and nowhere else. without this fold a later rename of the object
    // plans a duplicate create.
    let adapter = TestAdapter {
        observed: ObservedState::default(),
        report: ApplyReport {
            applied: vec![],
            resumed: vec![AppliedOp {
                uid: uid(1),
                type_name: t("dcim.site"),
                backend_id: Some(BackendId::Int(55)),
            }],
            ..Default::default()
        },
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = Plan {
        schema: Schema {
            types: BTreeMap::new(),
        },
        ops: vec![],
        summary: None,
        schema_preview: None,
    };
    let backend = Backend::Adapter(Box::new(adapter));
    futures::executor::block_on(apply_plan(&backend, &plan, &mut state, true)).unwrap();
    assert_eq!(
        state.backend_id(t("dcim.site"), uid(1)),
        Some(BackendId::Int(55))
    );
}

#[test]
fn apply_plan_keeps_the_mapping_a_resumed_op_has_no_id_for() {
    // a journal written before ids were recorded has none for any op; recovering
    // from it must not clear a mapping that is already good.
    let adapter = TestAdapter {
        observed: ObservedState::default(),
        report: ApplyReport {
            applied: vec![],
            resumed: vec![AppliedOp {
                uid: uid(1),
                type_name: t("dcim.site"),
                backend_id: None,
            }],
            ..Default::default()
        },
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    state.set_backend_id(t("dcim.site"), uid(1), BackendId::Int(55));
    let plan = Plan {
        schema: Schema {
            types: BTreeMap::new(),
        },
        ops: vec![],
        summary: None,
        schema_preview: None,
    };
    let backend = Backend::Adapter(Box::new(adapter));
    futures::executor::block_on(apply_plan(&backend, &plan, &mut state, true)).unwrap();
    assert_eq!(
        state.backend_id(t("dcim.site"), uid(1)),
        Some(BackendId::Int(55))
    );
}

#[test]
fn apply_plan_emitter_writes_and_provisions_nothing() {
    let adapter = TestAdapter {
        observed: ObservedState::default(),
        report: ApplyReport {
            applied: vec![AppliedOp {
                uid: uid(1),
                type_name: t("dcim.site"),
                backend_id: Some(BackendId::Int(77)),
            }],
            ..Default::default()
        },
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = Plan {
        schema: Schema {
            types: BTreeMap::new(),
        },
        ops: vec![],
        summary: None,
        schema_preview: None,
    };
    let backend = Backend::Emitter(Box::new(adapter));
    let report =
        futures::executor::block_on(apply_plan(&backend, &plan, &mut state, true)).unwrap();
    // emitters write but never provision.
    assert!(report.provision.is_empty());
    // the write report still updates the state mapping.
    assert_eq!(
        state.backend_id(t("dcim.site"), uid(1)),
        Some(BackendId::Int(77))
    );
}

/// provisions in both passes of one apply: a field from `ensure_schema`, a tag
/// from `write` (tags come from the ops, so only the write pass knows them).
struct TwoPassAdapter;

#[async_trait::async_trait]
impl Observer for TwoPassAdapter {
    async fn read(
        &self,
        _schema: &alembic_core::Schema,
        _types: &[TypeName],
        _state: &StateStore,
    ) -> anyhow::Result<ObservedState> {
        Ok(ObservedState::default())
    }
}

#[async_trait::async_trait]
impl Emitter for TwoPassAdapter {
    async fn write(
        &self,
        _schema: &alembic_core::Schema,
        _ops: &[Op],
        _state: &StateStore,
    ) -> anyhow::Result<ApplyReport> {
        Ok(ApplyReport {
            provision: ProvisionReport {
                created_tags: vec!["fabric".to_string()],
                ..Default::default()
            },
            ..Default::default()
        })
    }
}

#[async_trait::async_trait]
impl Adapter for TwoPassAdapter {
    async fn ensure_schema(
        &self,
        _schema: &alembic_core::Schema,
    ) -> anyhow::Result<ProvisionReport> {
        Ok(ProvisionReport {
            created_fields: vec!["dcim.site.role".to_string()],
            ..Default::default()
        })
    }
}

fn empty_plan() -> Plan {
    Plan {
        schema: Schema {
            types: BTreeMap::new(),
        },
        ops: vec![],
        summary: None,
        schema_preview: None,
    }
}

#[test]
fn apply_plan_merges_both_provision_passes() {
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let backend = Backend::Adapter(Box::new(TwoPassAdapter));
    let report =
        futures::executor::block_on(apply_plan(&backend, &empty_plan(), &mut state, true)).unwrap();
    // the write pass survives instead of being overwritten by ensure_schema's report,
    assert_eq!(report.provision.created_tags, vec!["fabric".to_string()]);
    // and ensure_schema's categories still come through.
    assert_eq!(
        report.provision.created_fields,
        vec!["dcim.site.role".to_string()]
    );
}

#[test]
fn apply_plan_keeps_an_emitters_write_provision() {
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    // the emitter arm has no ensure_schema report of its own; merging an empty one
    // must not blank what write reported.
    let backend = Backend::Emitter(Box::new(TwoPassAdapter));
    let report =
        futures::executor::block_on(apply_plan(&backend, &empty_plan(), &mut state, true)).unwrap();
    assert_eq!(report.provision.created_tags, vec!["fabric".to_string()]);
    assert!(report.provision.created_fields.is_empty());
}

#[test]
fn apply_plan_rejects_read_only_observer() {
    let adapter = TestAdapter {
        observed: ObservedState::default(),
        report: ApplyReport::default(),
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = Plan {
        schema: Schema {
            types: BTreeMap::new(),
        },
        ops: vec![],
        summary: None,
        schema_preview: None,
    };
    let backend = Backend::Observer(Box::new(adapter));
    let err = futures::executor::block_on(apply_plan(&backend, &plan, &mut state, true))
        .unwrap_err()
        .to_string();
    assert!(err.contains("read-only"));
}
