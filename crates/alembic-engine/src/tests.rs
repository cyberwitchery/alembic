use super::*;

/// test-side plumbing: most plan tests care about the plan alone, so this
/// keeps the call sites on the plan while `build_plan` also reports what
/// bootstrap wrote. adoption reporting has its own tests.
async fn build_plan(
    adapter: &(dyn Observer + '_),
    inventory: &alembic_core::Inventory,
    state: &mut StateStore,
    allow_delete: bool,
) -> anyhow::Result<Plan> {
    crate::build_plan(adapter, inventory, state, allow_delete, true)
        .await
        .map(|(plan, _)| plan)
}
use alembic_core::{
    FieldSchema, FieldType, Inventory, JsonMap, Key, Object, Schema, TypeName, TypeSchema, Uid,
};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
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
    Inventory {
        scope: None,
        schema,
        objects,
    }
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
        scope: None,
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
        inventory.scope.as_ref(),
        false,
        true,
    )
    .unwrap();

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
    let plan = plan(
        &desired.objects,
        &observed,
        &state,
        &desired.schema,
        desired.scope.as_ref(),
        false,
        true,
    )
    .unwrap();

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
    let plan = plan(
        &desired.objects,
        &observed,
        &state,
        &desired.schema,
        desired.scope.as_ref(),
        false,
        true,
    )
    .unwrap();

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
        None,
        false,
        true,
    )
    .unwrap();
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
        None,
        false,
        true,
    )
    .unwrap();
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
        None,
        false,
        true,
    )
    .unwrap();
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
    let plan = plan(
        &desired.objects,
        &observed,
        &state,
        &desired.schema,
        desired.scope.as_ref(),
        true,
        true,
    )
    .unwrap();
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
        _scope: &crate::state::ReadScope,
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

fn field_of(r#type: FieldType) -> FieldSchema {
    FieldSchema {
        r#type,
        required: true,
        nullable: false,
        description: None,
        format: None,
        pattern: None,
    }
}

fn ref_to(target: &str) -> FieldSchema {
    field_of(FieldType::Ref {
        target: target.to_string(),
    })
}

/// a site keyed on a slug, a device keyed on a ref to the site, an interface
/// keyed on a ref to the device. `depth` picks how much of the chain to take.
fn ref_chain_inventory(depth: usize) -> Inventory {
    let mut types = BTreeMap::new();
    types.insert(
        "dcim.site".to_string(),
        TypeSchema {
            key: BTreeMap::from([("slug".to_string(), field_of(FieldType::Slug))]),
            fields: BTreeMap::from([
                ("slug".to_string(), field_of(FieldType::Slug)),
                ("name".to_string(), field_of(FieldType::String)),
            ]),
        },
    );
    types.insert(
        "dcim.device".to_string(),
        TypeSchema {
            key: BTreeMap::from([
                ("site".to_string(), ref_to("dcim.site")),
                ("name".to_string(), field_of(FieldType::String)),
            ]),
            fields: BTreeMap::from([
                ("site".to_string(), ref_to("dcim.site")),
                ("name".to_string(), field_of(FieldType::String)),
            ]),
        },
    );
    types.insert(
        "dcim.interface".to_string(),
        TypeSchema {
            key: BTreeMap::from([
                ("device".to_string(), ref_to("dcim.device")),
                ("name".to_string(), field_of(FieldType::String)),
            ]),
            fields: BTreeMap::from([
                ("device".to_string(), ref_to("dcim.device")),
                ("name".to_string(), field_of(FieldType::String)),
            ]),
        },
    );

    // canonical uids, as `alembic import` writes them: identity is derived from
    // the key below the adapter, so a ref-typed key field has to name the uid
    // its target derives.
    let mut objects = vec![obj(
        chain_uid("dcim.site", "slug=fra1"),
        "dcim.site",
        "slug=fra1",
        json!({ "slug": "fra1", "name": "FRA1" }),
    )];
    if depth >= 1 {
        let key = format!("site={};name=leaf01", chain_uid("dcim.site", "slug=fra1"));
        objects.push(obj(
            chain_uid("dcim.device", &key),
            "dcim.device",
            &key,
            json!({ "site": chain_uid("dcim.site", "slug=fra1").to_string(), "name": "leaf01" }),
        ));
    }
    if depth >= 2 {
        let device = format!("site={};name=leaf01", chain_uid("dcim.site", "slug=fra1"));
        let key = format!("device={};name=eth0", chain_uid("dcim.device", &device));
        objects.push(obj(
            chain_uid("dcim.interface", &key),
            "dcim.interface",
            &key,
            json!({ "device": chain_uid("dcim.device", &device).to_string(), "name": "eth0" }),
        ));
    }
    Inventory {
        scope: None,
        schema: Schema { types },
        objects,
    }
}

/// the uid a type derives from a key, the shape `key_str` writes.
fn chain_uid(type_name: &str, key: &str) -> Uid {
    alembic_core::uid_v5(type_name, &alembic_core::key_string(&key_str(key)))
}

/// the uid of the chain object at `level` (0 site, 1 device, 2 interface).
fn chain_object_uid(level: usize) -> Uid {
    ref_chain_inventory(level).objects[level].uid
}

/// an adapter's read path in miniature: ref-typed fields come back as backend
/// ids and go through `resolve_ref_keyed_identity`, like the built-in adapters.
#[derive(Clone)]
struct RefChainAdapter {
    rows: Vec<(TypeName, BackendId, serde_json::Value)>,
    reads: std::sync::Arc<std::sync::Mutex<usize>>,
}

impl RefChainAdapter {
    fn new(rows: Vec<(TypeName, BackendId, serde_json::Value)>) -> Self {
        RefChainAdapter {
            rows,
            reads: std::sync::Arc::new(std::sync::Mutex::new(0)),
        }
    }

    fn reads(&self) -> usize {
        *self.reads.lock().unwrap()
    }
}

#[async_trait::async_trait]
impl Observer for RefChainAdapter {
    async fn read(
        &self,
        schema: &alembic_core::Schema,
        _types: &[TypeName],
        state: &StateStore,
        _scope: &crate::state::ReadScope,
    ) -> anyhow::Result<ObservedState> {
        *self.reads.lock().unwrap() += 1;
        let raw: Vec<RawNode> = self
            .rows
            .iter()
            .map(|(type_name, backend_id, attrs)| RawNode {
                type_name: type_name.clone(),
                backend_id: backend_id.clone(),
                attrs: attrs_map(attrs.clone()),
            })
            .collect();
        let mut mappings = StateMappings::from_state(state);
        let mut observed = ObservedState::default();
        for object in resolve_ref_keyed_identity(
            &raw,
            schema,
            &mut mappings,
            |node, type_schema, mappings| normalize_attrs_refs(&node.attrs, type_schema, mappings),
            |_, type_schema, attrs| build_key_from_schema(type_schema, attrs),
        )? {
            observed.insert(object)?;
        }
        Ok(observed)
    }
}

/// backend rows for the chain, refs held as backend ids (the shape a backend
/// that assigns its own ids returns).
fn ref_chain_rows(depth: usize) -> Vec<(TypeName, BackendId, serde_json::Value)> {
    let mut rows = vec![(
        t("dcim.site"),
        BackendId::Int(1),
        json!({ "slug": "fra1", "name": "FRA1" }),
    )];
    if depth >= 1 {
        rows.push((
            t("dcim.device"),
            BackendId::Int(2),
            json!({ "site": 1, "name": "leaf01" }),
        ));
    }
    if depth >= 2 {
        rows.push((
            t("dcim.interface"),
            BackendId::Int(3),
            json!({ "device": 2, "name": "eth0" }),
        ));
    }
    rows
}

#[test]
fn build_plan_bootstraps_object_keyed_on_a_ref() {
    let inventory = ref_chain_inventory(1);
    let adapter = RefChainAdapter::new(ref_chain_rows(1));
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan =
        futures::executor::block_on(build_plan(&adapter, &inventory, &mut state, false)).unwrap();

    assert!(plan.ops.is_empty(), "unexpected ops: {:?}", plan.ops);
    assert_eq!(
        state.backend_id(t("dcim.device"), chain_object_uid(1)),
        Some(BackendId::Int(2))
    );
    assert_eq!(adapter.reads(), 1);
}

#[test]
fn build_plan_bootstraps_a_chain_of_ref_keyed_objects() {
    let inventory = ref_chain_inventory(2);
    let adapter = RefChainAdapter::new(ref_chain_rows(2));
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan =
        futures::executor::block_on(build_plan(&adapter, &inventory, &mut state, false)).unwrap();

    // the whole chain resolves inside the one read the adapter takes.
    assert!(plan.ops.is_empty(), "unexpected ops: {:?}", plan.ops);
    assert_eq!(
        state.backend_id(t("dcim.interface"), chain_object_uid(2)),
        Some(BackendId::Int(3))
    );
    assert_eq!(adapter.reads(), 1);
}

#[test]
fn build_plan_bootstraps_a_chain_already_in_uid_space() {
    // control: a backend holding the same chain keyed by uid converges to the
    // same plan and the same mappings.
    let inventory = ref_chain_inventory(2);
    let adapter = RefChainAdapter::new(vec![
        (
            t("dcim.site"),
            BackendId::Int(1),
            json!({ "slug": "fra1", "name": "FRA1" }),
        ),
        (
            t("dcim.device"),
            BackendId::Int(2),
            json!({ "site": chain_object_uid(0).to_string(), "name": "leaf01" }),
        ),
        (
            t("dcim.interface"),
            BackendId::Int(3),
            json!({ "device": chain_object_uid(1).to_string(), "name": "eth0" }),
        ),
    ]);
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan =
        futures::executor::block_on(build_plan(&adapter, &inventory, &mut state, false)).unwrap();

    assert!(plan.ops.is_empty(), "unexpected ops: {:?}", plan.ops);
    assert_eq!(
        state.backend_id(t("dcim.interface"), chain_object_uid(2)),
        Some(BackendId::Int(3))
    );
    assert_eq!(adapter.reads(), 1);
}

#[test]
fn build_plan_refuses_a_key_ref_left_in_backend_id_space() {
    // the device row is absent from the read, so the interface keeps `device: 2`
    // and its key never matches the desired one, which names a uid.
    let inventory = ref_chain_inventory(2);
    let adapter = RefChainAdapter::new(vec![
        (
            t("dcim.site"),
            BackendId::Int(1),
            json!({ "slug": "fra1", "name": "FRA1" }),
        ),
        (
            t("dcim.interface"),
            BackendId::Int(3),
            json!({ "device": 2, "name": "eth0" }),
        ),
    ]);
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let error = futures::executor::block_on(build_plan(&adapter, &inventory, &mut state, false))
        .unwrap_err()
        .to_string();

    assert!(
        error.contains("reported 2 reference(s) as backend ids"),
        "{error}"
    );
    assert!(
        error.contains("dcim.interface.key.device -> dcim.device 2"),
        "{error}"
    );
    assert!(
        error.contains("dcim.interface.device -> dcim.device 2"),
        "{error}"
    );
    assert!(
        error.contains("no dcim.device with that backend id was observed"),
        "{error}"
    );
}

#[test]
fn build_plan_names_a_backend_id_ref_whose_target_the_read_holds() {
    // the site is in the same observation, so the adapter had the uid it needed
    // without reading again.
    let inventory = ref_chain_inventory(1);
    let mut observed = ObservedState::default();
    observed
        .insert(ObservedObject {
            type_name: t("dcim.site"),
            key: key_str("slug=fra1"),
            attrs: attrs_map(json!({ "slug": "fra1", "name": "FRA1" })),
            backend_id: Some(BackendId::Int(1)),
        })
        .unwrap();
    observed
        .insert(ObservedObject {
            type_name: t("dcim.device"),
            key: Key::from(BTreeMap::from([
                ("site".to_string(), json!(1)),
                ("name".to_string(), json!("leaf01")),
            ])),
            attrs: attrs_map(json!({ "site": 1, "name": "leaf01" })),
            backend_id: Some(BackendId::Int(2)),
        })
        .unwrap();
    let adapter = TestAdapter {
        observed,
        report: ApplyReport::default(),
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let error = futures::executor::block_on(build_plan(&adapter, &inventory, &mut state, false))
        .unwrap_err()
        .to_string();

    assert!(
        error.contains("dcim.device.key.site -> dcim.site 1"),
        "{error}"
    );
    assert!(
        error.contains("the dcim.site it names was observed, so the adapter can rewrite the id without reading again"),
        "{error}"
    );
}

/// a type whose only ref sits outside the key: the key matches either way, so an
/// unrewritten ref reads as a field diff rather than a missed adoption.
fn attr_ref_inventory() -> Inventory {
    let types = BTreeMap::from([
        (
            "dcim.site".to_string(),
            TypeSchema {
                key: BTreeMap::from([("slug".to_string(), field_of(FieldType::Slug))]),
                fields: BTreeMap::from([("slug".to_string(), field_of(FieldType::Slug))]),
            },
        ),
        (
            "circuits.termination".to_string(),
            TypeSchema {
                key: BTreeMap::from([("cid".to_string(), field_of(FieldType::String))]),
                fields: BTreeMap::from([
                    ("cid".to_string(), field_of(FieldType::String)),
                    ("site".to_string(), ref_to("dcim.site")),
                ]),
            },
        ),
    ]);
    Inventory {
        scope: None,
        schema: Schema { types },
        objects: vec![
            obj(uid(1), "dcim.site", "slug=fra1", json!({ "slug": "fra1" })),
            obj(
                uid(2),
                "circuits.termination",
                "cid=c1",
                json!({ "cid": "c1", "site": uid(1).to_string() }),
            ),
        ],
    }
}

fn attr_ref_observation(site: serde_json::Value) -> ObservedState {
    let mut observed = ObservedState::default();
    observed
        .insert(ObservedObject {
            type_name: t("dcim.site"),
            key: key_str("slug=fra1"),
            attrs: attrs_map(json!({ "slug": "fra1" })),
            backend_id: Some(BackendId::Int(1)),
        })
        .unwrap();
    observed
        .insert(ObservedObject {
            type_name: t("circuits.termination"),
            key: key_str("cid=c1"),
            attrs: attrs_map(json!({ "cid": "c1", "site": site })),
            backend_id: Some(BackendId::Int(7)),
        })
        .unwrap();
    observed
}

#[test]
fn build_plan_refuses_a_backend_id_ref_outside_the_key() {
    let adapter = TestAdapter {
        observed: attr_ref_observation(json!(1)),
        report: ApplyReport::default(),
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let error = futures::executor::block_on(build_plan(
        &adapter,
        &attr_ref_inventory(),
        &mut state,
        false,
    ))
    .unwrap_err()
    .to_string();

    assert!(
        error.contains("reported 1 reference(s) as backend ids"),
        "{error}"
    );
    assert!(
        error.contains("circuits.termination.site -> dcim.site 1"),
        "{error}"
    );
}

#[test]
fn build_plan_takes_the_same_observation_in_uid_space() {
    // the control for the two refusals above: the same rows with the ref
    // rewritten adopt both objects and plan nothing.
    let adapter = TestAdapter {
        observed: attr_ref_observation(json!(uid(1).to_string())),
        report: ApplyReport::default(),
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = futures::executor::block_on(build_plan(
        &adapter,
        &attr_ref_inventory(),
        &mut state,
        false,
    ))
    .unwrap();

    assert!(plan.ops.is_empty(), "unexpected ops: {:?}", plan.ops);
    assert_eq!(
        state.backend_id(t("circuits.termination"), uid(2)),
        Some(BackendId::Int(7))
    );
}

#[test]
fn build_plan_keeps_a_declared_uid_state_already_maps() {
    // a hand-authored inventory naming its own uids, the shape docs/ir.md writes
    // and `alembic map` emits: once state maps one, derivation must not take it
    // back, or the ref-keyed child never adopts.
    let declared_site: Uid = "00000000-0000-0000-0000-000000000001".parse().unwrap();
    let declared_device: Uid = "00000000-0000-0000-0000-000000000002".parse().unwrap();
    let inventory = Inventory {
        scope: None,
        schema: ref_chain_inventory(1).schema,
        objects: vec![
            obj(
                declared_site,
                "dcim.site",
                "slug=fra1",
                json!({ "slug": "fra1", "name": "FRA1" }),
            ),
            obj(
                declared_device,
                "dcim.device",
                &format!("site={declared_site};name=leaf01"),
                json!({ "site": declared_site.to_string(), "name": "leaf01" }),
            ),
        ],
    };

    let dir = tempdir().unwrap();
    let mut state = StateStore::load(dir.path().join("state.json")).unwrap();
    // the site adopts on its slug; the device cannot yet, its observed key holds
    // the uid the site derives rather than the declared one.
    let cold = RefChainAdapter::new(ref_chain_rows(1));
    futures::executor::block_on(build_plan(&cold, &inventory, &mut state, false)).unwrap();
    assert_eq!(
        state.backend_id(t("dcim.site"), declared_site),
        Some(BackendId::Int(1))
    );

    let warm = RefChainAdapter::new(ref_chain_rows(1));
    let plan =
        futures::executor::block_on(build_plan(&warm, &inventory, &mut state, false)).unwrap();
    assert!(plan.ops.is_empty(), "unexpected ops: {:?}", plan.ops);
    assert_eq!(
        state.backend_id(t("dcim.device"), declared_device),
        Some(BackendId::Int(2))
    );
}

#[test]
fn build_plan_reads_once_when_state_is_warm() {
    let inventory = ref_chain_inventory(2);
    let adapter = RefChainAdapter::new(ref_chain_rows(2));
    let dir = tempdir().unwrap();
    let mut state = StateStore::load(dir.path().join("state.json")).unwrap();
    futures::executor::block_on(build_plan(&adapter, &inventory, &mut state, false)).unwrap();

    let warm = RefChainAdapter::new(ref_chain_rows(2));
    let plan =
        futures::executor::block_on(build_plan(&warm, &inventory, &mut state, false)).unwrap();
    assert!(plan.ops.is_empty(), "unexpected ops: {:?}", plan.ops);
    assert_eq!(warm.reads(), 1);
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
fn insert_records_a_duplicate_natural_key_as_data() {
    // key ambiguity is data in the raw observation: both twins are held, and
    // only dereferencing the key fails, naming every candidate. an id-less
    // twin is named as `unknown`.
    let mut observed = ObservedState::default();
    let mk = |id: Option<u64>| ObservedObject {
        type_name: t("dcim.site"),
        key: key_str("site=fra1"),
        attrs: attrs_map(json!({ "name": "FRA1", "slug": "fra1" })),
        backend_id: id.map(BackendId::Int),
    };
    observed.insert(mk(Some(7))).unwrap();
    observed.insert(mk(None)).unwrap();
    assert_eq!(observed.len(), 2);
    let err = observed
        .unique_by_key(&t("dcim.site"), &key_string(&key_str("site=fra1")))
        .expect_err("dereferencing an ambiguous key must fail");
    let message = err.to_string();
    assert!(
        message.contains("2 dcim.site objects share the key"),
        "{message}"
    );
    assert!(message.contains("7, unknown"), "{message}");

    // ids stay unique and dereference cleanly beside the ambiguity.
    assert!(observed
        .by_backend_id(&t("dcim.site"), &BackendId::Int(7))
        .is_some());
    // an unambiguous key still answers.
    observed
        .insert(ObservedObject {
            type_name: t("dcim.site"),
            key: key_str("site=ams1"),
            attrs: attrs_map(json!({ "name": "AMS1", "slug": "ams1" })),
            backend_id: Some(BackendId::Int(8)),
        })
        .unwrap();
    assert!(observed
        .unique_by_key(&t("dcim.site"), &key_string(&key_str("site=ams1")))
        .unwrap()
        .is_some());
    // and the ambiguity listing names the one contested key with its holders.
    let ambiguous: Vec<_> = observed.ambiguities().collect();
    assert_eq!(ambiguous.len(), 1);
    assert_eq!(ambiguous[0].2.len(), 2);
}

#[test]
fn build_plan_names_both_backend_ids_for_a_colliding_key() {
    struct CollidingAdapter;

    #[async_trait::async_trait]
    impl Observer for CollidingAdapter {
        async fn read(
            &self,
            _schema: &alembic_core::Schema,
            _types: &[TypeName],
            _state: &StateStore,
            _scope: &crate::state::ReadScope,
        ) -> anyhow::Result<ObservedState> {
            let mut observed = ObservedState::default();
            for id in [7u64, 9] {
                observed.insert(ObservedObject {
                    type_name: t("dcim.site"),
                    key: key_str("site=fra1"),
                    attrs: attrs_map(json!({ "name": "FRA1", "slug": "fra1" })),
                    backend_id: Some(BackendId::Int(id)),
                })?;
            }
            Ok(observed)
        }
    }

    // adopting the contested key is the failure: never a choice among twins.
    let inventory = inv(vec![obj(
        uid(1),
        "dcim.site",
        "site=fra1",
        json!({ "name": "FRA1", "slug": "fra1" }),
    )]);
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let err =
        futures::executor::block_on(build_plan(&CollidingAdapter, &inventory, &mut state, false))
            .expect_err("adopting an ambiguous key must fail");
    let message = format!("{err:#}");
    assert!(
        message.contains("cannot adopt dcim.site {\"site\":\"fra1\"}"),
        "{message}"
    );
    assert!(message.contains("backend ids 7, 9"), "{message}");

    // an unrelated desired object is not denied by the neighbors' collision.
    let unrelated = inv(vec![obj(
        uid(2),
        "dcim.site",
        "site=ams1",
        json!({ "name": "AMS1", "slug": "ams1" }),
    )]);
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let (plan, bootstrap) = futures::executor::block_on(crate::build_plan(
        &CollidingAdapter,
        &unrelated,
        &mut state,
        false,
        true,
    ))
    .expect("unmanaged duplicate keys must not deny an unrelated plan");
    assert_eq!(plan.ops.len(), 1);
    assert!(matches!(plan.ops[0], Op::Create { .. }));
    assert!(bootstrap.is_empty());
}

#[test]
fn build_plan_observes_once_and_bootstraps() {
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
            _scope: &crate::state::ReadScope,
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
    // the second observation is stale, so a plan taken against it would report
    // an update: identity is resolved below the adapter, and one read is all
    // `observe` takes.
    let observation = |name: &str| {
        let mut observed = ObservedState::default();
        observed
            .insert(ObservedObject {
                type_name: t("dcim.site"),
                key: key_str("site=fra1"),
                attrs: attrs_map(json!({ "name": name, "slug": "fra1" })),
                backend_id: Some(BackendId::Int(1)),
            })
            .unwrap();
        observed
    };

    let states = std::sync::Arc::new(std::sync::Mutex::new(vec![
        observation("FRA1"),
        observation("stale"),
    ]));
    let adapter = ReobserveAdapter {
        states: states.clone(),
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan =
        futures::executor::block_on(build_plan(&adapter, &inventory, &mut state, false)).unwrap();
    assert!(plan.ops.is_empty(), "unexpected ops: {:?}", plan.ops);
    assert_eq!(states.lock().unwrap().len(), 1, "more than one read taken");
    assert_eq!(
        state.backend_id(t("dcim.site"), uid(1)),
        Some(BackendId::Int(1))
    );
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
            _scope: &crate::state::ReadScope,
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
    // deletes without the flag: refused, naming both, and the message points at
    // the flag. the names are the operator's only record of what would go.
    let err = guard_schema_deletes(&with_deletes, false).unwrap_err();
    assert!(err.to_string().contains("--allow-delete"), "{err}");
    assert!(err.to_string().contains("- type dcim.widget"), "{err}");
    assert!(
        err.to_string().contains("- field dcim.gadget.color"),
        "{err}"
    );
    // deletes with the flag: allowed.
    guard_schema_deletes(&with_deletes, true).unwrap();
}

#[test]
fn guard_drift_report_refuses_a_write_only_backend() {
    let adapter = || TestAdapter {
        observed: ObservedState::default(),
        report: ApplyReport::default(),
    };
    // an emitter is planned against an empty observation, so a report over it
    // would assert absence it never read.
    let emitter = Backend::Emitter(Box::new(adapter()));
    let err = guard_drift_report(&emitter).unwrap_err().to_string();
    // one string, not two: re-inlining a literal at either site breaks this.
    let refused_by_import = emitter.observer().err().unwrap().to_string();
    assert!(err.starts_with(&refused_by_import), "{err}");
    assert!(err.contains("without --report"), "{err}");

    // an observer still reports drift, it just cannot apply it.
    guard_drift_report(&Backend::Observer(Box::new(adapter()))).unwrap();
    guard_drift_report(&Backend::Adapter(Box::new(adapter()))).unwrap();
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
        _scope: &crate::state::ReadScope,
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

    async fn preview_schema(
        &self,
        _schema: &alembic_core::Schema,
    ) -> anyhow::Result<Option<ProvisionReport>> {
        Ok(Some(self.preview.clone()))
    }
}

impl Adapter for PreviewAdapter {}

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

/// write-only backend that provisions schema, counting the calls: provisioning
/// is a write, so an emitter reaches the same gate a read+write adapter does.
struct ProvisioningEmitter {
    provisioned: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    report: ProvisionReport,
    preview: Option<ProvisionReport>,
}

#[async_trait::async_trait]
impl Emitter for ProvisioningEmitter {
    async fn write(
        &self,
        _schema: &alembic_core::Schema,
        _ops: &[Op],
        _state: &StateStore,
    ) -> anyhow::Result<ApplyReport> {
        Ok(ApplyReport::default())
    }

    async fn ensure_schema(
        &self,
        _schema: &alembic_core::Schema,
    ) -> anyhow::Result<ProvisionReport> {
        self.provisioned
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(self.report.clone())
    }

    async fn preview_schema(
        &self,
        _schema: &alembic_core::Schema,
    ) -> anyhow::Result<Option<ProvisionReport>> {
        Ok(self.preview.clone())
    }
}

fn provisioning_emitter(
    report: ProvisionReport,
    preview: Option<ProvisionReport>,
) -> (Backend, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    let provisioned = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let backend = Backend::Emitter(Box::new(ProvisioningEmitter {
        provisioned: provisioned.clone(),
        report,
        preview,
    }));
    (backend, provisioned)
}

#[test]
fn apply_plan_provisions_over_a_write_only_backend() {
    let creates = ProvisionReport {
        created_fields: vec!["dcim.site.tier".to_string()],
        ..Default::default()
    };
    let (backend, provisioned) = provisioning_emitter(creates.clone(), Some(creates));
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let report =
        futures::executor::block_on(apply_plan(&backend, &empty_plan(), &mut state, false))
            .unwrap();
    assert_eq!(provisioned.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(report.provision.created_fields, vec!["dcim.site.tier"]);
}

#[test]
fn apply_plan_self_previews_a_write_only_backend_before_provisioning() {
    let (backend, provisioned) = provisioning_emitter(
        ProvisionReport::default(),
        Some(ProvisionReport {
            deleted_object_types: vec!["dcim.widget".to_string()],
            ..Default::default()
        }),
    );
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let result =
        futures::executor::block_on(apply_plan(&backend, &empty_plan(), &mut state, false));
    assert!(result.unwrap_err().to_string().contains("--allow-delete"));
    // refused before the schema write, not after it.
    assert_eq!(provisioned.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[test]
fn apply_plan_over_a_write_only_backend_that_provisions_nothing() {
    // django's shape, spelled out: the defaults it inherits answer an empty
    // report on both methods, so apply comes out with nothing to report.
    let (backend, provisioned) =
        provisioning_emitter(ProvisionReport::default(), Some(ProvisionReport::default()));
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let report =
        futures::executor::block_on(apply_plan(&backend, &empty_plan(), &mut state, false))
            .unwrap();
    assert_eq!(provisioned.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(report.provision.is_empty());
}

#[test]
fn apply_plan_refuses_an_emitter_that_cannot_preview() {
    // no preview means no gate, so the run is refused before ensure_schema
    // writes rather than reported in the past tense after it did.
    let (backend, provisioned) = provisioning_emitter(
        ProvisionReport {
            deleted_object_types: vec!["dcim.fossil".to_string()],
            ..Default::default()
        },
        None,
    );
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let err = futures::executor::block_on(apply_plan(&backend, &empty_plan(), &mut state, false))
        .unwrap_err();
    assert!(err.to_string().contains("cannot preview schema"), "{err}");
    assert!(
        err.to_string().contains("implement preview_schema"),
        "{err}"
    );
    assert!(err.to_string().contains("--allow-delete"), "{err}");
    assert_eq!(provisioned.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[test]
fn apply_plan_provisions_when_the_gate_ran_or_was_waived() {
    let dir = tempdir().unwrap();
    let clean = ProvisionReport {
        created_object_types: vec!["dcim.widget".to_string()],
        ..Default::default()
    };
    // the adapter previews, so the gate runs on what it reported and passes.
    let (backend, _) = provisioning_emitter(clean.clone(), Some(clean));
    let mut state = StateStore::load(dir.path().join("previewed.json")).unwrap();
    futures::executor::block_on(apply_plan(&backend, &empty_plan(), &mut state, false)).unwrap();

    // --allow-delete short-circuits the gate, so the unpreviewable one provisions.
    let (backend, provisioned) = provisioning_emitter(
        ProvisionReport {
            deleted_object_types: vec!["dcim.fossil".to_string()],
            ..Default::default()
        },
        None,
    );
    let mut state = StateStore::load(dir.path().join("waived.json")).unwrap();
    let report =
        futures::executor::block_on(apply_plan(&backend, &empty_plan(), &mut state, true)).unwrap();
    assert_eq!(provisioned.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(report.provision.deleted_object_types, vec!["dcim.fossil"]);
}

#[test]
fn an_emitter_overriding_neither_provisioning_method_passes_the_gate() {
    // the shape every emit-only adapter ships: the two defaults now agree that
    // it provisions nothing, so the gate has an honest empty preview to run on.
    struct NoProvisioning;
    #[async_trait::async_trait]
    impl Emitter for NoProvisioning {
        async fn write(
            &self,
            _schema: &Schema,
            _ops: &[Op],
            _state: &StateStore,
        ) -> anyhow::Result<ApplyReport> {
            Ok(ApplyReport::default())
        }
    }
    let backend = Backend::Emitter(Box::new(NoProvisioning));
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let report =
        futures::executor::block_on(apply_plan(&backend, &empty_plan(), &mut state, false))
            .unwrap();
    assert!(report.provision.is_empty());
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
        _scope: &crate::state::ReadScope,
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

impl Adapter for TwoPassAdapter {}

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
fn apply_plan_merges_both_provision_passes_over_a_write_only_backend() {
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    // the same adapter in the write-only box: which box holds it must not decide
    // whether the schema pass runs at all.
    let backend = Backend::Emitter(Box::new(TwoPassAdapter));
    let report =
        futures::executor::block_on(apply_plan(&backend, &empty_plan(), &mut state, true)).unwrap();
    assert_eq!(report.provision.created_tags, vec!["fabric".to_string()]);
    assert_eq!(
        report.provision.created_fields,
        vec!["dcim.site.role".to_string()]
    );
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

/// two types whose keys reference each other. the adapter's fixpoint settles
/// neither, so both come back keyed on backend ids.
fn ref_cycle_inventory() -> Inventory {
    let cycle = |field: &str, target: &str| TypeSchema {
        key: BTreeMap::from([
            (field.to_string(), ref_to(target)),
            ("name".to_string(), field_of(FieldType::String)),
        ]),
        fields: BTreeMap::from([
            (field.to_string(), ref_to(target)),
            ("name".to_string(), field_of(FieldType::String)),
        ]),
    };
    Inventory {
        scope: None,
        schema: Schema {
            types: BTreeMap::from([
                ("net.a".to_string(), cycle("b", "net.b")),
                ("net.b".to_string(), cycle("a", "net.a")),
            ]),
        },
        objects: vec![
            obj(
                uid(1),
                "net.a",
                &format!("b={};name=a1", uid(2)),
                json!({ "b": uid(2).to_string(), "name": "a1" }),
            ),
            obj(
                uid(2),
                "net.b",
                &format!("a={};name=b1", uid(1)),
                json!({ "a": uid(1).to_string(), "name": "b1" }),
            ),
        ],
    }
}

#[test]
fn build_plan_names_a_backend_id_ref_whose_target_is_keyed_on_a_cycle() {
    // both targets are in the observation, so only their own keys separate
    // this from a ref the adapter could still rewrite.
    let adapter = RefChainAdapter::new(vec![
        (
            t("net.a"),
            BackendId::Int(1),
            json!({ "b": 2, "name": "a1" }),
        ),
        (
            t("net.b"),
            BackendId::Int(2),
            json!({ "a": 1, "name": "b1" }),
        ),
    ]);
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let error = futures::executor::block_on(build_plan(
        &adapter,
        &ref_cycle_inventory(),
        &mut state,
        false,
    ))
    .unwrap_err()
    .to_string();

    assert!(
        error.contains("reported 4 reference(s) as backend ids"),
        "{error}"
    );
    assert!(error.contains("net.a.key.b -> net.b 2"), "{error}");
    assert!(error.contains("net.b.key.a -> net.a 1"), "{error}");
    assert!(
        error.contains("was observed, but its own key still holds a backend id"),
        "{error}"
    );
    // asserted absent, not merely outnumbered: every one of the four lands on
    // the new arm, so the old wording surviving anywhere is the regression.
    assert!(
        !error.contains("so the adapter can rewrite the id without reading again"),
        "{error}"
    );
}

#[test]
fn build_plan_names_a_backend_id_ref_whose_target_is_keyed_on_a_chain() {
    // every row is observed, so the site ref rewrites, the device key lands in
    // uid space and the interface ref rewrites after it. the arm reports the one
    // hop the guard looked at and promises nothing about the chain above it.
    let inventory = ref_chain_inventory(2);
    let mut observed = ObservedState::default();
    observed
        .insert(ObservedObject {
            type_name: t("dcim.site"),
            key: key_str("slug=fra1"),
            attrs: attrs_map(json!({ "slug": "fra1", "name": "FRA1" })),
            backend_id: Some(BackendId::Int(1)),
        })
        .unwrap();
    observed
        .insert(ObservedObject {
            type_name: t("dcim.device"),
            key: Key::from(BTreeMap::from([
                ("site".to_string(), json!(1)),
                ("name".to_string(), json!("leaf01")),
            ])),
            attrs: attrs_map(json!({ "site": 1, "name": "leaf01" })),
            backend_id: Some(BackendId::Int(2)),
        })
        .unwrap();
    observed
        .insert(ObservedObject {
            type_name: t("dcim.interface"),
            key: Key::from(BTreeMap::from([
                ("device".to_string(), json!(2)),
                ("name".to_string(), json!("eth0")),
            ])),
            attrs: attrs_map(json!({ "device": 2, "name": "eth0" })),
            backend_id: Some(BackendId::Int(3)),
        })
        .unwrap();
    let adapter = TestAdapter {
        observed,
        report: ApplyReport::default(),
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let error = futures::executor::block_on(build_plan(&adapter, &inventory, &mut state, false))
        .unwrap_err()
        .to_string();

    assert!(
        error.contains(
            "dcim.interface.key.device -> dcim.device 2: the dcim.device it names was observed, but its own key still holds a backend id"
        ),
        "{error}"
    );
    assert!(
        error.contains("dcim.device.key.site -> dcim.site 1: the dcim.site it names was observed, so the adapter can rewrite the id without reading again"),
        "{error}"
    );
    // the site ref above it rewrites, so a uid does derive for the device in one
    // read: the message may not say otherwise.
    assert!(!error.contains("no uid can be derived"), "{error}");
}

/// a backend holding a site and a device whose `site` attr is the site's backend
/// id. reads resolve that id back to a uid through the state store, the way the
/// built-in adapters do.
struct RefBackend;

#[async_trait::async_trait]
impl Observer for RefBackend {
    async fn read(
        &self,
        schema: &Schema,
        _types: &[TypeName],
        state: &StateStore,
        _scope: &crate::state::ReadScope,
    ) -> anyhow::Result<ObservedState> {
        let raw = vec![
            RawNode {
                type_name: t("dcim.site"),
                backend_id: BackendId::Int(1),
                attrs: attrs_map(json!({ "slug": "fra1" })),
            },
            RawNode {
                type_name: t("dcim.device"),
                backend_id: BackendId::Int(2),
                attrs: attrs_map(json!({ "name": "leaf01", "site": 1 })),
            },
        ];
        let mut mappings = StateMappings::from_state(state);
        let resolved = resolve_ref_keyed_identity(
            &raw,
            schema,
            &mut mappings,
            |node, type_schema, mappings| normalize_attrs_refs(&node.attrs, type_schema, mappings),
            |_, type_schema, attrs| build_key_from_schema(type_schema, attrs),
        )?;
        let mut observed = ObservedState::default();
        for object in resolved {
            observed.insert(object)?;
        }
        Ok(observed)
    }
}

fn ref_backend_schema() -> Schema {
    let field = |r#type: FieldType, required: bool| FieldSchema {
        r#type,
        required,
        nullable: !required,
        description: None,
        format: None,
        pattern: None,
    };
    let mut types = BTreeMap::new();
    types.insert(
        "dcim.site".to_string(),
        TypeSchema {
            key: BTreeMap::from([("slug".to_string(), field(FieldType::String, true))]),
            fields: BTreeMap::new(),
        },
    );
    types.insert(
        "dcim.device".to_string(),
        TypeSchema {
            key: BTreeMap::from([("name".to_string(), field(FieldType::String, true))]),
            fields: BTreeMap::from([(
                "site".to_string(),
                field(
                    FieldType::Ref {
                        target: "dcim.site".to_string(),
                    },
                    false,
                ),
            )]),
        },
    );
    Schema { types }
}

/// the two backend objects as an inventory claiming `site_uid` and `device_uid`,
/// the device referencing the site by uid.
fn ref_backend_inventory(site_uid: Uid, device_uid: Uid) -> Inventory {
    Inventory {
        scope: None,
        schema: ref_backend_schema(),
        objects: vec![
            obj(site_uid, "dcim.site", "slug=fra1", json!({})),
            obj(
                device_uid,
                "dcim.device",
                "name=leaf01",
                json!({ "site": site_uid.to_string() }),
            ),
        ],
    }
}

/// every backend id state holds answers to one uid, and that uid maps back to it.
fn backend_ids_are_unambiguous(state: &StateStore) -> bool {
    state.all_mappings().iter().all(|(type_name, mapping)| {
        mapping.values().collect::<BTreeSet<_>>().iter().all(|id| {
            state
                .uid_for_backend_id(type_name, id)
                .is_some_and(|uid| mapping.get(&uid) == Some(*id))
        })
    })
}

#[tokio::test]
async fn plan_converges_once_the_desired_set_claims_new_uids() {
    let dir = tempdir().unwrap();
    let mut state = StateStore::load(dir.path().join("state.json")).unwrap();
    // the mappings a settled run left behind. they sort after the uids the
    // inventory below claims, and the state inversion keeps the last uid it
    // walks, so leaving them in state is enough to decide every ref.
    state.set_backend_id(t("dcim.site"), uid(900), BackendId::Int(1));
    state.set_backend_id(t("dcim.device"), uid(901), BackendId::Int(2));

    let inventory = ref_backend_inventory(uid(10), uid(11));
    let first = build_plan(&RefBackend, &inventory, &mut state, false)
        .await
        .unwrap();
    // the first plan reads before it adopts, so the device's ref still resolves
    // to the uid state held: one update, and the harness is doing something.
    assert_eq!(first.ops.len(), 1, "{:?}", first.ops);

    let second = build_plan(&RefBackend, &inventory, &mut state, false)
        .await
        .unwrap();
    assert!(
        second.ops.is_empty(),
        "the model is settled and replanned anyway: {:?}",
        second.ops
    );
    assert!(backend_ids_are_unambiguous(&state));
}

#[tokio::test]
async fn plan_only_runs_leave_the_mapping_count_flat() {
    let dir = tempdir().unwrap();
    let mut state = StateStore::load(dir.path().join("state.json")).unwrap();
    // an observer can never be applied through, so a plan-only loop is the only
    // thing that ever writes here and nothing prunes behind it.
    for round in 0..4u128 {
        let inventory = ref_backend_inventory(uid(100 + round * 2), uid(101 + round * 2));
        build_plan(&RefBackend, &inventory, &mut state, false)
            .await
            .unwrap();
    }
    let mappings: usize = state.all_mappings().values().map(|m| m.len()).sum();
    assert_eq!(mappings, 2, "{:?}", state.all_mappings());
}

/// the state a run before this left behind: `dcim.site` mapped from both the
/// uid the inventory declares and a stale one that sorts after it.
fn write_doubled_state(path: &std::path::Path) {
    std::fs::write(
        path,
        r#"{"mappings":{"dcim.device":{"00000000-0000-0000-0000-00000000000b":2},"dcim.site":{"00000000-0000-0000-0000-00000000000a":1,"00000000-0000-0000-0000-000000000384":1}}}"#,
    )
    .unwrap();
}

#[tokio::test]
async fn plan_converges_on_a_state_file_that_already_doubled_a_backend_id() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("state.json");
    write_doubled_state(&path);
    let mut state = StateStore::load(&path).unwrap();

    let inventory = ref_backend_inventory(uid(10), uid(11));
    for round in 0..4 {
        let plan = build_plan(&RefBackend, &inventory, &mut state, false)
            .await
            .unwrap();
        assert!(plan.ops.is_empty(), "round {round}: {:?}", plan.ops);
    }
    assert!(backend_ids_are_unambiguous(&state));
    assert_eq!(
        state.backend_id(t("dcim.site"), uid(10)),
        Some(BackendId::Int(1))
    );
}

#[test]
fn a_doubled_state_file_inverts_to_one_uid_per_backend_id() {
    // why the readers that invert state go through the index: the forward map
    // they used to fold is not single-valued and the index is.
    let dir = tempdir().unwrap();
    let path = dir.path().join("state.json");
    write_doubled_state(&path);
    let state = StateStore::load(&path).unwrap();
    assert_eq!(state.all_mappings()[&t("dcim.site")].len(), 2);

    let by_id = state_mappings_by_id(&state, |id| match id {
        BackendId::Int(n) => Some(*n),
        BackendId::String(_) => None,
    });
    assert_eq!(by_id["dcim.site"], BTreeMap::from([(1, uid(10))]));
    assert_eq!(by_id["dcim.device"], BTreeMap::from([(2, uid(11))]));
}

/// a backend holding a site with a non-key attr, and a device referencing it by
/// backend id, so a rename shows up as an update rather than as nothing.
struct RenameBackend;

#[async_trait::async_trait]
impl Observer for RenameBackend {
    async fn read(
        &self,
        schema: &Schema,
        _types: &[TypeName],
        state: &StateStore,
        _scope: &crate::state::ReadScope,
    ) -> anyhow::Result<ObservedState> {
        let raw = vec![
            RawNode {
                type_name: t("dcim.site"),
                backend_id: BackendId::Int(1),
                attrs: attrs_map(json!({ "slug": "fra1", "status": "active" })),
            },
            RawNode {
                type_name: t("dcim.device"),
                backend_id: BackendId::Int(2),
                attrs: attrs_map(json!({ "name": "leaf01", "site": 1 })),
            },
        ];
        let mut mappings = StateMappings::from_state(state);
        let resolved = resolve_ref_keyed_identity(
            &raw,
            schema,
            &mut mappings,
            |node, type_schema, mappings| normalize_attrs_refs(&node.attrs, type_schema, mappings),
            |_, type_schema, attrs| build_key_from_schema(type_schema, attrs),
        )?;
        let mut observed = ObservedState::default();
        for object in resolved {
            observed.insert(object)?;
        }
        Ok(observed)
    }
}

fn rename_schema() -> Schema {
    let field = |r#type: FieldType, required: bool| FieldSchema {
        r#type,
        required,
        nullable: !required,
        description: None,
        format: None,
        pattern: None,
    };
    let mut types = BTreeMap::new();
    types.insert(
        "dcim.site".to_string(),
        TypeSchema {
            key: BTreeMap::from([("slug".to_string(), field(FieldType::String, true))]),
            fields: BTreeMap::from([("status".to_string(), field(FieldType::String, false))]),
        },
    );
    types.insert(
        "dcim.device".to_string(),
        TypeSchema {
            key: BTreeMap::from([("name".to_string(), field(FieldType::String, true))]),
            fields: BTreeMap::from([(
                "site".to_string(),
                field(
                    FieldType::Ref {
                        target: "dcim.site".to_string(),
                    },
                    false,
                ),
            )]),
        },
    );
    Schema { types }
}

/// the site renamed to `fra2` under `site_uid`, with a status the backend does
/// not hold, plus the device pointing at it.
fn renamed_inventory(site_uid: Uid) -> Inventory {
    Inventory {
        scope: None,
        schema: rename_schema(),
        objects: vec![
            obj(
                site_uid,
                "dcim.site",
                "slug=fra2",
                json!({ "status": "planned" }),
            ),
            obj(
                uid(11),
                "dcim.device",
                "name=leaf01",
                json!({ "site": site_uid.to_string() }),
            ),
        ],
    }
}

/// a state file mapping `dcim.site` backend id 1 from both uids, and the device
/// from its own.
fn write_doubled_site_state(path: &std::path::Path, first: Uid, second: Uid) {
    std::fs::write(
        path,
        format!(
            r#"{{"mappings":{{"dcim.device":{{"{}":2}},"dcim.site":{{"{first}":1,"{second}":1}}}}}}"#,
            uid(11)
        ),
    )
    .unwrap();
}

fn site_ops(plan: &Plan) -> Vec<&Op> {
    plan.ops
        .iter()
        .filter(|op| op.type_name() == &t("dcim.site"))
        .collect()
}

#[tokio::test]
async fn a_rename_stays_an_update_when_the_stale_uid_sorts_first() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("state.json");
    write_doubled_site_state(&path, uid(1), uid(900));
    let mut state = StateStore::load(&path).unwrap();

    let plan = build_plan(
        &RenameBackend,
        &renamed_inventory(uid(900)),
        &mut state,
        false,
    )
    .await
    .unwrap();
    let ops = site_ops(&plan);
    assert_eq!(ops.len(), 1, "{:?}", plan.ops);
    assert!(
        matches!(ops[0], Op::Update { uid: u, .. } if *u == uid(900)),
        "the rename must stay an update of the declared uid: {:?}",
        plan.ops
    );
    assert_eq!(
        state.uid_for_backend_id(&t("dcim.site"), &BackendId::Int(1)),
        Some(uid(900))
    );
    assert_eq!(state.all_mappings()[&t("dcim.site")].len(), 1);
}

#[tokio::test]
async fn a_rename_emits_no_delete_when_the_stale_uid_sorts_first() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("state.json");
    write_doubled_site_state(&path, uid(1), uid(900));
    let mut state = StateStore::load(&path).unwrap();

    let plan = build_plan(
        &RenameBackend,
        &renamed_inventory(uid(900)),
        &mut state,
        true,
    )
    .await
    .unwrap();
    assert!(
        !plan.ops.iter().any(|op| matches!(op, Op::Delete { .. })),
        "a rename must not delete the object it renames: {:?}",
        plan.ops
    );
}

#[tokio::test]
async fn a_rename_stays_an_update_when_the_stale_uid_sorts_last() {
    // the other arm of the same tiebreak: which uid sorts first decides nothing.
    let dir = tempdir().unwrap();
    let path = dir.path().join("state.json");
    write_doubled_site_state(&path, uid(1), uid(900));
    let mut state = StateStore::load(&path).unwrap();

    let plan = build_plan(&RenameBackend, &renamed_inventory(uid(1)), &mut state, true)
        .await
        .unwrap();
    let ops = site_ops(&plan);
    assert_eq!(ops.len(), 1, "{:?}", plan.ops);
    assert!(
        matches!(ops[0], Op::Update { uid: u, .. } if *u == uid(1)),
        "{:?}",
        plan.ops
    );
}

#[tokio::test]
async fn plan_converges_when_the_stale_uid_sorts_before_the_declared_uid() {
    // the arm `plan_converges_once_the_desired_set_claims_new_uids` cannot reach:
    // the uid state answers with at load is not the one the inventory claims.
    let dir = tempdir().unwrap();
    let path = dir.path().join("state.json");
    write_doubled_site_state(&path, uid(1), uid(900));
    let mut state = StateStore::load(&path).unwrap();

    let inventory = ref_backend_inventory(uid(900), uid(11));
    let first = build_plan(&RefBackend, &inventory, &mut state, false)
        .await
        .unwrap();
    assert_eq!(first.ops.len(), 1, "{:?}", first.ops);
    for round in 1..4 {
        let plan = build_plan(&RefBackend, &inventory, &mut state, false)
            .await
            .unwrap();
        assert!(plan.ops.is_empty(), "round {round}: {:?}", plan.ops);
    }
    assert!(backend_ids_are_unambiguous(&state));
    assert_eq!(
        state.uid_for_backend_id(&t("dcim.site"), &BackendId::Int(1)),
        Some(uid(900))
    );
    assert_eq!(state.all_mappings()[&t("dcim.site")].len(), 1);
}

#[tokio::test]
async fn the_inventory_decides_which_uid_answers_when_it_declares_both() {
    // an inventory declaring both uids contradicts itself: one backend object
    // cannot be two objects. the one declared last owns the backend id, the other
    // is planned by key like any object state does not map.
    let dir = tempdir().unwrap();
    let path = dir.path().join("state.json");
    write_doubled_site_state(&path, uid(1), uid(900));
    let mut state = StateStore::load(&path).unwrap();

    let inventory = Inventory {
        scope: None,
        schema: rename_schema(),
        objects: vec![
            obj(
                uid(1),
                "dcim.site",
                "slug=fra1",
                json!({ "status": "active" }),
            ),
            obj(
                uid(900),
                "dcim.site",
                "slug=fra2",
                json!({ "status": "planned" }),
            ),
        ],
    };
    build_plan(&RenameBackend, &inventory, &mut state, false)
        .await
        .unwrap();
    assert_eq!(
        state.uid_for_backend_id(&t("dcim.site"), &BackendId::Int(1)),
        Some(uid(900))
    );
    assert_eq!(state.all_mappings()[&t("dcim.site")].len(), 1);
}

/// a single-site observer for the adoption-visibility tests: one dcim.site,
/// key slug=fra1, backend id 5, attrs matching `adoption_inventory`.
struct AdoptionBackend;

#[async_trait::async_trait]
impl Observer for AdoptionBackend {
    async fn read(
        &self,
        _schema: &Schema,
        _types: &[TypeName],
        _state: &StateStore,
        _scope: &crate::state::ReadScope,
    ) -> anyhow::Result<ObservedState> {
        let mut observed = ObservedState::default();
        observed.insert(ObservedObject {
            type_name: t("dcim.site"),
            key: key_str("slug=fra1"),
            attrs: attrs_map(json!({ "status": "active" })),
            backend_id: Some(BackendId::Int(5)),
        })?;
        Ok(observed)
    }
}

fn adoption_inventory(site_uid: Uid) -> Inventory {
    Inventory {
        scope: None,
        schema: rename_schema(),
        objects: vec![obj(
            site_uid,
            "dcim.site",
            "slug=fra1",
            json!({ "status": "active" }),
        )],
    }
}

/// brownfield adoption is a semantic event: the run binds identity and says so,
/// and the binding it persists is the one it reported.
#[tokio::test]
async fn a_key_adoption_is_reported_alongside_the_binding_it_writes() {
    let mut state = StateStore::new(None, StateData::default());
    let inventory = adoption_inventory(uid(1));
    let (plan, bootstrap) =
        crate::build_plan(&AdoptionBackend, &inventory, &mut state, false, true)
            .await
            .unwrap();
    assert!(plan.ops.is_empty(), "converged adoption plans nothing");
    assert_eq!(bootstrap.adoptions.len(), 1);
    let adoption = &bootstrap.adoptions[0];
    assert_eq!(adoption.type_name, t("dcim.site"));
    assert_eq!(adoption.uid, uid(1));
    assert_eq!(adoption.backend_id, BackendId::Int(5));
    assert_eq!(
        state.backend_id(t("dcim.site"), uid(1)),
        Some(BackendId::Int(5)),
        "the reported adoption is the binding that was written"
    );
}

/// --no-adopt: state-known objects still match; nothing new is adopted, so an
/// unknown declared object plans as a create and identity memory stays empty.
#[tokio::test]
async fn no_adopt_plans_unknown_objects_as_creates() {
    let mut state = StateStore::new(None, StateData::default());
    let inventory = adoption_inventory(uid(1));
    let (plan, bootstrap) =
        crate::build_plan(&AdoptionBackend, &inventory, &mut state, false, false)
            .await
            .unwrap();
    assert!(bootstrap.is_empty());
    assert_eq!(plan.ops.len(), 1);
    assert!(matches!(plan.ops[0], Op::Create { .. }));
    assert_eq!(state.backend_id(t("dcim.site"), uid(1)), None);

    // a state-known object still matches without adoption.
    let mut warm = StateStore::new(None, StateData::default());
    warm.set_backend_id(t("dcim.site"), uid(1), BackendId::Int(5));
    let (plan, bootstrap) =
        crate::build_plan(&AdoptionBackend, &inventory, &mut warm, false, false)
            .await
            .unwrap();
    assert!(bootstrap.is_empty());
    assert!(plan.ops.is_empty(), "state-known objects still converge");
}

/// adopting an object another uid used to answer for supersedes that binding,
/// and the supersede is reported next to the adoption.
#[tokio::test]
async fn a_superseding_adoption_reports_the_displaced_uid() {
    let mut state = StateStore::new(None, StateData::default());
    state.set_backend_id(t("dcim.site"), uid(9), BackendId::Int(5));
    let inventory = adoption_inventory(uid(1));
    let (_, bootstrap) = crate::build_plan(&AdoptionBackend, &inventory, &mut state, false, true)
        .await
        .unwrap();
    assert_eq!(bootstrap.adoptions.len(), 1);
    assert_eq!(bootstrap.superseded.len(), 1);
    let superseded = &bootstrap.superseded[0];
    assert_eq!(superseded.superseded, uid(9));
    assert_eq!(superseded.by, uid(1));
    assert_eq!(
        state.backend_id(t("dcim.site"), uid(9)),
        None,
        "the displaced uid lost its binding"
    );
}

/// identity is the uid alone: declaring an object under a new type keeps its
/// uid and re-materializes it, so the plan is a create and a delete carrying
/// one uid, with the create ordered first by `sort_ops_for_apply`.
#[tokio::test]
async fn a_retype_plans_create_and_delete_under_one_uid() {
    let mut state = StateStore::new(None, StateData::default());
    state.set_backend_id(t("dcim.site"), uid(1), BackendId::Int(5));
    let mut inventory = adoption_inventory(uid(1));
    // rename_schema declares dcim.site only; add the target vocabulary.
    let site_schema = inventory.schema.types["dcim.site"].clone();
    inventory
        .schema
        .types
        .insert("location.site".to_string(), site_schema);
    inventory.objects = vec![obj(
        uid(1),
        "location.site",
        "slug=fra1",
        json!({ "status": "active" }),
    )];

    let (plan, _) = crate::build_plan(&AdoptionBackend, &inventory, &mut state, true, true)
        .await
        .unwrap();
    let mut kinds: Vec<_> = plan
        .ops
        .iter()
        .map(|op| (op.uid(), op.type_name().as_str().to_string()))
        .collect();
    kinds.sort();
    assert_eq!(plan.ops.len(), 2, "{:?}", plan.ops);
    assert!(
        plan.ops.iter().all(|op| op.uid() == uid(1)),
        "one logical object: {kinds:?}"
    );

    let ordered = sort_ops_for_apply(&plan.ops, &plan.schema);
    assert!(matches!(ordered[0], Op::Create { .. }));
    assert!(matches!(ordered[1], Op::Delete { .. }));
}

/// deletion addresses objects by backend id, so unmanaged same-key twins are
/// deletable without ever being told apart by key: both plan as deletes, each
/// carrying its own id, and drift lists both as extra.
#[tokio::test]
async fn unmanaged_twins_both_plan_as_deletes_by_id() {
    struct TwinAdapter;

    #[async_trait::async_trait]
    impl Observer for TwinAdapter {
        async fn read(
            &self,
            _schema: &alembic_core::Schema,
            _types: &[TypeName],
            _state: &StateStore,
            _scope: &crate::state::ReadScope,
        ) -> anyhow::Result<ObservedState> {
            let mut observed = ObservedState::default();
            for id in [7u64, 9] {
                observed.insert(ObservedObject {
                    type_name: t("dcim.site"),
                    key: key_str("site=dup"),
                    attrs: attrs_map(json!({ "name": "Ghost", "slug": "dup" })),
                    backend_id: Some(BackendId::Int(id)),
                })?;
            }
            Ok(observed)
        }
    }

    let desired = inv(vec![obj(
        uid(1),
        "dcim.site",
        "site=ams1",
        json!({ "name": "AMS1", "slug": "ams1" }),
    )]);
    let mut state = StateStore::new(None, StateData::default());
    let (plan, _) = crate::build_plan(&TwinAdapter, &desired, &mut state, true, true)
        .await
        .unwrap();

    let mut delete_ids: Vec<_> = plan
        .ops
        .iter()
        .filter_map(|op| match op {
            Op::Delete { backend_id, .. } => backend_id.clone(),
            _ => None,
        })
        .collect();
    delete_ids.sort_by_key(|id| id.to_string());
    assert_eq!(delete_ids, vec![BackendId::Int(7), BackendId::Int(9)]);

    let drift = crate::DriftReport::from_plan(&plan);
    assert_eq!(drift.extra.len(), 2, "both twins surface as extra");
}

/// records the advisory scope it was handed, so a test can assert what the
/// engine asked for, and optionally honors it — a narrowing adapter and one
/// that ignores the hint must leave the engine at the same outcome.
struct ScopeRecorder {
    observed: ObservedState,
    honor: bool,
    seen: std::sync::Arc<std::sync::Mutex<Option<crate::state::ReadScope>>>,
}

impl ScopeRecorder {
    fn new(observed: ObservedState, honor: bool) -> Self {
        Self {
            observed,
            honor,
            seen: Default::default(),
        }
    }

    fn seen(&self) -> crate::state::ReadScope {
        self.seen.lock().unwrap().clone().expect("read never ran")
    }
}

#[async_trait::async_trait]
impl Observer for ScopeRecorder {
    async fn read(
        &self,
        _schema: &alembic_core::Schema,
        _types: &[TypeName],
        _state: &StateStore,
        scope: &crate::state::ReadScope,
    ) -> anyhow::Result<ObservedState> {
        *self.seen.lock().unwrap() = Some(scope.clone());
        if !self.honor {
            return Ok(self.observed.clone());
        }
        let mut narrowed = ObservedState::default();
        for object in self.observed.clone().into_objects() {
            let keep = match scope.for_type(&object.type_name) {
                None => true,
                Some(hint) => hint.wants(&object),
            };
            if keep {
                narrowed.insert(object)?;
            }
        }
        Ok(narrowed)
    }
}

#[async_trait::async_trait]
impl Emitter for ScopeRecorder {
    async fn write(
        &self,
        _schema: &alembic_core::Schema,
        _ops: &[Op],
        _state: &StateStore,
    ) -> anyhow::Result<ApplyReport> {
        Ok(ApplyReport::default())
    }
}

impl Adapter for ScopeRecorder {}

/// four backend objects, one per cell of the hint: one both halves name, one
/// only a declared key names, one the plan has no claim on at all, and one
/// whose key drifted on the backend after state bound it, which only the
/// backend-ids half still names.
fn scope_backend() -> ObservedState {
    let mut observed = ObservedState::default();
    for (key, id, name) in [
        ("site=fra1", 10u64, "OLD"),
        ("site=ber1", 11, "BER1"),
        ("site=ams1", 12, "AMS1"),
        ("site=lon1-renamed", 13, "LON1 renamed"),
    ] {
        observed
            .insert(ObservedObject {
                type_name: t("dcim.site"),
                key: key_str(key),
                attrs: attrs_map(json!({ "name": name, "slug": key })),
                backend_id: Some(BackendId::Int(id)),
            })
            .unwrap();
    }
    observed
}

fn scope_inventory() -> Inventory {
    inv(vec![
        obj(
            uid(1),
            "dcim.site",
            "site=fra1",
            json!({ "name": "FRA1", "slug": "site=fra1" }),
        ),
        obj(
            uid(2),
            "dcim.site",
            "site=ber1",
            json!({ "name": "BER1", "slug": "site=ber1" }),
        ),
        obj(
            uid(4),
            "dcim.site",
            "site=lon1",
            json!({ "name": "LON1", "slug": "site=lon1" }),
        ),
    ])
}

/// the bindings [`scope_backend`] is observed under: `uid(4)` is bound to the
/// object whose key has since drifted, so its identity survives only through
/// the backend id.
const SCOPE_BOUND: [(u128, u64); 2] = [(1, 10), (4, 13)];

fn scope_state(dir: &std::path::Path) -> StateStore {
    let mut state = StateStore::load(dir.join("state.json")).unwrap();
    for (u, id) in SCOPE_BOUND {
        state.set_backend_id(t("dcim.site"), uid(u), BackendId::Int(id));
    }
    state
}

#[test]
fn plan_hints_state_ids_and_desired_keys() {
    let dir = tempdir().unwrap();
    let adapter = ScopeRecorder::new(scope_backend(), false);
    let mut state = scope_state(dir.path());
    futures::executor::block_on(build_plan(&adapter, &scope_inventory(), &mut state, false))
        .unwrap();

    let scope = adapter.seen();
    let hint = scope.for_type(&t("dcim.site")).expect("narrowed");
    assert_eq!(
        hint.backend_ids,
        &BTreeSet::from([BackendId::Int(10), BackendId::Int(13)]),
        "state-bound ids only"
    );
    assert_eq!(
        hint.keys().cloned().collect::<Vec<_>>(),
        vec![
            key_str("site=ber1"),
            key_str("site=fra1"),
            key_str("site=lon1")
        ],
        "every declared key, ordered"
    );
}

/// a ref-keyed type's declared key is in uid space, which no backend can be
/// queried in and which the adapter's own rows only reach after
/// `resolve_ref_keyed_identity` has run over the batch it already fetched. the
/// hint holds such a type out whole rather than naming a key nothing matches.
#[test]
fn a_ref_keyed_type_is_read_whole() {
    let dir = tempdir().unwrap();
    let adapter = ScopeRecorder::new(ObservedState::default(), false);
    let mut state = StateStore::load(dir.path().join("state.json")).unwrap();
    futures::executor::block_on(build_plan(
        &adapter,
        &ref_chain_inventory(2),
        &mut state,
        false,
    ))
    .unwrap();

    let scope = adapter.seen();
    assert!(!scope.is_full(), "the run still narrows what it can");
    assert!(
        scope.for_type(&t("dcim.site")).is_some(),
        "a string-keyed type narrows"
    );
    for ref_keyed in [t("dcim.device"), t("dcim.interface")] {
        assert!(
            scope.for_type(&ref_keyed).is_none(),
            "{ref_keyed} is keyed on a ref, so it is read whole"
        );
    }
}

#[test]
fn detect_deletes_reads_unscoped() {
    let dir = tempdir().unwrap();
    let adapter = ScopeRecorder::new(scope_backend(), false);
    let mut state = scope_state(dir.path());
    futures::executor::block_on(build_plan(&adapter, &scope_inventory(), &mut state, true))
        .unwrap();

    assert!(
        adapter.seen().is_full(),
        "extra is defined against the full observation"
    );
}

#[test]
fn import_reads_unscoped() {
    let adapter = ScopeRecorder::new(scope_backend(), false);
    let dir = tempdir().unwrap();
    let state = scope_state(dir.path());
    let inventory = scope_inventory();
    futures::executor::block_on(crate::import_inventory(
        &adapter,
        &inventory.schema,
        &[t("dcim.site")],
        &state,
    ))
    .unwrap();

    assert!(adapter.seen().is_full(), "import converts whole types");
}

/// a numerically keyed type: the backend answers the vlan id as a float, which
/// is the same key to the engine and a different one to a structural compare.
fn key_num(field: &str, value: serde_json::Value) -> Key {
    Key::from(BTreeMap::from([(field.to_string(), value)]))
}

fn vlan_backend() -> ObservedState {
    let mut observed = ObservedState::default();
    observed
        .insert(ObservedObject {
            type_name: t("ipam.vlan"),
            key: key_num("vid", json!(100.0)),
            attrs: attrs_map(json!({ "name": "OLD" })),
            backend_id: Some(BackendId::Int(7)),
        })
        .unwrap();
    observed
}

fn vlan_inventory() -> Inventory {
    inv(vec![Object::new(
        uid(3),
        t("ipam.vlan"),
        key_num("vid", json!(100)),
        attrs_map(json!({ "name": "NEW" })),
    )
    .unwrap()])
}

/// constraint made executable: an adapter that narrows to exactly what the hint
/// names and one that ignores it entirely must leave plan, apply and identity
/// memory identical.
fn narrowing_changes_nothing(
    backend: &ObservedState,
    inventory: &Inventory,
    bound: &[(TypeName, Uid, BackendId)],
) {
    let dir = tempdir().unwrap();

    let mut outcomes = Vec::new();
    for (index, honor) in [(0, false), (1, true)] {
        let adapter = ScopeRecorder::new(backend.clone(), honor);
        let run = dir.path().join(index.to_string());
        std::fs::create_dir_all(&run).unwrap();
        let mut state = StateStore::load(run.join("state.json")).unwrap();
        for (type_name, uid, backend_id) in bound {
            state.set_backend_id(type_name.clone(), *uid, backend_id.clone());
        }
        let (plan, bootstrap) = futures::executor::block_on(crate::build_plan(
            &adapter, inventory, &mut state, false, true,
        ))
        .unwrap();
        let report = futures::executor::block_on(crate::apply_plan(
            &Backend::Adapter(Box::new(adapter)),
            &plan,
            &mut state,
            false,
        ))
        .unwrap();
        outcomes.push((
            plan.ops,
            bootstrap.adoptions,
            report,
            state.all_mappings().clone(),
        ));
    }

    let honored = outcomes.pop().unwrap();
    let ignored = outcomes.pop().unwrap();
    assert_eq!(ignored.0, honored.0, "plans diverge");
    assert_eq!(ignored.1, honored.1, "adoptions diverge");
    assert_eq!(
        format!("{:?}", ignored.2),
        format!("{:?}", honored.2),
        "apply reports diverge"
    );
    assert_eq!(ignored.3, honored.3, "identity memory diverges");
    assert!(
        !honored.1.is_empty(),
        "the fixture must exercise adoption by key"
    );
}

#[test]
fn narrowing_to_the_hint_changes_nothing() {
    let bound: Vec<_> = SCOPE_BOUND
        .iter()
        .map(|(u, id)| (t("dcim.site"), uid(*u), BackendId::Int(*id)))
        .collect();
    narrowing_changes_nothing(&scope_backend(), &scope_inventory(), &bound);
}

/// the same constraint over a numerically keyed type, where the backend answers
/// `100.0` and the inventory declares `100`. the engine matches on `key_string`
/// and adopts across that; a filter comparing the hint's keys structurally does
/// not, drops the object, and plans a create against one that exists.
#[test]
fn narrowing_to_the_hint_changes_nothing_for_a_numeric_key() {
    narrowing_changes_nothing(&vlan_backend(), &vlan_inventory(), &[]);
}

#[test]
fn a_narrow_hint_naming_nothing_is_not_a_full_read() {
    let empty = crate::state::ReadScope::Narrowed {
        backend_ids: Default::default(),
        keys: Default::default(),
        unnarrowed: Default::default(),
    };
    assert!(!empty.is_full());
    let hint = empty.for_type(&t("dcim.site")).expect("narrowed, not full");
    assert!(hint.is_empty());
    assert!(crate::state::ReadScope::Full
        .for_type(&t("dcim.site"))
        .is_none());
}
