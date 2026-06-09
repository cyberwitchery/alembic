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
    observed.insert(ObservedObject {
        type_name: t("dcim.site"),
        key: key_str("site=fra1"),
        attrs: attrs_map(json!({ "name": "OLD", "slug": "fra1" })),
        backend_id: Some(BackendId::Int(100)),
    });

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
    observed.insert(ObservedObject {
        type_name: t("services.vpn"),
        key: key_str("vpn=corp"),
        attrs: from.into(),
        backend_id: Some(BackendId::Int(10)),
    });

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
    observed.insert(ObservedObject {
        type_name: t("dcim.site"),
        key: key_str("site=fra1"),
        attrs: attrs_map(json!({
            "name": "FRA1",
            "slug": "fra1",
            "status": "active",
            "description": ""
        })),
        backend_id: Some(BackendId::Int(1)),
    });

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
    observed.insert(ObservedObject {
        type_name: t("dcim.device"),
        key: key_str("site=fra1/device=leaf01"),
        attrs: desired.attrs.clone(),
        backend_id: Some(BackendId::Int(1)),
    });
    observed.insert(ObservedObject {
        type_name: t("dcim.interface"),
        key: key_str("device=leaf01;interface=eth0"),
        attrs: attrs_map(json!({
            "name": "eth0",
            "device": uid(82).to_string()
        })),
        backend_id: Some(BackendId::Int(1)),
    });

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
    observed.insert(ObservedObject {
        type_name: t("ipam.prefix"),
        key: key_str("prefix=10.0.0.0/24"),
        attrs: attrs_map(json!({ "prefix": "10.0.0.0/24" })),
        backend_id: Some(BackendId::Int(1)),
    });

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
fn state_store_roundtrip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("state.json");
    let mut store = StateStore::load(&path).unwrap();
    store.set_backend_id(t("dcim.site"), uid(99), BackendId::Int(123));
    futures::executor::block_on(store.save_async()).unwrap();

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
fn state_store_save_errors_on_bad_parent() {
    let dir = tempdir().unwrap();
    let blocking_parent = dir.path().join("state.json");
    std::fs::write(&blocking_parent, "file").unwrap();
    let path = blocking_parent.join("child.json");
    let store = StateStore::load(&path).unwrap();
    let err = futures::executor::block_on(store.save_async()).unwrap_err();
    assert!(err.to_string().contains("create state dir"));
}

#[tokio::test]
async fn state_store_async_roundtrip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("async_state.json");
    let mut store = StateStore::load(&path).unwrap();
    store.set_backend_id(t("dcim.site"), uid(100), BackendId::Int(456));
    store.save_async().await.unwrap();

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
async fn state_store_postgres_prevent_double_save_when_configured() {
    let Ok(url) = std::env::var("ALEMBIC_TEST_POSTGRES_URL") else {
        return;
    };
    let key = format!("alembic-test-{}", Uuid::new_v4());

    let store = StateStore::load_postgres(url.clone(), key.clone(), PostgresTlsMode::Disable)
        .await
        .unwrap();
    store.save_async().await.unwrap();
    store
        .save_async()
        .await
        .expect_err("saving twice should fail");
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

    // Another client connects to the database
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
    // Should succeed without error even with no backend
    store.load_async().await.unwrap();
}

#[tokio::test]
async fn state_store_save_async_no_backend() {
    let store = StateStore::new(None, StateData::default());
    // Should succeed without error even with no backend
    store.save_async().await.unwrap();
}

#[test]
fn state_store_new_without_backend() {
    let data = StateData::default();
    let store = StateStore::new(None, data);
    assert!(store.all_mappings().is_empty());
    // save_async should succeed with no backend
    futures::executor::block_on(store.save_async()).unwrap();
}

#[test]
fn plan_generates_deletes_when_enabled() {
    let desired = inv(vec![]);
    let mut observed = ObservedState::default();
    observed.insert(ObservedObject {
        type_name: t("dcim.site"),
        key: key_str("site=orphan"),
        attrs: attrs_map(json!({ "name": "orphan", "slug": "orphan" })),
        backend_id: Some(BackendId::Int(10)),
    });

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
impl Adapter for TestAdapter {
    async fn read(
        &self,
        _schema: &alembic_core::Schema,
        _types: &[TypeName],
        _state: &StateStore,
    ) -> anyhow::Result<ObservedState> {
        Ok(self.observed.clone())
    }

    async fn write(
        &self,
        _schema: &alembic_core::Schema,
        _ops: &[Op],
        _state: &StateStore,
    ) -> anyhow::Result<ApplyReport> {
        Ok(self.report.clone())
    }
}

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
fn build_plan_bootstraps_state_by_key() {
    let inventory = inv(vec![obj(
        uid(1),
        "dcim.site",
        "site=fra1",
        json!({ "name": "FRA1", "slug": "fra1" }),
    )]);
    let mut observed = ObservedState::default();
    observed.insert(ObservedObject {
        type_name: t("dcim.site"),
        key: key_str("site=fra1"),
        attrs: attrs_map(json!({ "name": "FRA1", "slug": "fra1" })),
        backend_id: Some(BackendId::Int(10)),
    });
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
fn build_plan_reobserves_after_bootstrap() {
    #[derive(Clone)]
    struct ReobserveAdapter {
        states: std::sync::Arc<std::sync::Mutex<Vec<ObservedState>>>,
    }

    #[async_trait::async_trait]
    impl Adapter for ReobserveAdapter {
        async fn read(
            &self,
            _schema: &alembic_core::Schema,
            _types: &[TypeName],
            _state: &StateStore,
        ) -> anyhow::Result<ObservedState> {
            let mut states = self.states.lock().unwrap();
            Ok(states.remove(0))
        }

        async fn write(
            &self,
            _schema: &alembic_core::Schema,
            _ops: &[Op],
            _state: &StateStore,
        ) -> anyhow::Result<ApplyReport> {
            Ok(ApplyReport::default())
        }
    }

    let inventory = inv(vec![obj(
        uid(1),
        "dcim.site",
        "site=fra1",
        json!({ "name": "FRA1", "slug": "fra1" }),
    )]);
    let mut first = ObservedState::default();
    first.insert(ObservedObject {
        type_name: t("dcim.site"),
        key: key_str("site=fra1"),
        attrs: attrs_map(json!({ "name": "FRA1", "slug": "fra1" })),
        backend_id: Some(BackendId::Int(1)),
    });
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
    impl Adapter for ScopeAdapter {
        async fn read(
            &self,
            _schema: &alembic_core::Schema,
            types: &[TypeName],
            _state: &StateStore,
        ) -> anyhow::Result<ObservedState> {
            *self.seen.lock().unwrap() = types.to_vec();
            Ok(ObservedState::default())
        }

        async fn write(
            &self,
            _schema: &alembic_core::Schema,
            _ops: &[Op],
            _state: &StateStore,
        ) -> anyhow::Result<ApplyReport> {
            Ok(ApplyReport::default())
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
    };
    let result = futures::executor::block_on(apply_plan(&adapter, &plan, &mut state, false));
    assert!(result.is_err());
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
    };
    futures::executor::block_on(apply_plan(&adapter, &plan, &mut state, true)).unwrap();
    assert_eq!(
        state.backend_id(t("dcim.site"), uid(1)),
        Some(BackendId::Int(55))
    );
}
