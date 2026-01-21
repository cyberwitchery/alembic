use super::*;
use crate::project_default;
use alembic_core::{Inventory, JsonMap, Object, TypeName, Uid};
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

fn obj(uid: Uid, type_name: &str, key: &str, attrs: serde_json::Value) -> Object {
    Object::new(uid, t(type_name), key.to_string(), attrs_map(attrs)).unwrap()
}

fn inv(objects: Vec<Object>) -> Inventory {
    Inventory {
        schema: None,
        objects,
    }
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
    key: "site=child"
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
objects:
  - uid: "00000000-0000-0000-0000-000000000002"
    type: dcim.site
    key: "site=base"
    attrs:
      name: "Base"
      slug: "base"
"#,
            child.file_name().unwrap().to_str().unwrap()
        ),
    )
    .unwrap();

    let inventory = load_brew(&base).unwrap();
    assert_eq!(inventory.objects.len(), 2);
}

#[test]
fn load_json_brew() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("brew.json");
    std::fs::write(
        &path,
        r#"{ "objects": [ { "uid": "00000000-0000-0000-0000-000000000010", "type": "dcim.site", "key": "site=fra1", "attrs": { "name": "FRA1", "slug": "fra1" } } ] }"#,
    )
    .unwrap();

    let inventory = load_brew(&path).unwrap();
    assert_eq!(inventory.objects.len(), 1);
}

#[test]
fn load_generic_kind_as_generic_attrs() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("generic.yaml");
    std::fs::write(
        &path,
        r#"objects:
  - uid: "00000000-0000-0000-0000-000000000010"
    type: services.vpn
    key: "vpn=corp"
    attrs:
      peers:
        - name: site1
          ip: 10.0.0.1
      pre_shared_key: "secret"
  - uid: "00000000-0000-0000-0000-000000000011"
    type: dcim.site
    key: "site=fra1"
    attrs:
      name: "FRA1"
      slug: "fra1"
"#,
    )
    .unwrap();

    let inventory = load_brew(&path).unwrap();
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
        r#"objects:
  - uid: "00000000-0000-0000-0000-000000000020"
    type: dcim.site
    key: "site=a"
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
    key: "site=b"
    attrs:
      name: "B"
      slug: "b"
"#,
            a.file_name().unwrap().to_str().unwrap()
        ),
    )
    .unwrap();

    let inventory = load_brew(&b).unwrap();
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
objects:
  - uid: "00000000-0000-0000-0000-000000000030"
    type: dcim.site
    key: "site=a"
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
    key: "site=b"
    attrs:
      name: "B"
      slug: "b"
"#,
            a.file_name().unwrap().to_str().unwrap()
        ),
    )
    .unwrap();

    let inventory = load_brew(&a).unwrap();
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

    let err = load_brew(&base).unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("load brew") || message.contains("read brew"),
        "unexpected error: {message}"
    );
}

#[test]
fn load_errors_on_invalid_yaml() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("invalid.yaml");
    std::fs::write(&path, "objects: [").unwrap();

    let err = load_brew(&path).unwrap_err();
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
    let inventory = Inventory {
        schema: None,
        objects,
    };
    let result = validate(&inventory);
    assert!(result.is_err());
}

#[test]
fn detects_missing_references() {
    let objects = vec![obj(
        uid(2),
        "dcim.interface",
        "device=leaf01/interface=eth0",
        json!({
            "name": "eth0",
            "device": uid(3).to_string(),
            "if_type": "1000base-t",
            "enabled": true
        }),
    )];
    let inventory = Inventory {
        schema: Some(alembic_core::Schema {
            types: BTreeMap::from([(
                "dcim.interface".to_string(),
                alembic_core::TypeSchema {
                    fields: BTreeMap::from([(
                        "device".to_string(),
                        alembic_core::FieldSchema {
                            r#type: alembic_core::FieldType::Ref {
                                target: "dcim.device".to_string(),
                            },
                            required: true,
                            nullable: false,
                            description: None,
                        },
                    )]),
                },
            )]),
        }),
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
            "site=fra1/device=leaf01",
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

    let inventory = Inventory {
        schema: None,
        objects,
    };
    let observed = ObservedState::default();
    let state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let projected = project_default(&inventory.objects);
    let plan = plan(&projected, &observed, &state, false);

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
        key: "site=fra1".to_string(),
        attrs: attrs_map(json!({ "name": "OLD", "slug": "fra1" })),
        projection: crate::ProjectionData::default(),
        backend_id: Some(100),
    });

    let state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let projected = project_default(&desired.objects);
    let plan = plan(&projected, &observed, &state, false);

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
        "vpn=corp".to_string(),
        to.into(),
    )
    .unwrap()]);

    let mut observed = ObservedState::default();
    observed.insert(ObservedObject {
        type_name: t("services.vpn"),
        key: "vpn=corp".to_string(),
        attrs: from.into(),
        projection: crate::ProjectionData::default(),
        backend_id: Some(10),
    });

    let state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let projected = project_default(&desired.objects);
    let plan = plan(&projected, &observed, &state, false);

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
fn planner_includes_projected_custom_fields() {
    let mut object = obj(
        uid(70),
        "dcim.site",
        "site=fra1",
        json!({ "name": "FRA1", "slug": "fra1" }),
    );
    object
        .attrs
        .insert("model.fabric".to_string(), json!("fra1"));

    let spec: crate::ProjectionSpec = serde_yaml::from_str(
        r#"
version: 1
backend: netbox
rules:
  - name: cf
    on_type: dcim.site
    from_attrs:
      prefix: "model."
    to:
      custom_fields:
        strategy: strip_prefix
        prefix: "model."
"#,
    )
    .unwrap();

    let projected = crate::apply_projection(&spec, &[object]).unwrap();

    let mut observed = ObservedState::default();
    let mut fields = BTreeMap::new();
    fields.insert("fabric".to_string(), json!("old"));
    observed.insert(ObservedObject {
        type_name: t("dcim.site"),
        key: "site=fra1".to_string(),
        attrs: attrs_map(json!({ "name": "FRA1", "slug": "fra1" })),
        projection: crate::ProjectionData {
            custom_fields: Some(fields),
            tags: None,
            local_context: None,
        },
        backend_id: None,
    });

    let state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = plan(&projected, &observed, &state, false);
    let changes = match &plan.ops[0] {
        Op::Update { changes, .. } => changes,
        _ => panic!("expected update"),
    };
    assert!(changes.iter().any(|change| change.field == "custom_fields"));
}

#[test]
fn planner_ignores_optional_nulls() {
    let desired = obj(
        uid(80),
        "dcim.site",
        "site=fra1",
        json!({ "name": "FRA1", "slug": "fra1" }),
    );
    let projected = project_default(std::slice::from_ref(&desired));

    let mut observed = ObservedState::default();
    observed.insert(ObservedObject {
        type_name: t("dcim.site"),
        key: "site=fra1".to_string(),
        attrs: attrs_map(json!({
            "name": "FRA1",
            "slug": "fra1",
            "status": "active",
            "description": ""
        })),
        projection: crate::ProjectionData::default(),
        backend_id: Some(1),
    });

    let state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = plan(&projected, &observed, &state, false);
    assert!(plan.ops.is_empty());
}

#[test]
fn planner_ignores_unprojected_custom_fields() {
    let desired = obj(
        uid(81),
        "dcim.site",
        "site=fra1",
        json!({ "name": "FRA1", "slug": "fra1" }),
    );
    let mut desired_fields = BTreeMap::new();
    desired_fields.insert("fabric".to_string(), json!("fra1"));
    let projected = ProjectedInventory {
        objects: vec![ProjectedObject {
            base: desired.clone(),
            projection: crate::ProjectionData {
                custom_fields: Some(desired_fields),
                tags: None,
                local_context: None,
            },
            projection_inputs: BTreeSet::new(),
        }],
    };

    let mut observed_fields = BTreeMap::new();
    observed_fields.insert("fabric".to_string(), json!("fra1"));
    observed_fields.insert("extra".to_string(), json!("ignored"));
    let mut observed = ObservedState::default();
    observed.insert(ObservedObject {
        type_name: t("dcim.site"),
        key: "site=fra1".to_string(),
        attrs: desired.attrs.clone(),
        projection: crate::ProjectionData {
            custom_fields: Some(observed_fields),
            tags: None,
            local_context: None,
        },
        backend_id: Some(1),
    });

    let state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = plan(&projected, &observed, &state, false);
    assert!(plan.ops.is_empty());
}

#[test]
fn planner_matches_backend_id_by_kind() {
    let desired = obj(
        uid(82),
        "dcim.device",
        "site=fra1/device=leaf01",
        json!({
            "name": "leaf01",
            "site": uid(1).to_string(),
            "role": "leaf",
            "device_type": "leaf"
        }),
    );
    let projected = project_default(std::slice::from_ref(&desired));

    let mut observed = ObservedState::default();
    observed.insert(ObservedObject {
        type_name: t("dcim.device"),
        key: "site=fra1/device=leaf01".to_string(),
        attrs: desired.attrs.clone(),
        projection: crate::ProjectionData::default(),
        backend_id: Some(1),
    });
    observed.insert(ObservedObject {
        type_name: t("dcim.interface"),
        key: "device=leaf01/interface=eth0".to_string(),
        attrs: attrs_map(json!({
            "name": "eth0",
            "device": uid(82).to_string()
        })),
        projection: crate::ProjectionData::default(),
        backend_id: Some(1),
    });

    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    state.set_backend_id(t("dcim.device"), desired.uid, 1);
    let plan = plan(&projected, &observed, &state, false);
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
    let projected = project_default(std::slice::from_ref(&desired));

    let mut observed = ObservedState::default();
    observed.insert(ObservedObject {
        type_name: t("ipam.prefix"),
        key: "prefix=10.0.0.0/24".to_string(),
        attrs: attrs_map(json!({ "prefix": "10.0.0.0/24" })),
        projection: crate::ProjectionData::default(),
        backend_id: Some(1),
    });

    let state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = plan(&projected, &observed, &state, false);
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
    store.set_backend_id(t("dcim.site"), uid(99), 123);
    store.save().unwrap();

    let reloaded = StateStore::load(&path).unwrap();
    assert_eq!(reloaded.backend_id(t("dcim.site"), uid(99)), Some(123));
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
    store.save().unwrap();
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
    let err = store.save().unwrap_err();
    assert!(err.to_string().contains("create state dir"));
}

#[test]
fn plan_generates_deletes_when_enabled() {
    let desired = inv(vec![]);
    let mut observed = ObservedState::default();
    observed.insert(ObservedObject {
        type_name: t("dcim.site"),
        key: "site=orphan".to_string(),
        attrs: attrs_map(json!({ "name": "orphan", "slug": "orphan" })),
        projection: crate::ProjectionData::default(),
        backend_id: Some(10),
    });

    let state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let projected = project_default(&desired.objects);
    let plan = plan(&projected, &observed, &state, true);
    assert!(plan.ops.iter().any(|op| matches!(op, Op::Delete { .. })));
}

#[test]
fn apply_order_puts_deletes_last() {
    let ops = vec![
        Op::Delete {
            uid: uid(1),
            type_name: t("dcim.device"),
            key: "site=fra1/device=leaf01".to_string(),
            backend_id: Some(2),
        },
        Op::Create {
            uid: uid(2),
            type_name: t("dcim.site"),
            desired: crate::ProjectedObject {
                base: obj(
                    uid(2),
                    "dcim.site",
                    "site=fra1",
                    json!({ "name": "FRA1", "slug": "fra1" }),
                ),
                projection: crate::ProjectionData::default(),
                projection_inputs: BTreeSet::new(),
            },
        },
    ];

    let ordered = sort_ops_for_apply(&ops);
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
    async fn observe(&self, _types: &[TypeName]) -> anyhow::Result<ObservedState> {
        Ok(self.observed.clone())
    }

    async fn apply(&self, _ops: &[Op]) -> anyhow::Result<ApplyReport> {
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
        report: ApplyReport { applied: vec![] },
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
        key: "site=fra1".to_string(),
        attrs: attrs_map(json!({ "name": "FRA1", "slug": "fra1" })),
        projection: crate::ProjectionData::default(),
        backend_id: Some(10),
    });
    let adapter = TestAdapter {
        observed,
        report: ApplyReport { applied: vec![] },
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan =
        futures::executor::block_on(build_plan(&adapter, &inventory, &mut state, false)).unwrap();
    assert!(plan.ops.is_empty());
    assert_eq!(state.backend_id(t("dcim.site"), uid(1)), Some(10));
}

#[test]
fn build_plan_reobserves_after_bootstrap() {
    #[derive(Clone)]
    struct ReobserveAdapter {
        states: std::sync::Arc<std::sync::Mutex<Vec<ObservedState>>>,
    }

    #[async_trait::async_trait]
    impl Adapter for ReobserveAdapter {
        async fn observe(&self, _types: &[TypeName]) -> anyhow::Result<ObservedState> {
            let mut states = self.states.lock().unwrap();
            Ok(states.remove(0))
        }

        async fn apply(&self, _ops: &[Op]) -> anyhow::Result<ApplyReport> {
            Ok(ApplyReport { applied: vec![] })
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
        key: "site=fra1".to_string(),
        attrs: attrs_map(json!({ "name": "FRA1", "slug": "fra1" })),
        projection: crate::ProjectionData::default(),
        backend_id: Some(1),
    });
    let mut second = first.clone();
    second.capabilities = crate::BackendCapabilities::default();

    let adapter = ReobserveAdapter {
        states: std::sync::Arc::new(std::sync::Mutex::new(vec![first, second])),
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan =
        futures::executor::block_on(build_plan(&adapter, &inventory, &mut state, false)).unwrap();
    assert!(plan.ops.is_empty());
}

#[test]
fn apply_plan_blocks_deletes_without_flag() {
    let adapter = TestAdapter {
        observed: ObservedState::default(),
        report: ApplyReport { applied: vec![] },
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = Plan {
        ops: vec![Op::Delete {
            uid: uid(1),
            type_name: t("dcim.site"),
            key: "site=fra1".to_string(),
            backend_id: Some(1),
        }],
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
                backend_id: Some(55),
            }],
        },
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = Plan { ops: vec![] };
    futures::executor::block_on(apply_plan(&adapter, &plan, &mut state, true)).unwrap();
    assert_eq!(state.backend_id(t("dcim.site"), uid(1)), Some(55));
}
