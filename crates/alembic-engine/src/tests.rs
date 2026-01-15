use super::*;
use alembic_core::{Attrs, DeviceAttrs, InterfaceAttrs, Inventory, Kind, Object, SiteAttrs, Uid};
use tempfile::tempdir;
use uuid::Uuid;

fn uid(n: u128) -> Uid {
    Uuid::from_u128(n)
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
    kind: dcim.site
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
    kind: dcim.site
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
        r#"{ "objects": [ { "uid": "00000000-0000-0000-0000-000000000010", "kind": "dcim.site", "key": "site=fra1", "attrs": { "name": "FRA1", "slug": "fra1" } } ] }"#,
    )
    .unwrap();

    let inventory = load_brew(&path).unwrap();
    assert_eq!(inventory.objects.len(), 1);
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
    kind: dcim.site
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
    kind: dcim.site
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
    kind: dcim.site
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
    kind: dcim.site
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
        Object::new(
            uid(1),
            "site=a".to_string(),
            Attrs::Site(SiteAttrs {
                name: "A".to_string(),
                slug: "a".to_string(),
                status: None,
                description: None,
            }),
        ),
        Object::new(
            uid(1),
            "site=b".to_string(),
            Attrs::Site(SiteAttrs {
                name: "B".to_string(),
                slug: "b".to_string(),
                status: None,
                description: None,
            }),
        ),
    ];
    let inventory = Inventory { objects };
    let result = validate(&inventory);
    assert!(result.is_err());
}

#[test]
fn detects_missing_references() {
    let objects = vec![Object::new(
        uid(2),
        "device=leaf01/interface=eth0".to_string(),
        Attrs::Interface(InterfaceAttrs {
            name: "eth0".to_string(),
            device: uid(3),
            if_type: Some("1000base-t".to_string()),
            enabled: Some(true),
            description: None,
        }),
    )];
    let inventory = Inventory { objects };
    let result = validate(&inventory);
    assert!(result.is_err());
}

#[test]
fn plans_in_stable_order() {
    let site_uid = uid(10);
    let device_uid = uid(11);
    let objects = vec![
        Object::new(
            device_uid,
            "site=fra1/device=leaf01".to_string(),
            Attrs::Device(DeviceAttrs {
                name: "leaf01".to_string(),
                site: site_uid,
                role: "leaf".to_string(),
                device_type: "leaf-switch".to_string(),
                status: None,
            }),
        ),
        Object::new(
            site_uid,
            "site=fra1".to_string(),
            Attrs::Site(SiteAttrs {
                name: "FRA1".to_string(),
                slug: "fra1".to_string(),
                status: None,
                description: None,
            }),
        ),
    ];

    let inventory = Inventory { objects };
    let observed = ObservedState::default();
    let state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = plan(&inventory, &observed, &state, false);

    assert_eq!(plan.ops.len(), 2);
    let kinds: Vec<Kind> = plan
        .ops
        .iter()
        .map(|op| match op {
            Op::Create { kind, .. } => *kind,
            _ => panic!("unexpected op"),
        })
        .collect();

    assert_eq!(kinds, vec![Kind::DcimSite, Kind::DcimDevice]);
}

#[test]
fn detects_attribute_diff() {
    let uid = uid(20);
    let desired = Inventory {
        objects: vec![Object::new(
            uid,
            "site=fra1".to_string(),
            Attrs::Site(SiteAttrs {
                name: "FRA1".to_string(),
                slug: "fra1".to_string(),
                status: None,
                description: None,
            }),
        )],
    };

    let mut observed = ObservedState::default();
    observed.insert(ObservedObject {
        kind: Kind::DcimSite,
        key: "site=fra1".to_string(),
        attrs: Attrs::Site(SiteAttrs {
            name: "OLD".to_string(),
            slug: "fra1".to_string(),
            status: None,
            description: None,
        }),
        backend_id: Some(100),
    });

    let state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = plan(&desired, &observed, &state, false);

    assert_eq!(plan.ops.len(), 1);
    match &plan.ops[0] {
        Op::Update { changes, .. } => {
            assert!(changes.iter().any(|c| c.field == "name"));
        }
        _ => panic!("expected update"),
    }
}

#[test]
fn state_store_roundtrip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("state.json");
    let mut store = StateStore::load(&path).unwrap();
    store.set_backend_id(Kind::DcimSite, uid(99), 123);
    store.save().unwrap();

    let reloaded = StateStore::load(&path).unwrap();
    assert_eq!(reloaded.backend_id(Kind::DcimSite, uid(99)), Some(123));
    assert!(reloaded.all_mappings().contains_key(&Kind::DcimSite));

    let mut reloaded = reloaded;
    reloaded.remove_backend_id(Kind::DcimSite, uid(99));
    assert_eq!(reloaded.backend_id(Kind::DcimSite, uid(99)), None);
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
    let desired = Inventory { objects: vec![] };
    let mut observed = ObservedState::default();
    observed.insert(ObservedObject {
        kind: Kind::DcimSite,
        key: "site=orphan".to_string(),
        attrs: Attrs::Site(SiteAttrs {
            name: "orphan".to_string(),
            slug: "orphan".to_string(),
            status: None,
            description: None,
        }),
        backend_id: Some(10),
    });

    let state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = plan(&desired, &observed, &state, true);
    assert!(plan.ops.iter().any(|op| matches!(op, Op::Delete { .. })));
}

#[test]
fn apply_order_puts_deletes_last() {
    let ops = vec![
        Op::Delete {
            uid: uid(1),
            kind: Kind::DcimDevice,
            key: "site=fra1/device=leaf01".to_string(),
            backend_id: Some(2),
        },
        Op::Create {
            uid: uid(2),
            kind: Kind::DcimSite,
            desired: Object::new(
                uid(2),
                "site=fra1".to_string(),
                Attrs::Site(SiteAttrs {
                    name: "FRA1".to_string(),
                    slug: "fra1".to_string(),
                    status: None,
                    description: None,
                }),
            ),
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
    async fn observe(&self, _kinds: &[Kind]) -> anyhow::Result<ObservedState> {
        Ok(self.observed.clone())
    }

    async fn apply(&self, _ops: &[Op]) -> anyhow::Result<ApplyReport> {
        Ok(self.report.clone())
    }
}

#[test]
fn build_plan_creates_ops() {
    let inventory = Inventory {
        objects: vec![Object::new(
            uid(1),
            "site=fra1".to_string(),
            Attrs::Site(SiteAttrs {
                name: "FRA1".to_string(),
                slug: "fra1".to_string(),
                status: None,
                description: None,
            }),
        )],
    };
    let adapter = TestAdapter {
        observed: ObservedState::default(),
        report: ApplyReport { applied: vec![] },
    };
    let state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan =
        futures::executor::block_on(build_plan(&adapter, &inventory, &state, false)).unwrap();
    assert_eq!(plan.ops.len(), 1);
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
            kind: Kind::DcimSite,
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
                kind: Kind::DcimSite,
                backend_id: Some(55),
            }],
        },
    };
    let mut state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
    let plan = Plan { ops: vec![] };
    futures::executor::block_on(apply_plan(&adapter, &plan, &mut state, true)).unwrap();
    assert_eq!(state.backend_id(Kind::DcimSite, uid(1)), Some(55));
}
