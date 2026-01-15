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
