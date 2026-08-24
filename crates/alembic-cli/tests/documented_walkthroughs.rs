//! drives the commands the published walkthroughs and case studies print, over
//! the fixtures they link, and pins the output each page states. every step here
//! is backend-free, so the pages stay checkable without a live netbox or
//! nautobot. the pages are the specification: if one of these fails, the page is
//! right and the code or the fixture is wrong.

mod support;

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::process::Command;

use serde_json::Value;
use support::{bin_path, run_command, walkthrough_path, walkthroughs_dir};
use tempfile::tempdir;

/// the parsed ir `alembic map` writes for a documented inventory + spec pair.
fn run_documented_map(inventory: &str, spec: &str) -> Value {
    let out = tempdir().expect("temp dir");
    let ir = out.path().join("ir.json");

    let mut cmd = Command::new(bin_path());
    cmd.args([
        "map",
        "-f",
        walkthrough_path(inventory).to_str().unwrap(),
        "--spec",
        walkthrough_path(spec).to_str().unwrap(),
        "-o",
        ir.to_str().unwrap(),
    ]);
    run_command(cmd, &format!("map {inventory} through {spec}"));

    serde_json::from_str(&fs::read_to_string(&ir).expect("map wrote its ir"))
        .expect("map's ir parses as json")
}

fn parse_yaml(path: &Path) -> Value {
    serde_yaml::from_str(&fs::read_to_string(path).expect("read fixture")).expect("fixture parses")
}

fn type_names(doc: &Value) -> BTreeSet<String> {
    doc["schema"]["types"]
        .as_object()
        .expect("schema declares types")
        .keys()
        .cloned()
        .collect()
}

fn field_names(doc: &Value, ty: &str) -> BTreeSet<String> {
    doc["schema"]["types"][ty]["fields"]
        .as_object()
        .map(|fields| fields.keys().cloned().collect())
        .unwrap_or_default()
}

/// the single object of `ty` whose key field `field` is `value`.
fn object<'a>(ir: &'a Value, ty: &str, field: &str, value: &str) -> &'a Value {
    let matches: Vec<&Value> = ir["objects"]
        .as_array()
        .expect("ir carries objects")
        .iter()
        .filter(|o| o["type"] == ty && o["key"][field] == value)
        .collect();
    assert_eq!(
        matches.len(),
        1,
        "expected exactly one {ty} with {field}={value}; ir:\n{ir:#}"
    );
    matches[0]
}

fn uid(object: &Value) -> &str {
    object["uid"].as_str().expect("object carries a uid")
}

// (a) every inventory the walkthroughs ship still loads. the directory is read
// rather than listed, so a fixture added later is gated without touching this
// file; the map specs in there are not inventories, so `validate -f` on them is
// the wrong command and they are asserted through `map` below instead.

#[test]
fn every_committed_walkthrough_inventory_validates() {
    let dir = walkthroughs_dir();
    let mut inventories = Vec::new();
    let mut specs = Vec::new();

    for entry in fs::read_dir(&dir).expect("walkthroughs dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().is_none_or(|ext| ext != "yaml") {
            continue;
        }
        let doc = parse_yaml(&path);
        // an inventory carries `objects`, a map spec carries `rules`. a fixture
        // that is neither is a shape nothing here knows how to check.
        match (doc.get("objects").is_some(), doc.get("rules").is_some()) {
            (true, false) => inventories.push(path),
            (false, true) => specs.push(path),
            _ => panic!(
                "{} is neither an inventory (`objects:`) nor a map spec (`rules:`)",
                path.display()
            ),
        }
    }

    assert!(!inventories.is_empty(), "no walkthrough inventory found");
    assert!(!specs.is_empty(), "no walkthrough map spec found");

    for inventory in inventories {
        let mut cmd = Command::new(bin_path());
        cmd.args(["validate", "-f", inventory.to_str().unwrap()]);
        run_command(cmd, &format!("validate {}", inventory.display()));
    }
}

// (b) docs/examples/01-basic-dcim-ipam.md: one rename plus a `match: "*"`
// passthrough carries every other type through with its schema, and the ip's
// ref still lands on eth0 after map re-derives uids.

#[test]
fn example_01_maps_the_neutral_model_to_netbox_names() {
    let source = parse_yaml(&walkthrough_path("01-basic.yaml"));
    let spec = parse_yaml(&walkthrough_path("01-netbox-map.yaml"));
    let ir = run_documented_map("01-basic.yaml", "01-netbox-map.yaml");

    // "the target `schema` only declares the one type you reshape".
    assert_eq!(
        type_names(&spec),
        BTreeSet::from(["ipam.ip_address".to_string()]),
        "the spec should declare only the reshaped type"
    );
    // "the `match: \"*\"` passthrough carries every other type through unchanged",
    // schema included: the output declares every source type, not just the one.
    assert_eq!(type_names(&ir), type_names(&source));
    assert_eq!(
        field_names(&ir, "dcim.interface"),
        field_names(&source, "dcim.interface"),
        "passthrough should carry a type's fields, not only its name"
    );

    // the rename: netbox's `assigned_object` replaces the neutral name.
    let mapped_fields = field_names(&ir, "ipam.ip_address");
    assert!(
        mapped_fields.contains("assigned_object"),
        "{mapped_fields:?}"
    );
    assert!(
        !mapped_fields.contains("assigned_interface"),
        "{mapped_fields:?}"
    );

    // "the ip still points at its interface, and every object keeps the
    // identity you authored": eth0, not eth1, under the authored uid.
    let eth0 = object(&ir, "dcim.interface", "name", "eth0");
    let ip = object(&ir, "ipam.ip_address", "address", "10.0.0.10/24");
    assert_eq!(ip["attrs"]["assigned_object"], uid(eth0));
    assert!(ip["attrs"].get("assigned_interface").is_none());
    let authored = object(&source, "dcim.interface", "name", "eth0");
    assert_eq!(uid(eth0), uid(authored), "a 1:1 map inherits identity");
}

// (c) docs/case-studies/01-evaluate-dcim-systems.md: one source of truth, one
// small map per candidate, two backend-shaped irs.

#[test]
fn case_study_01_stands_one_model_up_into_two_backends() {
    let netbox = run_documented_map("eval-fabric.yaml", "eval-fabric-netbox.yaml");
    let nautobot = run_documented_map("eval-fabric.yaml", "eval-fabric-nautobot.yaml");

    // netbox keeps `dcim.site` and renames the ip's assignment.
    let site = object(&netbox, "dcim.site", "slug", "fra1");
    assert_eq!(site["attrs"]["name"], "Frankfurt DC1");
    assert_eq!(
        netbox["objects"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|o| o["type"] == "dcim.location")
            .count(),
        0
    );
    let netbox_ip = object(&netbox, "ipam.ip_address", "address", "10.0.0.10/24");
    let netbox_eth0 = object(&netbox, "dcim.interface", "name", "eth0");
    assert_eq!(netbox_ip["attrs"]["assigned_object"], uid(netbox_eth0));

    // nautobot keys a location by its human name and points the device at it.
    let location = object(&nautobot, "dcim.location", "name", "Frankfurt DC1");
    let nautobot_device = object(&nautobot, "dcim.device", "name", "leaf01");
    assert_eq!(nautobot_device["attrs"]["location"], uid(location));
    assert!(nautobot_device["attrs"].get("site").is_none());
    // "the interface and ip pass through because nautobot already uses their
    // neutral names".
    let nautobot_ip = object(&nautobot, "ipam.ip_address", "address", "10.0.0.10/24");
    let nautobot_eth0 = object(&nautobot, "dcim.interface", "name", "eth0");
    assert_eq!(
        nautobot_ip["attrs"]["assigned_interface"],
        uid(nautobot_eth0)
    );

    // one source of truth, one identity: every object is the same logical
    // object in both backend-shaped irs, the reshaped site/location included.
    let netbox_device = object(&netbox, "dcim.device", "name", "leaf01");
    assert_eq!(uid(netbox_device), uid(nautobot_device));
    assert_eq!(uid(netbox_eth0), uid(nautobot_eth0));
    assert_eq!(
        uid(site),
        uid(location),
        "one logical site, two vocabularies"
    );
}

// (d) docs/case-studies/02-nautobot-to-netbox.md: the migration the page walks
// through, from its trimmed nautobot ir to netbox's vocabulary.

#[test]
fn case_study_02_migrates_nautobot_shaped_ir_into_netbox() {
    let source = parse_yaml(&walkthrough_path("nautobot-ir.yaml"));
    let ir = run_documented_map("nautobot-ir.yaml", "map-nautobot-to-netbox.yaml");

    // "the location's name `Frankfurt DC1` becomes a site with slug `frankfurt-dc1`".
    let site = object(&ir, "dcim.site", "slug", "frankfurt-dc1");
    assert_eq!(site["attrs"]["name"], "Frankfurt DC1");
    assert_eq!(site["attrs"]["slug"], "frankfurt-dc1");

    // "a reference-valued field becomes the plain value netbox expects", lowered
    // from the status object's name `Active`.
    assert_eq!(site["attrs"]["status"], "active");
    let device = object(&ir, "dcim.device", "name", "leaf01");
    assert_eq!(device["attrs"]["status"], "active");
    assert_eq!(device["attrs"]["role"], "leaf");

    // "the device's `site` ref points at the site" -- which is the migrated
    // location itself: the translation keeps its identity, slug change and all.
    assert_eq!(device["attrs"]["site"], uid(site));
    assert!(device["attrs"].get("location").is_none());
    let location = object(&source, "dcim.location", "name", "Frankfurt DC1");
    assert_eq!(
        uid(site),
        uid(location),
        "the migrated site is the same logical object"
    );

    // netbox has no status objects: with no passthrough rule, nothing else lands.
    assert_eq!(
        type_names(&ir),
        BTreeSet::from(["dcim.site".to_string(), "dcim.device".to_string()])
    );
    assert_eq!(ir["objects"].as_array().unwrap().len(), 2, "{ir:#}");
}
