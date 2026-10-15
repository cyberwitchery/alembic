//! generate files for testing (a huge plan.json or inventory.json)

use alembic_adapter_sdk::Op;
use alembic_core::{key_string, uid_v5, Inventory, JsonMap, Key, Object, Schema, TypeName, Uid};
use alembic_engine::Plan;
use anyhow::{ensure, Context, Result};
use clap::{Parser, ValueEnum};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

const DEVICE: &str = "dcim.device";
const SITE: &str = "dcim.site";
const ROLE: &str = "dcim.device_role";
const MODEL: &str = "dcim.device_type";
const MANUFACTURER: &str = "dcim.manufacturer";
const INTERFACE: &str = "dcim.interface";
const CABLE: &str = "dcim.cable";

// the devices are spread over fixed support pools, so the artifact stays linear
// in the device count while every declared ref resolves. keep `MANUFACTURERS`
// no larger than `MODELS`: `objects` emits `min(n, pool)` of each, which only
// covers the manufacturers the models reference while that holds.
const MANUFACTURERS: usize = 2;
const MODELS: usize = 4;
const ROLES: usize = 4;
const SITES: usize = 8;

/// what the generator emits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Kind {
    Plan,
    Inventory,
}

impl Kind {
    fn default_output(self) -> &'static str {
        match self {
            Kind::Plan => "plan.json",
            Kind::Inventory => "inventory.json",
        }
    }
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// the number of devices to generate; the support objects, interfaces and cables come on top.
    #[clap(short, long, default_value = "10")]
    num_devices: usize,
    /// the number of interfaces, spread over the devices; defaults to one per device.
    /// neighbours are paired with a cable, so an odd one out, or every interface on
    /// a single device, stays uncabled. needs at least one device.
    #[clap(short = 'i', long)]
    num_interfaces: Option<usize>,
    /// the kind of file to generate.
    #[clap(short, long, value_enum, default_value_t = Kind::Plan)]
    kind: Kind,
    /// output file; defaults to plan.json or inventory.json, per --kind.
    #[clap(short, long)]
    output: Option<PathBuf>,
}

impl Args {
    fn num_interfaces(&self) -> usize {
        self.num_interfaces.unwrap_or(self.num_devices)
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let num_interfaces = args.num_interfaces();
    let output = args
        .output
        .unwrap_or_else(|| PathBuf::from(args.kind.default_output()));
    write_json(
        &render(args.kind, args.num_devices, num_interfaces)?,
        &output,
    )
}

/// build the requested artifact and render it as pretty json.
fn render(kind: Kind, num_devices: usize, num_interfaces: usize) -> Result<String> {
    match kind {
        Kind::Plan => serde_json::to_string_pretty(&build_plan(num_devices, num_interfaces)?),
        Kind::Inventory => {
            serde_json::to_string_pretty(&build_inventory(num_devices, num_interfaces)?)
        }
    }
    .context("serializing artifact")
}

/// build a plan of create operations, one per generated object.
fn build_plan(num_devices: usize, num_interfaces: usize) -> Result<Plan> {
    let ops = objects(num_devices, num_interfaces)?
        .into_iter()
        .map(create_op)
        .collect();
    Ok(Plan {
        schema: schema()?,
        ops,
        summary: None,
        schema_preview: None,
        effects: None,
    })
}

/// build an inventory of the same objects the plan creates.
fn build_inventory(num_devices: usize, num_interfaces: usize) -> Result<Inventory> {
    Ok(Inventory {
        schema: schema()?,
        scope: None,
        objects: objects(num_devices, num_interfaces)?,
    })
}

fn create_op(desired: Object) -> Op {
    Op::Create {
        uid: desired.uid,
        type_name: desired.type_name.clone(),
        desired,
    }
}

fn write_json(json: &str, path: &Path) -> Result<()> {
    fs::write(path, json).with_context(|| format!("writing to {}", path.display()))
}

/// the objects a generated artifact carries: the support pools first, then the
/// devices.
///
/// each pool is capped at the device count as well as its own size, so every
/// emitted support object is referenced by at least one device.
fn objects(num_devices: usize, num_interfaces: usize) -> Result<Vec<Object>> {
    let mut objects = Vec::new();
    for i in 0..num_devices.min(MANUFACTURERS) {
        objects.push(manufacturer(i)?);
    }
    for i in 0..num_devices.min(MODELS) {
        objects.push(model(i)?);
    }
    for i in 0..num_devices.min(ROLES) {
        objects.push(role(i)?);
    }
    for i in 0..num_devices.min(SITES) {
        objects.push(site(i)?);
    }

    for i in 0..num_devices {
        objects.push(device(i)?);
    }

    ensure!(
        num_devices > 0 || num_interfaces == 0,
        "{num_interfaces} interfaces need at least one device to sit on"
    );
    for i in 0..num_interfaces {
        objects.push(interface(i, i % num_devices)?);
    }

    // neighbours sit on neighbouring devices, so a cable joins two devices;
    // with only one there is nothing to cable it to.
    if num_devices < 2 {
        return Ok(objects);
    }
    for a in (0..num_interfaces / 2).map(|pair| 2 * pair) {
        objects.push(cable((a, a % num_devices), (a + 1, (a + 1) % num_devices))?);
    }

    Ok(objects)
}

fn manufacturer(i: usize) -> Result<Object> {
    let slug = slug_of(MANUFACTURER, i);
    slug_object(
        MANUFACTURER,
        &slug,
        [("name".to_string(), json!(format!("Manufacturer {i}")))],
    )
}

fn model(i: usize) -> Result<Object> {
    let slug = slug_of(MODEL, i);
    slug_object(
        MODEL,
        &slug,
        [
            (
                "manufacturer".to_string(),
                object_ref(MANUFACTURER, i % MANUFACTURERS),
            ),
            ("model".to_string(), json!(format!("Model {i}"))),
        ],
    )
}

fn role(i: usize) -> Result<Object> {
    let slug = slug_of(ROLE, i);
    slug_object(
        ROLE,
        &slug,
        [("name".to_string(), json!(format!("Role {i}")))],
    )
}

fn site(i: usize) -> Result<Object> {
    let slug = slug_of(SITE, i);
    slug_object(
        SITE,
        &slug,
        [
            ("name".to_string(), json!(format!("Site {i}"))),
            ("status".to_string(), json!("active")),
        ],
    )
}

/// build a single `dcim.device`, pointing at one member of each support pool.
fn device(i: usize) -> Result<Object> {
    let name = device_name(i);
    // `name` is declared in both `key` and `fields`, so it is carried in both.
    object(
        DEVICE,
        device_key(i),
        [
            ("name".to_string(), json!(name)),
            ("site".to_string(), object_ref(SITE, i % SITES)),
            ("role".to_string(), object_ref(ROLE, i % ROLES)),
            ("device_type".to_string(), object_ref(MODEL, i % MODELS)),
            ("status".to_string(), json!("active")),
        ],
    )
}

fn device_name(i: usize) -> String {
    format!("device_{i}")
}

fn device_key(i: usize) -> Key {
    Key::from(BTreeMap::from([(
        "name".to_string(),
        json!(device_name(i)),
    )]))
}

/// build a single `dcim.interface` on device `device`.
fn interface(i: usize, device: usize) -> Result<Object> {
    let key = interface_key(i, device);
    // `device` and `name` are declared in both `key` and `fields`, so they are carried in both.
    let mut attrs = BTreeMap::clone(&key);
    attrs.extend([
        ("type".to_string(), json!("25gbase-x-sfp28")),
        ("enabled".to_string(), Value::Bool(true)),
    ]);
    object(INTERFACE, key, attrs)
}

fn interface_name(i: usize) -> String {
    format!("interface_{i}")
}

fn interface_key(i: usize, device: usize) -> Key {
    Key::from(BTreeMap::from([
        (
            "device".to_string(),
            json!(uid_of(DEVICE, &device_key(device)).to_string()),
        ),
        ("name".to_string(), json!(interface_name(i))),
    ]))
}

/// build a single `dcim.cable` joining interfaces `a` and `b`, each given as
/// `(interface, device)`. the label names both ends, so a cable is the same
/// identity across runs only while it joins the same two interfaces.
fn cable(a: (usize, usize), b: (usize, usize)) -> Result<Object> {
    let end = |(i, device)| format!("{}:{}", device_name(device), interface_name(i));
    let label = json!(format!("{}-{}", end(a), end(b)));
    let termination = |(i, device)| json!([uid_of(INTERFACE, &interface_key(i, device))]);
    object(
        CABLE,
        Key::from(BTreeMap::from([("label".to_string(), label.clone())])),
        [
            ("label".to_string(), label),
            ("a_terminations".to_string(), termination(a)),
            ("b_terminations".to_string(), termination(b)),
        ],
    )
}

/// the support types are keyed on a slug and all carry it as an attribute too.
fn slug_object(
    type_name: &str,
    slug: &str,
    attrs: impl IntoIterator<Item = (String, Value)>,
) -> Result<Object> {
    let mut all = BTreeMap::from([("slug".to_string(), json!(slug))]);
    all.extend(attrs);
    object(type_name, slug_key(slug), all)
}

/// build an object whose uid is derived from its key, as the engine's own
/// canonical path does. `Object::new` rejects an empty key, so a pool that
/// stopped filling one could not be written out.
fn object(
    type_name: &str,
    key: Key,
    attrs: impl IntoIterator<Item = (String, Value)>,
) -> Result<Object> {
    let uid = uid_of(type_name, &key);
    Object::new(
        uid,
        TypeName::new(type_name),
        key,
        JsonMap::from(BTreeMap::from_iter(attrs)),
    )
    .with_context(|| format!("building a {type_name}"))
}

/// the uid of the `type_name` object keyed on `key`.
fn uid_of(type_name: &str, key: &Key) -> Uid {
    uid_v5(type_name, &key_string(key))
}

fn slug_of(type_name: &str, i: usize) -> String {
    // the type name is already a valid slug bar the dot separating its parts.
    format!("{}_{i}", type_name.replace('.', "_"))
}

fn slug_key(slug: &str) -> Key {
    Key::from(BTreeMap::from([("slug".to_string(), json!(slug))]))
}

/// the uid of support object `i`, as a ref attribute value.
fn object_ref(type_name: &str, i: usize) -> Value {
    json!(support_uid(type_name, i).to_string())
}

fn support_uid(type_name: &str, i: usize) -> Uid {
    uid_of(type_name, &slug_key(&slug_of(type_name, i)))
}

/// the schema shared by every generated artifact.
fn schema() -> Result<Schema> {
    let schema_yaml = r"
types:
    dcim.manufacturer:
      key:
        slug:
          type: slug
      fields:
        name:
          type: string
        slug:
          type: slug
    dcim.device_role:
      key:
        slug:
          type: slug
      fields:
        name:
          type: string
        slug:
          type: slug
    dcim.device_type:
      key:
        slug:
          type: slug
      fields:
        manufacturer:
          type: ref
          target: dcim.manufacturer
        model:
          type: string
        slug:
          type: slug
    dcim.site:
      key:
        slug:
          type: slug
      fields:
        name:
          type: string
        slug:
          type: slug
        status:
          type: string
    dcim.device:
      key:
        name:
          type: slug
      fields:
        name:
          type: slug
        site:
          type: ref
          target: dcim.site
        role:
          type: ref
          target: dcim.device_role
        device_type:
          type: ref
          target: dcim.device_type
        status:
          type: string
    dcim.interface:
      key:
        device:
          type: ref
          target: dcim.device
        name:
          type: slug
      fields:
        name:
          type: slug
        device:
          type: ref
          target: dcim.device
        type:
          type: string
        enabled:
          type: bool
    dcim.cable:
      key: { label: { type: string } }
      fields:
        label: { type: string }
        a_terminations: { type: list_ref, target: dcim.interface }
        b_terminations: { type: list_ref, target: dcim.interface }";

    alembic_engine::yaml::from_str(schema_yaml).context("parsing embedded schema")
}

#[cfg(test)]
mod tests {
    use super::*;
    use alembic_core::validate_inventory;
    use std::collections::BTreeSet;

    const INTERFACES: usize = 8;

    fn parse(args: &[&str]) -> Args {
        Args::try_parse_from(["alembic-file-generator"].iter().chain(args))
            .expect("args should parse")
    }

    fn num_interfaces(args: &[&str]) -> usize {
        parse(args).num_interfaces()
    }

    #[test]
    fn interfaces_default_to_one_per_device() {
        assert_eq!(num_interfaces(&["-n", "1000"]), 1000);
        assert_eq!(num_interfaces(&[]), 10);
        assert_eq!(num_interfaces(&["-n", "0"]), 0);
    }

    #[test]
    fn an_explicit_interface_count_wins() {
        assert_eq!(num_interfaces(&["-n", "1000", "-i", "8"]), 8);
    }

    fn create_ops(plan: &Plan) -> Vec<&Op> {
        plan.ops
            .iter()
            .filter(|op| matches!(op, Op::Create { .. }))
            .collect()
    }

    fn devices(objects: &[Object]) -> Vec<&Object> {
        objects
            .iter()
            .filter(|object| object.type_name.as_str() == DEVICE)
            .collect()
    }

    /// the plan's own objects, lifted into an inventory carrying its schema.
    fn plan_as_inventory(plan: &Plan) -> Inventory {
        Inventory {
            scope: None,
            schema: plan.schema.clone(),
            objects: plan
                .ops
                .iter()
                .map(|op| match op {
                    Op::Create { desired, .. } => desired.clone(),
                    other => panic!("expected a create op, got {other:?}"),
                })
                .collect(),
        }
    }

    fn errors(inventory: &Inventory) -> Vec<String> {
        validate_inventory(inventory)
            .errors
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    #[test]
    fn every_declared_type_is_generated() {
        // the schema is only worth carrying for the types the generator emits,
        // so the claim is checked against the schema rather than a second list.
        let inventory = build_inventory(20, INTERFACES).expect("inventory should build");
        let emitted: BTreeSet<_> = inventory
            .objects
            .iter()
            .map(|object| object.type_name.to_string())
            .collect();
        for type_name in inventory.schema.types.keys() {
            assert!(
                emitted.contains(type_name),
                "schema declares {type_name} but nothing generates it"
            );
        }
    }

    #[test]
    fn generated_inventory_validates_against_the_embedded_schema() {
        // the regression the generator shipped: empty keys made every device
        // one identity, and `validate` rejected the lot.
        for n in [0, 1, 2, 3, 17, 40] {
            let inventory = build_inventory(n, n).expect("inventory should build");
            assert_eq!(errors(&inventory), Vec::<String>::new(), "at n = {n}");
        }
    }

    #[test]
    fn generated_plan_objects_validate_as_an_inventory() {
        for n in [0, 1, 3, 17] {
            let plan = build_plan(n, n).expect("plan should build");
            let inventory = plan_as_inventory(&plan);
            assert_eq!(errors(&inventory), Vec::<String>::new(), "at n = {n}");
        }
    }

    #[test]
    fn every_object_derives_its_uid_from_its_key() {
        let inventory = build_inventory(30, INTERFACES).expect("inventory should build");
        for object in &inventory.objects {
            assert_eq!(
                object.uid,
                uid_v5(object.type_name.as_str(), &key_string(&object.key)),
                "{} {:?}",
                object.type_name,
                object.key
            );
        }
    }

    /// the uids some object's attrs point at.
    fn referenced(objects: &[Object]) -> BTreeSet<Uid> {
        objects
            .iter()
            .flat_map(|object| object.attrs.values())
            .flat_map(|value| {
                value
                    .as_array()
                    .map_or(std::slice::from_ref(value), Vec::as_slice)
            })
            .filter_map(|value| value.as_str())
            .filter_map(|raw| Uid::parse_str(raw).ok())
            .collect()
    }

    /// the names of the interfaces no cable ends on.
    fn uncabled(objects: &[Object]) -> Vec<String> {
        let referenced = referenced(objects);
        objects
            .iter()
            .filter(|object| object.type_name.as_str() == INTERFACE)
            .filter(|object| !referenced.contains(&object.uid))
            .map(|object| object.attrs["name"].as_str().expect("a name").to_string())
            .collect()
    }

    #[test]
    fn every_support_object_is_referenced() {
        // an unreferenced pool member is dead weight in every generated file;
        // which interfaces stay uncabled is pinned by its own tests below.
        for (n, i) in [(1, 8), (3, 8), (9, 7), (40, 8), (40, 40)] {
            let inventory = build_inventory(n, i).expect("inventory should build");
            let referenced = referenced(&inventory.objects);
            for object in &inventory.objects {
                if [DEVICE, INTERFACE, CABLE].contains(&object.type_name.as_str()) {
                    continue;
                }
                assert!(
                    referenced.contains(&object.uid),
                    "nothing references {} at n = {n}, i = {i}",
                    key_string(&object.key)
                );
            }
        }
    }

    #[test]
    fn every_interface_is_cabled_at_an_even_count() {
        for (n, i) in [(2, 8), (3, 8), (9, 2), (40, 40)] {
            let objects = objects(n, i).expect("objects should build");
            assert_eq!(
                uncabled(&objects),
                Vec::<String>::new(),
                "at n = {n}, i = {i}"
            );
        }
    }

    #[test]
    fn an_odd_interface_out_is_left_uncabled() {
        for (n, i) in [(2, 1), (5, 7), (9, 9)] {
            let objects = objects(n, i).expect("objects should build");
            assert_eq!(
                uncabled(&objects),
                vec![interface_name(i - 1)],
                "at n = {n}, i = {i}"
            );
        }
    }

    #[test]
    fn support_pools_are_fixed_so_growth_is_linear() {
        // interfaces and cables scale with their own count, so only the pools
        // are counted here.
        let support = |n| {
            let inventory = build_inventory(n, n).expect("inventory should build");
            inventory
                .objects
                .iter()
                .filter(|object| ![DEVICE, INTERFACE, CABLE].contains(&object.type_name.as_str()))
                .count()
        };
        let pools = MANUFACTURERS + MODELS + ROLES + SITES;
        assert_eq!(support(0), 0);
        assert_eq!(support(1), 4);
        assert_eq!(support(1000), pools);
        assert_eq!(support(2000), pools);
    }

    #[test]
    fn the_default_interface_count_grows_with_the_devices() {
        let pools = MANUFACTURERS + MODELS + ROLES + SITES;
        for n in [1000, 2001] {
            let objects = objects(n, n).expect("objects should build");
            // a device, an interface and half a cable each.
            assert_eq!(objects.len(), pools + n + n + n / 2, "at n = {n}");
        }
    }

    /// the device each interface sits on, by interface uid.
    fn interface_devices(objects: &[Object]) -> BTreeMap<String, Value> {
        objects
            .iter()
            .filter(|object| object.type_name.as_str() == INTERFACE)
            .map(|object| (object.uid.to_string(), object.attrs["device"].clone()))
            .collect()
    }

    fn cables(objects: &[Object]) -> Vec<&Object> {
        objects
            .iter()
            .filter(|object| object.type_name.as_str() == CABLE)
            .collect()
    }

    #[test]
    fn interface_count_is_independent_of_the_device_count() {
        for (n, i) in [(3, 8), (20, 8), (5, 0), (5, 7)] {
            let objects = objects(n, i).expect("objects should build");
            assert_eq!(interface_devices(&objects).len(), i, "at n = {n}, i = {i}");
            assert_eq!(cables(&objects).len(), i / 2, "at n = {n}, i = {i}");
        }
    }

    #[test]
    fn interfaces_are_spread_over_the_devices_in_turn() {
        let objects = objects(3, 8).expect("objects should build");
        for (i, object) in objects
            .iter()
            .filter(|object| object.type_name.as_str() == INTERFACE)
            .enumerate()
        {
            let device = device(i % 3).expect("device should build");
            assert_eq!(object.attrs["device"], json!(device.uid.to_string()));
        }
    }

    #[test]
    fn interfaces_without_devices_are_rejected() {
        assert_eq!(
            objects(0, 8).unwrap_err().to_string(),
            "8 interfaces need at least one device to sit on"
        );
        assert!(render(Kind::Plan, 0, 8).is_err());
        let objects = objects(0, 0).expect("objects should build");
        assert!(objects.is_empty());
    }

    #[test]
    fn one_device_means_no_cables() {
        let objects = objects(1, 8).expect("objects should build");
        assert_eq!(interface_devices(&objects).len(), 8);
        assert!(cables(&objects).is_empty());
        assert_eq!(uncabled(&objects).len(), 8);
    }

    #[test]
    fn a_cable_is_labelled_by_both_of_its_ends() {
        let joined = cable((0, 0), (1, 1)).expect("cable should build");
        assert_eq!(
            joined.key.get("label"),
            Some(&json!("device_0:interface_0-device_1:interface_1"))
        );

        // the same pair of interface names on other devices is another cable.
        let elsewhere = cable((0, 0), (1, 2)).expect("cable should build");
        assert_ne!(joined.uid, elsewhere.uid);
    }

    #[test]
    fn cables_join_two_devices_once_there_are_two() {
        for (n, i) in [(2, 8), (3, 8), (4, 8), (8, 8), (2, 20), (20, 8)] {
            let objects = objects(n, i).expect("objects should build");
            let on = interface_devices(&objects);
            for cable in cables(&objects) {
                let end = |side: &str| {
                    let uid = cable.attrs[side][0].as_str().expect("a uid string");
                    on[uid].clone()
                };
                assert_ne!(
                    end("a_terminations"),
                    end("b_terminations"),
                    "a cable loops on one device at n = {n}, i = {i}"
                );
            }
        }
    }

    #[test]
    fn build_plan_has_an_op_per_generated_object() {
        let plan = build_plan(7, INTERFACES).expect("plan should build");
        let objects = objects(7, INTERFACES).expect("objects should build");
        assert_eq!(plan.ops.len(), objects.len());
        assert_eq!(create_ops(&plan).len(), objects.len());
        assert_eq!(devices(&objects).len(), 7);
        assert_eq!(plan_as_inventory(&plan).objects, objects);
    }

    #[test]
    fn build_plan_with_zero_ops_is_empty() {
        let plan = build_plan(0, 0).expect("empty plan should build");
        assert!(plan.ops.is_empty());
        let inventory = build_inventory(0, 0).expect("empty inventory should build");
        assert!(inventory.objects.is_empty());
    }

    #[test]
    fn build_inventory_carries_the_same_schema_as_the_plan() {
        let plan = build_plan(3, INTERFACES).expect("plan should build");
        let inventory = build_inventory(3, INTERFACES).expect("inventory should build");
        assert_eq!(inventory.schema, plan.schema);
        assert_eq!(
            inventory.objects,
            objects(3, INTERFACES).expect("objects should build"),
            "the two kinds describe the same objects"
        );
    }

    #[test]
    fn every_op_agrees_with_the_object_it_carries() {
        let plan = build_plan(9, INTERFACES).expect("plan should build");
        for op in &plan.ops {
            let Op::Create {
                uid,
                type_name,
                desired,
            } = op
            else {
                panic!("expected a create op, got {op:?}");
            };
            assert_eq!(desired.uid, *uid);
            assert_eq!(desired.type_name, *type_name);
        }
    }

    #[test]
    fn device_carries_its_name_in_the_key_and_resolves_its_refs() {
        let device = device(42).expect("device should build");
        assert_eq!(device.type_name, TypeName::new(DEVICE));
        assert_eq!(device.key.get("name"), Some(&json!("device_42")));
        assert_eq!(device.attrs.get("name"), Some(&json!("device_42")));
        assert_eq!(device.uid, uid_v5(DEVICE, &key_string(&device.key)));

        // 42 % 8, 42 % 4 and 42 % 4 pick the pool members.
        assert_eq!(device.attrs.get("site"), Some(&object_ref(SITE, 2)));
        assert_eq!(device.attrs.get("role"), Some(&object_ref(ROLE, 2)));
        assert_eq!(device.attrs.get("device_type"), Some(&object_ref(MODEL, 2)));
    }

    #[test]
    fn interface_names_are_unique_per_device_not_globally() {
        let on = |device| interface(0, device).expect("interface should build");
        let (a, b) = (on(1), on(2));
        assert_eq!(a.key.get("name"), b.key.get("name"));
        assert_eq!(a.key.get("device"), a.attrs.get("device"));
        assert_ne!(key_string(&a.key), key_string(&b.key));
        assert_ne!(a.uid, b.uid);
    }

    #[test]
    fn generation_is_deterministic_down_to_the_bytes() {
        // `Key`'s field order is what `key_string` hashes and what the uid is
        // derived from, so the rendered bytes are pinned rather than assumed.
        assert_eq!(
            key_string(&device(1).expect("device should build").key),
            r#"{"name":"device_1"}"#
        );
        assert_eq!(
            key_string(&manufacturer(1).expect("manufacturer should build").key),
            r#"{"slug":"dcim_manufacturer_1"}"#
        );

        for kind in [Kind::Plan, Kind::Inventory] {
            let first = render(kind, 5, INTERFACES).expect("artifact should render");
            let second = render(kind, 5, INTERFACES).expect("artifact should render");
            assert_eq!(first, second, "{kind:?} is not byte-stable");
        }
    }

    /// render an artifact through the write path and read it back.
    fn round_trip(kind: Kind, num_devices: usize) -> String {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("out.json");
        let json = render(kind, num_devices, INTERFACES).expect("artifact should render");
        write_json(&json, &path).expect("artifact should write");
        fs::read_to_string(&path).expect("output should exist")
    }

    #[test]
    fn written_json_round_trips_as_the_type_it_names() {
        let plan: Plan = serde_json::from_str(&round_trip(Kind::Plan, 3))
            .expect("output should be valid plan json");
        assert_eq!(create_ops(&plan).len(), plan.ops.len());
        assert_eq!(devices(&plan_as_inventory(&plan).objects).len(), 3);

        let inventory: Inventory = serde_json::from_str(&round_trip(Kind::Inventory, 3))
            .expect("output should be valid inventory json");
        assert_eq!(devices(&inventory.objects).len(), 3);
        assert_eq!(errors(&inventory), Vec::<String>::new());
    }

    #[test]
    fn write_json_errors_on_unwritable_path() {
        let dir = tempfile::tempdir().expect("temp dir");
        // a path whose parent directory does not exist cannot be written.
        let path = dir.path().join("does/not/exist/plan.json");
        let json = render(Kind::Plan, 1, INTERFACES).expect("plan should render");
        let err = write_json(&json, &path).unwrap_err();
        assert_eq!(err.to_string(), format!("writing to {}", path.display()));
    }

    #[test]
    fn output_defaults_follow_the_kind() {
        assert_eq!(Kind::Plan.default_output(), "plan.json");
        assert_eq!(Kind::Inventory.default_output(), "inventory.json");
    }
}
