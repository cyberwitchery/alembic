//! generate files for testing (a huge plan.json or inventory.json)

use alembic_core::{key_string, uid_v5, Inventory, JsonMap, Key, Object, Schema, TypeName, Uid};
use alembic_engine::{Op, Plan};
use anyhow::{Context, Result};
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

// the devices are spread over fixed support pools, so the artifact stays linear
// in the device count while every declared ref resolves. keep `MANUFACTURERS`
// no larger than `MODELS`: `objects` emits `min(n, pool)` of each, which only
// covers the manufacturers the models reference while that holds.
const MANUFACTURERS: u128 = 2;
const MODELS: u128 = 4;
const ROLES: u128 = 4;
const SITES: u128 = 8;

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
    /// the number of devices to generate; the support objects they reference come on top.
    #[clap(short, long, alias = "num-ops", default_value = "10")]
    num_devices: u128,
    /// the kind of file to generate.
    #[clap(short, long, value_enum, default_value_t = Kind::Plan)]
    kind: Kind,
    /// output file; defaults to plan.json or inventory.json, per --kind.
    #[clap(short, long)]
    output: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let output = args
        .output
        .unwrap_or_else(|| PathBuf::from(args.kind.default_output()));
    write_json(&render(args.kind, args.num_devices)?, &output)
}

/// build the requested artifact and render it as pretty json.
fn render(kind: Kind, num_devices: u128) -> Result<String> {
    match kind {
        Kind::Plan => serde_json::to_string_pretty(&build_plan(num_devices)?),
        Kind::Inventory => serde_json::to_string_pretty(&build_inventory(num_devices)?),
    }
    .context("serializing artifact")
}

/// build a plan of create operations, one per generated object.
fn build_plan(num_devices: u128) -> Result<Plan> {
    let ops = objects(num_devices)?.into_iter().map(create_op).collect();
    Ok(Plan {
        schema: schema()?,
        ops,
        summary: None,
        schema_preview: None,
    })
}

/// build an inventory of the same objects the plan creates.
fn build_inventory(num_devices: u128) -> Result<Inventory> {
    Ok(Inventory {
        schema: schema()?,
        objects: objects(num_devices)?,
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
fn objects(num_devices: u128) -> Result<Vec<Object>> {
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
    Ok(objects)
}

fn manufacturer(i: u128) -> Result<Object> {
    let slug = slug_of(MANUFACTURER, i);
    slug_object(
        MANUFACTURER,
        &slug,
        [("name".to_string(), json!(format!("Manufacturer {i}")))],
    )
}

fn model(i: u128) -> Result<Object> {
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

fn role(i: u128) -> Result<Object> {
    let slug = slug_of(ROLE, i);
    slug_object(
        ROLE,
        &slug,
        [("name".to_string(), json!(format!("Role {i}")))],
    )
}

fn site(i: u128) -> Result<Object> {
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
fn device(i: u128) -> Result<Object> {
    let name = device_name(i);
    // `name` is declared in both `key` and `fields`, so it is carried in both.
    object(
        DEVICE,
        Key::from(BTreeMap::from([("name".to_string(), json!(name))])),
        [
            ("name".to_string(), json!(name)),
            ("site".to_string(), object_ref(SITE, i % SITES)),
            ("role".to_string(), object_ref(ROLE, i % ROLES)),
            ("device_type".to_string(), object_ref(MODEL, i % MODELS)),
            ("status".to_string(), json!("active")),
        ],
    )
}

fn device_name(i: u128) -> String {
    format!("device_{i}")
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
    let uid = uid_v5(type_name, &key_string(&key));
    Object::new(
        uid,
        TypeName::new(type_name),
        key,
        JsonMap::from(BTreeMap::from_iter(attrs)),
    )
    .with_context(|| format!("building a {type_name}"))
}

fn slug_of(type_name: &str, i: u128) -> String {
    // the type name is already a valid slug bar the dot separating its parts.
    format!("{}_{i}", type_name.replace('.', "_"))
}

fn slug_key(slug: &str) -> Key {
    Key::from(BTreeMap::from([("slug".to_string(), json!(slug))]))
}

/// the uid of support object `i`, as a ref attribute value.
fn object_ref(type_name: &str, i: u128) -> Value {
    json!(support_uid(type_name, i).to_string())
}

fn support_uid(type_name: &str, i: u128) -> Uid {
    uid_v5(type_name, &key_string(&slug_key(&slug_of(type_name, i))))
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
          type: string";

    serde_yaml::from_str(schema_yaml).context("parsing embedded schema")
}

#[cfg(test)]
mod tests {
    use super::*;
    use alembic_core::validate_inventory;
    use std::collections::BTreeSet;

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
    fn embedded_schema_parses() {
        let schema = schema().expect("embedded schema should parse");
        for type_name in [
            "dcim.manufacturer",
            "dcim.device_role",
            "dcim.device_type",
            "dcim.site",
            "dcim.device",
        ] {
            assert!(
                schema.types.contains_key(type_name),
                "schema is missing {type_name}"
            );
        }
    }

    #[test]
    fn every_declared_type_is_generated() {
        // the schema is only worth carrying for the types the generator emits,
        // so the claim is checked against the schema rather than a second list.
        let inventory = build_inventory(20).expect("inventory should build");
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
            let inventory = build_inventory(n).expect("inventory should build");
            assert_eq!(errors(&inventory), Vec::<String>::new(), "at n = {n}");
        }
    }

    #[test]
    fn generated_plan_objects_validate_as_an_inventory() {
        for n in [0, 1, 3, 17] {
            let plan = build_plan(n).expect("plan should build");
            let inventory = plan_as_inventory(&plan);
            assert_eq!(errors(&inventory), Vec::<String>::new(), "at n = {n}");
        }
    }

    #[test]
    fn devices_are_distinct_identities() {
        let inventory = build_inventory(50).expect("inventory should build");
        let devices = devices(&inventory.objects);
        assert_eq!(devices.len(), 50);

        // n devices are n keys, not n copies of one: `duplicate key:
        // dcim.device::{}` is what an empty key produced.
        let keys: BTreeSet<_> = devices
            .iter()
            .map(|device| key_string(&device.key))
            .collect();
        assert_eq!(keys.len(), 50);
        assert!(!keys.contains(r#"{}"#), "a device carries an empty key");
    }

    #[test]
    fn every_object_derives_its_uid_from_its_key() {
        let inventory = build_inventory(30).expect("inventory should build");
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

    #[test]
    fn every_support_object_is_referenced() {
        // an unreferenced pool member is dead weight in every generated file.
        for n in [1, 3, 9, 40] {
            let inventory = build_inventory(n).expect("inventory should build");
            let referenced: BTreeSet<_> = inventory
                .objects
                .iter()
                .flat_map(|object| object.attrs.values())
                .filter_map(|value| value.as_str())
                .filter_map(|raw| Uid::parse_str(raw).ok())
                .collect();
            for object in &inventory.objects {
                if object.type_name.as_str() == DEVICE {
                    continue;
                }
                assert!(
                    referenced.contains(&object.uid),
                    "nothing references {} at n = {n}",
                    key_string(&object.key)
                );
            }
        }
    }

    #[test]
    fn support_pools_are_fixed_so_growth_is_linear() {
        let support = |n| {
            let inventory = build_inventory(n).expect("inventory should build");
            inventory.objects.len() - devices(&inventory.objects).len()
        };
        let pools = (MANUFACTURERS + MODELS + ROLES + SITES) as usize;
        assert_eq!(support(0), 0);
        assert_eq!(support(1), 4);
        assert_eq!(support(1000), pools);
        assert_eq!(support(2000), pools);
    }

    #[test]
    fn build_plan_has_an_op_per_generated_object() {
        let plan = build_plan(7).expect("plan should build");
        let objects = objects(7).expect("objects should build");
        assert_eq!(plan.ops.len(), objects.len());
        assert_eq!(create_ops(&plan).len(), objects.len());
        assert_eq!(devices(&objects).len(), 7);
    }

    #[test]
    fn build_plan_with_zero_ops_is_empty() {
        let plan = build_plan(0).expect("empty plan should build");
        assert!(plan.ops.is_empty());
        let inventory = build_inventory(0).expect("empty inventory should build");
        assert!(inventory.objects.is_empty());
    }

    #[test]
    fn build_plan_carries_the_schema() {
        let plan = build_plan(1).expect("plan should build");
        assert!(plan.schema.types.contains_key(DEVICE));
        // the generator never fills in a summary or a schema preview.
        assert!(plan.summary.is_none());
        assert!(plan.schema_preview.is_none());
    }

    #[test]
    fn build_inventory_carries_the_same_schema_as_the_plan() {
        let plan = build_plan(3).expect("plan should build");
        let inventory = build_inventory(3).expect("inventory should build");
        assert_eq!(inventory.schema, plan.schema);
        assert_eq!(
            inventory.objects,
            objects(3).expect("objects should build"),
            "the two kinds describe the same objects"
        );
    }

    #[test]
    fn every_op_agrees_with_the_object_it_carries() {
        let plan = build_plan(9).expect("plan should build");
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
            let first = render(kind, 5).expect("artifact should render");
            let second = render(kind, 5).expect("artifact should render");
            assert_eq!(first, second, "{kind:?} is not byte-stable");
        }
    }

    /// render an artifact through the write path and read it back.
    fn round_trip(kind: Kind, num_devices: u128) -> String {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("out.json");
        let json = render(kind, num_devices).expect("artifact should render");
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
        let json = render(Kind::Plan, 1).expect("plan should render");
        assert!(write_json(&json, &path).is_err());
    }

    #[test]
    fn output_defaults_follow_the_kind() {
        assert_eq!(Kind::Plan.default_output(), "plan.json");
        assert_eq!(Kind::Inventory.default_output(), "inventory.json");
    }
}
