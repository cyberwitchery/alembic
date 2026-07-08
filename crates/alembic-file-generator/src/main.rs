//! generate files for testing (e.g. a huge plan.json)

use alembic_core::{JsonMap, Key, Object, Schema, TypeName, Uid};
use alembic_engine::{Op, Plan};
use anyhow::{Context, Result};
use clap::Parser;
use serde_json::json;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

/// the type name every generated object uses.
const DEVICE_TYPE: &str = "dcim.device";

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// The number of operations (objects) to include in the plan.
    #[clap(short, long, default_value = "10")]
    num_ops: u128,
    /// Output file
    #[clap(short, long, default_value = "plan.json")]
    output: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let plan = build_plan(args.num_ops)?;
    write_plan(&plan, &args.output)
}

/// build a plan of `num_ops` create operations, one per synthetic `dcim.device`.
fn build_plan(num_ops: u128) -> Result<Plan> {
    let ops = (0..num_ops).map(device_create_op).collect();
    Ok(Plan {
        schema: schema()?,
        ops,
        summary: None,
        schema_preview: None,
    })
}

/// serialize `plan` as pretty JSON and write it to `path`.
fn write_plan(plan: &Plan, path: &Path) -> Result<()> {
    let json = serde_json::to_string_pretty(plan).context("serializing plan")?;
    fs::write(path, json).with_context(|| format!("writing plan to {}", path.display()))
}

/// build a single `Create` op for a `dcim.device` named `device_{i}`.
fn device_create_op(i: u128) -> Op {
    let uid = Uid::from_u128(i);
    let type_name = TypeName::new(DEVICE_TYPE);
    let attrs = BTreeMap::from([("name".to_string(), json!(format!("device_{i}")))]);
    Op::Create {
        uid,
        type_name: type_name.clone(),
        desired: Object {
            uid,
            type_name,
            key: Key::default(),
            attrs: JsonMap::from(attrs),
            source: None,
        },
    }
}

/// the schema shared by every generated plan.
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
          type: string
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

    fn create_ops(plan: &Plan) -> Vec<&Op> {
        plan.ops
            .iter()
            .filter(|op| matches!(op, Op::Create { .. }))
            .collect()
    }

    #[test]
    fn embedded_schema_parses() {
        let schema = schema().expect("embedded schema should parse");
        // the schema defines the five dcim types the generator references.
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
    fn build_plan_has_requested_op_count() {
        let plan = build_plan(7).expect("plan should build");
        assert_eq!(plan.ops.len(), 7);
        assert_eq!(create_ops(&plan).len(), 7);
    }

    #[test]
    fn build_plan_with_zero_ops_is_empty() {
        let plan = build_plan(0).expect("empty plan should build");
        assert!(plan.ops.is_empty());
    }

    #[test]
    fn build_plan_carries_the_schema() {
        let plan = build_plan(1).expect("plan should build");
        assert!(plan.schema.types.contains_key(DEVICE_TYPE));
        // the generator never fills in a summary or a schema preview.
        assert!(plan.summary.is_none());
        assert!(plan.schema_preview.is_none());
    }

    #[test]
    fn device_op_is_a_create_with_derived_uid_and_name() {
        let op = device_create_op(42);
        let Op::Create {
            uid,
            type_name,
            desired,
        } = &op
        else {
            panic!("expected a create op, got {op:?}");
        };
        assert_eq!(*uid, Uid::from_u128(42));
        assert_eq!(*type_name, TypeName::new(DEVICE_TYPE));
        assert_eq!(desired.uid, *uid);
        assert_eq!(desired.type_name, *type_name);
        assert_eq!(desired.attrs.get("name"), Some(&json!("device_42")));
    }

    #[test]
    fn uids_are_distinct_and_stable_across_runs() {
        let first = build_plan(5).expect("plan should build");
        let second = build_plan(5).expect("plan should build");

        let uid_of = |op: &Op| match op {
            Op::Create { uid, .. } => *uid,
            _ => panic!("expected create op"),
        };

        let first_uids: Vec<_> = first.ops.iter().map(uid_of).collect();
        let second_uids: Vec<_> = second.ops.iter().map(uid_of).collect();

        // generation is deterministic: same input, same uids.
        assert_eq!(first_uids, second_uids);
        // and every uid within a plan is unique.
        let mut sorted = first_uids.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), first_uids.len());
    }

    #[test]
    fn write_plan_emits_valid_json_that_round_trips() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("plan.json");

        let plan = build_plan(3).expect("plan should build");
        write_plan(&plan, &path).expect("plan should write");

        let written = fs::read_to_string(&path).expect("output should exist");
        let parsed: Plan =
            serde_json::from_str(&written).expect("output should be valid plan json");
        assert_eq!(parsed.ops.len(), 3);
        assert_eq!(create_ops(&parsed).len(), 3);
    }

    #[test]
    fn write_plan_errors_on_unwritable_path() {
        let dir = tempfile::tempdir().expect("temp dir");
        // a path whose parent directory does not exist cannot be written.
        let path = dir.path().join("does/not/exist/plan.json");
        let plan = build_plan(1).expect("plan should build");
        assert!(write_plan(&plan, &path).is_err());
    }
}
