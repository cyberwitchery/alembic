//! human-readable, plan-framed rendering of a plan's operations (create /
//! update / delete), for the `plan` command's default output. this is the
//! "see before write" view: it reads the plan only and never writes.
//!
//! this is distinct from [`crate::DriftReport`], which presents the same plan
//! as drift (changed / missing / extra, desired-vs-observed). here the framing
//! is the operations apply will perform.

use crate::types::{Op, Plan};
use alembic_core::key_string;
use std::fmt::Write;

/// how many operations to list per category before truncating the tail.
const MAX_LISTED: usize = 50;

/// render a plan's operations as a human-readable summary: a one-line count
/// header, then each op grouped under create / update / delete with its type and
/// human key (and per-field `from -> to` for updates). long categories are
/// truncated with an `... and N more` line so a large plan stays readable.
pub fn render_plan(plan: &Plan) -> String {
    let mut create = Vec::new();
    let mut update = Vec::new();
    let mut delete = Vec::new();
    for op in &plan.ops {
        match op {
            Op::Create {
                type_name, desired, ..
            } => create.push(format!("  {} {}", type_name, key_string(&desired.key))),
            Op::Update {
                type_name,
                desired,
                changes,
                ..
            } => {
                let mut line = format!("  {} {}", type_name, key_string(&desired.key));
                for change in changes {
                    let _ = write!(
                        line,
                        "\n    {}: {} -> {}",
                        change.field, change.from, change.to
                    );
                }
                update.push(line);
            }
            Op::Delete { type_name, key, .. } => {
                delete.push(format!("  {} {}", type_name, key_string(key)))
            }
        }
    }

    let mut out = format!(
        "plan: {} to create, {} to update, {} to delete",
        create.len(),
        update.len(),
        delete.len()
    );
    for (label, lines) in [
        ("create", &create),
        ("update", &update),
        ("delete", &delete),
    ] {
        if lines.is_empty() {
            continue;
        }
        let _ = write!(out, "\n\n{label}:");
        for line in lines.iter().take(MAX_LISTED) {
            let _ = write!(out, "\n{line}");
        }
        if lines.len() > MAX_LISTED {
            let _ = write!(out, "\n  ... and {} more", lines.len() - MAX_LISTED);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{FieldChange, Op, Plan};
    use alembic_core::{Key, Object, Schema, TypeName, Uid};
    use std::collections::BTreeMap;

    fn key(v: &str) -> Key {
        Key::from(BTreeMap::from([(
            "name".to_string(),
            serde_json::Value::String(v.to_string()),
        )]))
    }

    fn object(uid: u128, type_name: &str, k: &str) -> Object {
        Object::new(
            Uid::from_u128(uid),
            TypeName::new(type_name),
            key(k),
            Default::default(),
        )
        .unwrap()
    }

    fn plan_of(ops: Vec<Op>) -> Plan {
        Plan {
            schema: Schema {
                types: BTreeMap::new(),
            },
            ops,
            summary: None,
            schema_preview: None,
        }
    }

    #[test]
    fn renders_each_category_with_keys_and_field_changes() {
        let plan = plan_of(vec![
            Op::Create {
                uid: Uid::from_u128(1),
                type_name: TypeName::new("dcim.device"),
                desired: object(1, "dcim.device", "leaf01"),
            },
            Op::Update {
                uid: Uid::from_u128(2),
                type_name: TypeName::new("dcim.interface"),
                desired: object(2, "dcim.interface", "eth0"),
                changes: vec![FieldChange {
                    field: "type".to_string(),
                    from: serde_json::json!("access"),
                    to: serde_json::json!("trunk"),
                }],
                backend_id: None,
            },
        ]);
        let out = render_plan(&plan);
        assert!(
            out.starts_with("plan: 1 to create, 1 to update, 0 to delete"),
            "{out}"
        );
        assert!(out.contains("create:"));
        assert!(out.contains("dcim.device"));
        assert!(out.contains("update:"));
        assert!(out.contains(r#"type: "access" -> "trunk""#), "{out}");
        assert!(!out.contains("delete:"));
    }

    #[test]
    fn truncates_a_long_category() {
        let ops = (0..MAX_LISTED as u128 + 5)
            .map(|i| Op::Create {
                uid: Uid::from_u128(i),
                type_name: TypeName::new("dcim.device"),
                desired: object(i, "dcim.device", &format!("d{i}")),
            })
            .collect();
        let out = render_plan(&plan_of(ops));
        assert!(out.contains("... and 5 more"), "{out}");
    }
}
