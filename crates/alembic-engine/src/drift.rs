//! read-only drift report: desired-vs-observed, surfaced from a plan.
//!
//! a [`DriftReport`] is the [`Plan`] diff (declared intent vs observed backend
//! state) presented as a standalone, human- and machine-readable report. it is
//! one-way by construction: it only ever describes how observed state diverges
//! from intent and never writes observed state back into the inventory or state
//! store. there is deliberately no "adopt observed" mode.

use crate::types::{
    Adoption, BootstrapReport, FieldChange, Op, Plan, ProvisionReport, SupersededBinding,
};
use alembic_core::{key_string, Key, TypeName};
use serde::{Deserialize, Serialize};
use std::fmt;

/// an object present in both intent and backend, but with diverging fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangedEntry {
    /// object type.
    pub type_name: TypeName,
    /// human key identifying the object.
    pub key: Key,
    /// per-field divergences (field, observed `from`, desired `to`).
    pub changes: Vec<FieldChange>,
}

/// an object that exists on only one side of the diff (missing or extra).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DriftEntry {
    /// object type.
    pub type_name: TypeName,
    /// human key identifying the object.
    pub key: Key,
}

/// read-only report of how observed backend state diverges from declared intent.
///
/// built from a [`Plan`] (the diff already computed by `plan()`): updates become
/// [`changed`](DriftReport::changed), creates become
/// [`missing`](DriftReport::missing) (declared but absent from the backend), and
/// deletes become [`extra`](DriftReport::extra) (present on the backend but not
/// declared), and the plan's schema preview is carried across unchanged. it is
/// a one-way projection only, never a write path.
///
/// `Deserialize` is here so a consumer can read a written report back; it is
/// never a way to feed observed state into an inventory or state store.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DriftReport {
    /// objects present in both intent and backend but with diverging fields.
    pub changed: Vec<ChangedEntry>,
    /// objects declared in intent but absent from the backend.
    pub missing: Vec<DriftEntry>,
    /// objects present in the backend but not declared in intent.
    pub extra: Vec<DriftEntry>,
    /// read-only preview of the schema provisioning apply would perform, carried
    /// from the plan. `None` when the backend cannot preview schema (or was not
    /// asked). not a drift category: it counts towards neither `len` nor
    /// `is_empty`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_preview: Option<ProvisionReport>,
    /// backend objects this run bound to declared uids by key match. identity
    /// memory changed, so the machine-readable report carries it too. not a
    /// drift category: adoption describes identity, not divergence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub adopted: Vec<Adoption>,
    /// identity bindings the run's adoptions superseded.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub superseded: Vec<SupersededBinding>,
}

impl DriftReport {
    /// build a drift report from a plan's operations.
    ///
    /// this reads the plan only; it never mutates the plan, inventory, or any
    /// backend/state, honoring the one-way invariant.
    pub fn from_plan(plan: &Plan) -> Self {
        let mut report = DriftReport::default();
        for op in &plan.ops {
            match op {
                Op::Update {
                    type_name,
                    desired,
                    changes,
                    ..
                } => report.changed.push(ChangedEntry {
                    type_name: type_name.clone(),
                    key: desired.key.clone(),
                    changes: changes.clone(),
                }),
                Op::Create {
                    type_name, desired, ..
                } => report.missing.push(DriftEntry {
                    type_name: type_name.clone(),
                    key: desired.key.clone(),
                }),
                Op::Delete { type_name, key, .. } => report.extra.push(DriftEntry {
                    type_name: type_name.clone(),
                    key: key.clone(),
                }),
            }
        }
        report.schema_preview = plan.schema_preview.clone();
        report
    }

    /// total number of drifted objects across all categories.
    pub fn len(&self) -> usize {
        self.changed.len() + self.missing.len() + self.extra.len()
    }

    /// true when observed state matches declared intent (no drift in any category).
    pub fn is_empty(&self) -> bool {
        self.changed.is_empty() && self.missing.is_empty() && self.extra.is_empty()
    }
}

impl From<&Plan> for DriftReport {
    fn from(plan: &Plan) -> Self {
        DriftReport::from_plan(plan)
    }
}

impl DriftReport {
    /// carry the bootstrap report's identity events into the machine-readable
    /// document, so a written report names what the run adopted.
    pub fn with_bootstrap(mut self, bootstrap: &BootstrapReport) -> Self {
        self.adopted = bootstrap.adoptions.clone();
        self.superseded = bootstrap.superseded.clone();
        self
    }
}

impl fmt::Display for DriftReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            return write!(
                f,
                "no drift: observed backend state matches declared intent"
            );
        }

        write!(
            f,
            "drift report: {} changed, {} missing, {} extra",
            self.changed.len(),
            self.missing.len(),
            self.extra.len()
        )?;

        if !self.changed.is_empty() {
            write!(f, "\n\nchanged (present but diverged):")?;
            for entry in &self.changed {
                write!(f, "\n  {} {}", entry.type_name, key_string(&entry.key))?;
                for change in &entry.changes {
                    write!(
                        f,
                        "\n    {}: {} -> {}",
                        change.field, change.from, change.to
                    )?;
                }
            }
        }

        if !self.missing.is_empty() {
            write!(f, "\n\nmissing (declared in intent, absent from backend):")?;
            for entry in &self.missing {
                write!(f, "\n  {} {}", entry.type_name, key_string(&entry.key))?;
            }
        }

        if !self.extra.is_empty() {
            write!(f, "\n\nextra (present in backend, not declared in intent):")?;
            for entry in &self.extra {
                write!(f, "\n  {} {}", entry.type_name, key_string(&entry.key))?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BackendId, Op, Plan};
    use alembic_core::{JsonMap, Key, Object, Schema, TypeName, Uid};
    use serde_json::json;
    use std::collections::BTreeMap;

    fn make_key(slug: &str) -> Key {
        let mut k = BTreeMap::new();
        k.insert("slug".to_string(), json!(slug));
        Key::from(k)
    }

    fn make_attrs(pairs: &[(&str, serde_json::Value)]) -> JsonMap {
        let mut m = BTreeMap::new();
        for (k, v) in pairs {
            m.insert(k.to_string(), v.clone());
        }
        JsonMap::from(m)
    }

    fn make_object(uid: u128, type_name: &str, slug: &str, attrs: JsonMap) -> Object {
        Object::new(
            Uid::from_u128(uid),
            TypeName::new(type_name),
            make_key(slug),
            attrs,
        )
        .unwrap()
    }

    fn empty_schema() -> Schema {
        Schema {
            types: BTreeMap::new(),
        }
    }

    fn plan_with(ops: Vec<Op>) -> Plan {
        let mut plan = Plan {
            schema: empty_schema(),
            ops,
            summary: None,
            schema_preview: None,
        };
        plan.summary = Some(plan.summary());
        plan
    }

    fn create_op(uid: u128, type_name: &str, slug: &str) -> Op {
        Op::Create {
            uid: Uid::from_u128(uid),
            type_name: TypeName::new(type_name),
            desired: make_object(uid, type_name, slug, make_attrs(&[("name", json!("X"))])),
        }
    }

    fn update_op(uid: u128, type_name: &str, slug: &str, changes: Vec<FieldChange>) -> Op {
        Op::Update {
            uid: Uid::from_u128(uid),
            type_name: TypeName::new(type_name),
            desired: make_object(uid, type_name, slug, make_attrs(&[("name", json!("new"))])),
            changes,
            backend_id: Some(BackendId::Int(100)),
        }
    }

    fn delete_op(uid: u128, type_name: &str, slug: &str) -> Op {
        Op::Delete {
            uid: Uid::from_u128(uid),
            type_name: TypeName::new(type_name),
            key: make_key(slug),
            backend_id: Some(BackendId::Int(200)),
        }
    }

    fn name_change() -> FieldChange {
        FieldChange {
            field: "name".to_string(),
            from: json!("old"),
            to: json!("new"),
        }
    }

    // --- construction ---

    #[test]
    fn empty_plan_yields_empty_report() {
        let report = DriftReport::from_plan(&plan_with(vec![]));
        assert!(report.is_empty());
        assert_eq!(report.len(), 0);
        assert!(report.changed.is_empty());
        assert!(report.missing.is_empty());
        assert!(report.extra.is_empty());
    }

    #[test]
    fn only_changed() {
        let report = DriftReport::from_plan(&plan_with(vec![update_op(
            1,
            "dcim.site",
            "fra1",
            vec![name_change()],
        )]));
        assert!(!report.is_empty());
        assert_eq!(report.len(), 1);
        assert_eq!(report.changed.len(), 1);
        assert!(report.missing.is_empty());
        assert!(report.extra.is_empty());

        let entry = &report.changed[0];
        assert_eq!(entry.type_name, TypeName::new("dcim.site"));
        assert_eq!(entry.key, make_key("fra1"));
        assert_eq!(entry.changes.len(), 1);
        assert_eq!(entry.changes[0].field, "name");
        assert_eq!(entry.changes[0].from, json!("old"));
        assert_eq!(entry.changes[0].to, json!("new"));
    }

    #[test]
    fn only_missing() {
        let report = DriftReport::from_plan(&plan_with(vec![create_op(1, "dcim.site", "ams1")]));
        assert_eq!(report.len(), 1);
        assert!(report.changed.is_empty());
        assert_eq!(report.missing.len(), 1);
        assert!(report.extra.is_empty());

        let entry = &report.missing[0];
        assert_eq!(entry.type_name, TypeName::new("dcim.site"));
        assert_eq!(entry.key, make_key("ams1"));
    }

    #[test]
    fn only_extra() {
        let report =
            DriftReport::from_plan(&plan_with(vec![delete_op(1, "dcim.device", "leaf01")]));
        assert_eq!(report.len(), 1);
        assert!(report.changed.is_empty());
        assert!(report.missing.is_empty());
        assert_eq!(report.extra.len(), 1);

        let entry = &report.extra[0];
        assert_eq!(entry.type_name, TypeName::new("dcim.device"));
        assert_eq!(entry.key, make_key("leaf01"));
    }

    #[test]
    fn mixed_categories() {
        let report = DriftReport::from_plan(&plan_with(vec![
            create_op(1, "dcim.site", "ams1"),
            update_op(2, "dcim.site", "fra1", vec![name_change()]),
            delete_op(3, "dcim.device", "leaf01"),
        ]));
        assert_eq!(report.len(), 3);
        assert_eq!(report.changed.len(), 1);
        assert_eq!(report.missing.len(), 1);
        assert_eq!(report.extra.len(), 1);
    }

    #[test]
    fn no_drift_when_plan_has_only_noop_updates() {
        // an update with no field changes still surfaces as a changed entry with
        // an empty change list; the report mirrors the plan exactly.
        let report =
            DriftReport::from_plan(&plan_with(vec![update_op(1, "dcim.site", "fra1", vec![])]));
        assert_eq!(report.changed.len(), 1);
        assert!(report.changed[0].changes.is_empty());
    }

    #[test]
    fn carries_the_schema_preview_from_the_plan() {
        let mut plan = plan_with(vec![create_op(1, "dcim.site", "ams1")]);
        plan.schema_preview = Some(ProvisionReport {
            created_fields: vec!["dcim.site.asset_tag".to_string()],
            ..Default::default()
        });
        let report = DriftReport::from_plan(&plan);
        assert_eq!(report.schema_preview, plan.schema_preview);
    }

    #[test]
    fn a_plan_without_a_preview_yields_a_report_without_one() {
        let report = DriftReport::from_plan(&plan_with(vec![create_op(1, "dcim.site", "ams1")]));
        assert_eq!(report.schema_preview, None);
    }

    #[test]
    fn the_schema_preview_is_not_a_drift_category() {
        // it describes pending schema work, not object divergence, so a report
        // carrying only a preview is still "no drift".
        let mut plan = plan_with(vec![]);
        plan.schema_preview = Some(ProvisionReport {
            created_fields: vec!["dcim.site.asset_tag".to_string()],
            ..Default::default()
        });
        let report = DriftReport::from_plan(&plan);
        assert!(report.is_empty());
        assert_eq!(report.len(), 0);
        assert_eq!(
            report.to_string(),
            "no drift: observed backend state matches declared intent"
        );
    }

    // --- display ---

    #[test]
    fn display_empty_is_human_readable() {
        let report = DriftReport::default();
        assert_eq!(
            report.to_string(),
            "no drift: observed backend state matches declared intent"
        );
    }

    #[test]
    fn display_groups_by_category() {
        let report = DriftReport::from_plan(&plan_with(vec![
            create_op(1, "dcim.site", "ams1"),
            update_op(2, "dcim.site", "fra1", vec![name_change()]),
            delete_op(3, "dcim.device", "leaf01"),
        ]));
        let text = report.to_string();
        assert!(text.starts_with("drift report: 1 changed, 1 missing, 1 extra"));
        assert!(text.contains("changed (present but diverged):"));
        assert!(text.contains("missing (declared in intent, absent from backend):"));
        assert!(text.contains("extra (present in backend, not declared in intent):"));
        // field diff is rendered as `field: from -> to` with json-rendered values.
        assert!(text.contains("name: \"old\" -> \"new\""));
        // keys are rendered alongside their type.
        assert!(text.contains("dcim.device"));
        assert!(text.contains("leaf01"));
        // no trailing newline, so the cli can println! cleanly.
        assert!(!text.ends_with('\n'));
    }

    #[test]
    fn display_omits_empty_categories() {
        let report = DriftReport::from_plan(&plan_with(vec![create_op(1, "dcim.site", "ams1")]));
        let text = report.to_string();
        assert!(text.contains("missing (declared in intent, absent from backend):"));
        assert!(!text.contains("changed (present but diverged):"));
        assert!(!text.contains("extra (present in backend, not declared in intent):"));
    }

    // --- serialization (machine-readable) ---

    #[test]
    fn serializes_to_json() {
        let report = DriftReport::from_plan(&plan_with(vec![
            create_op(1, "dcim.site", "ams1"),
            update_op(2, "dcim.site", "fra1", vec![name_change()]),
            delete_op(3, "dcim.device", "leaf01"),
        ]));
        let value: serde_json::Value = serde_json::to_value(&report).unwrap();
        assert_eq!(value["changed"].as_array().unwrap().len(), 1);
        assert_eq!(value["missing"].as_array().unwrap().len(), 1);
        assert_eq!(value["extra"].as_array().unwrap().len(), 1);
        assert_eq!(value["changed"][0]["type_name"], "dcim.site");
        assert_eq!(value["changed"][0]["changes"][0]["field"], "name");
        assert_eq!(value["changed"][0]["changes"][0]["from"], "old");
        assert_eq!(value["changed"][0]["changes"][0]["to"], "new");
        assert_eq!(value["missing"][0]["key"]["slug"], "ams1");
        assert_eq!(value["extra"][0]["key"]["slug"], "leaf01");
    }

    #[test]
    fn omits_the_schema_preview_when_the_plan_has_none() {
        // the three categories are the whole document for a backend that cannot
        // preview, exactly as before the preview was carried.
        let raw = serde_json::to_string(&DriftReport::default()).unwrap();
        assert_eq!(raw, r#"{"changed":[],"missing":[],"extra":[]}"#);
    }

    #[test]
    fn writes_an_empty_preview_as_an_empty_object() {
        // present and empty is "nothing to provision", which is the steady state
        // once the backend already carries every declared field; absent above is
        // "cannot preview". the two must not serialize alike.
        let mut plan = plan_with(vec![]);
        plan.schema_preview = Some(ProvisionReport::default());
        let raw = serde_json::to_string(&DriftReport::from_plan(&plan)).unwrap();
        assert_eq!(
            raw,
            r#"{"changed":[],"missing":[],"extra":[],"schema_preview":{}}"#
        );
    }

    #[test]
    fn serializes_the_schema_preview_alongside_the_categories() {
        let mut plan = plan_with(vec![]);
        plan.schema_preview = Some(ProvisionReport {
            created_fields: vec!["dcim.site.asset_tag".to_string()],
            ..Default::default()
        });
        let report = DriftReport::from_plan(&plan);
        let value: serde_json::Value = serde_json::to_value(&report).unwrap();
        assert_eq!(
            value["schema_preview"]["created_fields"][0],
            "dcim.site.asset_tag"
        );

        // and reads back, so a consumer sees what apply would provision.
        let round: DriftReport = serde_json::from_value(value).unwrap();
        assert_eq!(round, report);
    }
}
