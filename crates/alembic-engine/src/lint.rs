//! linting for retort and projection specs.

use crate::projection::{CustomFieldStrategy, ProjectionSpec};
use crate::retort::{Emit, EmitSpec, EmitUid, Retort};
use alembic_core::Kind;
use serde_yaml::Value as YamlValue;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Default)]
pub struct LintReport {
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
}

impl LintReport {
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }
}

pub fn lint_specs(retort: Option<&Retort>, projection: Option<&ProjectionSpec>) -> LintReport {
    let mut report = LintReport::default();

    let retort_summary = retort.map(retort_summary);
    if let Some(retort) = retort {
        lint_retort_templates(retort, &mut report);
    }
    if let Some(spec) = projection {
        lint_projection_rules(spec, retort_summary.as_ref(), &mut report);
    }
    if let (Some(retort), Some(spec)) = (retort, projection) {
        lint_projection_coverage(retort, spec, &mut report);
    }

    report
}

struct RetortSummary {
    kinds: BTreeSet<String>,
    emitted_x: BTreeMap<String, BTreeSet<String>>,
}

fn retort_summary(retort: &Retort) -> RetortSummary {
    let mut kinds = BTreeSet::new();
    let mut emitted_x: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for rule in &retort.rules {
        for emit in emits_for_rule(rule) {
            let kind = Kind::parse(&emit.kind).as_string();
            kinds.insert(kind.clone());
            if emit.x.is_empty() {
                continue;
            }
            let entry = emitted_x.entry(kind).or_default();
            for key in emit.x.keys() {
                entry.insert(key.clone());
            }
        }
    }

    RetortSummary { kinds, emitted_x }
}

fn emits_for_rule(rule: &crate::retort::Rule) -> Vec<&Emit> {
    match &rule.emit {
        EmitSpec::Single(emit) => vec![emit],
        EmitSpec::Multi(emits) => emits.iter().collect(),
    }
}

fn lint_retort_templates(retort: &Retort, report: &mut LintReport) {
    for rule in &retort.rules {
        let rule_vars: BTreeSet<String> = rule.vars.keys().cloned().collect();
        let uid_vars: BTreeSet<String> = rule
            .uids
            .keys()
            .map(|name| format!("uids.{name}"))
            .collect();

        for emit in emits_for_rule(rule) {
            let mut allowed_vars = rule_vars.clone();
            allowed_vars.extend(emit.vars.keys().cloned());
            allowed_vars.extend(uid_vars.iter().cloned());

            lint_template_string(
                &emit.key,
                &allowed_vars,
                report,
                &format!("retort rule {} emit key", rule.name),
            );

            if let Some(uid_spec) = &emit.uid {
                match uid_spec {
                    EmitUid::Template(template) => lint_template_string(
                        template,
                        &allowed_vars,
                        report,
                        &format!("retort rule {} emit uid", rule.name),
                    ),
                    EmitUid::V5 { v5 } => {
                        lint_template_string(
                            &v5.kind,
                            &allowed_vars,
                            report,
                            &format!("retort rule {} emit uid kind", rule.name),
                        );
                        lint_template_string(
                            &v5.stable,
                            &allowed_vars,
                            report,
                            &format!("retort rule {} emit uid stable", rule.name),
                        );
                    }
                }
            }

            for (key, value) in &emit.attrs {
                lint_template_value(
                    value,
                    &allowed_vars,
                    report,
                    &format!("retort rule {} emit attrs.{key}", rule.name),
                );
            }
            for (key, value) in &emit.x {
                lint_template_value(
                    value,
                    &allowed_vars,
                    report,
                    &format!("retort rule {} emit x.{key}", rule.name),
                );
            }
        }
    }
}

fn lint_projection_rules(
    spec: &ProjectionSpec,
    retort_summary: Option<&RetortSummary>,
    report: &mut LintReport,
) {
    if spec.version != 1 {
        report.errors.push(format!(
            "projection version {} is unsupported",
            spec.version
        ));
    }
    if spec.backend.trim().is_empty() {
        report
            .errors
            .push("projection backend is required".to_string());
    }

    for rule in &spec.rules {
        if rule.on_kind.trim().is_empty() {
            report.errors.push(format!(
                "projection rule {}: on_kind is required",
                rule.name
            ));
        }

        let selector_count = rule.from_x.prefix.is_some() as u8
            + rule.from_x.key.is_some() as u8
            + (!rule.from_x.map.is_empty()) as u8;
        if selector_count != 1 {
            report.errors.push(format!(
                "projection rule {} (kind {}): from_x must include exactly one of prefix, key, or map",
                rule.name, rule.on_kind
            ));
        }

        for transform in &rule.from_x.transform {
            match transform {
                crate::projection::TransformSpec::Simple(name) => {
                    if name != "stringify" && name != "drop_if_null" {
                        report.errors.push(format!(
                            "projection rule {}: unknown transform {name}",
                            rule.name
                        ));
                    }
                }
                crate::projection::TransformSpec::Join { .. }
                | crate::projection::TransformSpec::Default { .. } => {}
            }
        }

        if let Some(custom_fields) = &rule.to.custom_fields {
            if matches!(custom_fields.strategy, CustomFieldStrategy::StripPrefix)
                && custom_fields.prefix.is_none()
                && rule.from_x.prefix.is_none()
            {
                report.errors.push(format!(
                    "projection rule {} (kind {}): missing prefix for strip_prefix",
                    rule.name, rule.on_kind
                ));
            }
        }

        if let Some(local_context) = &rule.to.local_context {
            if rule.on_kind != "dcim.device" {
                report.errors.push(format!(
                    "projection rule {} (kind {}): local_context only supported for dcim.device",
                    rule.name, rule.on_kind
                ));
            }
            if matches!(local_context.strategy, CustomFieldStrategy::StripPrefix)
                && local_context.prefix.is_none()
                && rule.from_x.prefix.is_none()
            {
                report.errors.push(format!(
                    "projection rule {} (kind {}): missing prefix for strip_prefix",
                    rule.name, rule.on_kind
                ));
            }
        }

        if let Some(summary) = retort_summary {
            if rule.on_kind != "*" && !summary.kinds.contains(&rule.on_kind) {
                report.errors.push(format!(
                    "projection rule {} references unknown kind {}",
                    rule.name, rule.on_kind
                ));
            }
        }
    }
}

fn lint_projection_coverage(retort: &Retort, spec: &ProjectionSpec, report: &mut LintReport) {
    let summary = retort_summary(retort);
    for (kind, keys) in summary.emitted_x {
        for key in keys {
            if !projection_consumes_key(spec, &kind, &key) {
                report.warnings.push(format!(
                    "retort emits x key {} for kind {} but no projection rule consumes it",
                    key, kind
                ));
            }
        }
    }
}

fn projection_consumes_key(spec: &ProjectionSpec, kind: &str, key: &str) -> bool {
    spec.rules.iter().any(|rule| {
        if rule.on_kind != "*" && rule.on_kind != kind {
            return false;
        }
        if let Some(prefix) = &rule.from_x.prefix {
            return key.starts_with(prefix);
        }
        if let Some(rule_key) = &rule.from_x.key {
            return key == rule_key;
        }
        if !rule.from_x.map.is_empty() {
            return rule.from_x.map.contains_key(key);
        }
        false
    })
}

fn lint_template_value(
    value: &YamlValue,
    allowed: &BTreeSet<String>,
    report: &mut LintReport,
    context: &str,
) {
    match value {
        YamlValue::String(raw) => lint_template_string(raw, allowed, report, context),
        YamlValue::Sequence(items) => {
            for item in items {
                lint_template_value(item, allowed, report, context);
            }
        }
        YamlValue::Mapping(map) => {
            for (key, value) in map {
                if let YamlValue::String(raw) = key {
                    lint_template_string(raw, allowed, report, context);
                }
                lint_template_value(value, allowed, report, context);
            }
        }
        _ => {}
    }
}

fn lint_template_string(
    raw: &str,
    allowed: &BTreeSet<String>,
    report: &mut LintReport,
    context: &str,
) {
    if !raw.contains("${") {
        return;
    }
    match extract_template_vars(raw) {
        Ok(vars) => {
            for name in vars {
                if !allowed.contains(&name) {
                    report
                        .errors
                        .push(format!("{context}: missing var {name} in template {raw}"));
                }
            }
        }
        Err(message) => report
            .errors
            .push(format!("{context}: {message} in template {raw}")),
    }
}

fn extract_template_vars(template: &str) -> Result<Vec<String>, String> {
    let mut vars = Vec::new();
    let mut rest = template;
    while let Some(start) = rest.find("${") {
        let after_start = &rest[start + 2..];
        let Some(end) = after_start.find('}') else {
            return Err("unterminated template".to_string());
        };
        let name = &after_start[..end];
        if name.trim().is_empty() {
            return Err("empty template var".to_string());
        }
        vars.push(name.to_string());
        rest = &after_start[end + 1..];
    }
    Ok(vars)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml::Value as YamlValue;

    fn parse_retort(raw: &str) -> Retort {
        serde_yaml::from_str::<Retort>(raw).unwrap()
    }

    fn parse_projection(raw: &str) -> ProjectionSpec {
        serde_yaml::from_str::<ProjectionSpec>(raw).unwrap()
    }

    #[test]
    fn lint_reports_missing_template_var() {
        let retort = parse_retort(
            r#"
version: 1
rules:
  - name: devices
    select: devices
    vars:
      name:
        from: name
    emit:
      kind: dcim.device
      key: "device=${missing}"
      attrs: {}
"#,
        );

        let report = lint_specs(Some(&retort), None);
        assert!(!report.errors.is_empty());
        assert!(report.errors[0].contains("missing var missing"));
    }

    #[test]
    fn lint_reports_unknown_projection_kind() {
        let retort = parse_retort(
            r#"
version: 1
rules:
  - name: sites
    select: sites
    emit:
      kind: dcim.site
      key: "site=fra1"
      attrs: {}
"#,
        );
        let projection = parse_projection(
            r#"
version: 1
backend: netbox
rules:
  - name: bad
    on_kind: dcim.rack
    from_x:
      key: foo
    to:
      custom_fields:
        strategy: direct
"#,
        );

        let report = lint_specs(Some(&retort), Some(&projection));
        assert!(report.errors.iter().any(|msg| msg.contains("unknown kind")));
    }

    #[test]
    fn lint_warns_on_unprojected_key() {
        let retort = parse_retort(
            r#"
version: 1
rules:
  - name: sites
    select: sites
    emit:
      kind: dcim.site
      key: "site=fra1"
      attrs: {}
      x:
        model.serial: "x"
"#,
        );
        let projection = parse_projection(
            r#"
version: 1
backend: netbox
rules:
  - name: model
    on_kind: dcim.site
    from_x:
      key: model.asset
    to:
      custom_fields:
        strategy: direct
"#,
        );

        let report = lint_specs(Some(&retort), Some(&projection));
        assert_eq!(report.errors.len(), 0);
        assert_eq!(report.warnings.len(), 1);
    }

    #[test]
    fn lint_reports_missing_prefix_for_strip_prefix() {
        let projection = parse_projection(
            r#"
version: 1
backend: netbox
rules:
  - name: model
    on_kind: dcim.site
    from_x:
      key: model.serial
    to:
      custom_fields:
        strategy: strip_prefix
"#,
        );

        let report = lint_specs(None, Some(&projection));
        assert!(report
            .errors
            .iter()
            .any(|msg| msg.contains("missing prefix")));
    }

    #[test]
    fn lint_template_value_traverses_mapping_keys() {
        let mut map = serde_yaml::Mapping::new();
        map.insert(
            YamlValue::String("name".to_string()),
            YamlValue::String("v".to_string()),
        );
        map.insert(
            YamlValue::String("${missing}".to_string()),
            YamlValue::String("v".to_string()),
        );
        let value = YamlValue::Mapping(map);
        let allowed = BTreeSet::new();
        let mut report = LintReport::default();
        lint_template_value(&value, &allowed, &mut report, "mapping");
        assert_eq!(report.errors.len(), 1);
    }
}
