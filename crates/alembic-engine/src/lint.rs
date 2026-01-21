//! linting for retort and projection specs.

use crate::projection::{CustomFieldStrategy, ProjectionSpec};
use crate::retort::{Emit, EmitSpec, EmitUid, Retort};
use serde_yaml::Value as YamlValue;
use std::collections::BTreeSet;

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

    let retort_types = retort.map(retort_types);
    if let Some(retort) = retort {
        lint_retort_templates(retort, &mut report);
    }
    if let Some(spec) = projection {
        lint_projection_rules(spec, retort_types.as_ref(), &mut report);
    }

    report
}

fn retort_types(retort: &Retort) -> BTreeSet<String> {
    let mut types = BTreeSet::new();
    for rule in &retort.rules {
        for emit in emits_for_rule(rule) {
            types.insert(emit.type_name.clone());
        }
    }
    types
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

            for (key, value) in &emit.key {
                lint_template_value(
                    value,
                    &allowed_vars,
                    report,
                    &format!("retort rule {} emit key.{key}", rule.name),
                );
            }

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
                            &v5.type_name,
                            &allowed_vars,
                            report,
                            &format!("retort rule {} emit uid type", rule.name),
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
        }
    }
}

fn lint_projection_rules(
    spec: &ProjectionSpec,
    retort_types: Option<&BTreeSet<String>>,
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
        if rule.on_type.trim().is_empty() {
            report.errors.push(format!(
                "projection rule {}: on_type is required",
                rule.name
            ));
        }

        let selector_count = rule.from_attrs.prefix.is_some() as u8
            + rule.from_attrs.key.is_some() as u8
            + (!rule.from_attrs.map.is_empty()) as u8;
        if selector_count != 1 {
            report.errors.push(format!(
                "projection rule {} (type {}): from_attrs must include exactly one of prefix, key, or map",
                rule.name, rule.on_type
            ));
        }

        for transform in &rule.from_attrs.transform {
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
                && rule.from_attrs.prefix.is_none()
            {
                report.errors.push(format!(
                    "projection rule {} (type {}): missing prefix for strip_prefix",
                    rule.name, rule.on_type
                ));
            }
        }

        if let Some(local_context) = &rule.to.local_context {
            if matches!(local_context.strategy, CustomFieldStrategy::StripPrefix)
                && local_context.prefix.is_none()
                && rule.from_attrs.prefix.is_none()
            {
                report.errors.push(format!(
                    "projection rule {} (type {}): missing prefix for strip_prefix",
                    rule.name, rule.on_type
                ));
            }
        }

        if let Some(types) = retort_types {
            if rule.on_type != "*" && !types.contains(&rule.on_type) {
                report.errors.push(format!(
                    "projection rule {} references unknown type {}",
                    rule.name, rule.on_type
                ));
            }
        }
    }
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
schema:
  types:
    dcim.device:
      key:
        device:
          type: slug
      fields: {}
rules:
  - name: devices
    select: devices
    vars:
      name:
        from: name
    emit:
      type: dcim.device
      key:
        device: "device=${missing}"
      attrs: {}
"#,
        );

        let report = lint_specs(Some(&retort), None);
        assert!(!report.errors.is_empty());
        assert!(report.errors[0].contains("missing var missing"));
    }

    #[test]
    fn lint_reports_unknown_projection_type() {
        let retort = parse_retort(
            r#"
version: 1
schema:
  types:
    dcim.site:
      key:
        site:
          type: slug
      fields: {}
rules:
  - name: sites
    select: sites
    emit:
      type: dcim.site
      key:
        site: "fra1"
      attrs: {}
"#,
        );
        let projection = parse_projection(
            r#"
version: 1
backend: netbox
rules:
  - name: bad
    on_type: dcim.rack
    from_attrs:
      key: foo
    to:
      custom_fields:
        strategy: direct
"#,
        );

        let report = lint_specs(Some(&retort), Some(&projection));
        assert!(report.errors.iter().any(|msg| msg.contains("unknown type")));
    }

    #[test]
    fn lint_reports_missing_prefix_for_strip_prefix() {
        let projection = parse_projection(
            r#"
version: 1
backend: netbox
rules:
  - name: model
    on_type: dcim.site
    from_attrs:
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

    #[test]
    fn lint_reports_projection_version_and_backend_errors() {
        let projection = parse_projection(
            r#"
version: 2
backend: ""
rules:
  - name: model
    on_type: dcim.site
    from_attrs:
      key: model.serial
    to:
      custom_fields:
        strategy: direct
"#,
        );

        let report = lint_specs(None, Some(&projection));
        assert!(report
            .errors
            .iter()
            .any(|msg| msg.contains("projection version")));
        assert!(report
            .errors
            .iter()
            .any(|msg| msg.contains("projection backend")));
    }

    #[test]
    fn lint_reports_projection_selector_errors() {
        let projection = parse_projection(
            r#"
version: 1
backend: netbox
rules:
  - name: model
    on_type: dcim.site
    from_attrs:
      key: model.serial
      prefix: model.
    to:
      custom_fields:
        strategy: direct
"#,
        );

        let report = lint_specs(None, Some(&projection));
        assert!(report
            .errors
            .iter()
            .any(|msg| msg.contains("from_attrs must include exactly one")));
    }

    #[test]
    fn lint_reports_projection_unknown_transform() {
        let projection = parse_projection(
            r#"
version: 1
backend: netbox
rules:
  - name: model
    on_type: dcim.site
    from_attrs:
      key: model.serial
      transform:
        - unknown
    to:
      custom_fields:
        strategy: direct
"#,
        );

        let report = lint_specs(None, Some(&projection));
        assert!(report
            .errors
            .iter()
            .any(|msg| msg.contains("unknown transform")));
    }

    #[test]
    fn lint_reports_template_parse_errors() {
        assert_eq!(
            extract_template_vars("name=${").unwrap_err(),
            "unterminated template"
        );
        assert_eq!(
            extract_template_vars("name=${}").unwrap_err(),
            "empty template var"
        );
    }
}
