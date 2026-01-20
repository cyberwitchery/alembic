//! extraction of canonical inventory from backend state.

use crate::projection::{CustomFieldStrategy, ProjectionSpec};
use crate::types::ObservedObject;
use crate::Adapter;
use alembic_core::{uid_v5, Inventory, Kind, Object};
use anyhow::Result;
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct ExtractReport {
    pub inventory: Inventory,
    pub warnings: Vec<String>,
}

pub async fn extract_inventory(
    adapter: &dyn Adapter,
    projection: Option<&ProjectionSpec>,
) -> Result<ExtractReport> {
    let kinds = vec![
        Kind::DcimSite,
        Kind::DcimDevice,
        Kind::DcimInterface,
        Kind::IpamPrefix,
        Kind::IpamIpAddress,
    ];
    let observed = adapter.observe(&kinds).await?;

    let mut objects: Vec<ObservedObject> = observed.by_key.values().cloned().collect();
    objects.sort_by(|a, b| {
        (a.kind.as_string(), a.key.clone()).cmp(&(b.kind.as_string(), b.key.clone()))
    });

    let mut warnings = Vec::new();
    let mut inventory_objects = Vec::new();
    for object in objects {
        let uid = uid_v5(&object.kind.as_string(), &object.key);
        let x = if let Some(spec) = projection {
            let (x, mut object_warnings) = invert_projection_for_object(spec, &object);
            warnings.append(&mut object_warnings);
            x
        } else {
            BTreeMap::new()
        };
        inventory_objects.push(Object {
            uid,
            kind: object.kind,
            key: object.key,
            attrs: object.attrs,
            x,
        });
    }

    Ok(ExtractReport {
        inventory: Inventory {
            objects: inventory_objects,
        },
        warnings,
    })
}

fn invert_projection_for_object(
    spec: &ProjectionSpec,
    object: &ObservedObject,
) -> (BTreeMap<String, Value>, Vec<String>) {
    let mut warnings = Vec::new();
    let mut x = BTreeMap::new();

    let mut remaining_fields = object.projection.custom_fields.clone().unwrap_or_default();
    let mut tags = object.projection.tags.clone();
    let local_context = object.projection.local_context.clone();

    for rule in &spec.rules {
        if !rule_matches(&rule.on_kind, &object.kind) {
            continue;
        }
        if !rule.from_x.transform.is_empty() {
            warnings.push(format!(
                "projection rule {} (kind {}): transforms are not inverted during extract",
                rule.name, object.kind
            ));
        }

        if let Some(target) = &rule.to.custom_fields {
            invert_custom_fields(
                &mut x,
                &mut remaining_fields,
                &target.strategy,
                target.prefix.as_ref().or(rule.from_x.prefix.as_ref()),
                target.field.as_ref(),
                &rule.from_x.key,
                &rule.from_x.map,
                &rule.name,
                &object.kind,
                &mut warnings,
            );
        }

        if let Some(target) = &rule.to.tags {
            if target.source != "value" {
                warnings.push(format!(
                    "projection rule {} (kind {}): tags source must be 'value'",
                    rule.name, object.kind
                ));
                continue;
            }
            if let Some(current_tags) = tags.take() {
                let tag_value = Value::Array(current_tags.into_iter().map(Value::String).collect());
                if let Some(key) = &rule.from_x.key {
                    insert_x_value(&mut x, key, tag_value, &rule.name, &mut warnings);
                } else if !rule.from_x.map.is_empty() {
                    for key in rule.from_x.map.keys() {
                        insert_x_value(&mut x, key, tag_value.clone(), &rule.name, &mut warnings);
                    }
                } else if let Some(prefix) = &rule.from_x.prefix {
                    let key = format!("{prefix}tags");
                    warnings.push(format!(
                        "projection rule {} (kind {}): inferred tag key {key}",
                        rule.name, object.kind
                    ));
                    insert_x_value(&mut x, &key, tag_value, &rule.name, &mut warnings);
                } else {
                    insert_x_value(&mut x, "tags", tag_value, &rule.name, &mut warnings);
                }
            }
        }

        if let Some(target) = &rule.to.local_context {
            let Some(context_value) = local_context.as_ref() else {
                continue;
            };
            let Some(fields) = extract_root_fields(context_value, &target.root) else {
                continue;
            };
            let mut remaining_context = fields;
            invert_local_context(
                &mut x,
                &mut remaining_context,
                &target.strategy,
                target.prefix.as_ref().or(rule.from_x.prefix.as_ref()),
                &rule.from_x.map,
                &rule.name,
                &object.kind,
                &mut warnings,
            );
        }
    }

    for (field, value) in remaining_fields {
        insert_x_value(&mut x, &field, value, "unmapped", &mut warnings);
    }

    if let Some(unmapped_tags) = tags {
        let tag_value = Value::Array(unmapped_tags.into_iter().map(Value::String).collect());
        insert_x_value(&mut x, "tags", tag_value, "unmapped", &mut warnings);
    }

    (x, warnings)
}

fn rule_matches(on_kind: &str, kind: &Kind) -> bool {
    on_kind == "*" || on_kind == kind.as_string()
}

fn invert_custom_fields(
    x: &mut BTreeMap<String, Value>,
    remaining: &mut BTreeMap<String, Value>,
    strategy: &CustomFieldStrategy,
    prefix: Option<&String>,
    field: Option<&String>,
    rule_key: &Option<String>,
    rule_map: &BTreeMap<String, String>,
    rule: &str,
    kind: &Kind,
    warnings: &mut Vec<String>,
) {
    match strategy {
        CustomFieldStrategy::StripPrefix => {
            let Some(prefix) = prefix else {
                warnings.push(format!(
                    "projection rule {} (kind {}): missing prefix for strip_prefix",
                    rule, kind
                ));
                return;
            };
            let fields: Vec<String> = remaining.keys().cloned().collect();
            for name in fields {
                if let Some(value) = remaining.remove(&name) {
                    let key = format!("{prefix}{name}");
                    insert_x_value(x, &key, value, rule, warnings);
                }
            }
        }
        CustomFieldStrategy::Explicit => {
            let mut inverse = BTreeMap::new();
            for (x_key, field) in rule_map {
                inverse.insert(field.clone(), x_key.clone());
            }
            let fields: Vec<String> = remaining.keys().cloned().collect();
            for name in fields {
                if let Some(x_key) = inverse.get(&name) {
                    if let Some(value) = remaining.remove(&name) {
                        insert_x_value(x, x_key, value, rule, warnings);
                    }
                }
            }
        }
        CustomFieldStrategy::Direct => {
            if let Some(field) = field {
                if let Some(value) = remaining.remove(field) {
                    let key = if let Some(key) = rule_key {
                        key.clone()
                    } else if !rule_map.is_empty() {
                        if rule_map.len() > 1 {
                            warnings.push(format!(
                                "projection rule {} (kind {}): multiple map keys for direct field {field}",
                                rule, kind
                            ));
                        }
                        rule_map
                            .keys()
                            .next()
                            .cloned()
                            .unwrap_or_else(|| field.clone())
                    } else if let Some(prefix) = prefix {
                        format!("{prefix}{field}")
                    } else {
                        field.clone()
                    };
                    insert_x_value(x, &key, value, rule, warnings);
                }
            } else {
                let fields: Vec<String> = remaining.keys().cloned().collect();
                for name in fields {
                    if let Some(value) = remaining.remove(&name) {
                        let key = if let Some(prefix) = prefix {
                            format!("{prefix}{name}")
                        } else {
                            name
                        };
                        insert_x_value(x, &key, value, rule, warnings);
                    }
                }
            }
        }
    }
}

fn invert_local_context(
    x: &mut BTreeMap<String, Value>,
    remaining: &mut BTreeMap<String, Value>,
    strategy: &CustomFieldStrategy,
    prefix: Option<&String>,
    rule_map: &BTreeMap<String, String>,
    rule: &str,
    kind: &Kind,
    warnings: &mut Vec<String>,
) {
    match strategy {
        CustomFieldStrategy::StripPrefix => {
            let Some(prefix) = prefix else {
                warnings.push(format!(
                    "projection rule {} (kind {}): missing prefix for strip_prefix",
                    rule, kind
                ));
                return;
            };
            let fields: Vec<String> = remaining.keys().cloned().collect();
            for name in fields {
                if let Some(value) = remaining.remove(&name) {
                    let key = format!("{prefix}{name}");
                    insert_x_value(x, &key, value, rule, warnings);
                }
            }
        }
        CustomFieldStrategy::Explicit => {
            let mut inverse = BTreeMap::new();
            for (x_key, field) in rule_map {
                inverse.insert(field.clone(), x_key.clone());
            }
            let fields: Vec<String> = remaining.keys().cloned().collect();
            for name in fields {
                if let Some(x_key) = inverse.get(&name) {
                    if let Some(value) = remaining.remove(&name) {
                        insert_x_value(x, x_key, value, rule, warnings);
                    }
                }
            }
        }
        CustomFieldStrategy::Direct => {
            let fields: Vec<String> = remaining.keys().cloned().collect();
            for name in fields {
                if let Some(value) = remaining.remove(&name) {
                    let key = if let Some(prefix) = prefix {
                        format!("{prefix}{name}")
                    } else {
                        name
                    };
                    insert_x_value(x, &key, value, rule, warnings);
                }
            }
        }
    }
}

fn extract_root_fields(value: &Value, root: &str) -> Option<BTreeMap<String, Value>> {
    let mut current = value;
    if root.trim().is_empty() {
        return None;
    }
    for segment in root.split('.') {
        let Value::Object(map) = current else {
            return None;
        };
        current = map.get(segment)?;
    }
    let Value::Object(map) = current else {
        return None;
    };
    Some(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
}

fn insert_x_value(
    x: &mut BTreeMap<String, Value>,
    key: &str,
    value: Value,
    rule: &str,
    warnings: &mut Vec<String>,
) {
    if x.contains_key(key) {
        warnings.push(format!(
            "projection rule {rule}: duplicate x key {key} during extract"
        ));
        return;
    }
    x.insert(key.to_string(), value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ObservedState;
    use crate::ProjectionData;
    use alembic_core::Attrs;
    use async_trait::async_trait;
    use futures::executor::block_on;

    struct MockAdapter {
        observed: ObservedState,
    }

    #[async_trait]
    impl Adapter for MockAdapter {
        async fn observe(&self, _kinds: &[Kind]) -> anyhow::Result<ObservedState> {
            Ok(self.observed.clone())
        }

        async fn apply(&self, _ops: &[crate::Op]) -> anyhow::Result<crate::ApplyReport> {
            unimplemented!("not used in extract tests")
        }
    }

    fn observed_state() -> ObservedState {
        let mut state = ObservedState::default();
        state.insert(ObservedObject {
            kind: Kind::DcimSite,
            key: "site=fra1".to_string(),
            attrs: Attrs::Site(alembic_core::SiteAttrs {
                name: "FRA1".to_string(),
                slug: "fra1".to_string(),
                status: Some("active".to_string()),
                description: None,
            }),
            projection: ProjectionData {
                custom_fields: None,
                tags: None,
                local_context: None,
            },
            backend_id: Some(1),
        });
        state
    }

    #[test]
    fn extract_inventory_uses_stable_uid() {
        let adapter = MockAdapter {
            observed: observed_state(),
        };
        let report = block_on(extract_inventory(&adapter, None)).unwrap();
        assert_eq!(report.inventory.objects.len(), 1);
        let object = &report.inventory.objects[0];
        assert_eq!(object.key, "site=fra1");
        assert_eq!(object.uid, uid_v5("dcim.site", "site=fra1"));
    }

    #[test]
    fn extract_inverts_custom_fields_and_tags() {
        let mut state = ObservedState::default();
        let mut custom_fields = BTreeMap::new();
        custom_fields.insert("serial".to_string(), Value::String("abc".to_string()));
        state.insert(ObservedObject {
            kind: Kind::DcimDevice,
            key: "device=leaf01".to_string(),
            attrs: Attrs::Device(alembic_core::DeviceAttrs {
                name: "leaf01".to_string(),
                site: uid_v5("dcim.site", "site=fra1"),
                role: "leaf".to_string(),
                device_type: "leaf-switch".to_string(),
                status: Some("active".to_string()),
            }),
            projection: ProjectionData {
                custom_fields: Some(custom_fields),
                tags: Some(vec!["fabric".to_string()]),
                local_context: None,
            },
            backend_id: Some(2),
        });
        let adapter = MockAdapter { observed: state };
        let projection: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: model
    on_kind: dcim.device
    from_x:
      prefix: "model."
    to:
      custom_fields:
        strategy: strip_prefix
        prefix: "model."
  - name: tags
    on_kind: dcim.device
    from_x:
      key: "model.tags"
    to:
      tags:
        source: value
"#,
        )
        .unwrap();
        let report = block_on(extract_inventory(&adapter, Some(&projection))).unwrap();
        let object = &report.inventory.objects[0];
        assert_eq!(
            object.x.get("model.serial"),
            Some(&Value::String("abc".to_string()))
        );
        assert_eq!(
            object.x.get("model.tags"),
            Some(&Value::Array(vec![Value::String("fabric".to_string())]))
        );
    }

    #[test]
    fn extract_preserves_unmapped_custom_fields() {
        let mut state = ObservedState::default();
        let mut custom_fields = BTreeMap::new();
        custom_fields.insert("owner".to_string(), Value::String("infra".to_string()));
        state.insert(ObservedObject {
            kind: Kind::DcimSite,
            key: "site=fra1".to_string(),
            attrs: Attrs::Site(alembic_core::SiteAttrs {
                name: "FRA1".to_string(),
                slug: "fra1".to_string(),
                status: Some("active".to_string()),
                description: None,
            }),
            projection: ProjectionData {
                custom_fields: Some(custom_fields),
                tags: None,
                local_context: None,
            },
            backend_id: Some(1),
        });
        let adapter = MockAdapter { observed: state };
        let projection: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: noop
    on_kind: dcim.site
    from_x:
      key: "model.serial"
    to:
      custom_fields:
        strategy: direct
"#,
        )
        .unwrap();
        let report = block_on(extract_inventory(&adapter, Some(&projection))).unwrap();
        let object = &report.inventory.objects[0];
        assert_eq!(
            object.x.get("owner"),
            Some(&Value::String("infra".to_string()))
        );
    }

    #[test]
    fn extract_warns_on_transforms() {
        let mut state = ObservedState::default();
        let mut custom_fields = BTreeMap::new();
        custom_fields.insert("serial".to_string(), Value::String("abc".to_string()));
        state.insert(ObservedObject {
            kind: Kind::DcimDevice,
            key: "device=leaf01".to_string(),
            attrs: Attrs::Device(alembic_core::DeviceAttrs {
                name: "leaf01".to_string(),
                site: uid_v5("dcim.site", "site=fra1"),
                role: "leaf".to_string(),
                device_type: "leaf-switch".to_string(),
                status: Some("active".to_string()),
            }),
            projection: ProjectionData {
                custom_fields: Some(custom_fields),
                tags: None,
                local_context: None,
            },
            backend_id: Some(2),
        });
        let adapter = MockAdapter { observed: state };
        let projection: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: model
    on_kind: dcim.device
    from_x:
      prefix: "model."
      transform:
        - stringify
    to:
      custom_fields:
        strategy: strip_prefix
        prefix: "model."
"#,
        )
        .unwrap();
        let report = block_on(extract_inventory(&adapter, Some(&projection))).unwrap();
        assert!(report
            .warnings
            .iter()
            .any(|warn| warn.contains("transforms are not inverted")));
    }

    #[test]
    fn extract_inverts_local_context() {
        let mut state = ObservedState::default();
        let mut local_map = serde_json::Map::new();
        local_map.insert("role".to_string(), Value::String("leaf".to_string()));
        let mut root = serde_json::Map::new();
        root.insert("system".to_string(), Value::Object(local_map));
        state.insert(ObservedObject {
            kind: Kind::DcimDevice,
            key: "device=leaf01".to_string(),
            attrs: Attrs::Device(alembic_core::DeviceAttrs {
                name: "leaf01".to_string(),
                site: uid_v5("dcim.site", "site=fra1"),
                role: "leaf".to_string(),
                device_type: "leaf-switch".to_string(),
                status: Some("active".to_string()),
            }),
            projection: ProjectionData {
                custom_fields: None,
                tags: None,
                local_context: Some(Value::Object(root)),
            },
            backend_id: Some(2),
        });
        let adapter = MockAdapter { observed: state };
        let projection: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: local
    on_kind: dcim.device
    from_x:
      prefix: "context."
    to:
      local_context:
        root: "system"
        strategy: strip_prefix
        prefix: "context."
"#,
        )
        .unwrap();
        let report = block_on(extract_inventory(&adapter, Some(&projection))).unwrap();
        let object = &report.inventory.objects[0];
        assert_eq!(
            object.x.get("context.role"),
            Some(&Value::String("leaf".to_string()))
        );
    }

    #[test]
    fn extract_warns_on_duplicate_x_key() {
        let mut state = ObservedState::default();
        let mut custom_fields = BTreeMap::new();
        custom_fields.insert("field_a".to_string(), Value::String("a".to_string()));
        custom_fields.insert("field_b".to_string(), Value::String("b".to_string()));
        state.insert(ObservedObject {
            kind: Kind::DcimSite,
            key: "site=fra1".to_string(),
            attrs: Attrs::Site(alembic_core::SiteAttrs {
                name: "FRA1".to_string(),
                slug: "fra1".to_string(),
                status: Some("active".to_string()),
                description: None,
            }),
            projection: ProjectionData {
                custom_fields: Some(custom_fields),
                tags: None,
                local_context: None,
            },
            backend_id: Some(1),
        });
        let adapter = MockAdapter { observed: state };
        let projection: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: map_a
    on_kind: dcim.site
    from_x:
      map:
        model.same: field_a
    to:
      custom_fields:
        strategy: explicit
  - name: map_b
    on_kind: dcim.site
    from_x:
      map:
        model.same: field_b
    to:
      custom_fields:
        strategy: explicit
"#,
        )
        .unwrap();
        let report = block_on(extract_inventory(&adapter, Some(&projection))).unwrap();
        assert!(report
            .warnings
            .iter()
            .any(|warn| warn.contains("duplicate x key")));
    }

    #[test]
    fn extract_warns_on_direct_multiple_map_keys() {
        let mut state = ObservedState::default();
        let mut custom_fields = BTreeMap::new();
        custom_fields.insert("serial".to_string(), Value::String("abc".to_string()));
        state.insert(ObservedObject {
            kind: Kind::DcimDevice,
            key: "device=leaf01".to_string(),
            attrs: Attrs::Device(alembic_core::DeviceAttrs {
                name: "leaf01".to_string(),
                site: uid_v5("dcim.site", "site=fra1"),
                role: "leaf".to_string(),
                device_type: "leaf-switch".to_string(),
                status: Some("active".to_string()),
            }),
            projection: ProjectionData {
                custom_fields: Some(custom_fields),
                tags: None,
                local_context: None,
            },
            backend_id: Some(2),
        });
        let adapter = MockAdapter { observed: state };
        let projection: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: model
    on_kind: dcim.device
    from_x:
      map:
        model.serial: serial
        model.serial_v2: serial
    to:
      custom_fields:
        strategy: direct
        field: serial
"#,
        )
        .unwrap();
        let report = block_on(extract_inventory(&adapter, Some(&projection))).unwrap();
        assert!(report
            .warnings
            .iter()
            .any(|warn| warn.contains("multiple map keys")));
    }

    #[test]
    fn extract_infers_tag_key_from_prefix() {
        let mut state = ObservedState::default();
        state.insert(ObservedObject {
            kind: Kind::DcimDevice,
            key: "device=leaf01".to_string(),
            attrs: Attrs::Device(alembic_core::DeviceAttrs {
                name: "leaf01".to_string(),
                site: uid_v5("dcim.site", "site=fra1"),
                role: "leaf".to_string(),
                device_type: "leaf-switch".to_string(),
                status: Some("active".to_string()),
            }),
            projection: ProjectionData {
                custom_fields: None,
                tags: Some(vec!["fabric".to_string()]),
                local_context: None,
            },
            backend_id: Some(2),
        });
        let adapter = MockAdapter { observed: state };
        let projection: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: tags
    on_kind: dcim.device
    from_x:
      prefix: "model."
    to:
      tags:
        source: value
"#,
        )
        .unwrap();
        let report = block_on(extract_inventory(&adapter, Some(&projection))).unwrap();
        let object = &report.inventory.objects[0];
        assert_eq!(
            object.x.get("model.tags"),
            Some(&Value::Array(vec![Value::String("fabric".to_string())]))
        );
        assert!(report
            .warnings
            .iter()
            .any(|warn| warn.contains("inferred tag key")));
    }

    #[test]
    fn extract_inserts_tags_with_default_key() {
        let mut state = ObservedState::default();
        state.insert(ObservedObject {
            kind: Kind::DcimDevice,
            key: "device=leaf01".to_string(),
            attrs: Attrs::Device(alembic_core::DeviceAttrs {
                name: "leaf01".to_string(),
                site: uid_v5("dcim.site", "site=fra1"),
                role: "leaf".to_string(),
                device_type: "leaf-switch".to_string(),
                status: Some("active".to_string()),
            }),
            projection: ProjectionData {
                custom_fields: None,
                tags: Some(vec!["fabric".to_string()]),
                local_context: None,
            },
            backend_id: Some(2),
        });
        let adapter = MockAdapter { observed: state };
        let projection: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: tags
    on_kind: dcim.device
    from_x:
      key: tags
    to:
      tags:
        source: value
"#,
        )
        .unwrap();
        let report = block_on(extract_inventory(&adapter, Some(&projection))).unwrap();
        let object = &report.inventory.objects[0];
        assert_eq!(
            object.x.get("tags"),
            Some(&Value::Array(vec![Value::String("fabric".to_string())]))
        );
    }

    #[test]
    fn extract_warns_on_non_value_tag_source() {
        let mut state = ObservedState::default();
        state.insert(ObservedObject {
            kind: Kind::DcimDevice,
            key: "device=leaf01".to_string(),
            attrs: Attrs::Device(alembic_core::DeviceAttrs {
                name: "leaf01".to_string(),
                site: uid_v5("dcim.site", "site=fra1"),
                role: "leaf".to_string(),
                device_type: "leaf-switch".to_string(),
                status: Some("active".to_string()),
            }),
            projection: ProjectionData {
                custom_fields: None,
                tags: Some(vec!["fabric".to_string()]),
                local_context: None,
            },
            backend_id: Some(2),
        });
        let adapter = MockAdapter { observed: state };
        let projection: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: tags
    on_kind: dcim.device
    from_x:
      key: tags
    to:
      tags:
        source: key
"#,
        )
        .unwrap();
        let report = block_on(extract_inventory(&adapter, Some(&projection))).unwrap();
        assert!(report
            .warnings
            .iter()
            .any(|warn| warn.contains("tags source must be 'value'")));
        let object = &report.inventory.objects[0];
        assert_eq!(
            object.x.get("tags"),
            Some(&Value::Array(vec![Value::String("fabric".to_string())]))
        );
    }

    #[test]
    fn extract_warns_on_missing_strip_prefix_for_custom_fields() {
        let mut state = ObservedState::default();
        let mut custom_fields = BTreeMap::new();
        custom_fields.insert("serial".to_string(), Value::String("abc".to_string()));
        state.insert(ObservedObject {
            kind: Kind::DcimDevice,
            key: "device=leaf01".to_string(),
            attrs: Attrs::Device(alembic_core::DeviceAttrs {
                name: "leaf01".to_string(),
                site: uid_v5("dcim.site", "site=fra1"),
                role: "leaf".to_string(),
                device_type: "leaf-switch".to_string(),
                status: Some("active".to_string()),
            }),
            projection: ProjectionData {
                custom_fields: Some(custom_fields),
                tags: None,
                local_context: None,
            },
            backend_id: Some(2),
        });
        let adapter = MockAdapter { observed: state };
        let projection: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: model
    on_kind: dcim.device
    from_x:
      key: model.serial
    to:
      custom_fields:
        strategy: strip_prefix
"#,
        )
        .unwrap();
        let report = block_on(extract_inventory(&adapter, Some(&projection))).unwrap();
        assert!(report
            .warnings
            .iter()
            .any(|warn| warn.contains("missing prefix")));
        let object = &report.inventory.objects[0];
        assert_eq!(
            object.x.get("serial"),
            Some(&Value::String("abc".to_string()))
        );
    }

    #[test]
    fn extract_warns_on_missing_strip_prefix_for_local_context() {
        let mut state = ObservedState::default();
        let mut local_map = serde_json::Map::new();
        local_map.insert("role".to_string(), Value::String("leaf".to_string()));
        let mut root = serde_json::Map::new();
        root.insert("system".to_string(), Value::Object(local_map));
        state.insert(ObservedObject {
            kind: Kind::DcimDevice,
            key: "device=leaf01".to_string(),
            attrs: Attrs::Device(alembic_core::DeviceAttrs {
                name: "leaf01".to_string(),
                site: uid_v5("dcim.site", "site=fra1"),
                role: "leaf".to_string(),
                device_type: "leaf-switch".to_string(),
                status: Some("active".to_string()),
            }),
            projection: ProjectionData {
                custom_fields: None,
                tags: None,
                local_context: Some(Value::Object(root)),
            },
            backend_id: Some(2),
        });
        let adapter = MockAdapter { observed: state };
        let projection: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: local
    on_kind: dcim.device
    from_x:
      key: context.role
    to:
      local_context:
        root: "system"
        strategy: strip_prefix
"#,
        )
        .unwrap();
        let report = block_on(extract_inventory(&adapter, Some(&projection))).unwrap();
        assert!(report
            .warnings
            .iter()
            .any(|warn| warn.contains("missing prefix")));
        let object = &report.inventory.objects[0];
        assert!(object.x.is_empty());
    }
}
