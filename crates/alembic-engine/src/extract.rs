//! extraction of canonical inventory from backend state.

use crate::projection::{CustomFieldStrategy, ProjectionSpec};
use crate::types::ObservedObject;
use crate::Adapter;
use alembic_core::{key_string, uid_v5, Inventory, JsonMap, Object, Schema, TypeName};
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
    schema: &Schema,
    projection: Option<&ProjectionSpec>,
) -> Result<ExtractReport> {
    let types = projection
        .map(|spec| {
            spec.rules
                .iter()
                .filter_map(|rule| {
                    if rule.on_type.trim().is_empty() || rule.on_type == "*" {
                        None
                    } else {
                        Some(TypeName::new(rule.on_type.clone()))
                    }
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let observed = adapter.observe(schema, &types).await?;

    let mut objects: Vec<ObservedObject> = observed.by_key.values().cloned().collect();
    objects.sort_by(|a, b| {
        (a.type_name.as_str().to_string(), key_string(&a.key))
            .cmp(&(b.type_name.as_str().to_string(), key_string(&b.key)))
    });

    let mut warnings = Vec::new();
    let mut inventory_objects = Vec::new();
    for object in objects {
        let uid = uid_v5(object.type_name.as_str(), &key_string(&object.key));
        let attrs_extra = if let Some(spec) = projection {
            let (attrs_extra, mut object_warnings) = invert_projection_for_object(spec, &object);
            warnings.append(&mut object_warnings);
            attrs_extra
        } else {
            JsonMap::default()
        };
        let mut attrs = object.attrs;
        merge_attrs(
            &mut attrs,
            attrs_extra,
            &object.type_name,
            &key_string(&object.key),
            &mut warnings,
        );
        inventory_objects.push(Object {
            uid,
            type_name: object.type_name,
            key: object.key,
            attrs,
        });
    }

    Ok(ExtractReport {
        inventory: Inventory {
            schema: schema.clone(),
            objects: inventory_objects,
        },
        warnings,
    })
}

fn invert_projection_for_object(
    spec: &ProjectionSpec,
    object: &ObservedObject,
) -> (JsonMap, Vec<String>) {
    let mut warnings = Vec::new();
    let mut attrs_extra = JsonMap::default();

    let mut remaining_fields = object.projection.custom_fields.clone().unwrap_or_default();
    let mut tags = object.projection.tags.clone();
    let local_context = object.projection.local_context.clone();

    for rule in &spec.rules {
        if !rule_matches(&rule.on_type, &object.type_name) {
            continue;
        }
        if !rule.from_attrs.transform.is_empty() {
            warnings.push(format!(
                "projection rule {} (type {}): transforms are not inverted during extract",
                rule.name, object.type_name
            ));
        }

        if let Some(target) = &rule.to.custom_fields {
            invert_custom_fields(
                &mut attrs_extra,
                &mut remaining_fields,
                &target.strategy,
                target.prefix.as_ref().or(rule.from_attrs.prefix.as_ref()),
                target.field.as_ref(),
                &rule.from_attrs.key,
                &rule.from_attrs.map,
                &rule.name,
                &object.type_name,
                &mut warnings,
            );
        }

        if let Some(target) = &rule.to.tags {
            if target.source != "value" {
                warnings.push(format!(
                    "projection rule {} (type {}): tags source must be 'value'",
                    rule.name, object.type_name
                ));
                continue;
            }
            if let Some(current_tags) = tags.take() {
                let tag_value = Value::Array(current_tags.into_iter().map(Value::String).collect());
                if let Some(key) = &rule.from_attrs.key {
                    insert_attr_value(&mut attrs_extra, key, tag_value, &rule.name, &mut warnings);
                } else if !rule.from_attrs.map.is_empty() {
                    for key in rule.from_attrs.map.keys() {
                        insert_attr_value(
                            &mut attrs_extra,
                            key,
                            tag_value.clone(),
                            &rule.name,
                            &mut warnings,
                        );
                    }
                } else if let Some(prefix) = &rule.from_attrs.prefix {
                    let key = format!("{prefix}tags");
                    warnings.push(format!(
                        "projection rule {} (type {}): inferred tag key {key}",
                        rule.name, object.type_name
                    ));
                    insert_attr_value(&mut attrs_extra, &key, tag_value, &rule.name, &mut warnings);
                } else {
                    insert_attr_value(
                        &mut attrs_extra,
                        "tags",
                        tag_value,
                        &rule.name,
                        &mut warnings,
                    );
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
                &mut attrs_extra,
                &mut remaining_context,
                &target.strategy,
                target.prefix.as_ref().or(rule.from_attrs.prefix.as_ref()),
                &rule.from_attrs.map,
                &rule.name,
                &object.type_name,
                &mut warnings,
            );
        }
    }

    for (field, value) in remaining_fields {
        insert_attr_value(&mut attrs_extra, &field, value, "unmapped", &mut warnings);
    }

    if let Some(unmapped_tags) = tags {
        let tag_value = Value::Array(unmapped_tags.into_iter().map(Value::String).collect());
        insert_attr_value(
            &mut attrs_extra,
            "tags",
            tag_value,
            "unmapped",
            &mut warnings,
        );
    }

    (attrs_extra, warnings)
}

fn rule_matches(on_type: &str, type_name: &TypeName) -> bool {
    on_type == "*" || on_type == type_name.as_str()
}

#[allow(clippy::too_many_arguments)]
fn invert_custom_fields(
    attrs_extra: &mut JsonMap,
    remaining: &mut BTreeMap<String, Value>,
    strategy: &CustomFieldStrategy,
    prefix: Option<&String>,
    field: Option<&String>,
    rule_key: &Option<String>,
    rule_map: &BTreeMap<String, String>,
    rule: &str,
    type_name: &TypeName,
    warnings: &mut Vec<String>,
) {
    match strategy {
        CustomFieldStrategy::StripPrefix => {
            let Some(prefix) = prefix else {
                warnings.push(format!(
                    "projection rule {} (type {}): missing prefix for strip_prefix",
                    rule, type_name
                ));
                return;
            };
            let fields: Vec<String> = remaining.keys().cloned().collect();
            for name in fields {
                if let Some(value) = remaining.remove(&name) {
                    let key = format!("{prefix}{name}");
                    insert_attr_value(attrs_extra, &key, value, rule, warnings);
                }
            }
        }
        CustomFieldStrategy::Explicit => {
            let mut inverse = BTreeMap::new();
            for (attr_key, field) in rule_map {
                inverse.insert(field.clone(), attr_key.clone());
            }
            let fields: Vec<String> = remaining.keys().cloned().collect();
            for name in fields {
                if let Some(attr_key) = inverse.get(&name) {
                    if let Some(value) = remaining.remove(&name) {
                        insert_attr_value(attrs_extra, attr_key, value, rule, warnings);
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
                                "projection rule {} (type {}): multiple map keys for direct field {field}",
                                rule, type_name
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
                    insert_attr_value(attrs_extra, &key, value, rule, warnings);
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
                        insert_attr_value(attrs_extra, &key, value, rule, warnings);
                    }
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn invert_local_context(
    attrs_extra: &mut JsonMap,
    remaining: &mut BTreeMap<String, Value>,
    strategy: &CustomFieldStrategy,
    prefix: Option<&String>,
    rule_map: &BTreeMap<String, String>,
    rule: &str,
    type_name: &TypeName,
    warnings: &mut Vec<String>,
) {
    match strategy {
        CustomFieldStrategy::StripPrefix => {
            let Some(prefix) = prefix else {
                warnings.push(format!(
                    "projection rule {} (type {}): missing prefix for strip_prefix",
                    rule, type_name
                ));
                return;
            };
            let fields: Vec<String> = remaining.keys().cloned().collect();
            for name in fields {
                if let Some(value) = remaining.remove(&name) {
                    let key = format!("{prefix}{name}");
                    insert_attr_value(attrs_extra, &key, value, rule, warnings);
                }
            }
        }
        CustomFieldStrategy::Explicit => {
            let mut inverse = BTreeMap::new();
            for (attr_key, field) in rule_map {
                inverse.insert(field.clone(), attr_key.clone());
            }
            let fields: Vec<String> = remaining.keys().cloned().collect();
            for name in fields {
                if let Some(attr_key) = inverse.get(&name) {
                    if let Some(value) = remaining.remove(&name) {
                        insert_attr_value(attrs_extra, attr_key, value, rule, warnings);
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
                    insert_attr_value(attrs_extra, &key, value, rule, warnings);
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

fn insert_attr_value(
    attrs_extra: &mut JsonMap,
    key: &str,
    value: Value,
    rule: &str,
    warnings: &mut Vec<String>,
) {
    if attrs_extra.contains_key(key) {
        warnings.push(format!(
            "projection rule {rule}: duplicate attrs key {key} during extract"
        ));
        return;
    }
    attrs_extra.insert(key.to_string(), value);
}

fn merge_attrs(
    attrs: &mut JsonMap,
    attrs_extra: JsonMap,
    type_name: &TypeName,
    key: &str,
    warnings: &mut Vec<String>,
) {
    for (attr_key, value) in attrs_extra.0 {
        if attrs.contains_key(&attr_key) {
            warnings.push(format!(
                "extract: duplicate attrs key {attr_key} for type {type_name} key {key}"
            ));
            continue;
        }
        attrs.insert(attr_key, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BackendId, ObservedState};
    use crate::ProjectionData;
    use alembic_core::{FieldSchema, FieldType, JsonMap, Key, Schema, TypeName, TypeSchema, Uid};
    use async_trait::async_trait;
    use futures::executor::block_on;
    use serde_json::json;
    use std::collections::BTreeMap;

    struct MockAdapter {
        observed: ObservedState,
    }

    #[async_trait]
    impl Adapter for MockAdapter {
        async fn observe(
            &self,
            _schema: &Schema,
            _types: &[TypeName],
        ) -> anyhow::Result<ObservedState> {
            Ok(self.observed.clone())
        }

        async fn apply(
            &self,
            _schema: &Schema,
            _ops: &[crate::Op],
        ) -> anyhow::Result<crate::ApplyReport> {
            unimplemented!("not used in extract tests")
        }
    }

    fn observed_state() -> ObservedState {
        let mut state = ObservedState::default();
        state.insert(ObservedObject {
            type_name: TypeName::new("dcim.site"),
            key: key_str("site=fra1"),
            attrs: attrs_map(json!({
                "name": "FRA1",
                "slug": "fra1",
                "status": "active"
            })),
            projection: ProjectionData {
                custom_fields: None,
                tags: None,
                local_context: None,
            },
            backend_id: Some(BackendId::Int(1)),
        });
        state
    }

    fn key_str(raw: &str) -> Key {
        let mut map = BTreeMap::new();
        for segment in raw.split('/') {
            let (field, value) = segment
                .split_once('=')
                .unwrap_or_else(|| panic!("invalid key segment: {segment}"));
            map.insert(
                field.to_string(),
                serde_json::Value::String(value.to_string()),
            );
        }
        Key::from(map)
    }

    fn attrs_map(value: serde_json::Value) -> JsonMap {
        let serde_json::Value::Object(map) = value else {
            panic!("attrs must be a json object");
        };
        map.into_iter().collect::<BTreeMap<_, _>>().into()
    }

    fn schema_for_observed(state: &ObservedState) -> Schema {
        let mut types: BTreeMap<String, TypeSchema> = BTreeMap::new();
        for object in state.by_key.values() {
            let entry = types
                .entry(object.type_name.as_str().to_string())
                .or_insert_with(|| TypeSchema {
                    key: BTreeMap::new(),
                    fields: BTreeMap::new(),
                });
            for field in object.key.keys() {
                entry.key.entry(field.clone()).or_insert(FieldSchema {
                    r#type: FieldType::Json,
                    required: true,
                    nullable: false,
                    description: None,
                });
            }
            for field in object.attrs.keys() {
                entry.fields.entry(field.clone()).or_insert(FieldSchema {
                    r#type: FieldType::Json,
                    required: false,
                    nullable: true,
                    description: None,
                });
            }
        }
        Schema { types }
    }

    fn site_attrs(name: &str, slug: &str, status: Option<&str>) -> JsonMap {
        let mut value = json!({ "name": name, "slug": slug });
        if let serde_json::Value::Object(ref mut map) = value {
            if let Some(status) = status {
                map.insert("status".to_string(), json!(status));
            }
        }
        attrs_map(value)
    }

    fn device_attrs(
        name: &str,
        site: Uid,
        role: &str,
        device_type: &str,
        status: Option<&str>,
    ) -> JsonMap {
        let mut value = json!({
            "name": name,
            "site": site.to_string(),
            "role": role,
            "device_type": device_type
        });
        if let serde_json::Value::Object(ref mut map) = value {
            if let Some(status) = status {
                map.insert("status".to_string(), json!(status));
            }
        }
        attrs_map(value)
    }

    #[test]
    fn extract_inventory_uses_stable_uid() {
        let adapter = MockAdapter {
            observed: observed_state(),
        };
        let schema = schema_for_observed(&adapter.observed);
        let report = block_on(extract_inventory(&adapter, &schema, None)).unwrap();
        assert_eq!(report.inventory.objects.len(), 1);
        let object = &report.inventory.objects[0];
        assert_eq!(object.key, key_str("site=fra1"));
        assert_eq!(object.uid, uid_v5("dcim.site", "site=fra1"));
    }

    #[test]
    fn extract_inverts_custom_fields_and_tags() {
        let mut state = ObservedState::default();
        let mut custom_fields = BTreeMap::new();
        custom_fields.insert("serial".to_string(), Value::String("abc".to_string()));
        state.insert(ObservedObject {
            type_name: TypeName::new("dcim.device"),
            key: key_str("device=leaf01"),
            attrs: device_attrs(
                "leaf01",
                uid_v5("dcim.site", "site=fra1"),
                "leaf",
                "leaf-switch",
                Some("active"),
            ),
            projection: ProjectionData {
                custom_fields: Some(custom_fields),
                tags: Some(vec!["fabric".to_string()]),
                local_context: None,
            },
            backend_id: Some(BackendId::Int(2)),
        });
        let adapter = MockAdapter { observed: state };
        let schema = schema_for_observed(&adapter.observed);
        let projection: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: model
    on_type: dcim.device
    from_attrs:
      prefix: "model."
    to:
      custom_fields:
        strategy: strip_prefix
        prefix: "model."
  - name: tags
    on_type: dcim.device
    from_attrs:
      key: "model.tags"
    to:
      tags:
        source: value
"#,
        )
        .unwrap();
        let report = block_on(extract_inventory(&adapter, &schema, Some(&projection))).unwrap();
        let object = &report.inventory.objects[0];
        assert_eq!(
            object.attrs.get("model.serial"),
            Some(&Value::String("abc".to_string()))
        );
        assert_eq!(
            object.attrs.get("model.tags"),
            Some(&Value::Array(vec![Value::String("fabric".to_string())]))
        );
    }

    #[test]
    fn extract_preserves_unmapped_custom_fields() {
        let mut state = ObservedState::default();
        let mut custom_fields = BTreeMap::new();
        custom_fields.insert("owner".to_string(), Value::String("infra".to_string()));
        state.insert(ObservedObject {
            type_name: TypeName::new("dcim.site"),
            key: key_str("site=fra1"),
            attrs: site_attrs("FRA1", "fra1", Some("active")),
            projection: ProjectionData {
                custom_fields: Some(custom_fields),
                tags: None,
                local_context: None,
            },
            backend_id: Some(BackendId::Int(1)),
        });
        let adapter = MockAdapter { observed: state };
        let projection: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: noop
    on_type: dcim.site
    from_attrs:
      key: "model.serial"
    to:
      custom_fields:
        strategy: direct
"#,
        )
        .unwrap();
        let schema = schema_for_observed(&adapter.observed);
        let report = block_on(extract_inventory(&adapter, &schema, Some(&projection))).unwrap();
        let object = &report.inventory.objects[0];
        assert_eq!(
            object.attrs.get("owner"),
            Some(&Value::String("infra".to_string()))
        );
    }

    #[test]
    fn extract_warns_on_transforms() {
        let mut state = ObservedState::default();
        let mut custom_fields = BTreeMap::new();
        custom_fields.insert("serial".to_string(), Value::String("abc".to_string()));
        state.insert(ObservedObject {
            type_name: TypeName::new("dcim.device"),
            key: key_str("device=leaf01"),
            attrs: device_attrs(
                "leaf01",
                uid_v5("dcim.site", "site=fra1"),
                "leaf",
                "leaf-switch",
                Some("active"),
            ),
            projection: ProjectionData {
                custom_fields: Some(custom_fields),
                tags: None,
                local_context: None,
            },
            backend_id: Some(BackendId::Int(2)),
        });
        let adapter = MockAdapter { observed: state };
        let projection: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: model
    on_type: dcim.device
    from_attrs:
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
        let schema = schema_for_observed(&adapter.observed);
        let report = block_on(extract_inventory(&adapter, &schema, Some(&projection))).unwrap();
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
            type_name: TypeName::new("dcim.device"),
            key: key_str("device=leaf01"),
            attrs: device_attrs(
                "leaf01",
                uid_v5("dcim.site", "site=fra1"),
                "leaf",
                "leaf-switch",
                Some("active"),
            ),
            projection: ProjectionData {
                custom_fields: None,
                tags: None,
                local_context: Some(Value::Object(root)),
            },
            backend_id: Some(BackendId::Int(2)),
        });
        let adapter = MockAdapter { observed: state };
        let projection: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: local
    on_type: dcim.device
    from_attrs:
      prefix: "context."
    to:
      local_context:
        root: "system"
        strategy: strip_prefix
        prefix: "context."
"#,
        )
        .unwrap();
        let schema = schema_for_observed(&adapter.observed);
        let report = block_on(extract_inventory(&adapter, &schema, Some(&projection))).unwrap();
        let object = &report.inventory.objects[0];
        assert_eq!(
            object.attrs.get("context.role"),
            Some(&Value::String("leaf".to_string()))
        );
    }

    #[test]
    fn extract_warns_on_duplicate_attrs_key() {
        let mut state = ObservedState::default();
        let mut custom_fields = BTreeMap::new();
        custom_fields.insert("field_a".to_string(), Value::String("a".to_string()));
        custom_fields.insert("field_b".to_string(), Value::String("b".to_string()));
        state.insert(ObservedObject {
            type_name: TypeName::new("dcim.site"),
            key: key_str("site=fra1"),
            attrs: site_attrs("FRA1", "fra1", Some("active")),
            projection: ProjectionData {
                custom_fields: Some(custom_fields),
                tags: None,
                local_context: None,
            },
            backend_id: Some(BackendId::Int(1)),
        });
        let adapter = MockAdapter { observed: state };
        let projection: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: map_a
    on_type: dcim.site
    from_attrs:
      map:
        model.same: field_a
    to:
      custom_fields:
        strategy: explicit
  - name: map_b
    on_type: dcim.site
    from_attrs:
      map:
        model.same: field_b
    to:
      custom_fields:
        strategy: explicit
"#,
        )
        .unwrap();
        let schema = schema_for_observed(&adapter.observed);
        let report = block_on(extract_inventory(&adapter, &schema, Some(&projection))).unwrap();
        assert!(report
            .warnings
            .iter()
            .any(|warn| warn.contains("duplicate attrs key")));
    }

    #[test]
    fn extract_warns_on_direct_multiple_map_keys() {
        let mut state = ObservedState::default();
        let mut custom_fields = BTreeMap::new();
        custom_fields.insert("serial".to_string(), Value::String("abc".to_string()));
        state.insert(ObservedObject {
            type_name: TypeName::new("dcim.device"),
            key: key_str("device=leaf01"),
            attrs: device_attrs(
                "leaf01",
                uid_v5("dcim.site", "site=fra1"),
                "leaf",
                "leaf-switch",
                Some("active"),
            ),
            projection: ProjectionData {
                custom_fields: Some(custom_fields),
                tags: None,
                local_context: None,
            },
            backend_id: Some(BackendId::Int(2)),
        });
        let adapter = MockAdapter { observed: state };
        let projection: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: model
    on_type: dcim.device
    from_attrs:
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
        let schema = schema_for_observed(&adapter.observed);
        let report = block_on(extract_inventory(&adapter, &schema, Some(&projection))).unwrap();
        assert!(report
            .warnings
            .iter()
            .any(|warn| warn.contains("multiple map keys")));
    }

    #[test]
    fn extract_infers_tag_key_from_prefix() {
        let mut state = ObservedState::default();
        state.insert(ObservedObject {
            type_name: TypeName::new("dcim.device"),
            key: key_str("device=leaf01"),
            attrs: device_attrs(
                "leaf01",
                uid_v5("dcim.site", "site=fra1"),
                "leaf",
                "leaf-switch",
                Some("active"),
            ),
            projection: ProjectionData {
                custom_fields: None,
                tags: Some(vec!["fabric".to_string()]),
                local_context: None,
            },
            backend_id: Some(BackendId::Int(2)),
        });
        let adapter = MockAdapter { observed: state };
        let projection: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: tags
    on_type: dcim.device
    from_attrs:
      prefix: "model."
    to:
      tags:
        source: value
"#,
        )
        .unwrap();
        let schema = schema_for_observed(&adapter.observed);
        let report = block_on(extract_inventory(&adapter, &schema, Some(&projection))).unwrap();
        let object = &report.inventory.objects[0];
        assert_eq!(
            object.attrs.get("model.tags"),
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
            type_name: TypeName::new("dcim.device"),
            key: key_str("device=leaf01"),
            attrs: device_attrs(
                "leaf01",
                uid_v5("dcim.site", "site=fra1"),
                "leaf",
                "leaf-switch",
                Some("active"),
            ),
            projection: ProjectionData {
                custom_fields: None,
                tags: Some(vec!["fabric".to_string()]),
                local_context: None,
            },
            backend_id: Some(BackendId::Int(2)),
        });
        let adapter = MockAdapter { observed: state };
        let projection: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: tags
    on_type: dcim.device
    from_attrs:
      key: tags
    to:
      tags:
        source: value
"#,
        )
        .unwrap();
        let schema = schema_for_observed(&adapter.observed);
        let report = block_on(extract_inventory(&adapter, &schema, Some(&projection))).unwrap();
        let object = &report.inventory.objects[0];
        assert_eq!(
            object.attrs.get("tags"),
            Some(&Value::Array(vec![Value::String("fabric".to_string())]))
        );
    }

    #[test]
    fn extract_warns_on_non_value_tag_source() {
        let mut state = ObservedState::default();
        state.insert(ObservedObject {
            type_name: TypeName::new("dcim.device"),
            key: key_str("device=leaf01"),
            attrs: device_attrs(
                "leaf01",
                uid_v5("dcim.site", "site=fra1"),
                "leaf",
                "leaf-switch",
                Some("active"),
            ),
            projection: ProjectionData {
                custom_fields: None,
                tags: Some(vec!["fabric".to_string()]),
                local_context: None,
            },
            backend_id: Some(BackendId::Int(2)),
        });
        let adapter = MockAdapter { observed: state };
        let projection: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: tags
    on_type: dcim.device
    from_attrs:
      key: tags
    to:
      tags:
        source: key
"#,
        )
        .unwrap();
        let schema = schema_for_observed(&adapter.observed);
        let report = block_on(extract_inventory(&adapter, &schema, Some(&projection))).unwrap();
        assert!(report
            .warnings
            .iter()
            .any(|warn| warn.contains("tags source must be 'value'")));
        let object = &report.inventory.objects[0];
        assert_eq!(
            object.attrs.get("tags"),
            Some(&Value::Array(vec![Value::String("fabric".to_string())]))
        );
    }

    #[test]
    fn extract_warns_on_missing_strip_prefix_for_custom_fields() {
        let mut state = ObservedState::default();
        let mut custom_fields = BTreeMap::new();
        custom_fields.insert("serial".to_string(), Value::String("abc".to_string()));
        state.insert(ObservedObject {
            type_name: TypeName::new("dcim.device"),
            key: key_str("device=leaf01"),
            attrs: device_attrs(
                "leaf01",
                uid_v5("dcim.site", "site=fra1"),
                "leaf",
                "leaf-switch",
                Some("active"),
            ),
            projection: ProjectionData {
                custom_fields: Some(custom_fields),
                tags: None,
                local_context: None,
            },
            backend_id: Some(BackendId::Int(2)),
        });
        let adapter = MockAdapter { observed: state };
        let projection: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: model
    on_type: dcim.device
    from_attrs:
      key: model.serial
    to:
      custom_fields:
        strategy: strip_prefix
"#,
        )
        .unwrap();
        let schema = schema_for_observed(&adapter.observed);
        let report = block_on(extract_inventory(&adapter, &schema, Some(&projection))).unwrap();
        assert!(report
            .warnings
            .iter()
            .any(|warn| warn.contains("missing prefix")));
        let object = &report.inventory.objects[0];
        assert_eq!(
            object.attrs.get("serial"),
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
            type_name: TypeName::new("dcim.device"),
            key: key_str("device=leaf01"),
            attrs: device_attrs(
                "leaf01",
                uid_v5("dcim.site", "site=fra1"),
                "leaf",
                "leaf-switch",
                Some("active"),
            ),
            projection: ProjectionData {
                custom_fields: None,
                tags: None,
                local_context: Some(Value::Object(root)),
            },
            backend_id: Some(BackendId::Int(2)),
        });
        let adapter = MockAdapter { observed: state };
        let projection: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: local
    on_type: dcim.device
    from_attrs:
      key: context.role
    to:
      local_context:
        root: "system"
        strategy: strip_prefix
"#,
        )
        .unwrap();
        let schema = schema_for_observed(&adapter.observed);
        let report = block_on(extract_inventory(&adapter, &schema, Some(&projection))).unwrap();
        assert!(report
            .warnings
            .iter()
            .any(|warn| warn.contains("missing prefix")));
        let object = &report.inventory.objects[0];
        assert!(object.attrs.get("context.role").is_none());
    }
}