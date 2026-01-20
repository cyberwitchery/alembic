//! projection spec handling and application.

use alembic_core::{JsonMap, Kind, Object};
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProjectionData {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_fields: Option<BTreeMap<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_context: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectedObject {
    pub base: Object,
    #[serde(default)]
    pub projection: ProjectionData,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectedInventory {
    pub objects: Vec<ProjectedObject>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BackendCapabilities {
    #[serde(default)]
    pub custom_fields_by_kind: BTreeMap<String, BTreeSet<String>>,
    #[serde(default)]
    pub tags: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MissingCustomField {
    pub rule: String,
    pub kind: String,
    pub x_key: String,
    pub field: String,
    pub sample: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MissingTag {
    pub rule: String,
    pub kind: String,
    pub x_key: String,
    pub tag: String,
}

#[derive(Debug, Deserialize)]
pub struct ProjectionSpec {
    pub version: u32,
    pub backend: String,
    #[serde(default)]
    pub rules: Vec<ProjectionRule>,
}

#[derive(Debug, Deserialize)]
pub struct ProjectionRule {
    pub name: String,
    pub on_kind: String,
    pub from_x: FromX,
    pub to: ProjectionTarget,
}

#[derive(Debug, Deserialize)]
pub struct FromX {
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub map: BTreeMap<String, String>,
    #[serde(default)]
    pub transform: Vec<TransformSpec>,
}

#[derive(Debug, Deserialize)]
pub struct ProjectionTarget {
    #[serde(default)]
    pub custom_fields: Option<CustomFieldsTarget>,
    #[serde(default)]
    pub tags: Option<TagsTarget>,
    #[serde(default)]
    pub local_context: Option<LocalContextTarget>,
}

#[derive(Debug, Deserialize)]
pub struct CustomFieldsTarget {
    pub strategy: CustomFieldStrategy,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub field: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CustomFieldStrategy {
    StripPrefix,
    Explicit,
    Direct,
}

#[derive(Debug, Deserialize)]
pub struct TagsTarget {
    pub source: String,
}

#[derive(Debug, Deserialize)]
pub struct LocalContextTarget {
    pub root: String,
    pub strategy: CustomFieldStrategy,
    #[serde(default)]
    pub prefix: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum TransformSpec {
    Simple(String),
    Join { join: String },
    Default { default: Value },
}

#[derive(Debug)]
enum Transform {
    Stringify,
    DropIfNull,
    Join(String),
    Default(Value),
}

pub fn apply_projection(spec: &ProjectionSpec, inventory: &[Object]) -> Result<ProjectedInventory> {
    if spec.version != 1 {
        return Err(anyhow!(
            "projection version {} is unsupported",
            spec.version
        ));
    }
    if spec.backend.trim().is_empty() {
        return Err(anyhow!("projection backend is required"));
    }

    let mut objects = Vec::new();
    for object in inventory {
        let mut projection = ProjectionData::default();
        let mut tag_set = BTreeSet::new();
        let mut tags_defined = false;

        for rule in &spec.rules {
            if !rule_matches(&rule.on_kind, &object.kind) {
                continue;
            }
            if rule.to.local_context.is_some() && !matches!(object.kind, Kind::DcimDevice) {
                return Err(anyhow!(
                    "projection rule {} (kind {}): local_context only supported for dcim.device",
                    rule.name,
                    object.kind
                ));
            }
            let entries = select_x_entries(&object.x, &rule.from_x, &rule.name, &object.kind)?;
            if entries.is_empty() {
                continue;
            }
            let transforms = parse_transforms(&rule.from_x.transform, &rule.name)?;
            let mut mapped = BTreeMap::new();

            for (x_key, value) in entries {
                let mut value =
                    apply_transforms(value, &transforms, &rule.name, &object.kind, &x_key)?;
                if let Some(value) = value.take() {
                    mapped.insert(x_key, value);
                }
            }

            if let Some(custom_fields) = &rule.to.custom_fields {
                let prefix = custom_fields
                    .prefix
                    .as_ref()
                    .or(rule.from_x.prefix.as_ref());
                for (x_key, value) in mapped.iter() {
                    let field_name = match custom_fields.strategy {
                        CustomFieldStrategy::StripPrefix => {
                            let Some(prefix) = prefix else {
                                return Err(rule_error(
                                    &rule.name,
                                    &object.kind,
                                    x_key,
                                    "missing prefix for strip_prefix",
                                ));
                            };
                            if !x_key.starts_with(prefix) {
                                return Err(rule_error(
                                    &rule.name,
                                    &object.kind,
                                    x_key,
                                    "x key missing required prefix",
                                ));
                            }
                            x_key.strip_prefix(prefix).unwrap_or(x_key).to_string()
                        }
                        CustomFieldStrategy::Explicit => {
                            let Some(target) = rule.from_x.map.get(x_key) else {
                                return Err(rule_error(
                                    &rule.name,
                                    &object.kind,
                                    x_key,
                                    "missing explicit map entry",
                                ));
                            };
                            target.clone()
                        }
                        CustomFieldStrategy::Direct => {
                            if let Some(field) = &custom_fields.field {
                                field.clone()
                            } else {
                                x_key.clone()
                            }
                        }
                    };
                    projection
                        .custom_fields
                        .get_or_insert_with(BTreeMap::new)
                        .insert(field_name, value.clone());
                }
            }

            if let Some(tags_target) = &rule.to.tags {
                tags_defined = true;
                if tags_target.source != "value" {
                    return Err(rule_error(
                        &rule.name,
                        &object.kind,
                        "",
                        "tags target source must be 'value'",
                    ));
                }
                for (_x_key, value) in mapped.iter() {
                    match value {
                        Value::Array(items) => {
                            for item in items {
                                let Some(tag) = item.as_str() else {
                                    return Err(rule_error(
                                        &rule.name,
                                        &object.kind,
                                        "model.tags",
                                        "tag values must be strings",
                                    ));
                                };
                                tag_set.insert(tag.to_string());
                            }
                        }
                        _ => {
                            return Err(rule_error(
                                &rule.name,
                                &object.kind,
                                "model.tags",
                                "tag values must be a list of strings",
                            ));
                        }
                    }
                }
            }

            if let Some(local_context) = &rule.to.local_context {
                let prefix = local_context
                    .prefix
                    .as_ref()
                    .or(rule.from_x.prefix.as_ref());
                let mut local_map = BTreeMap::new();
                for (x_key, value) in mapped.iter() {
                    let field_name = match local_context.strategy {
                        CustomFieldStrategy::StripPrefix => {
                            let Some(prefix) = prefix else {
                                return Err(rule_error(
                                    &rule.name,
                                    &object.kind,
                                    x_key,
                                    "missing prefix for strip_prefix",
                                ));
                            };
                            if !x_key.starts_with(prefix) {
                                return Err(rule_error(
                                    &rule.name,
                                    &object.kind,
                                    x_key,
                                    "x key missing required prefix",
                                ));
                            }
                            x_key.strip_prefix(prefix).unwrap_or(x_key).to_string()
                        }
                        CustomFieldStrategy::Explicit => {
                            let Some(target) = rule.from_x.map.get(x_key) else {
                                return Err(rule_error(
                                    &rule.name,
                                    &object.kind,
                                    x_key,
                                    "missing explicit map entry",
                                ));
                            };
                            target.clone()
                        }
                        CustomFieldStrategy::Direct => x_key.clone(),
                    };
                    local_map.insert(field_name, value.clone());
                }
                let context_value = insert_root(local_context.root.as_str(), local_map)?;
                projection.local_context = Some(merge_context(
                    projection.local_context.take(),
                    context_value,
                ));
            }
        }

        if tags_defined {
            projection.tags = Some(tag_set.into_iter().collect());
        }
        objects.push(ProjectedObject {
            base: object.clone(),
            projection,
        });
    }

    Ok(ProjectedInventory { objects })
}

pub fn load_projection(path: impl AsRef<Path>) -> Result<ProjectionSpec> {
    let path = path.as_ref();
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("read projection: {}", path.display()))?;
    let spec: ProjectionSpec = serde_yaml::from_str(&raw)
        .with_context(|| format!("parse projection: {}", path.display()))?;
    Ok(spec)
}

pub fn project_default(inventory: &[Object]) -> ProjectedInventory {
    let objects = inventory
        .iter()
        .cloned()
        .map(|base| ProjectedObject {
            base,
            projection: ProjectionData::default(),
        })
        .collect();
    ProjectedInventory { objects }
}

pub fn validate_projection_strict(
    spec: &ProjectionSpec,
    inventory: &[Object],
    capabilities: &BackendCapabilities,
) -> Result<()> {
    let missing = missing_custom_fields(spec, inventory, capabilities)?;
    if let Some(entry) = missing.first() {
        return Err(anyhow!(
            "projection strict: rule {} (kind {}, x {}, field {}) is missing in backend",
            entry.rule,
            entry.kind,
            entry.x_key,
            entry.field
        ));
    }
    Ok(())
}

fn rule_matches(on_kind: &str, kind: &Kind) -> bool {
    on_kind == "*" || on_kind == kind.as_string()
}

pub fn missing_custom_fields(
    spec: &ProjectionSpec,
    inventory: &[Object],
    capabilities: &BackendCapabilities,
) -> Result<Vec<MissingCustomField>> {
    let mut missing = BTreeMap::new();

    for object in inventory {
        if object.kind.is_custom() {
            continue;
        }
        let kind_str = object.kind.as_string();
        let known_fields = capabilities
            .custom_fields_by_kind
            .get(&kind_str)
            .cloned()
            .unwrap_or_default();

        for rule in &spec.rules {
            if !rule_matches(&rule.on_kind, &object.kind) {
                continue;
            }
            if rule.to.custom_fields.is_none() {
                continue;
            }
            let entries = select_x_entries(&object.x, &rule.from_x, &rule.name, &object.kind)?;
            if entries.is_empty() {
                continue;
            }
            let transforms = parse_transforms(&rule.from_x.transform, &rule.name)?;
            let custom_fields = rule.to.custom_fields.as_ref().unwrap();
            let prefix = custom_fields
                .prefix
                .as_ref()
                .or(rule.from_x.prefix.as_ref());

            for (x_key, value) in entries {
                let value = apply_transforms(value, &transforms, &rule.name, &object.kind, &x_key)?;
                let Some(value) = value else {
                    continue;
                };
                let field_name = match custom_fields.strategy {
                    CustomFieldStrategy::StripPrefix => {
                        let Some(prefix) = prefix else {
                            return Err(rule_error(
                                &rule.name,
                                &object.kind,
                                &x_key,
                                "missing prefix for strip_prefix",
                            ));
                        };
                        if !x_key.starts_with(prefix) {
                            return Err(rule_error(
                                &rule.name,
                                &object.kind,
                                &x_key,
                                "x key missing required prefix",
                            ));
                        }
                        x_key.strip_prefix(prefix).unwrap_or(&x_key).to_string()
                    }
                    CustomFieldStrategy::Explicit => {
                        let Some(target) = rule.from_x.map.get(&x_key) else {
                            return Err(rule_error(
                                &rule.name,
                                &object.kind,
                                &x_key,
                                "missing explicit map entry",
                            ));
                        };
                        target.clone()
                    }
                    CustomFieldStrategy::Direct => {
                        if let Some(field) = &custom_fields.field {
                            field.clone()
                        } else {
                            x_key.clone()
                        }
                    }
                };
                if !known_fields.contains(&field_name) {
                    let key = (
                        rule.name.clone(),
                        kind_str.clone(),
                        x_key.clone(),
                        field_name,
                    );
                    missing.entry(key).or_insert(value);
                }
            }
        }
    }

    let mut entries: Vec<MissingCustomField> = missing
        .into_iter()
        .map(|((rule, kind, x_key, field), sample)| MissingCustomField {
            rule,
            kind,
            x_key,
            field,
            sample,
        })
        .collect();
    entries.sort_by(|a, b| {
        (
            a.kind.clone(),
            a.field.clone(),
            a.rule.clone(),
            a.x_key.clone(),
        )
            .cmp(&(
                b.kind.clone(),
                b.field.clone(),
                b.rule.clone(),
                b.x_key.clone(),
            ))
    });
    Ok(entries)
}

pub fn missing_tags(
    spec: &ProjectionSpec,
    inventory: &[Object],
    capabilities: &BackendCapabilities,
) -> Result<Vec<MissingTag>> {
    let mut missing = BTreeSet::new();

    for object in inventory {
        let kind_str = object.kind.as_string();
        for rule in &spec.rules {
            if !rule_matches(&rule.on_kind, &object.kind) {
                continue;
            }
            if rule.to.tags.is_none() {
                continue;
            }
            let entries = select_x_entries(&object.x, &rule.from_x, &rule.name, &object.kind)?;
            if entries.is_empty() {
                continue;
            }
            let transforms = parse_transforms(&rule.from_x.transform, &rule.name)?;
            for (x_key, value) in entries {
                let value = apply_transforms(value, &transforms, &rule.name, &object.kind, &x_key)?;
                let Some(value) = value else {
                    continue;
                };
                match value {
                    Value::Array(items) => {
                        for item in items {
                            let Some(tag) = item.as_str() else {
                                return Err(rule_error(
                                    &rule.name,
                                    &object.kind,
                                    &x_key,
                                    "tag values must be strings",
                                ));
                            };
                            if !capabilities.tags.contains(tag) {
                                missing.insert((
                                    rule.name.clone(),
                                    kind_str.clone(),
                                    x_key.clone(),
                                    tag.to_string(),
                                ));
                            }
                        }
                    }
                    _ => {
                        return Err(rule_error(
                            &rule.name,
                            &object.kind,
                            &x_key,
                            "tag values must be a list of strings",
                        ));
                    }
                }
            }
        }
    }

    let mut entries: Vec<MissingTag> = missing
        .into_iter()
        .map(|(rule, kind, x_key, tag)| MissingTag {
            rule,
            kind,
            x_key,
            tag,
        })
        .collect();
    entries.sort_by(|a, b| {
        (
            a.kind.clone(),
            a.tag.clone(),
            a.rule.clone(),
            a.x_key.clone(),
        )
            .cmp(&(
                b.kind.clone(),
                b.tag.clone(),
                b.rule.clone(),
                b.x_key.clone(),
            ))
    });
    Ok(entries)
}

fn select_x_entries(
    x: &JsonMap,
    from: &FromX,
    rule: &str,
    kind: &Kind,
) -> Result<BTreeMap<String, Value>> {
    let mut entries = BTreeMap::new();
    let selector_count =
        from.prefix.is_some() as u8 + from.key.is_some() as u8 + (!from.map.is_empty()) as u8;
    if selector_count != 1 {
        return Err(anyhow!(
            "projection rule {rule} (kind {kind}): from_x must include exactly one of prefix, key, or map"
        ));
    }
    if let Some(prefix) = &from.prefix {
        for (key, value) in x.iter() {
            if key.starts_with(prefix) {
                entries.insert(key.clone(), value.clone());
            }
        }
    }
    if let Some(key) = &from.key {
        if let Some(value) = x.get(key) {
            entries.insert(key.clone(), value.clone());
        }
    }
    if !from.map.is_empty() {
        for key in from.map.keys() {
            if let Some(value) = x.get(key) {
                entries.insert(key.clone(), value.clone());
            }
        }
    }
    Ok(entries)
}

fn parse_transforms(specs: &[TransformSpec], rule: &str) -> Result<Vec<Transform>> {
    let mut transforms = Vec::new();
    for spec in specs {
        match spec {
            TransformSpec::Simple(name) => match name.as_str() {
                "stringify" => transforms.push(Transform::Stringify),
                "drop_if_null" => transforms.push(Transform::DropIfNull),
                other => return Err(anyhow!("projection rule {rule}: unknown transform {other}")),
            },
            TransformSpec::Join { join } => transforms.push(Transform::Join(join.clone())),
            TransformSpec::Default { default } => {
                transforms.push(Transform::Default(default.clone()))
            }
        }
    }
    Ok(transforms)
}

fn apply_transforms(
    mut value: Value,
    transforms: &[Transform],
    rule: &str,
    kind: &Kind,
    x_key: &str,
) -> Result<Option<Value>> {
    for transform in transforms {
        match transform {
            Transform::Stringify => {
                if !value.is_string() {
                    value = Value::String(value.to_string());
                }
            }
            Transform::DropIfNull => {
                if value.is_null() {
                    return Ok(None);
                }
            }
            Transform::Join(sep) => {
                let Value::Array(items) = value else {
                    return Err(rule_error(rule, kind, x_key, "join requires array"));
                };
                let mut parts = Vec::new();
                for item in items {
                    let Some(item) = item.as_str() else {
                        return Err(rule_error(rule, kind, x_key, "join requires string items"));
                    };
                    parts.push(item.to_string());
                }
                value = Value::String(parts.join(sep));
            }
            Transform::Default(default) => {
                if value.is_null() {
                    value = default.clone();
                }
            }
        }
    }
    Ok(Some(value))
}

fn insert_root(root: &str, values: BTreeMap<String, Value>) -> Result<Value> {
    if root.trim().is_empty() {
        return Err(anyhow!("local context root must be non-empty"));
    }
    let segments: Vec<&str> = root.split('.').collect();
    let mut current = Value::Object(values.into_iter().collect());
    for segment in segments.into_iter().rev() {
        if segment.trim().is_empty() {
            return Err(anyhow!("local context root contains empty segment"));
        }
        let mut map = serde_json::Map::new();
        map.insert(segment.to_string(), current);
        current = Value::Object(map);
    }
    Ok(current)
}

fn merge_context(existing: Option<Value>, incoming: Value) -> Value {
    match (existing, incoming) {
        (None, value) => value,
        (Some(Value::Object(mut base)), Value::Object(add)) => {
            for (key, value) in add {
                base.insert(key, value);
            }
            Value::Object(base)
        }
        (Some(value), _) => value,
    }
}

fn rule_error(rule: &str, kind: &Kind, x_key: &str, message: &str) -> anyhow::Error {
    if x_key.is_empty() {
        anyhow!("projection rule {rule} (kind {kind}): {message}")
    } else {
        anyhow!("projection rule {rule} (kind {kind}, x {x_key}): {message}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alembic_core::{Attrs, DeviceAttrs, SiteAttrs, Uid};
    use serde_json::json;
    use uuid::Uuid;

    fn uid(value: u128) -> Uid {
        Uuid::from_u128(value)
    }

    fn site_object(x: BTreeMap<String, Value>) -> Object {
        Object {
            uid: uid(1),
            kind: Kind::DcimSite,
            key: "site=fra1".to_string(),
            attrs: Attrs::Site(SiteAttrs {
                name: "FRA1".to_string(),
                slug: "fra1".to_string(),
                status: None,
                description: None,
            }),
            x: x.into(),
        }
    }

    fn device_object(x: BTreeMap<String, Value>) -> Object {
        Object {
            uid: uid(2),
            kind: Kind::DcimDevice,
            key: "device=leaf01".to_string(),
            attrs: Attrs::Device(DeviceAttrs {
                name: "leaf01".to_string(),
                site: uid(1),
                role: "leaf".to_string(),
                device_type: "leaf-switch".to_string(),
                status: None,
            }),
            x: x.into(),
        }
    }

    #[test]
    fn prefix_mapping_to_custom_fields() {
        let mut x = BTreeMap::new();
        x.insert("model.fabric".to_string(), json!("fra1"));
        x.insert("model.role_hint".to_string(), json!("leaf"));
        let spec: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: device_model
    on_kind: dcim.site
    from_x:
      prefix: "model."
    to:
      custom_fields:
        strategy: strip_prefix
        prefix: "model."
"#,
        )
        .unwrap();

        let inventory = vec![site_object(x)];
        let projected = apply_projection(&spec, &inventory).unwrap();
        let projection = &projected.objects[0].projection;
        let fields = projection.custom_fields.as_ref().unwrap();
        assert_eq!(fields.get("fabric"), Some(&json!("fra1")));
        assert_eq!(fields.get("role_hint"), Some(&json!("leaf")));
    }

    #[test]
    fn explicit_map_projection() {
        let mut x = BTreeMap::new();
        x.insert("model.fabric".to_string(), json!("fra1"));
        let spec: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: device_map
    on_kind: dcim.site
    from_x:
      map:
        model.fabric: fabric_name
    to:
      custom_fields:
        strategy: explicit
"#,
        )
        .unwrap();

        let inventory = vec![site_object(x)];
        let projected = apply_projection(&spec, &inventory).unwrap();
        let projection = &projected.objects[0].projection;
        let fields = projection.custom_fields.as_ref().unwrap();
        assert_eq!(fields.get("fabric_name"), Some(&json!("fra1")));
    }

    #[test]
    fn tags_validation_rejects_non_list() {
        let mut x = BTreeMap::new();
        x.insert("model.tags".to_string(), json!("oops"));
        let spec: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: tags
    on_kind: dcim.site
    from_x:
      key: "model.tags"
    to:
      tags:
        source: value
"#,
        )
        .unwrap();

        let inventory = vec![site_object(x)];
        let err = apply_projection(&spec, &inventory).unwrap_err();
        assert!(err.to_string().contains("tag values must be a list"));
    }

    #[test]
    fn strict_mode_rejects_unknown_fields() {
        let mut x = BTreeMap::new();
        x.insert("model.fabric".to_string(), json!("fra1"));
        let spec: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: device_model
    on_kind: dcim.site
    from_x:
      prefix: "model."
    to:
      custom_fields:
        strategy: strip_prefix
        prefix: "model."
"#,
        )
        .unwrap();
        let inventory = vec![site_object(x)];
        let mut caps = BackendCapabilities::default();
        caps.custom_fields_by_kind.insert(
            "dcim.site".to_string(),
            BTreeSet::from(["role_hint".to_string()]),
        );
        let err = validate_projection_strict(&spec, &inventory, &caps).unwrap_err();
        assert!(err.to_string().contains("missing in backend"));
    }

    #[test]
    fn projection_is_deterministic() {
        let mut x = BTreeMap::new();
        x.insert("model.fabric".to_string(), json!("fra1"));
        x.insert("model.role_hint".to_string(), json!("leaf"));
        let spec: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: device_model
    on_kind: dcim.site
    from_x:
      prefix: "model."
    to:
      custom_fields:
        strategy: strip_prefix
        prefix: "model."
"#,
        )
        .unwrap();

        let inventory = vec![site_object(x)];
        let first = apply_projection(&spec, &inventory).unwrap();
        let second = apply_projection(&spec, &inventory).unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn missing_custom_fields_includes_rule_and_key() {
        let mut x = BTreeMap::new();
        x.insert("model.fabric".to_string(), json!("fra1"));
        let spec: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: device_model
    on_kind: dcim.site
    from_x:
      prefix: "model."
    to:
      custom_fields:
        strategy: strip_prefix
        prefix: "model."
"#,
        )
        .unwrap();
        let inventory = vec![site_object(x)];
        let caps = BackendCapabilities::default();
        let missing = missing_custom_fields(&spec, &inventory, &caps).unwrap();
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].rule, "device_model");
        assert_eq!(missing[0].x_key, "model.fabric");
        assert_eq!(missing[0].field, "fabric");
    }

    #[test]
    fn missing_tags_lists_unknown_tags() {
        let mut x = BTreeMap::new();
        x.insert("model.tags".to_string(), json!(["blue", "edge"]));
        let spec: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: model_tags
    on_kind: dcim.site
    from_x:
      key: "model.tags"
    to:
      tags:
        source: value
"#,
        )
        .unwrap();
        let inventory = vec![site_object(x)];
        let mut caps = BackendCapabilities::default();
        caps.tags.insert("blue".to_string());
        let missing = missing_tags(&spec, &inventory, &caps).unwrap();
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].rule, "model_tags");
        assert_eq!(missing[0].x_key, "model.tags");
        assert_eq!(missing[0].tag, "edge");
    }

    #[test]
    fn transform_default_applies() {
        let mut x = BTreeMap::new();
        x.insert("model.empty".to_string(), Value::Null);
        let spec: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: defaults
    on_kind: dcim.site
    from_x:
      prefix: "model."
      transform:
        - default: "fallback"
    to:
      custom_fields:
        strategy: strip_prefix
        prefix: "model."
"#,
        )
        .unwrap();

        let inventory = vec![site_object(x)];
        let projected = apply_projection(&spec, &inventory).unwrap();
        let projection = &projected.objects[0].projection;
        let fields = projection.custom_fields.as_ref().unwrap();
        assert_eq!(fields.get("empty"), Some(&json!("fallback")));
    }

    #[test]
    fn transform_drop_if_null_skips_entry() {
        let mut x = BTreeMap::new();
        x.insert("model.empty".to_string(), Value::Null);
        let spec: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: drop_null
    on_kind: dcim.site
    from_x:
      prefix: "model."
      transform:
        - drop_if_null
    to:
      custom_fields:
        strategy: strip_prefix
        prefix: "model."
"#,
        )
        .unwrap();

        let inventory = vec![site_object(x)];
        let projected = apply_projection(&spec, &inventory).unwrap();
        assert!(projected.objects[0].projection.custom_fields.is_none());
    }

    #[test]
    fn transform_join_requires_array() {
        let mut x = BTreeMap::new();
        x.insert("model.tags".to_string(), json!("oops"));
        let spec: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: joiner
    on_kind: dcim.site
    from_x:
      prefix: "model."
      transform:
        - join: ","
    to:
      custom_fields:
        strategy: strip_prefix
        prefix: "model."
"#,
        )
        .unwrap();

        let inventory = vec![site_object(x)];
        let err = apply_projection(&spec, &inventory).unwrap_err();
        assert!(err.to_string().contains("join requires array"));
    }

    #[test]
    fn local_context_requires_device_kind() {
        let mut x = BTreeMap::new();
        x.insert("policy.level".to_string(), json!("prod"));
        let spec: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: policy_ctx
    on_kind: dcim.site
    from_x:
      prefix: "policy."
    to:
      local_context:
        root: "alembic.policy"
        strategy: strip_prefix
        prefix: "policy."
"#,
        )
        .unwrap();

        let inventory = vec![site_object(x)];
        let err = apply_projection(&spec, &inventory).unwrap_err();
        assert!(err
            .to_string()
            .contains("local_context only supported for dcim.device"));
    }

    #[test]
    fn local_context_root_rejects_empty_segments() {
        let mut x = BTreeMap::new();
        x.insert("policy.level".to_string(), json!("prod"));
        let spec: ProjectionSpec = serde_yaml::from_str(
            r#"
version: 1
backend: netbox
rules:
  - name: policy_ctx
    on_kind: dcim.device
    from_x:
      prefix: "policy."
    to:
      local_context:
        root: "alembic..policy"
        strategy: strip_prefix
        prefix: "policy."
"#,
        )
        .unwrap();

        let inventory = vec![device_object(x)];
        let err = apply_projection(&spec, &inventory).unwrap_err();
        assert!(err
            .to_string()
            .contains("local context root contains empty segment"));
    }
}
