//! retort mapping: compile raw yaml into canonical ir.

use alembic_core::{key_string, uid_v5, Inventory, JsonMap, Key, Object, Schema, TypeName, Uid};
use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use serde_json::{Map as JsonObject, Value as JsonValue};
use serde_yaml::{Mapping as YamlMapping, Value as YamlValue};
use std::collections::BTreeMap;
use std::path::Path;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct Retort {
    pub version: u32,
    pub schema: Schema,
    #[serde(default)]
    pub rules: Vec<Rule>,
}

#[derive(Debug, Deserialize)]
pub struct Rule {
    pub name: String,
    pub select: String,
    /// Rule-level vars extracted once and shared by all emits.
    #[serde(default)]
    pub vars: BTreeMap<String, VarSpec>,
    /// Named UIDs computed once and available as `${uids.name}` in emits.
    #[serde(default)]
    pub uids: BTreeMap<String, EmitUid>,
    /// Single emit (backward compat) or list of emits.
    pub emit: EmitSpec,
}

/// Either a single emit (backward compatible) or a list of emits.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum EmitSpec {
    Single(Emit),
    Multi(Vec<Emit>),
}

#[derive(Debug, Deserialize)]
pub struct Emit {
    #[serde(rename = "type", alias = "kind")]
    pub type_name: String,
    pub key: BTreeMap<String, YamlValue>,
    #[serde(default)]
    pub uid: Option<EmitUid>,
    #[serde(default)]
    pub vars: BTreeMap<String, VarSpec>,
    #[serde(default)]
    pub attrs: BTreeMap<String, YamlValue>,
}

#[derive(Debug, Deserialize)]
pub struct VarSpec {
    pub from: String,
    #[serde(default)]
    pub required: bool,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum EmitUid {
    V5 { v5: UidV5Spec },
    Template(String),
}

#[derive(Debug, Deserialize)]
pub struct UidV5Spec {
    #[serde(rename = "type", alias = "kind")]
    pub type_name: String,
    pub stable: String,
}

#[derive(Debug, Clone)]
enum SelectorToken {
    Key(String),
    Index(usize),
    Wildcard,
}

#[derive(Debug, Clone)]
enum PathToken {
    Key(String),
    Index(usize),
}

#[derive(Debug)]
struct RelativePath {
    up: usize,
    selectors: Vec<SelectorToken>,
}

pub fn load_retort(path: impl AsRef<Path>) -> Result<Retort> {
    let path = path.as_ref();
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("read retort: {}", path.display()))?;
    let retort: Retort =
        serde_yaml::from_str(&raw).with_context(|| format!("parse retort: {}", path.display()))?;
    Ok(retort)
}

pub fn load_raw_yaml(path: impl AsRef<Path>) -> Result<YamlValue> {
    let path = path.as_ref();
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("read raw yaml: {}", path.display()))?;
    let value: YamlValue =
        serde_yaml::from_str(&raw).with_context(|| format!("parse yaml: {}", path.display()))?;
    Ok(value)
}

pub fn is_brew_format(raw: &YamlValue) -> bool {
    let YamlValue::Mapping(map) = raw else {
        return false;
    };
    map.contains_key(YamlValue::String("objects".to_string()))
}

pub fn compile_retort(raw: &YamlValue, retort: &Retort) -> Result<Inventory> {
    if retort.version != 1 {
        return Err(anyhow!("retort version {} is unsupported", retort.version));
    }

    let mut objects = Vec::new();

    for rule in &retort.rules {
        let selectors = parse_selector_path(&rule.select)
            .with_context(|| format!("rule {}: invalid select path", rule.name))?;
        let mut selected = Vec::new();
        select_paths(raw, &selectors, &mut Vec::new(), &mut selected);

        let emits = match &rule.emit {
            EmitSpec::Single(emit) => vec![emit],
            EmitSpec::Multi(emits) => emits.iter().collect(),
        };

        for path in selected {
            // Extract rule-level vars first (shared by all emits).
            let mut vars = extract_vars(raw, &path, &rule.vars, &rule.name)?;

            // Compute named UIDs and add them as uids.X vars.
            for (uid_name, uid_spec) in &rule.uids {
                let uid = resolve_named_uid(uid_spec, &vars, &rule.name, uid_name)?;
                vars.insert(
                    format!("uids.{}", uid_name),
                    JsonValue::String(uid.to_string()),
                );
            }

            // Process each emit.
            for emit in &emits {
                // Merge emit-level vars with rule-level vars (emit takes precedence).
                let mut emit_vars = vars.clone();
                let emit_specific_vars = extract_vars(raw, &path, &emit.vars, &rule.name)?;
                emit_vars.extend(emit_specific_vars);

                let key = render_key(&emit.key, &emit_vars, &rule.name)?;
                let uid = resolve_emit_uid(emit, &emit_vars, &rule.name, &key)?;
                let type_name = TypeName::new(render_template(
                    &emit.type_name,
                    &emit_vars,
                    &rule.name,
                    "type",
                )?);
                let attrs = render_attrs(&emit.attrs, &emit_vars, &rule.name, "attrs")?;
                let object = build_object(uid, type_name, key, attrs)?;
                objects.push(object);
            }
        }
    }

    objects.sort_by(|a, b| {
        inventory_sort_key(&a.type_name, &a.key).cmp(&inventory_sort_key(&b.type_name, &b.key))
    });

    let inventory = Inventory {
        schema: retort.schema.clone(),
        objects,
    };
    crate::validate(&inventory)?;
    Ok(inventory)
}

fn build_object(
    uid: Uid,
    type_name: TypeName,
    key: Key,
    attrs: JsonObject<String, JsonValue>,
) -> Result<Object> {
    let attrs_value = JsonValue::Object(attrs);
    let attrs = to_object_map(attrs_value)?;
    Ok(Object::new(uid, type_name, key, attrs)?)
}

fn to_object_map(value: JsonValue) -> Result<JsonMap> {
    match value {
        JsonValue::Object(map) => Ok(map.into_iter().collect::<BTreeMap<_, _>>().into()),
        _ => Err(anyhow!("attrs must be an object")),
    }
}

fn extract_vars(
    raw: &YamlValue,
    path: &[PathToken],
    specs: &BTreeMap<String, VarSpec>,
    rule: &str,
) -> Result<BTreeMap<String, JsonValue>> {
    let mut vars = BTreeMap::new();
    for (name, spec) in specs {
        let rel = parse_relative_path(&spec.from)
            .with_context(|| format!("rule {rule}: invalid var path for {name}: {}", spec.from))?;
        let values = extract_values(raw, path, &rel)?;
        if values.is_empty() {
            if spec.required {
                return Err(anyhow!(
                    "rule {rule}: missing required var {name} from {}",
                    spec.from
                ));
            }
            continue;
        }
        let json_value = if values.len() == 1 {
            yaml_to_json(values[0].clone())?
        } else {
            let mut items = Vec::new();
            for value in values {
                items.push(yaml_to_json(value.clone())?);
            }
            JsonValue::Array(items)
        };
        vars.insert(name.clone(), json_value);
    }
    Ok(vars)
}

fn resolve_emit_uid(
    emit: &Emit,
    vars: &BTreeMap<String, JsonValue>,
    rule: &str,
    key: &Key,
) -> Result<Uid> {
    match emit.uid.as_ref() {
        Some(EmitUid::V5 { v5 }) => resolve_uid_v5(v5, vars, rule, "uid"),
        Some(EmitUid::Template(template)) => resolve_uid_template(template, vars, rule),
        None => Ok(uid_v5(&emit.type_name, &key_string(key))),
    }
}

fn resolve_named_uid(
    uid_spec: &EmitUid,
    vars: &BTreeMap<String, JsonValue>,
    rule: &str,
    uid_name: &str,
) -> Result<Uid> {
    let context = format!("uids.{}", uid_name);
    match uid_spec {
        EmitUid::V5 { v5 } => resolve_uid_v5(v5, vars, rule, &context),
        EmitUid::Template(template) => resolve_uid_template(template, vars, rule),
    }
}

fn resolve_uid_v5(
    spec: &UidV5Spec,
    vars: &BTreeMap<String, JsonValue>,
    rule: &str,
    context: &str,
) -> Result<Uid> {
    let kind = render_template(&spec.type_name, vars, rule, context)?;
    let stable = render_template(&spec.stable, vars, rule, context)?;
    if kind.trim().is_empty() || stable.trim().is_empty() {
        return Err(anyhow!(
            "rule {rule}: uid v5 requires non-empty type and stable values"
        ));
    }
    Ok(uid_v5(&kind, &stable))
}

fn resolve_uid_template(
    template: &str,
    vars: &BTreeMap<String, JsonValue>,
    rule: &str,
) -> Result<Uid> {
    let rendered = render_template(template, vars, rule, "uid")?;
    let parsed = Uuid::parse_str(&rendered)
        .with_context(|| format!("rule {rule}: uid template is not a valid uuid: {rendered}"))?;
    Ok(parsed)
}

fn render_attrs(
    attrs: &BTreeMap<String, YamlValue>,
    vars: &BTreeMap<String, JsonValue>,
    rule: &str,
    context: &str,
) -> Result<JsonObject<String, JsonValue>> {
    let mut map = JsonObject::new();
    for (key, value) in attrs {
        let rendered = render_yaml_value(value, vars, rule, context, false)?;
        if let Some(value) = rendered {
            map.insert(key.clone(), value);
        }
    }
    Ok(map)
}

fn render_key(
    key: &BTreeMap<String, YamlValue>,
    vars: &BTreeMap<String, JsonValue>,
    rule: &str,
) -> Result<Key> {
    let mut map = BTreeMap::new();
    for (field, value) in key {
        let context = format!("key.{field}");
        let rendered = render_yaml_value(value, vars, rule, &context, false)?;
        let Some(value) = rendered else {
            return Err(anyhow!("rule {rule}: missing value for {context}"));
        };
        map.insert(field.clone(), value);
    }
    Ok(Key::from(map))
}

fn render_yaml_value(
    value: &YamlValue,
    vars: &BTreeMap<String, JsonValue>,
    rule: &str,
    context: &str,
    allow_missing: bool,
) -> Result<Option<JsonValue>> {
    match value {
        YamlValue::String(raw) => render_string_value(raw, vars, rule, context, allow_missing),
        YamlValue::Sequence(items) => {
            let mut rendered = Vec::new();
            for item in items {
                let value = render_yaml_value(item, vars, rule, context, allow_missing)?;
                match value {
                    Some(value) => rendered.push(value),
                    None => {
                        if allow_missing {
                            return Ok(None);
                        }
                        return Err(anyhow!("rule {rule}: missing value in {context}"));
                    }
                }
            }
            Ok(Some(JsonValue::Array(rendered)))
        }
        YamlValue::Mapping(map) => {
            if let Some((optional, spec)) = parse_uid_mapping(map) {
                return render_uid_mapping(&spec, vars, rule, context, optional);
            }

            let mut rendered = JsonObject::new();
            for (key, value) in map {
                let key = key
                    .as_str()
                    .ok_or_else(|| anyhow!("rule {rule}: {context} keys must be strings"))?
                    .to_string();
                let value = render_yaml_value(value, vars, rule, context, allow_missing)?;
                match value {
                    Some(value) => {
                        rendered.insert(key, value);
                    }
                    None => {
                        if allow_missing {
                            return Ok(None);
                        }
                        return Err(anyhow!("rule {rule}: missing value in {context}"));
                    }
                }
            }
            Ok(Some(JsonValue::Object(rendered)))
        }
        _ => Ok(Some(yaml_to_json(value.clone())?)),
    }
}

fn render_uid_mapping(
    spec: &UidV5Spec,
    vars: &BTreeMap<String, JsonValue>,
    rule: &str,
    context: &str,
    optional: bool,
) -> Result<Option<JsonValue>> {
    let kind = render_template_optional(&spec.type_name, vars, rule, context, optional)?;
    let stable = render_template_optional(&spec.stable, vars, rule, context, optional)?;
    let (Some(kind), Some(stable)) = (kind, stable) else {
        return Ok(None);
    };
    if kind.trim().is_empty() || stable.trim().is_empty() {
        if optional {
            return Ok(None);
        }
        return Err(anyhow!(
            "rule {rule}: uid mapping requires non-empty type and stable"
        ));
    }
    let uid = uid_v5(&kind, &stable);
    Ok(Some(JsonValue::String(uid.to_string())))
}

fn render_string_value(
    raw: &str,
    vars: &BTreeMap<String, JsonValue>,
    rule: &str,
    context: &str,
    allow_missing: bool,
) -> Result<Option<JsonValue>> {
    if let Some(var) = placeholder_only(raw) {
        if let Some(value) = vars.get(var) {
            if value.is_null() && allow_missing {
                return Ok(None);
            }
            return Ok(Some(value.clone()));
        }
        if allow_missing {
            return Ok(None);
        }
        return Err(anyhow!("rule {rule}: missing var {var} in {context}"));
    }

    if raw.contains("${") {
        let rendered = render_template_optional(raw, vars, rule, context, allow_missing)?;
        return Ok(rendered.map(JsonValue::String));
    }

    Ok(Some(JsonValue::String(raw.to_string())))
}

fn render_template(
    template: &str,
    vars: &BTreeMap<String, JsonValue>,
    rule: &str,
    context: &str,
) -> Result<String> {
    render_template_optional(template, vars, rule, context, false)?
        .ok_or_else(|| anyhow!("rule {rule}: missing vars for template {template}"))
}

fn render_template_optional(
    template: &str,
    vars: &BTreeMap<String, JsonValue>,
    rule: &str,
    context: &str,
    allow_missing: bool,
) -> Result<Option<String>> {
    let mut rendered = String::new();
    let mut rest = template;

    while let Some(start) = rest.find("${") {
        rendered.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find('}') else {
            return Err(anyhow!(
                "rule {rule}: unterminated template in {context}: {template}"
            ));
        };
        let name = &after[..end];
        let value = vars.get(name);
        let Some(value) = value else {
            if allow_missing {
                return Ok(None);
            }
            return Err(anyhow!("rule {rule}: missing var {name} in {context}"));
        };
        if value.is_null() && allow_missing {
            return Ok(None);
        }
        let Some(value) = value.as_str() else {
            return Err(anyhow!(
                "rule {rule}: var {name} in {context} must be a string"
            ));
        };
        rendered.push_str(value);
        rest = &after[end + 1..];
    }
    rendered.push_str(rest);
    Ok(Some(rendered))
}

fn placeholder_only(input: &str) -> Option<&str> {
    if !input.starts_with("${") || !input.ends_with('}') {
        return None;
    }
    let inner = &input[2..input.len() - 1];
    if inner.contains("${") || inner.contains('}') || inner.is_empty() {
        return None;
    }
    Some(inner)
}

fn parse_uid_mapping(map: &YamlMapping) -> Option<(bool, UidV5Spec)> {
    if map.len() != 1 {
        return None;
    }
    let (key, value) = map.iter().next()?;
    let key = key.as_str()?;
    let optional = match key {
        "uid" => false,
        "uid?" => true,
        _ => return None,
    };
    let YamlValue::Mapping(inner) = value else {
        return None;
    };
    let kind = inner
        .get(YamlValue::String("type".to_string()))
        .or_else(|| inner.get(YamlValue::String("kind".to_string())))?;
    let stable = inner.get(YamlValue::String("stable".to_string()))?;
    let kind = kind.as_str()?.to_string();
    let stable = stable.as_str()?.to_string();
    Some((
        optional,
        UidV5Spec {
            type_name: kind,
            stable,
        },
    ))
}

fn parse_selector_path(path: &str) -> Result<Vec<SelectorToken>> {
    if !path.starts_with('/') {
        return Err(anyhow!("select path must start with '/'"));
    }
    let mut tokens = Vec::new();
    for segment in path.trim_start_matches('/').split('/') {
        if segment.is_empty() {
            continue;
        }
        tokens.push(parse_selector_segment(segment)?);
    }
    Ok(tokens)
}

fn parse_selector_segment(segment: &str) -> Result<SelectorToken> {
    if segment == "*" {
        return Ok(SelectorToken::Wildcard);
    }
    if let Ok(index) = segment.parse::<usize>() {
        return Ok(SelectorToken::Index(index));
    }
    Ok(SelectorToken::Key(segment.to_string()))
}

fn parse_relative_path(path: &str) -> Result<RelativePath> {
    let mut rest = path.trim();
    let mut up = 0;
    while rest.starts_with('^') {
        up += 1;
        rest = &rest[1..];
        if rest.starts_with('.') {
            rest = &rest[1..];
        }
    }
    if rest.starts_with('.') {
        rest = &rest[1..];
    }
    if rest.starts_with('/') {
        rest = &rest[1..];
    }
    let selectors = if rest.is_empty() {
        Vec::new()
    } else {
        rest.split('/')
            .filter(|s| !s.is_empty())
            .map(parse_selector_segment)
            .collect::<Result<Vec<_>>>()?
    };
    Ok(RelativePath { up, selectors })
}

fn select_paths(
    value: &YamlValue,
    selectors: &[SelectorToken],
    current_path: &mut Vec<PathToken>,
    results: &mut Vec<Vec<PathToken>>,
) {
    if selectors.is_empty() {
        results.push(current_path.clone());
        return;
    }

    match selectors[0].clone() {
        SelectorToken::Key(key) => {
            if let YamlValue::Mapping(map) = value {
                if let Some(value) = map.get(YamlValue::String(key.clone())) {
                    current_path.push(PathToken::Key(key));
                    select_paths(value, &selectors[1..], current_path, results);
                    current_path.pop();
                }
            }
        }
        SelectorToken::Index(index) => {
            if let YamlValue::Sequence(items) = value {
                if let Some(value) = items.get(index) {
                    current_path.push(PathToken::Index(index));
                    select_paths(value, &selectors[1..], current_path, results);
                    current_path.pop();
                }
            }
        }
        SelectorToken::Wildcard => match value {
            YamlValue::Sequence(items) => {
                for (index, value) in items.iter().enumerate() {
                    current_path.push(PathToken::Index(index));
                    select_paths(value, &selectors[1..], current_path, results);
                    current_path.pop();
                }
            }
            YamlValue::Mapping(map) => {
                for (key, value) in map {
                    let Some(key) = key.as_str() else {
                        continue;
                    };
                    current_path.push(PathToken::Key(key.to_string()));
                    select_paths(value, &selectors[1..], current_path, results);
                    current_path.pop();
                }
            }
            _ => {}
        },
    }
}

fn extract_values<'a>(
    raw: &'a YamlValue,
    path: &[PathToken],
    rel: &RelativePath,
) -> Result<Vec<&'a YamlValue>> {
    let base_path = ancestor_path(raw, path, rel.up)?;
    let Some(base_value) = value_at_path(raw, &base_path) else {
        return Ok(Vec::new());
    };
    let mut results = Vec::new();
    select_values(base_value, &rel.selectors, &mut results);
    Ok(results)
}

fn ancestor_path(raw: &YamlValue, path: &[PathToken], up: usize) -> Result<Vec<PathToken>> {
    let mut current: Vec<PathToken> = path.to_vec();
    for _ in 0..up {
        if current.is_empty() {
            return Err(anyhow!("relative path escapes above root"));
        }
        current.pop();
        loop {
            let Some(value) = value_at_path(raw, &current) else {
                break;
            };
            if matches!(value, YamlValue::Sequence(_)) {
                if current.is_empty() {
                    break;
                }
                current.pop();
            } else {
                break;
            }
        }
    }
    Ok(current)
}

fn value_at_path<'a>(value: &'a YamlValue, path: &[PathToken]) -> Option<&'a YamlValue> {
    let mut current = value;
    for token in path {
        match token {
            PathToken::Key(key) => {
                let YamlValue::Mapping(map) = current else {
                    return None;
                };
                current = map.get(YamlValue::String(key.clone()))?;
            }
            PathToken::Index(index) => {
                let YamlValue::Sequence(items) = current else {
                    return None;
                };
                current = items.get(*index)?;
            }
        }
    }
    Some(current)
}

fn select_values<'a>(
    value: &'a YamlValue,
    selectors: &[SelectorToken],
    results: &mut Vec<&'a YamlValue>,
) {
    if selectors.is_empty() {
        results.push(value);
        return;
    }
    match selectors[0].clone() {
        SelectorToken::Key(key) => {
            if let YamlValue::Mapping(map) = value {
                if let Some(value) = map.get(YamlValue::String(key)) {
                    select_values(value, &selectors[1..], results);
                }
            }
        }
        SelectorToken::Index(index) => {
            if let YamlValue::Sequence(items) = value {
                if let Some(value) = items.get(index) {
                    select_values(value, &selectors[1..], results);
                }
            }
        }
        SelectorToken::Wildcard => match value {
            YamlValue::Sequence(items) => {
                for value in items {
                    select_values(value, &selectors[1..], results);
                }
            }
            YamlValue::Mapping(map) => {
                for (key, value) in map {
                    if key.as_str().is_none() {
                        continue;
                    }
                    select_values(value, &selectors[1..], results);
                }
            }
            _ => {}
        },
    }
}

fn yaml_to_json(value: YamlValue) -> Result<JsonValue> {
    serde_json::to_value(value).map_err(|err| anyhow!("yaml to json failed: {err}"))
}

fn inventory_sort_key(type_name: &TypeName, key: &Key) -> (String, String) {
    (type_name.as_str().to_string(), key_string(key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::plan;
    use crate::state::StateStore;
    use crate::types::ObservedState;
    use tempfile::tempdir;

    fn parse_yaml(input: &str) -> YamlValue {
        serde_yaml::from_str(input).unwrap()
    }

    #[test]
    fn wildcard_selector_returns_all_nodes() {
        let raw = parse_yaml(
            r#"
sites:
  - slug: a
    devices:
      - name: d1
      - name: d2
  - slug: b
    devices:
      - name: d3
"#,
        );
        let selectors = parse_selector_path("/sites/*/devices/*").unwrap();
        let mut selected = Vec::new();
        select_paths(&raw, &selectors, &mut Vec::new(), &mut selected);
        assert_eq!(selected.len(), 3);
    }

    #[test]
    fn templates_substitute_and_error_on_missing() {
        let mut vars = BTreeMap::new();
        vars.insert("name".to_string(), JsonValue::String("leaf01".to_string()));
        let rendered = render_template("device=${name}", &vars, "devices", "key").unwrap();
        assert_eq!(rendered, "device=leaf01");

        let err = render_template("device=${missing}", &vars, "devices", "key").unwrap_err();
        assert!(err.to_string().contains("missing var"));
    }

    #[test]
    fn uid_v5_is_deterministic() {
        let first = uid_v5("dcim.site", "site=fra1");
        let second = uid_v5("dcim.site", "site=fra1");
        assert_eq!(first, second);
    }

    #[test]
    fn compile_raw_yaml_to_inventory() {
        let raw = parse_yaml(
            r#"
sites:
  - slug: fra1
    name: FRA1
    devices:
      - name: leaf01
        role: leaf
        device_type: leaf-switch
        model:
          fabric: fra1-fabric
          role_hint: leaf
          tags:
            - fabric
        interfaces:
          - name: eth0
          - name: eth1
prefixes:
  - site: fra1
    prefix: 10.0.0.0/24
    ips:
      - device: leaf01
        interface: eth0
        address: 10.0.0.10/24
"#,
        );
        let retort = parse_yaml(include_str!("../../../examples/retort.yaml"));
        let retort: Retort = serde_yaml::from_value(retort).unwrap();
        let inventory = compile_retort(&raw, &retort).unwrap();
        let json = serde_json::to_value(&inventory).unwrap();
        let objects = json.get("objects").unwrap().as_array().unwrap();
        assert_eq!(objects.len(), 6);
        let types: Vec<&str> = objects
            .iter()
            .map(|obj| obj.get("type").unwrap().as_str().unwrap())
            .collect();
        assert!(types.contains(&"dcim.site"));
        assert!(types.contains(&"dcim.device"));
    }

    #[test]
    fn plan_is_deterministic_across_runs() {
        let raw = parse_yaml(
            r#"
sites:
  - slug: fra1
    name: FRA1
"#,
        );
        let retort = parse_yaml(
            r#"
version: 1
schema:
  types:
    dcim.site:
      key:
        site:
          type: slug
      fields:
        name:
          type: string
        slug:
          type: slug
rules:
  - name: sites
    select: /sites/*
    emit:
      type: dcim.site
      key:
        site: "${slug}"
      vars:
        slug: { from: .slug, required: true }
        name: { from: .name, required: true }
      attrs:
        name: ${name}
        slug: ${slug}
"#,
        );
        let retort: Retort = serde_yaml::from_value(retort).unwrap();
        let inventory = compile_retort(&raw, &retort).unwrap();
        let state = StateStore::load(tempdir().unwrap().path().join("state.json")).unwrap();
        let observed = ObservedState::default();
        let projected = crate::project_default(&inventory.objects);
        let first = plan(&projected, &observed, &state, &inventory.schema, false);
        let second = plan(&projected, &observed, &state, &inventory.schema, false);
        assert_eq!(first.ops, second.ops);
    }

    #[test]
    fn parse_relative_path_tracks_parent_hops() {
        let rel = parse_relative_path("^^.slug").unwrap();
        assert_eq!(rel.up, 2);
        assert_eq!(rel.selectors.len(), 1);
    }

    #[test]
    fn render_uid_mapping_optional_skips_missing() {
        let vars = BTreeMap::new();
        let mapping: YamlValue = serde_yaml::from_str(
            r#"
uid?:
  type: "dcim.site"
  stable: "site=${slug}"
"#,
        )
        .unwrap();
        let rendered = render_yaml_value(&mapping, &vars, "rule", "attrs", false).unwrap();
        assert!(rendered.is_none());
    }

    #[test]
    fn render_uid_mapping_required_errors_on_missing() {
        let vars = BTreeMap::new();
        let mapping: YamlValue = serde_yaml::from_str(
            r#"
uid:
  type: "dcim.site"
  stable: "site=${slug}"
"#,
        )
        .unwrap();
        let err = render_yaml_value(&mapping, &vars, "rule", "attrs", false).unwrap_err();
        assert!(err.to_string().contains("missing var"));
    }

    #[test]
    fn template_errors_on_non_string_var() {
        let mut vars = BTreeMap::new();
        vars.insert("asn".to_string(), JsonValue::Number(65001.into()));
        let err = render_template("asn=${asn}", &vars, "rule", "key").unwrap_err();
        assert!(err.to_string().contains("must be a string"));
    }

    #[test]
    fn resolve_uid_template_rejects_invalid_uuid() {
        let vars = BTreeMap::new();
        let err = resolve_uid_template("not-a-uuid", &vars, "rule").unwrap_err();
        assert!(err.to_string().contains("uid template is not a valid uuid"));
    }

    #[test]
    fn multi_emit_produces_multiple_objects() {
        let raw = parse_yaml(
            r#"
fabrics:
  - name: fabric1
    site_slug: fra1
    vrf_name: blue
"#,
        );
        let retort = parse_yaml(
            r#"
version: 1
schema:
  types:
    dcim.site:
      key:
        site:
          type: slug
      fields:
        name:
          type: string
        slug:
          type: slug
    custom.vrf:
      key:
        vrf:
          type: slug
      fields:
        name:
          type: string
rules:
  - name: fabric
    select: /fabrics/*
    vars:
      site_slug: { from: .site_slug, required: true }
      vrf_name: { from: .vrf_name, required: true }
    emit:
      - type: dcim.site
        key:
          site: "${site_slug}"
        attrs:
          name: ${site_slug}
          slug: ${site_slug}
      - type: custom.vrf
        key:
          vrf: "${vrf_name}"
        attrs:
          name: ${vrf_name}
"#,
        );
        let retort: Retort = serde_yaml::from_value(retort).unwrap();
        let inventory = compile_retort(&raw, &retort).unwrap();
        assert_eq!(inventory.objects.len(), 2);
        // Objects are sorted by type then key.
        assert_eq!(inventory.objects[0].type_name.as_str(), "custom.vrf");
        assert_eq!(inventory.objects[1].type_name.as_str(), "dcim.site");
    }

    #[test]
    fn multi_emit_with_named_uids() {
        let raw = parse_yaml(
            r#"
fabrics:
  - site_slug: fra1
    vrf_name: blue
"#,
        );
        let retort = parse_yaml(
            r#"
version: 1
schema:
  types:
    dcim.site:
      key:
        site:
          type: slug
      fields:
        name:
          type: string
        slug:
          type: slug
    custom.vrf:
      key:
        vrf:
          type: slug
      fields:
        name:
          type: string
        site:
          type: ref
          target: dcim.site
rules:
  - name: fabric
    select: /fabrics/*
    vars:
      site_slug: { from: .site_slug, required: true }
      vrf_name: { from: .vrf_name, required: true }
    uids:
      site:
        v5:
          type: "dcim.site"
          stable: "site=${site_slug}"
    emit:
      - type: dcim.site
        key:
          site: "${site_slug}"
        uid: ${uids.site}
        attrs:
          name: ${site_slug}
          slug: ${site_slug}
      - type: custom.vrf
        key:
          vrf: "${vrf_name}"
        attrs:
          name: ${vrf_name}
          site: ${uids.site}
"#,
        );
        let retort: Retort = serde_yaml::from_value(retort).unwrap();
        let inventory = compile_retort(&raw, &retort).unwrap();
        assert_eq!(inventory.objects.len(), 2);

        let mut site = None;
        let mut vrf = None;
        for object in &inventory.objects {
            match object.type_name.as_str() {
                "dcim.site" => site = Some(object),
                "custom.vrf" => vrf = Some(object),
                _ => {}
            }
        }
        let site = site.expect("expected dcim.site");
        let vrf = vrf.expect("expected custom.vrf");

        // Site UID should match the named UID
        let expected_site_uid = uid_v5("dcim.site", "site=fra1");
        assert_eq!(site.uid, expected_site_uid);

        // VRF should reference the site UID in its attrs
        let vrf_attrs = &vrf.attrs;
        let site_ref = vrf_attrs.get("site").unwrap().as_str().unwrap();
        assert_eq!(site_ref, expected_site_uid.to_string());
    }

    #[test]
    fn multi_emit_is_deterministic() {
        let raw = parse_yaml(
            r#"
fabrics:
  - site_slug: fra1
    vrf_name: blue
  - site_slug: fra2
    vrf_name: red
"#,
        );
        let retort = parse_yaml(
            r#"
version: 1
schema:
  types:
    dcim.site:
      key:
        site:
          type: slug
      fields:
        slug:
          type: slug
    custom.vrf:
      key:
        vrf:
          type: slug
      fields:
        name:
          type: string
rules:
  - name: fabric
    select: /fabrics/*
    vars:
      site_slug: { from: .site_slug, required: true }
      vrf_name: { from: .vrf_name, required: true }
    emit:
      - type: dcim.site
        key:
          site: "${site_slug}"
        attrs:
          slug: ${site_slug}
      - type: custom.vrf
        key:
          vrf: "${vrf_name}"
        attrs:
          name: ${vrf_name}
"#,
        );
        let retort: Retort = serde_yaml::from_value(retort).unwrap();
        let first = compile_retort(&raw, &retort).unwrap();
        let second = compile_retort(&raw, &retort).unwrap();

        assert_eq!(first.objects.len(), 4);
        assert_eq!(first.objects.len(), second.objects.len());
        for (a, b) in first.objects.iter().zip(second.objects.iter()) {
            assert_eq!(a.uid, b.uid);
            assert_eq!(a.type_name, b.type_name);
            assert_eq!(a.key, b.key);
        }
    }

    #[test]
    fn emit_level_vars_override_rule_level() {
        let raw = parse_yaml(
            r#"
items:
  - name: item1
    override_name: overridden
"#,
        );
        let retort = parse_yaml(
            r#"
version: 1
schema:
  types:
    custom.first:
      key:
        first:
          type: slug
      fields:
        name:
          type: string
    custom.second:
      key:
        second:
          type: slug
      fields:
        name:
          type: string
rules:
  - name: items
    select: /items/*
    vars:
      name: { from: .name, required: true }
    emit:
      - type: custom.first
        key:
          first: "${name}"
        attrs:
          name: ${name}
      - type: custom.second
        key:
          second: "${name}"
        vars:
          name: { from: .override_name, required: true }
        attrs:
          name: ${name}
"#,
        );
        let retort: Retort = serde_yaml::from_value(retort).unwrap();
        let inventory = compile_retort(&raw, &retort).unwrap();
        assert_eq!(inventory.objects.len(), 2);

        let first = &inventory.objects[0];
        let second = &inventory.objects[1];

        // First uses rule-level var
        let first_attrs = &first.attrs;
        assert_eq!(first_attrs.get("name").unwrap().as_str().unwrap(), "item1");

        // Second uses emit-level var (overrides rule-level)
        let second_attrs = &second.attrs;
        assert_eq!(
            second_attrs.get("name").unwrap().as_str().unwrap(),
            "overridden"
        );
    }
}
