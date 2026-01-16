//! retort mapping: compile raw yaml into canonical ir.

use alembic_core::{uid_v5, Attrs, Inventory, Kind, Object, Uid};
use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use serde_json::{Map as JsonMap, Value as JsonValue};
use serde_yaml::{Mapping as YamlMapping, Value as YamlValue};
use std::collections::BTreeMap;
use std::path::Path;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct Retort {
    pub version: u32,
    #[serde(default)]
    pub rules: Vec<Rule>,
}

#[derive(Debug, Deserialize)]
pub struct Rule {
    pub name: String,
    pub select: String,
    pub emit: Emit,
}

#[derive(Debug, Deserialize)]
pub struct Emit {
    pub kind: String,
    pub key: String,
    #[serde(default)]
    pub uid: Option<EmitUid>,
    #[serde(default)]
    pub vars: BTreeMap<String, VarSpec>,
    #[serde(default)]
    pub attrs: BTreeMap<String, YamlValue>,
    #[serde(default)]
    pub x: BTreeMap<String, YamlValue>,
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
    pub kind: String,
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

        for path in selected {
            let vars = extract_vars(raw, &path, &rule.emit.vars, &rule.name)?;
            let key = render_template(&rule.emit.key, &vars, &rule.name, "key")?;
            let uid = resolve_emit_uid(&rule.emit, &vars, &rule.name, &key)?;
            let kind = Kind::parse(&rule.emit.kind);
            let attrs = render_attrs(&rule.emit.attrs, &vars, &rule.name, "attrs")?;
            let x = render_attrs(&rule.emit.x, &vars, &rule.name, "x")?;
            let object = build_object(uid, kind, key, attrs, x)?;
            objects.push(object);
        }
    }

    objects.sort_by(|a, b| {
        inventory_sort_key(&a.kind, &a.key).cmp(&inventory_sort_key(&b.kind, &b.key))
    });

    Ok(Inventory { objects })
}

fn build_object(
    uid: Uid,
    kind: Kind,
    key: String,
    attrs: JsonMap<String, JsonValue>,
    x: JsonMap<String, JsonValue>,
) -> Result<Object> {
    let attrs_value = JsonValue::Object(attrs);
    let parsed_attrs = match &kind {
        Kind::DcimSite => match serde_json::from_value(attrs_value.clone()) {
            Ok(parsed) => Attrs::Site(parsed),
            Err(_) => Attrs::Generic(to_object_map(attrs_value)?),
        },
        Kind::DcimDevice => match serde_json::from_value(attrs_value.clone()) {
            Ok(parsed) => Attrs::Device(parsed),
            Err(_) => Attrs::Generic(to_object_map(attrs_value)?),
        },
        Kind::DcimInterface => match serde_json::from_value(attrs_value.clone()) {
            Ok(parsed) => Attrs::Interface(parsed),
            Err(_) => Attrs::Generic(to_object_map(attrs_value)?),
        },
        Kind::IpamPrefix => match serde_json::from_value(attrs_value.clone()) {
            Ok(parsed) => Attrs::Prefix(parsed),
            Err(_) => Attrs::Generic(to_object_map(attrs_value)?),
        },
        Kind::IpamIpAddress => match serde_json::from_value(attrs_value.clone()) {
            Ok(parsed) => Attrs::IpAddress(parsed),
            Err(_) => Attrs::Generic(to_object_map(attrs_value)?),
        },
        Kind::Custom(_) => Attrs::Generic(to_object_map(attrs_value)?),
    };

    Ok(Object {
        uid,
        kind,
        key,
        attrs: parsed_attrs,
        x: x.into_iter().collect(),
    })
}

fn to_object_map(value: JsonValue) -> Result<BTreeMap<String, JsonValue>> {
    match value {
        JsonValue::Object(map) => Ok(map.into_iter().collect()),
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
    key: &str,
) -> Result<Uid> {
    match emit.uid.as_ref() {
        Some(EmitUid::V5 { v5 }) => resolve_uid_v5(v5, vars, rule, "uid"),
        Some(EmitUid::Template(template)) => resolve_uid_template(template, vars, rule),
        None => Ok(uid_v5(&emit.kind, key)),
    }
}

fn resolve_uid_v5(
    spec: &UidV5Spec,
    vars: &BTreeMap<String, JsonValue>,
    rule: &str,
    context: &str,
) -> Result<Uid> {
    let kind = render_template(&spec.kind, vars, rule, context)?;
    let stable = render_template(&spec.stable, vars, rule, context)?;
    if kind.trim().is_empty() || stable.trim().is_empty() {
        return Err(anyhow!(
            "rule {rule}: uid v5 requires non-empty kind and stable values"
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
) -> Result<JsonMap<String, JsonValue>> {
    let mut map = JsonMap::new();
    for (key, value) in attrs {
        let rendered = render_yaml_value(value, vars, rule, context, false)?;
        if let Some(value) = rendered {
            map.insert(key.clone(), value);
        }
    }
    Ok(map)
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

            let mut rendered = JsonMap::new();
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
    let kind = render_template_optional(&spec.kind, vars, rule, context, optional)?;
    let stable = render_template_optional(&spec.stable, vars, rule, context, optional)?;
    let (Some(kind), Some(stable)) = (kind, stable) else {
        return Ok(None);
    };
    if kind.trim().is_empty() || stable.trim().is_empty() {
        if optional {
            return Ok(None);
        }
        return Err(anyhow!(
            "rule {rule}: uid mapping requires non-empty kind and stable"
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
    let kind = inner.get(YamlValue::String("kind".to_string()))?;
    let stable = inner.get(YamlValue::String("stable".to_string()))?;
    let kind = kind.as_str()?.to_string();
    let stable = stable.as_str()?.to_string();
    Some((optional, UidV5Spec { kind, stable }))
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

fn inventory_sort_key(kind: &Kind, key: &str) -> (u8, String, String) {
    (kind_rank(kind), kind.as_string(), key.to_string())
}

fn kind_rank(kind: &Kind) -> u8 {
    match kind {
        Kind::DcimSite => 0,
        Kind::DcimDevice => 1,
        Kind::DcimInterface => 2,
        Kind::IpamPrefix => 3,
        Kind::IpamIpAddress => 4,
        Kind::Custom(_) => 10,
    }
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
        assert_eq!(
            objects[0].get("kind").unwrap().as_str().unwrap(),
            "dcim.site"
        );
        assert_eq!(
            objects[1].get("kind").unwrap().as_str().unwrap(),
            "dcim.device"
        );
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
rules:
  - name: sites
    select: /sites/*
    emit:
      kind: dcim.site
      key: "site=${slug}"
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
        let first = plan(&projected, &observed, &state, false);
        let second = plan(&projected, &observed, &state, false);
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
  kind: "dcim.site"
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
  kind: "dcim.site"
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
}
