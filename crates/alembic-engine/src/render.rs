use crate::retort::UidV5Spec;
use alembic_core::{uid_v5, Key};
use anyhow::{anyhow, Result};
use serde_json::{Map as JsonObject, Value as JsonValue};
use serde_yaml::{Mapping as YamlMapping, Value as YamlValue};
use std::collections::BTreeMap;

pub(crate) fn render_attrs(
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

pub(crate) fn render_key(
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

pub(crate) fn render_yaml_value(
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

pub(crate) fn yaml_to_json(value: YamlValue) -> Result<JsonValue> {
    serde_json::to_value(value).map_err(|err| anyhow!("yaml to json failed: {err}"))
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

pub(crate) fn render_template(
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml::Value as YamlValue;
    use std::collections::BTreeMap;

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
    fn templates_substitute_and_error_on_missing() {
        let mut vars = BTreeMap::new();
        vars.insert("name".to_string(), JsonValue::String("leaf01".to_string()));
        let rendered = render_template("device=${name}", &vars, "devices", "key").unwrap();
        assert_eq!(rendered, "device=leaf01");

        let err = render_template("device=${missing}", &vars, "devices", "key").unwrap_err();
        assert!(err.to_string().contains("missing var"));
    }

    #[test]
    fn template_errors_on_non_string_var() {
        let mut vars = BTreeMap::new();
        vars.insert("asn".to_string(), JsonValue::Number(65001.into()));
        let err = render_template("asn=${asn}", &vars, "rule", "key").unwrap_err();
        assert!(err.to_string().contains("must be a string"));
    }
}
