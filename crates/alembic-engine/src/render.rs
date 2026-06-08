use alembic_core::{uid_v5, Key};
use anyhow::{anyhow, Result};
use serde::Deserialize;
use serde_json::{Map as JsonObject, Value as JsonValue};
use serde_yaml::{Mapping as YamlMapping, Value as YamlValue};
use std::collections::BTreeMap;

/// a `v5: { type, stable }` uid spec, shared by the map emit path (`transform`)
/// and the `${ uid: { type, stable } }` attr form rendered here.
#[derive(Debug, Deserialize)]
pub struct UidV5Spec {
    #[serde(rename = "type", alias = "kind")]
    pub type_name: String,
    pub stable: String,
}

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
    if let Some(inner) = placeholder_only(raw) {
        let placeholder = parse_placeholder(inner);
        let Some(value) = vars.get(placeholder.name) else {
            if allow_missing {
                return Ok(None);
            }
            return Err(anyhow!(
                "rule {rule}: missing var {} in {context}",
                placeholder.name
            ));
        };
        if value.is_null() && allow_missing {
            return Ok(None);
        }
        // A lone `${var}` with no transforms preserves the raw typed value.
        if placeholder.transforms.is_empty() {
            return Ok(Some(value.clone()));
        }
        let rendered = apply_placeholder(value, &placeholder, rule, context)?;
        return Ok(Some(JsonValue::String(rendered)));
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

/// A parsed `${var|transform|...}` placeholder body: the variable name plus the
/// (possibly empty) ordered list of transforms to apply after coercion.
struct Placeholder<'a> {
    name: &'a str,
    transforms: Vec<&'a str>,
}

/// Parse the inside of a `${...}` placeholder into a var name and transform
/// pipeline. The single shared parser used by both the embedded-template path
/// (`render_template_optional`) and the lone-placeholder path
/// (`render_string_value`). Surrounding whitespace on the name and on each
/// transform is ignored, so `${ slug | upper }` and `${slug|upper}` are equal.
fn parse_placeholder(inner: &str) -> Placeholder<'_> {
    let mut parts = inner.split('|');
    let name = parts.next().unwrap_or("").trim();
    let transforms = parts.map(str::trim).collect();
    Placeholder { name, transforms }
}

/// Coerce a looked-up var to its string form, then apply the transform pipeline.
fn apply_placeholder(
    value: &JsonValue,
    placeholder: &Placeholder,
    rule: &str,
    context: &str,
) -> Result<String> {
    let rendered = coerce_to_string(value, placeholder.name, rule, context)?;
    apply_transforms(rendered, &placeholder.transforms, rule, context)
}

/// Render a scalar var as a string. Strings pass through; numbers and bools are
/// coerced to their natural form (`42`, `true`). Nulls, arrays, and objects
/// have no template representation and are an error.
fn coerce_to_string(value: &JsonValue, name: &str, rule: &str, context: &str) -> Result<String> {
    match value {
        JsonValue::String(value) => Ok(value.clone()),
        JsonValue::Number(value) => Ok(value.to_string()),
        JsonValue::Bool(value) => Ok(value.to_string()),
        JsonValue::Null => Err(anyhow!(
            "rule {rule}: var {name} in {context} is null and cannot be rendered as a string"
        )),
        JsonValue::Array(_) | JsonValue::Object(_) => Err(anyhow!(
            "rule {rule}: var {name} in {context} must be a scalar (string, number, or bool)"
        )),
    }
}

/// Apply transforms left-to-right. An unknown transform name is an error rather
/// than being silently ignored.
fn apply_transforms(
    mut value: String,
    transforms: &[&str],
    rule: &str,
    context: &str,
) -> Result<String> {
    for transform in transforms {
        value = match *transform {
            "upper" => value.to_uppercase(),
            "lower" => value.to_lowercase(),
            "trim" => value.trim().to_string(),
            // `mapping::slugify` lowercases, collapses runs of non-`[a-z0-9]`
            // to a single `-`, and trims leading/trailing `-`, producing a valid
            // `slug` value. An input with no ascii alphanumerics (e.g. `---`)
            // slugifies to an empty string, which is not a valid slug, so we
            // error rather than emit it.
            "slug" => {
                let slug = crate::mapping::slugify(&value);
                if slug.is_empty() {
                    return Err(anyhow!(
                        "rule {rule}: slug transform produced an empty slug in {context}"
                    ));
                }
                slug
            }
            other => {
                return Err(anyhow!(
                    "rule {rule}: unknown transform {other} in {context}"
                ))
            }
        };
    }
    Ok(value)
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
        let placeholder = parse_placeholder(&after[..end]);
        let Some(value) = vars.get(placeholder.name) else {
            if allow_missing {
                return Ok(None);
            }
            return Err(anyhow!(
                "rule {rule}: missing var {} in {context}",
                placeholder.name
            ));
        };
        if value.is_null() && allow_missing {
            return Ok(None);
        }
        rendered.push_str(&apply_placeholder(value, &placeholder, rule, context)?);
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
    fn template_coerces_number_var() {
        let mut vars = BTreeMap::new();
        vars.insert("asn".to_string(), JsonValue::Number(65001.into()));
        let rendered = render_template("asn=${asn}", &vars, "rule", "key").unwrap();
        assert_eq!(rendered, "asn=65001");
    }

    #[test]
    fn template_coerces_bool_var() {
        let mut vars = BTreeMap::new();
        vars.insert("enabled".to_string(), JsonValue::Bool(true));
        let rendered = render_template("flag=${enabled}", &vars, "rule", "key").unwrap();
        assert_eq!(rendered, "flag=true");
    }

    #[test]
    fn template_errors_on_array_var() {
        let mut vars = BTreeMap::new();
        vars.insert("tags".to_string(), serde_json::json!(["a", "b"]));
        let err = render_template("tags=${tags}", &vars, "rule", "key").unwrap_err();
        assert!(err.to_string().contains("must be a scalar"));
    }

    #[test]
    fn template_applies_upper_transform() {
        let mut vars = BTreeMap::new();
        vars.insert("name".to_string(), JsonValue::String("leaf01".to_string()));
        let rendered = render_template("${name|upper}", &vars, "rule", "key").unwrap();
        assert_eq!(rendered, "LEAF01");
    }

    #[test]
    fn template_applies_lower_transform() {
        let mut vars = BTreeMap::new();
        vars.insert("name".to_string(), JsonValue::String("LEAF01".to_string()));
        let rendered = render_template("${name|lower}", &vars, "rule", "key").unwrap();
        assert_eq!(rendered, "leaf01");
    }

    #[test]
    fn template_applies_trim_transform() {
        let mut vars = BTreeMap::new();
        vars.insert(
            "name".to_string(),
            JsonValue::String("  leaf01  ".to_string()),
        );
        let rendered = render_template("name=${name|trim}", &vars, "rule", "key").unwrap();
        assert_eq!(rendered, "name=leaf01");
    }

    #[test]
    fn template_applies_chained_transforms() {
        let mut vars = BTreeMap::new();
        vars.insert("x".to_string(), JsonValue::String("  leaf01  ".to_string()));
        let rendered = render_template("${x|trim|upper}", &vars, "rule", "key").unwrap();
        assert_eq!(rendered, "LEAF01");
    }

    #[test]
    fn template_errors_on_unknown_transform() {
        let mut vars = BTreeMap::new();
        vars.insert("name".to_string(), JsonValue::String("leaf01".to_string()));
        let err = render_template("${name|frobnicate}", &vars, "rule", "key").unwrap_err();
        assert!(err.to_string().contains("unknown transform frobnicate"));
    }

    #[test]
    fn lone_placeholder_without_transform_preserves_type() {
        let mut vars = BTreeMap::new();
        vars.insert("asn".to_string(), JsonValue::Number(65001.into()));
        let rendered = render_string_value("${asn}", &vars, "rule", "key", false)
            .unwrap()
            .unwrap();
        assert_eq!(rendered, JsonValue::Number(65001.into()));
    }

    #[test]
    fn lone_placeholder_with_transform_coerces_and_applies() {
        let mut vars = BTreeMap::new();
        vars.insert("name".to_string(), JsonValue::String("leaf01".to_string()));
        let rendered = render_string_value("${name|upper}", &vars, "rule", "key", false)
            .unwrap()
            .unwrap();
        assert_eq!(rendered, JsonValue::String("LEAF01".to_string()));
    }

    #[test]
    fn lone_placeholder_number_with_transform_coerces() {
        let mut vars = BTreeMap::new();
        vars.insert("asn".to_string(), JsonValue::Number(65001.into()));
        let rendered = render_string_value("${asn|trim}", &vars, "rule", "key", false)
            .unwrap()
            .unwrap();
        assert_eq!(rendered, JsonValue::String("65001".to_string()));
    }

    /// render `${name|slug}` over a single string var.
    fn slug_of(input: &str) -> String {
        let mut vars = BTreeMap::new();
        vars.insert("name".to_string(), JsonValue::String(input.to_string()));
        render_template("${name|slug}", &vars, "rule", "key").unwrap()
    }

    #[test]
    fn template_applies_slug_transform() {
        assert_eq!(slug_of("Frankfurt DC1"), "frankfurt-dc1");
        assert_eq!(slug_of("leaf-01"), "leaf-01"); // already a slug, unchanged
        assert_eq!(slug_of("  Spine  "), "spine"); // surrounding whitespace dropped
        assert_eq!(slug_of("a__b"), "a-b"); // underscores normalize to '-'
        assert_eq!(slug_of("AS65001"), "as65001"); // lowercased
        assert_eq!(slug_of("he\u{fc}llo"), "he-llo"); // non-ascii char -> single '-'
    }

    #[test]
    fn template_slug_errors_on_empty_result() {
        let mut vars = BTreeMap::new();
        vars.insert("name".to_string(), JsonValue::String("---".to_string()));
        let err = render_template("${name|slug}", &vars, "rule", "key").unwrap_err();
        assert!(err
            .to_string()
            .contains("slug transform produced an empty slug"));
    }

    #[test]
    fn template_chains_trim_and_slug() {
        let mut vars = BTreeMap::new();
        vars.insert(
            "x".to_string(),
            JsonValue::String("  Frankfurt DC1  ".to_string()),
        );
        let rendered = render_template("${x|trim|slug}", &vars, "rule", "key").unwrap();
        assert_eq!(rendered, "frankfurt-dc1");
    }

    #[test]
    fn slug_transform_outputs_valid_slug() {
        use alembic_core::{
            validate_inventory, FieldFormat, FieldSchema, FieldType, Inventory, JsonMap, Key,
            Object, Schema, TypeName, TypeSchema,
        };

        // route a slugified value through alembic-core's real `slug` format
        // validator, proving the transform emits something the schema accepts.
        fn is_valid_slug(candidate: &str) -> bool {
            let type_schema = TypeSchema {
                key: BTreeMap::from([(
                    "slug".to_string(),
                    FieldSchema {
                        r#type: FieldType::String,
                        required: true,
                        nullable: false,
                        description: None,
                        format: Some(FieldFormat::Slug),
                        pattern: None,
                    },
                )]),
                fields: BTreeMap::new(),
            };
            let mut key = BTreeMap::new();
            key.insert("slug".to_string(), serde_json::json!(candidate));
            let object = Object::new(
                uuid::Uuid::from_u128(1),
                TypeName::new("site"),
                Key::from(key),
                JsonMap::default(),
            )
            .unwrap();
            validate_inventory(&Inventory {
                schema: Schema {
                    types: BTreeMap::from([("site".to_string(), type_schema)]),
                },
                objects: vec![object],
            })
            .is_ok()
        }

        assert!(is_valid_slug(&slug_of("Frankfurt DC1")));
        assert!(is_valid_slug(&slug_of("AS65001")));
        // the validator genuinely discriminates: a non-slug is rejected.
        assert!(!is_valid_slug("Frankfurt DC1"));
    }
}
