use alembic_core::{uid_v5, Key, Uid};
use anyhow::{anyhow, Result};
use serde::Deserialize;
use serde_json::{Map as JsonObject, Value as JsonValue};
use serde_yaml::{Mapping as YamlMapping, Value as YamlValue};
use std::collections::BTreeMap;

/// a `v5: { type, stable }` uid spec, shared by the map emit path (`transform`)
/// and the `${ uid: { type, stable } }` attr form rendered here.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UidV5Spec {
    #[serde(rename = "type", alias = "kind")]
    pub type_name: String,
    pub stable: String,
}

/// the transform registry consulted by `${var|name}` pipelines: user-defined
/// starlark transforms (when the map spec carries a `transforms:` block and the
/// `starlark` feature is enabled) are checked first, then the built-in four
/// (`upper`, `lower`, `trim`, `slug`), so user transforms may shadow built-ins.
pub(crate) struct TransformRegistry {
    #[cfg(feature = "starlark")]
    user: Option<crate::starlark_transforms::StarlarkTransforms>,
}

impl TransformRegistry {
    /// a registry with no user transforms: the built-in four only.
    pub(crate) const EMPTY: TransformRegistry = TransformRegistry {
        #[cfg(feature = "starlark")]
        user: None,
    };

    #[cfg(feature = "starlark")]
    pub(crate) fn with_user(user: crate::starlark_transforms::StarlarkTransforms) -> Self {
        TransformRegistry { user: Some(user) }
    }

    /// call a user-defined transform. `Ok(None)` means `name` is not
    /// user-defined and the caller falls through to the built-ins; `Err` is a
    /// call failure (e.g. `fail()` inside the transform).
    fn call_user(
        &self,
        name: &str,
        value: &JsonValue,
        args: &[JsonValue],
    ) -> Result<Option<JsonValue>> {
        #[cfg(feature = "starlark")]
        if let Some(user) = &self.user {
            return user.call(name, value, args);
        }
        let _ = (name, value, args);
        Ok(None)
    }
}

/// per-rule render context: the var namespace, the transform registry, and the
/// rule name used in error messages. the `context` label stays a per-call
/// parameter because it varies within a rule (`key.slug`, `attrs`, ...).
pub(crate) struct RenderCtx<'a> {
    pub(crate) vars: &'a BTreeMap<String, JsonValue>,
    pub(crate) transforms: &'a TransformRegistry,
    pub(crate) rule: &'a str,
}

/// what a *transformed* lone placeholder renders to. an untransformed lone
/// `${var}` always preserves the var's typed value, and embedded templates are
/// always strings; this mode only decides the transformed-lone-placeholder
/// case.
#[derive(Clone, Copy)]
pub(crate) enum TransformedOutput {
    /// the transform result keeps its type (`attrs:` templates, where a user
    /// transform may return a list or dict).
    Typed,
    /// the transform result is coerced to a string with the scalar rules
    /// (`key:` templates and everything feeding uid derivation).
    String,
}

pub(crate) fn render_attrs(
    attrs: &BTreeMap<String, YamlValue>,
    ctx: &RenderCtx,
    context: &str,
) -> Result<JsonObject<String, JsonValue>> {
    let mut map = JsonObject::new();
    for (key, value) in attrs {
        let rendered = render_yaml_value(value, ctx, context, false, TransformedOutput::Typed)?;
        if let Some(value) = rendered {
            map.insert(key.clone(), value);
        }
    }
    Ok(map)
}

pub(crate) fn render_key(key: &BTreeMap<String, YamlValue>, ctx: &RenderCtx) -> Result<Key> {
    let mut map = BTreeMap::new();
    for (field, value) in key {
        let context = format!("key.{field}");
        let rendered = render_yaml_value(value, ctx, &context, false, TransformedOutput::String)?;
        let Some(value) = rendered else {
            return Err(anyhow!("rule {}: missing value for {context}", ctx.rule));
        };
        map.insert(field.clone(), value);
    }
    Ok(Key::from(map))
}

pub(crate) fn render_yaml_value(
    value: &YamlValue,
    ctx: &RenderCtx,
    context: &str,
    allow_missing: bool,
    output: TransformedOutput,
) -> Result<Option<JsonValue>> {
    let rendered = render_yaml_value_inner(value, ctx, context, allow_missing, output)?;
    // key/uid components feed uid derivation and must stay scalar (docs/map.md)
    if let (TransformedOutput::String, Some(value)) = (output, &rendered) {
        ensure_scalar(value, ctx.rule, context)?;
    }
    Ok(rendered)
}

fn render_yaml_value_inner(
    value: &YamlValue,
    ctx: &RenderCtx,
    context: &str,
    allow_missing: bool,
    output: TransformedOutput,
) -> Result<Option<JsonValue>> {
    match value {
        YamlValue::String(raw) => render_string_value(raw, ctx, context, allow_missing, output),
        YamlValue::Sequence(items) => {
            let mut rendered = Vec::new();
            for item in items {
                let value = render_yaml_value(item, ctx, context, allow_missing, output)?;
                match value {
                    Some(value) => rendered.push(value),
                    None => {
                        if allow_missing {
                            return Ok(None);
                        }
                        return Err(anyhow!("rule {}: missing value in {context}", ctx.rule));
                    }
                }
            }
            Ok(Some(JsonValue::Array(rendered)))
        }
        YamlValue::Mapping(map) => {
            if let Some((optional, spec)) = parse_uid_mapping(map) {
                return render_uid_mapping(&spec, ctx, context, optional);
            }

            let mut rendered = JsonObject::new();
            for (key, value) in map {
                let key = key
                    .as_str()
                    .ok_or_else(|| anyhow!("rule {}: {context} keys must be strings", ctx.rule))?
                    .to_string();
                let value = render_yaml_value(value, ctx, context, allow_missing, output)?;
                match value {
                    Some(value) => {
                        rendered.insert(key, value);
                    }
                    None => {
                        if allow_missing {
                            return Ok(None);
                        }
                        return Err(anyhow!("rule {}: missing value in {context}", ctx.rule));
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
    ctx: &RenderCtx,
    context: &str,
    optional: bool,
) -> Result<Option<JsonValue>> {
    let kind = render_template_optional(&spec.type_name, ctx, context, optional)?;
    let stable = render_template_optional(&spec.stable, ctx, context, optional)?;
    let (Some(kind), Some(stable)) = (kind, stable) else {
        return Ok(None);
    };
    // an optional uid whose components render empty is skipped, not an error.
    if optional && (kind.trim().is_empty() || stable.trim().is_empty()) {
        return Ok(None);
    }
    let uid = derive_v5_uid(&kind, &stable, ctx.rule)?;
    Ok(Some(JsonValue::String(uid.to_string())))
}

/// derive a `v5` uid from already-rendered `type`/`stable` components, rejecting
/// either trimming empty. shared by the `${uid: {...}}` attr form and the map
/// emit/named-uid `v5` specs.
pub(crate) fn derive_v5_uid(kind: &str, stable: &str, rule: &str) -> Result<Uid> {
    if kind.trim().is_empty() || stable.trim().is_empty() {
        return Err(anyhow!(
            "rule {rule}: uid requires non-empty type and stable values"
        ));
    }
    Ok(uid_v5(kind, stable))
}

/// look up `name` in the var namespace, applying the shared missing/null
/// short-circuit: a missing var (or, under `allow_missing`, a null one) yields
/// `Ok(None)`; a missing var without `allow_missing` is an error.
fn resolve_var<'a>(
    ctx: &RenderCtx<'a>,
    name: &str,
    context: &str,
    allow_missing: bool,
) -> Result<Option<&'a JsonValue>> {
    let Some(value) = ctx.vars.get(name) else {
        if allow_missing {
            return Ok(None);
        }
        return Err(anyhow!(
            "rule {}: missing var {name} in {context}",
            ctx.rule
        ));
    };
    if value.is_null() && allow_missing {
        return Ok(None);
    }
    Ok(Some(value))
}

fn render_string_value(
    raw: &str,
    ctx: &RenderCtx,
    context: &str,
    allow_missing: bool,
    output: TransformedOutput,
) -> Result<Option<JsonValue>> {
    if let Some(inner) = placeholder_only(raw) {
        let placeholder = parse_placeholder(inner, ctx.rule, context)?;
        let Some(value) = resolve_var(ctx, placeholder.name, context, allow_missing)? else {
            return Ok(None);
        };
        // a lone `${var}` with no transforms preserves its raw typed value;
        // render_yaml_value gates scalar-ness for key/uid output.
        if placeholder.transforms.is_empty() {
            return Ok(Some(value.clone()));
        }
        return match output {
            TransformedOutput::Typed => Ok(Some(apply_placeholder_typed(
                value,
                &placeholder,
                ctx,
                context,
            )?)),
            TransformedOutput::String => Ok(Some(JsonValue::String(apply_placeholder(
                value,
                &placeholder,
                ctx,
                context,
            )?))),
        };
    }

    if raw.contains("${") {
        let rendered = render_template_optional(raw, ctx, context, allow_missing)?;
        return Ok(rendered.map(JsonValue::String));
    }

    Ok(Some(JsonValue::String(raw.to_string())))
}

fn placeholder_only(input: &str) -> Option<&str> {
    if !input.starts_with("${") {
        return None;
    }
    let body = &input[2..];
    let end = find_placeholder_end(body)?;
    if end != body.len() - 1 {
        return None;
    }
    let inner = &body[..end];
    // the quote-aware `end == body.len() - 1` check above already proves
    // lone-ness, so don't reject a `${` sitting inside a quoted arg here.
    if inner.is_empty() {
        return None;
    }
    Some(inner)
}

/// byte index of the first `}` in `s` that is not inside a quoted string
/// literal, so transform arguments may contain `}` (e.g. `${x|strip("}")}`).
fn find_placeholder_end(s: &str) -> Option<usize> {
    let mut quote: Option<char> = None;
    let mut escaped = false;
    for (idx, c) in s.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match quote {
            Some(q) => match c {
                '\\' => escaped = true,
                c if c == q => quote = None,
                _ => {}
            },
            None => match c {
                '\'' | '"' => quote = Some(c),
                '}' => return Some(idx),
                _ => {}
            },
        }
    }
    None
}

/// split `s` on a top-level separator: occurrences inside quoted string
/// literals or parentheses do not split. shared by the `|` pipeline split and
/// the `,` argument split.
fn split_top_level(s: &str, separator: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut quote: Option<char> = None;
    let mut escaped = false;
    let mut depth = 0usize;
    for (idx, c) in s.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match quote {
            Some(q) => match c {
                '\\' => escaped = true,
                c if c == q => quote = None,
                _ => {}
            },
            None => match c {
                '\'' | '"' => quote = Some(c),
                '(' => depth += 1,
                ')' => depth = depth.saturating_sub(1),
                c if c == separator && depth == 0 => {
                    parts.push(&s[start..idx]);
                    start = idx + c.len_utf8();
                }
                _ => {}
            },
        }
    }
    parts.push(&s[start..]);
    parts
}

/// a parsed `${var|transform|...}` placeholder body: the variable name plus the
/// (possibly empty) ordered pipeline of transform calls.
struct Placeholder<'a> {
    name: &'a str,
    transforms: Vec<TransformCall<'a>>,
}

/// one stage of a transform pipeline: a bare `name` or a `name(arg, ...)` call
/// with literal arguments, parsed straight to json values.
struct TransformCall<'a> {
    name: &'a str,
    args: Vec<JsonValue>,
}

/// parse the inside of a `${...}` placeholder into a var name and transform
/// pipeline. the single shared parser used by both the embedded-template path
/// (`render_template_optional`) and the lone-placeholder path
/// (`render_string_value`). surrounding whitespace on the name and on each
/// transform is ignored, so `${ slug | upper }` and `${slug|upper}` are equal.
fn parse_placeholder<'a>(inner: &'a str, rule: &str, context: &str) -> Result<Placeholder<'a>> {
    let mut parts = split_top_level(inner, '|').into_iter();
    let name = parts.next().unwrap_or("").trim();
    if name.is_empty() {
        return Err(anyhow!(
            "rule {rule}: placeholder has no variable name in {context}"
        ));
    }
    let mut transforms = Vec::new();
    for segment in parts {
        transforms.push(parse_transform_call(segment, rule, context)?);
    }
    Ok(Placeholder { name, transforms })
}

fn parse_transform_call<'a>(
    segment: &'a str,
    rule: &str,
    context: &str,
) -> Result<TransformCall<'a>> {
    let segment = segment.trim();
    let Some(open) = segment.find('(') else {
        if segment.is_empty() {
            return Err(anyhow!(
                "rule {rule}: invalid transform call `{segment}` in {context}"
            ));
        }
        return Ok(TransformCall {
            name: segment,
            args: Vec::new(),
        });
    };
    let name = segment[..open].trim();
    if name.is_empty() || !segment.ends_with(')') {
        return Err(anyhow!(
            "rule {rule}: invalid transform call `{segment}` in {context}"
        ));
    }
    let args_str = &segment[open + 1..segment.len() - 1];
    let mut args = Vec::new();
    if !args_str.trim().is_empty() {
        for token in split_top_level(args_str, ',') {
            args.push(parse_literal_arg(token, rule, context)?);
        }
    }
    Ok(TransformCall { name, args })
}

/// parse one literal transform argument: a single- or double-quoted string
/// (with `\\`, `\'`, `\"`, `\n`, `\t` escapes), an integer, a float, or
/// `true`/`false`. no variable references and no nested calls.
fn parse_literal_arg(token: &str, rule: &str, context: &str) -> Result<JsonValue> {
    let token = token.trim();
    if let Some(quote) = token.chars().next().filter(|c| *c == '\'' || *c == '"') {
        return parse_string_literal(token, quote, rule, context);
    }
    match token {
        "true" => return Ok(JsonValue::Bool(true)),
        "false" => return Ok(JsonValue::Bool(false)),
        _ => {}
    }
    if let Ok(int) = token.parse::<i64>() {
        return Ok(JsonValue::Number(int.into()));
    }
    if let Ok(int) = token.parse::<u64>() {
        return Ok(JsonValue::Number(int.into()));
    }
    if let Ok(float) = token.parse::<f64>() {
        if let Some(number) = serde_json::Number::from_f64(float) {
            return Ok(JsonValue::Number(number));
        }
    }
    Err(anyhow!(
        "rule {rule}: invalid transform argument `{token}` in {context}"
    ))
}

fn parse_string_literal(token: &str, quote: char, rule: &str, context: &str) -> Result<JsonValue> {
    let invalid = || anyhow!("rule {rule}: invalid transform argument `{token}` in {context}");
    let mut out = String::new();
    let mut escaped = false;
    let mut closed = false;
    for c in token.chars().skip(1) {
        if closed {
            // trailing characters after the closing quote.
            return Err(invalid());
        }
        if escaped {
            out.push(match c {
                'n' => '\n',
                't' => '\t',
                other => other,
            });
            escaped = false;
        } else if c == '\\' {
            escaped = true;
        } else if c == quote {
            closed = true;
        } else {
            out.push(c);
        }
    }
    if !closed {
        return Err(invalid());
    }
    Ok(JsonValue::String(out))
}

/// run the transform pipeline typed, left to right. a user-defined transform
/// receives and returns typed values; a built-in coerces its input scalar to a
/// string (preserving the established `${num|upper}` behavior) and returns a
/// string. user transforms are consulted first, so they may shadow built-ins.
fn apply_placeholder_typed(
    value: &JsonValue,
    placeholder: &Placeholder,
    ctx: &RenderCtx,
    context: &str,
) -> Result<JsonValue> {
    let mut current = value.clone();
    for call in &placeholder.transforms {
        let user = ctx
            .transforms
            .call_user(call.name, &current, &call.args)
            .map_err(|err| {
                anyhow!(
                    "rule {}: transform {} failed in {context}: {err:#}",
                    ctx.rule,
                    call.name
                )
            })?;
        if let Some(result) = user {
            current = result;
            continue;
        }
        current = JsonValue::String(apply_builtin(
            call,
            &current,
            placeholder.name,
            ctx,
            context,
        )?);
    }
    Ok(current)
}

/// apply one built-in transform stage. built-ins take no arguments and operate
/// on the scalar-coerced string form of the current value.
fn apply_builtin(
    call: &TransformCall,
    current: &JsonValue,
    var_name: &str,
    ctx: &RenderCtx,
    context: &str,
) -> Result<String> {
    // classify the name first, so an unknown transform errors before the
    // argument check and coercion (the original precedence), then dispatch
    // exhaustively over the four built-ins.
    enum Builtin {
        Upper,
        Lower,
        Trim,
        Slug,
    }
    let builtin = match call.name {
        "upper" => Builtin::Upper,
        "lower" => Builtin::Lower,
        "trim" => Builtin::Trim,
        "slug" => Builtin::Slug,
        other => {
            return Err(anyhow!(
                "rule {}: unknown transform {other} in {context}",
                ctx.rule
            ))
        }
    };
    if !call.args.is_empty() {
        return Err(anyhow!(
            "rule {}: transform {} takes no arguments in {context}",
            ctx.rule,
            call.name
        ));
    }
    let value = coerce_to_string(current, var_name, ctx.rule, context)?;
    Ok(match builtin {
        Builtin::Upper => value.to_uppercase(),
        Builtin::Lower => value.to_lowercase(),
        Builtin::Trim => value.trim().to_string(),
        // `mapping::slugify` lowercases, collapses runs of non-`[a-z0-9]`
        // to a single `-`, and trims leading/trailing `-`, producing a valid
        // `slug` value. an input with no ascii alphanumerics (e.g. `---`)
        // slugifies to an empty string, which is not a valid slug, so we
        // error rather than emit it.
        Builtin::Slug => {
            let slug = crate::mapping::slugify(&value);
            if slug.is_empty() {
                return Err(anyhow!(
                    "rule {}: slug transform produced an empty slug in {context}",
                    ctx.rule
                ));
            }
            slug
        }
    })
}

/// run a single named transform (with literal args) against a value, typed.
/// backs `alembic map transform`, the cli iteration loop for user transforms.
pub(crate) fn apply_single_transform(
    registry: &TransformRegistry,
    name: &str,
    value: &JsonValue,
    args: &[JsonValue],
) -> Result<JsonValue> {
    let vars = BTreeMap::new();
    let ctx = RenderCtx {
        vars: &vars,
        transforms: registry,
        rule: "transform",
    };
    let placeholder = Placeholder {
        name: "value",
        transforms: vec![TransformCall {
            name,
            args: args.to_vec(),
        }],
    };
    apply_placeholder_typed(value, &placeholder, &ctx, "cli")
}

/// string-context pipeline: run typed, then coerce the result with the scalar
/// rules.
fn apply_placeholder(
    value: &JsonValue,
    placeholder: &Placeholder,
    ctx: &RenderCtx,
    context: &str,
) -> Result<String> {
    let typed = apply_placeholder_typed(value, placeholder, ctx, context)?;
    coerce_to_string(&typed, placeholder.name, ctx.rule, context)
}

/// whether a value has a scalar string form, without allocating one (cf. [`scalar_string`]).
fn is_scalar(value: &JsonValue) -> bool {
    matches!(
        value,
        JsonValue::String(_) | JsonValue::Number(_) | JsonValue::Bool(_)
    )
}

/// the scalar->string partition shared by predicate comparison, template
/// coercion, and key/uid scalar gating: strings clone through, numbers and bools
/// take their natural form (`42`, `true`); null, arrays, and objects have no
/// scalar string form.
pub(crate) fn scalar_string(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::String(value) => Some(value.clone()),
        JsonValue::Number(value) => Some(value.to_string()),
        JsonValue::Bool(value) => Some(value.to_string()),
        JsonValue::Null | JsonValue::Array(_) | JsonValue::Object(_) => None,
    }
}

/// render a scalar var as a string. strings pass through; numbers and bools are
/// coerced to their natural form (`42`, `true`). nulls, arrays, and objects
/// have no template representation and are an error.
fn coerce_to_string(value: &JsonValue, name: &str, rule: &str, context: &str) -> Result<String> {
    scalar_string(value).ok_or_else(|| {
        if value.is_null() {
            anyhow!(
                "rule {rule}: var {name} in {context} is null and cannot be rendered as a string"
            )
        } else {
            anyhow!(
                "rule {rule}: var {name} in {context} must be a scalar (string, number, or bool)"
            )
        }
    })
}

/// a key or uid component must be scalar; null, arrays, and objects have no
/// identity form (docs/map.md).
fn ensure_scalar(value: &JsonValue, rule: &str, context: &str) -> Result<()> {
    if is_scalar(value) {
        return Ok(());
    }
    Err(if value.is_null() {
        anyhow!("rule {rule}: {context} is null and cannot be rendered as a string")
    } else {
        anyhow!("rule {rule}: {context} must be a scalar (string, number, or bool)")
    })
}

pub(crate) fn render_template(template: &str, ctx: &RenderCtx, context: &str) -> Result<String> {
    render_template_optional(template, ctx, context, false)?
        .ok_or_else(|| anyhow!("rule {}: missing vars for template {template}", ctx.rule))
}

fn render_template_optional(
    template: &str,
    ctx: &RenderCtx,
    context: &str,
    allow_missing: bool,
) -> Result<Option<String>> {
    let mut rendered = String::new();
    let mut rest = template;

    while let Some(start) = rest.find("${") {
        rendered.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = find_placeholder_end(after) else {
            return Err(anyhow!(
                "rule {}: unterminated template in {context}: {template}",
                ctx.rule
            ));
        };
        let placeholder = parse_placeholder(&after[..end], ctx.rule, context)?;
        let Some(value) = resolve_var(ctx, placeholder.name, context, allow_missing)? else {
            return Ok(None);
        };
        rendered.push_str(&apply_placeholder(value, &placeholder, ctx, context)?);
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

    /// a render context over `vars` with no user transforms and a fixed rule
    /// name.
    fn ctx(vars: &BTreeMap<String, JsonValue>) -> RenderCtx<'_> {
        static EMPTY: TransformRegistry = TransformRegistry::EMPTY;
        RenderCtx {
            vars,
            transforms: &EMPTY,
            rule: "rule",
        }
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
        let rendered = render_yaml_value(
            &mapping,
            &ctx(&vars),
            "attrs",
            false,
            TransformedOutput::Typed,
        )
        .unwrap();
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
        let err = render_yaml_value(
            &mapping,
            &ctx(&vars),
            "attrs",
            false,
            TransformedOutput::Typed,
        )
        .unwrap_err();
        assert!(err.to_string().contains("missing var"));
    }

    #[test]
    fn render_uid_mapping_derives_uid_from_type_and_stable() {
        let mut vars = BTreeMap::new();
        vars.insert("slug".to_string(), JsonValue::String("fra1".to_string()));
        let mapping: YamlValue = serde_yaml::from_str(
            r#"
uid:
  type: "dcim.site"
  stable: "site=${slug}"
"#,
        )
        .unwrap();
        let rendered = render_yaml_value(
            &mapping,
            &ctx(&vars),
            "attrs",
            false,
            TransformedOutput::Typed,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            rendered,
            JsonValue::String(uid_v5("dcim.site", "site=fra1").to_string())
        );
    }

    #[test]
    fn templates_substitute_and_error_on_missing() {
        let mut vars = BTreeMap::new();
        vars.insert("name".to_string(), JsonValue::String("leaf01".to_string()));
        let rendered = render_template("device=${name}", &ctx(&vars), "key").unwrap();
        assert_eq!(rendered, "device=leaf01");

        let err = render_template("device=${missing}", &ctx(&vars), "key").unwrap_err();
        assert!(err.to_string().contains("missing var"));
    }

    #[test]
    fn template_coerces_number_var() {
        let mut vars = BTreeMap::new();
        vars.insert("asn".to_string(), JsonValue::Number(65001.into()));
        let rendered = render_template("asn=${asn}", &ctx(&vars), "key").unwrap();
        assert_eq!(rendered, "asn=65001");
    }

    #[test]
    fn template_coerces_bool_var() {
        let mut vars = BTreeMap::new();
        vars.insert("enabled".to_string(), JsonValue::Bool(true));
        let rendered = render_template("flag=${enabled}", &ctx(&vars), "key").unwrap();
        assert_eq!(rendered, "flag=true");
    }

    #[test]
    fn template_errors_on_array_var() {
        let mut vars = BTreeMap::new();
        vars.insert("tags".to_string(), serde_json::json!(["a", "b"]));
        let err = render_template("tags=${tags}", &ctx(&vars), "key").unwrap_err();
        assert!(err.to_string().contains("must be a scalar"));
    }

    #[test]
    fn template_applies_upper_transform() {
        let mut vars = BTreeMap::new();
        vars.insert("name".to_string(), JsonValue::String("leaf01".to_string()));
        let rendered = render_template("${name|upper}", &ctx(&vars), "key").unwrap();
        assert_eq!(rendered, "LEAF01");
    }

    #[test]
    fn template_applies_lower_transform() {
        let mut vars = BTreeMap::new();
        vars.insert("name".to_string(), JsonValue::String("LEAF01".to_string()));
        let rendered = render_template("${name|lower}", &ctx(&vars), "key").unwrap();
        assert_eq!(rendered, "leaf01");
    }

    #[test]
    fn template_applies_trim_transform() {
        let mut vars = BTreeMap::new();
        vars.insert(
            "name".to_string(),
            JsonValue::String("  leaf01  ".to_string()),
        );
        let rendered = render_template("name=${name|trim}", &ctx(&vars), "key").unwrap();
        assert_eq!(rendered, "name=leaf01");
    }

    #[test]
    fn template_applies_chained_transforms() {
        let mut vars = BTreeMap::new();
        vars.insert("x".to_string(), JsonValue::String("  leaf01  ".to_string()));
        let rendered = render_template("${x|trim|upper}", &ctx(&vars), "key").unwrap();
        assert_eq!(rendered, "LEAF01");
    }

    #[test]
    fn template_errors_on_unknown_transform() {
        let mut vars = BTreeMap::new();
        vars.insert("name".to_string(), JsonValue::String("leaf01".to_string()));
        let err = render_template("${name|frobnicate}", &ctx(&vars), "key").unwrap_err();
        assert!(err.to_string().contains("unknown transform frobnicate"));
    }

    #[test]
    fn builtin_transform_rejects_arguments() {
        let mut vars = BTreeMap::new();
        vars.insert("name".to_string(), JsonValue::String("leaf01".to_string()));
        let err = render_template("${name|upper(1)}", &ctx(&vars), "key").unwrap_err();
        assert!(err
            .to_string()
            .contains("transform upper takes no arguments"));
    }

    #[test]
    fn lone_placeholder_without_transform_preserves_type() {
        let mut vars = BTreeMap::new();
        vars.insert("asn".to_string(), JsonValue::Number(65001.into()));
        let rendered = render_string_value(
            "${asn}",
            &ctx(&vars),
            "key",
            false,
            TransformedOutput::Typed,
        )
        .unwrap()
        .unwrap();
        assert_eq!(rendered, JsonValue::Number(65001.into()));
    }

    #[test]
    fn lone_placeholder_with_transform_coerces_and_applies() {
        let mut vars = BTreeMap::new();
        vars.insert("name".to_string(), JsonValue::String("leaf01".to_string()));
        let rendered = render_string_value(
            "${name|upper}",
            &ctx(&vars),
            "key",
            false,
            TransformedOutput::String,
        )
        .unwrap()
        .unwrap();
        assert_eq!(rendered, JsonValue::String("LEAF01".to_string()));
    }

    #[test]
    fn lone_placeholder_number_with_transform_coerces() {
        let mut vars = BTreeMap::new();
        vars.insert("asn".to_string(), JsonValue::Number(65001.into()));
        let rendered = render_string_value(
            "${asn|trim}",
            &ctx(&vars),
            "key",
            false,
            TransformedOutput::String,
        )
        .unwrap()
        .unwrap();
        assert_eq!(rendered, JsonValue::String("65001".to_string()));
    }

    #[test]
    fn render_key_rejects_null_lone_placeholder() {
        let mut vars = BTreeMap::new();
        vars.insert("x".to_string(), JsonValue::Null);
        let mut key = BTreeMap::new();
        key.insert("slug".to_string(), YamlValue::String("${x}".to_string()));
        let err = render_key(&key, &ctx(&vars)).unwrap_err();
        assert!(err.to_string().contains("is null"), "{err:#}");
    }

    #[test]
    fn lone_placeholder_null_preserved_in_typed_mode() {
        let mut vars = BTreeMap::new();
        vars.insert("x".to_string(), JsonValue::Null);
        let rendered = render_string_value(
            "${x}",
            &ctx(&vars),
            "attrs",
            false,
            TransformedOutput::Typed,
        )
        .unwrap()
        .unwrap();
        assert_eq!(rendered, JsonValue::Null);
    }

    #[test]
    fn render_key_rejects_array_lone_placeholder() {
        let mut vars = BTreeMap::new();
        vars.insert("tags".to_string(), serde_json::json!(["a", "b"]));
        let mut key = BTreeMap::new();
        key.insert("k".to_string(), YamlValue::String("${tags}".to_string()));
        let err = render_key(&key, &ctx(&vars)).unwrap_err();
        assert!(err.to_string().contains("must be a scalar"), "{err:#}");
    }

    #[test]
    fn render_key_rejects_literal_sequence() {
        let vars = BTreeMap::new();
        let mut key = BTreeMap::new();
        key.insert(
            "k".to_string(),
            YamlValue::Sequence(vec![YamlValue::String("a".to_string())]),
        );
        let err = render_key(&key, &ctx(&vars)).unwrap_err();
        assert!(err.to_string().contains("must be a scalar"), "{err:#}");
    }

    #[test]
    fn render_key_rejects_literal_null() {
        let vars = BTreeMap::new();
        let mut key = BTreeMap::new();
        key.insert("k".to_string(), YamlValue::Null);
        let err = render_key(&key, &ctx(&vars)).unwrap_err();
        assert!(err.to_string().contains("is null"), "{err:#}");
    }

    #[test]
    fn render_attrs_keeps_composite() {
        let mut vars = BTreeMap::new();
        vars.insert("tags".to_string(), serde_json::json!(["a", "b"]));
        let mut attrs = BTreeMap::new();
        attrs.insert("tags".to_string(), YamlValue::String("${tags}".to_string()));
        let rendered = render_attrs(&attrs, &ctx(&vars), "attrs").unwrap();
        assert_eq!(
            rendered.get("tags").unwrap(),
            &serde_json::json!(["a", "b"])
        );
    }

    /// render `${name|slug}` over a single string var.
    fn slug_of(input: &str) -> String {
        let mut vars = BTreeMap::new();
        vars.insert("name".to_string(), JsonValue::String(input.to_string()));
        render_template("${name|slug}", &ctx(&vars), "key").unwrap()
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
        let err = render_template("${name|slug}", &ctx(&vars), "key").unwrap_err();
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
        let rendered = render_template("${x|trim|slug}", &ctx(&vars), "key").unwrap();
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

    /// one parsed transform call: name plus literal args.
    type ParsedCall = (String, Vec<JsonValue>);

    /// parse a full placeholder body, for grammar-only assertions.
    fn parse(inner: &str) -> Result<(String, Vec<ParsedCall>)> {
        let placeholder = parse_placeholder(inner, "rule", "key")?;
        Ok((
            placeholder.name.to_string(),
            placeholder
                .transforms
                .iter()
                .map(|call| (call.name.to_string(), call.args.clone()))
                .collect(),
        ))
    }

    #[test]
    fn parses_bare_transform_pipeline() {
        let (name, calls) = parse("x | trim | upper").unwrap();
        assert_eq!(name, "x");
        assert_eq!(
            calls,
            vec![("trim".to_string(), vec![]), ("upper".to_string(), vec![])]
        );
    }

    #[test]
    fn parses_literal_arguments() {
        let (_, calls) = parse(r#"x|f(1, "a", true, 2.5, -3, 'b')"#).unwrap();
        assert_eq!(
            calls,
            vec![(
                "f".to_string(),
                vec![
                    serde_json::json!(1),
                    serde_json::json!("a"),
                    serde_json::json!(true),
                    serde_json::json!(2.5),
                    serde_json::json!(-3),
                    serde_json::json!("b"),
                ]
            )]
        );
    }

    #[test]
    fn parses_wide_integer_arguments() {
        // i64::MAX, i64::MAX + 1, and u64::MAX must parse to exact integers,
        // not a lossy f64 (mirrors json_to_starlark's i64 -> u64 -> f64 order).
        let (_, calls) =
            parse("x|f(9223372036854775807, 9223372036854775808, 18446744073709551615)").unwrap();
        assert_eq!(
            calls,
            vec![(
                "f".to_string(),
                vec![
                    serde_json::json!(9223372036854775807i64),
                    serde_json::json!(9223372036854775808u64),
                    serde_json::json!(18446744073709551615u64),
                ]
            )]
        );
        // a non-numeric token is still rejected.
        assert!(parse("x|f(abc)").is_err());
    }

    #[test]
    fn parses_empty_argument_list() {
        let (_, calls) = parse("x|f()").unwrap();
        assert_eq!(calls, vec![("f".to_string(), vec![])]);
    }

    #[test]
    fn quoted_arguments_protect_separators() {
        let (_, calls) = parse(r#"x|f("a|b", "c,d", "e)f")"#).unwrap();
        assert_eq!(
            calls,
            vec![(
                "f".to_string(),
                vec![
                    serde_json::json!("a|b"),
                    serde_json::json!("c,d"),
                    serde_json::json!("e)f"),
                ]
            )]
        );
    }

    #[test]
    fn quoted_arguments_support_escapes() {
        let (_, calls) = parse(r#"x|f("a\"b", 'c\'d', "e\\f", "g\nh", "i\tj")"#).unwrap();
        assert_eq!(
            calls,
            vec![(
                "f".to_string(),
                vec![
                    serde_json::json!("a\"b"),
                    serde_json::json!("c'd"),
                    serde_json::json!("e\\f"),
                    serde_json::json!("g\nh"),
                    serde_json::json!("i\tj"),
                ]
            )]
        );
    }

    #[test]
    fn rejects_malformed_transform_calls() {
        for bad in [
            "x|f(",          // missing close paren
            "x|f(1",         // unterminated args
            "x|f)q(",        // name containing ')' before '('
            "x|(1)",         // empty name
            "x|",            // empty segment
            "x|f(unquoted)", // bare word is not a literal
            r#"x|f("a)"#,    // unterminated string
            r#"x|f("a"b)"#,  // trailing junk after string
            "x|f(1.2.3)",    // not a number
        ] {
            let err = parse(bad).unwrap_err();
            let message = err.to_string();
            assert!(
                message.contains("invalid transform"),
                "input {bad}: unexpected error {message}"
            );
        }
    }

    #[test]
    fn rejects_empty_variable_name() {
        // an empty name before the pipe must fail at parse, not later with a
        // misleading `missing var ''`, mirroring the empty-segment rejection.
        for bad in ["|upper", " | upper "] {
            let err = parse(bad).unwrap_err();
            assert!(
                err.to_string().contains("placeholder has no variable name"),
                "input {bad:?}: unexpected error {err}"
            );
        }
        // valid names (with and without transforms / whitespace) still parse.
        assert_eq!(parse("slug").unwrap().0, "slug");
        assert_eq!(parse("slug|upper").unwrap().0, "slug");
        assert_eq!(parse(" slug | upper ").unwrap().0, "slug");
    }

    #[test]
    fn brace_inside_quoted_argument_stays_in_placeholder() {
        // a `}` inside a quoted argument must not terminate the placeholder.
        let mut vars = BTreeMap::new();
        vars.insert("x".to_string(), JsonValue::String("v".to_string()));
        let err = render_template(r#"${x|f("}")}"#, &ctx(&vars), "key").unwrap_err();
        // reaches transform resolution (unknown f), not a parse/termination error.
        assert!(err.to_string().contains("unknown transform f"));
    }

    #[test]
    fn embedded_template_with_argument_call_renders() {
        let mut vars = BTreeMap::new();
        vars.insert("x".to_string(), JsonValue::String(" v ".to_string()));
        // built-in via the embedded path still works alongside text.
        let rendered = render_template("a ${x|trim} b", &ctx(&vars), "key").unwrap();
        assert_eq!(rendered, "a v b");
    }

    #[cfg(feature = "starlark")]
    mod starlark {
        use super::*;
        use crate::starlark_transforms::StarlarkTransforms;

        fn registry(source: &str) -> TransformRegistry {
            TransformRegistry::with_user(StarlarkTransforms::compile(source, "test", None).unwrap())
        }

        fn user_ctx<'a>(
            vars: &'a BTreeMap<String, JsonValue>,
            transforms: &'a TransformRegistry,
        ) -> RenderCtx<'a> {
            RenderCtx {
                vars,
                transforms,
                rule: "rule",
            }
        }

        fn string_var(value: &str) -> BTreeMap<String, JsonValue> {
            let mut vars = BTreeMap::new();
            vars.insert("x".to_string(), JsonValue::String(value.to_string()));
            vars
        }

        #[test]
        fn user_transform_shadows_builtin() {
            let transforms = registry("def upper(v):\n    return v + \"!\"\n");
            let vars = string_var("quiet");
            let rendered =
                render_template("${x|upper}", &user_ctx(&vars, &transforms), "key").unwrap();
            assert_eq!(rendered, "quiet!");
        }

        #[test]
        fn chaining_is_left_to_right_function_composition() {
            // ${x|f|g(2)} must equal g(f(x), 2).
            let transforms =
                registry("def f(v):\n    return v * 2\n\ndef g(v, n):\n    return v + n\n");
            let mut vars = BTreeMap::new();
            vars.insert("x".to_string(), JsonValue::Number(3.into()));
            let rendered = render_string_value(
                "${x|f|g(2)}",
                &user_ctx(&vars, &transforms),
                "attrs",
                false,
                TransformedOutput::Typed,
            )
            .unwrap()
            .unwrap();
            assert_eq!(rendered, serde_json::json!(8));
        }

        #[test]
        fn builtin_output_feeds_user_transform_as_a_string() {
            // ${x|upper|kind}: the built-in coerces the numeric var to a string
            // (apply_placeholder_typed wraps a built-in result as a json string),
            // so the downstream user transform sees a string, not the number 42.
            let transforms = registry("def kind(v):\n    return type(v)\n");
            let mut vars = BTreeMap::new();
            vars.insert("x".to_string(), JsonValue::Number(42.into()));
            let rendered =
                render_template("${x|upper|kind}", &user_ctx(&vars, &transforms), "attrs").unwrap();
            assert_eq!(rendered, "string");
        }

        #[test]
        fn typed_return_survives_in_attrs_mode() {
            let transforms = registry("def wrap(v):\n    return {\"v\": [v, True]}\n");
            let vars = string_var("a");
            let rendered = render_string_value(
                "${x|wrap}",
                &user_ctx(&vars, &transforms),
                "attrs",
                false,
                TransformedOutput::Typed,
            )
            .unwrap()
            .unwrap();
            assert_eq!(rendered, serde_json::json!({"v": ["a", true]}));
        }

        #[test]
        fn quoted_dollarbrace_arg_keeps_lone_placeholder_typed() {
            // a `${` inside a quoted transform arg must not demote a lone typed
            // placeholder to the string-wrapping embedded-template path.
            let transforms = registry("def numify(v, a):\n    return 42\n");
            let vars = string_var("ignored");
            let rendered = render_string_value(
                "${x|numify(\"${\")}",
                &user_ctx(&vars, &transforms),
                "attrs",
                false,
                TransformedOutput::Typed,
            )
            .unwrap()
            .unwrap();
            assert_eq!(rendered, serde_json::json!(42));
        }

        #[test]
        fn scalar_user_return_coerces_in_string_mode() {
            let transforms = registry("def answer(v):\n    return 42\n");
            let vars = string_var("ignored");
            let rendered = render_string_value(
                "${x|answer}",
                &user_ctx(&vars, &transforms),
                "key.slug",
                false,
                TransformedOutput::String,
            )
            .unwrap()
            .unwrap();
            assert_eq!(rendered, JsonValue::String("42".to_string()));
        }

        #[test]
        fn collection_user_return_errors_in_string_mode() {
            let transforms = registry("def wrap(v):\n    return [v]\n");
            let vars = string_var("a");
            let err = render_string_value(
                "${x|wrap}",
                &user_ctx(&vars, &transforms),
                "key.slug",
                false,
                TransformedOutput::String,
            )
            .unwrap_err();
            assert!(err.to_string().contains("must be a scalar"), "{err:#}");
        }

        #[test]
        fn null_user_return_errors_in_string_mode() {
            // a user transform returning None yields json null, which has no
            // scalar string form: coerce_to_string must reject it rather than
            // render an empty or `null` string.
            let transforms = registry("def nullify(v):\n    return None\n");
            let vars = string_var("a");
            let err = render_string_value(
                "${x|nullify}",
                &user_ctx(&vars, &transforms),
                "key.slug",
                false,
                TransformedOutput::String,
            )
            .unwrap_err();
            assert!(err.to_string().contains("is null"), "{err:#}");
        }

        #[test]
        fn fail_surfaces_in_rule_error_shape() {
            let transforms = registry("def f(v):\n    fail(\"bad value\")\n");
            let vars = string_var("a");
            let err = render_template("${x|f}", &user_ctx(&vars, &transforms), "key").unwrap_err();
            let message = err.to_string();
            assert!(
                message.contains("rule rule: transform f failed in key"),
                "{message}"
            );
        }

        #[test]
        fn unknown_transform_still_errors_with_user_transforms_loaded() {
            let transforms = registry("def f(v):\n    return v\n");
            let vars = string_var("a");
            let err =
                render_template("${x|missing}", &user_ctx(&vars, &transforms), "key").unwrap_err();
            assert!(err.to_string().contains("unknown transform missing"));
        }

        #[test]
        fn embedded_template_runs_user_transform() {
            let transforms = registry("def host(v):\n    return v.split(\"/\")[0]\n");
            let vars = string_var("10.0.0.1/24");
            let rendered =
                render_template("addr=${x|host}", &user_ctx(&vars, &transforms), "key").unwrap();
            assert_eq!(rendered, "addr=10.0.0.1");
        }

        #[test]
        fn apply_single_transform_runs_user_and_builtin() {
            let transforms = registry("def cidr_host(v):\n    return v.split(\"/\")[0]\n");
            let result = apply_single_transform(
                &transforms,
                "cidr_host",
                &serde_json::json!("10.0.0.1/24"),
                &[],
            )
            .unwrap();
            assert_eq!(result, serde_json::json!("10.0.0.1"));
            let result =
                apply_single_transform(&transforms, "upper", &serde_json::json!("q"), &[]).unwrap();
            assert_eq!(result, serde_json::json!("Q"));
            let err = apply_single_transform(&transforms, "nope", &serde_json::json!("q"), &[])
                .unwrap_err();
            assert!(err.to_string().contains("unknown transform nope"));
        }

        /// the built-in four reimplemented in starlark must agree with the
        /// native implementations across a corpus -- a standing proof the
        /// substrate is sufficient to express them.
        #[test]
        fn builtin_four_reimplemented_in_starlark_agree() {
            let transforms = registry(
                r#"
def upper_star(v):
    return v.upper()

def lower_star(v):
    return v.lower()

def trim_star(v):
    return v.strip()

def slug_star(v):
    out = ""
    for c in v.elems():
        lower = c.lower()
        if ("a" <= lower and lower <= "z") or ("0" <= lower and lower <= "9"):
            out += lower
        elif out != "" and not out.endswith("-"):
            out += "-"
    if out.endswith("-"):
        out = out[:-1]
    if out == "":
        fail("slug produced an empty slug")
    return out
"#,
            );
            let corpus = [
                "Frankfurt DC1",
                "leaf-01",
                "  Spine  ",
                "a__b",
                "AS65001",
                "he\u{fc}llo",
                "MiXeD 42",
                "---",
                "",
            ];
            let pairs = [
                ("upper", "upper_star"),
                ("lower", "lower_star"),
                ("trim", "trim_star"),
                ("slug", "slug_star"),
            ];
            for input in corpus {
                for (builtin, star) in pairs {
                    let vars = string_var(input);
                    let native = render_template(
                        &format!("${{x|{builtin}}}"),
                        &user_ctx(&vars, &transforms),
                        "key",
                    );
                    let starred = render_template(
                        &format!("${{x|{star}}}"),
                        &user_ctx(&vars, &transforms),
                        "key",
                    );
                    match (native, starred) {
                        (Ok(a), Ok(b)) => {
                            assert_eq!(a, b, "{builtin} vs {star} on {input:?}")
                        }
                        (Err(_), Err(_)) => {}
                        (native, starred) => panic!(
                            "{builtin} vs {star} diverge on {input:?}: {native:?} vs {starred:?}"
                        ),
                    }
                }
            }
        }
    }
}
