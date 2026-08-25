//! map: ir -> ir transformation.
//!
//! map takes an inventory and re-emits it under a different vocabulary --
//! renaming types and fields, dropping or deriving values, rewiring references --
//! using the shared render/emit half (`render_key`, `render_attrs`, templates +
//! transforms). identity is the uid alone: a one-to-one emit and a passthrough
//! carry the source object's uid unchanged, a multi-emit declares each emitted
//! object's uid explicitly, and a group emit keeps value identity, minted from
//! the rendered target `(type, key)`. only explicitly re-identified objects and
//! group emits move refs, so the rewrite passes are quiet for the common case.
//!
//! a rule selects source objects by a type-name pattern with optional field
//! predicates and emits one or more target objects per match (fan-out), or -- with
//! `group_by` -- buckets the matched objects and emits once per group (N->1
//! aggregation). `lookups` follow a ref to read a field from the object it points
//! at, so an emit can pull a value off a related object.

use crate::predicate::{parse_predicates, Predicate, PredicateOp};
use crate::render::{
    render_attrs, render_key, render_template, RenderCtx, TransformRegistry, UidV5Spec,
};
use alembic_core::{
    key_string, uid_v5, FieldType, Inventory, JsonMap, Key, Object, Schema, TypeName, Uid,
};
use anyhow::{anyhow, Context, Result};
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::Value as JsonValue;
use serde_yaml::Value as YamlValue;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// uid override for an emit: a deterministic `v5: { type, stable }`, an
/// explicit uuid-string template, or the keyword `target`, which mints value
/// identity from the rendered target `(type, key)` -- the one spelling that can
/// reproduce the canonical mint, since `key_string` is not reachable from a
/// template.
#[derive(Debug)]
pub enum EmitUid {
    V5 { v5: UidV5Spec },
    Template(String),
    Target,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EmitUidV5 {
    v5: UidV5Spec,
}

// dispatched on the value's shape rather than `#[serde(untagged)]`: untagged
// swallows the chosen variant's own error, so a typo inside `v5:` would report
// "did not match any variant" instead of naming the field.
impl<'de> Deserialize<'de> for EmitUid {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct EmitUidVisitor;

        impl<'de> Visitor<'de> for EmitUidVisitor {
            type Value = EmitUid;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a uuid template string, the keyword `target`, or a `v5:` mapping")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<EmitUid, E> {
                if value == "target" {
                    return Ok(EmitUid::Target);
                }
                Ok(EmitUid::Template(value.to_string()))
            }

            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<EmitUid, A::Error> {
                EmitUidV5::deserialize(de::value::MapAccessDeserializer::new(map))
                    .map(|spec| EmitUid::V5 { v5: spec.v5 })
            }
        }

        deserializer.deserialize_any(EmitUidVisitor)
    }
}

/// a map specification: the target schema plus the transformation rules.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MapSpec {
    /// target schema; the output inventory is validated against it.
    #[serde(default)]
    pub schema: Schema,
    #[serde(default)]
    pub rules: Vec<MapRule>,
    /// user-defined starlark transforms, consulted by `${var|name}` pipelines
    /// before the built-ins (requires the `starlark` feature).
    #[serde(default)]
    pub transforms: Option<TransformsSpec>,
    /// directory of the spec file, captured by `load_map_spec` and used to
    /// resolve `transforms.file` and starlark `load()` paths. `None` for specs
    /// parsed from strings: a relative `transforms.file` then resolves against
    /// the process cwd and `load()` is an error.
    #[serde(skip)]
    pub base_dir: Option<PathBuf>,
}

/// the `transforms:` block of a map spec: starlark source from a file or
/// inline. exactly one of the two must be set.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransformsSpec {
    /// path to a starlark file; a relative path resolves against the spec file.
    #[serde(default)]
    pub file: Option<PathBuf>,
    /// inline starlark source.
    #[serde(default)]
    pub inline: Option<String>,
}

/// a single ir->ir rule: select source objects, emit one or more target objects
/// each. `uids` declares named uids (computed once per matched source) referenced
/// as `${uids.name}` in emits -- the mechanism for wiring cross-object refs in a
/// multi-emit restructure.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MapRule {
    pub name: String,
    /// source selector: a type-name pattern with optional field
    /// predicates, e.g. `dcim.site`, `dcim.*`, or `dcim.device[attrs.role=leaf]`.
    pub r#match: String,
    /// when set, the matched objects are bucketed by this rendered template and
    /// the rule emits once per group (N->1 aggregation) instead of once per
    /// object. emits then draw on `${group.key}`, `${group.count}`, and
    /// per-member values collected into lists as `${group.items.<path>}`.
    #[serde(default)]
    pub group_by: Option<String>,
    /// reference lookups: each resolves a uid (`ref`) to an object in the input
    /// and binds one of its fields (`get`) as `${lookup.name}`, so an emit can
    /// read a value from the object a ref points at. resolved before `uids`.
    #[serde(default)]
    pub lookups: BTreeMap<String, Lookup>,
    /// named uids computed once and available as `${uids.name}` in emits.
    #[serde(default)]
    pub uids: BTreeMap<String, EmitUid>,
    /// a single emit (a mapping) or a list of emits.
    pub emit: EmitSpec,
}

/// a reference lookup: render `ref` to a uid, find that object in the input, and
/// read the dotted field path `get` from it (e.g. `attrs.name`).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lookup {
    pub r#ref: String,
    pub get: String,
}

/// `emit: passthrough`, a single emit (a mapping), or a list of emits.
#[derive(Debug)]
pub enum EmitSpec {
    /// `emit: passthrough` copies each matched source object unchanged (and
    /// carries its source-schema type into the output), but only for objects no
    /// other rule emits. paired with `match: "*"`, it is the terse "reshape the
    /// exceptions, pass the rest through" rule.
    Keyword(EmitKeyword),
    Single(MapEmit),
    Multi(Vec<MapEmit>),
}

// dispatched on the value's shape for the same reason as `EmitUid`: with
// `#[serde(untagged)]` a typo'd key inside an emit fails its variant and falls
// through, reporting the enum instead of the field.
impl<'de> Deserialize<'de> for EmitSpec {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct EmitSpecVisitor;

        impl<'de> Visitor<'de> for EmitSpecVisitor {
            type Value = EmitSpec;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("`passthrough`, an emit mapping, or a list of emit mappings")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<EmitSpec, E> {
                EmitKeyword::deserialize(de::value::StrDeserializer::new(value))
                    .map(EmitSpec::Keyword)
            }

            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<EmitSpec, A::Error> {
                MapEmit::deserialize(de::value::MapAccessDeserializer::new(map))
                    .map(EmitSpec::Single)
            }

            fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<EmitSpec, A::Error> {
                Vec::<MapEmit>::deserialize(de::value::SeqAccessDeserializer::new(seq))
                    .map(EmitSpec::Multi)
            }
        }

        deserializer.deserialize_any(EmitSpecVisitor)
    }
}

/// bare-word emit modes.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmitKeyword {
    Passthrough,
}

/// one emitted target object.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MapEmit {
    /// target type name (templates allowed).
    #[serde(rename = "type", alias = "kind")]
    pub type_name: String,
    pub key: BTreeMap<String, YamlValue>,
    /// optional uid override. absent, a one-to-one emit inherits the source
    /// uid and a group emit mints from the rendered target `(type, key)`; a
    /// multi-emit must set it on every emitted object.
    #[serde(default)]
    pub uid: Option<EmitUid>,
    #[serde(default)]
    pub attrs: BTreeMap<String, YamlValue>,
}

/// a compiled `match` selector: a type-name pattern plus field predicates over
/// the source object (`dcim.device[attrs.role=leaf]`). predicates are evaluated
/// against the same dotted var namespace as templates (`attrs.*`, `key.*`,
/// `type`, `uid`).
struct Matcher {
    glob: TypeGlob,
    predicates: Vec<Predicate>,
}

/// a type-name pattern: `*` (any), a trailing-`*` prefix (`dcim.*`), or exact.
enum TypeGlob {
    Any,
    Prefix(String),
    Exact(String),
}

impl Matcher {
    fn parse(selector: &str) -> Result<Self> {
        let selector = selector.trim();
        let (base, predicates) = match selector.find('[') {
            Some(idx) => (selector[..idx].trim(), parse_predicates(&selector[idx..])?),
            None => (selector, Vec::new()),
        };
        if base.is_empty() {
            return Err(anyhow!("match selector requires a type pattern"));
        }
        let glob = if base == "*" {
            TypeGlob::Any
        } else if let Some(prefix) = base.strip_suffix('*') {
            TypeGlob::Prefix(prefix.to_string())
        } else {
            TypeGlob::Exact(base.to_string())
        };
        Ok(Self { glob, predicates })
    }

    fn type_matches(&self, type_name: &str) -> bool {
        match &self.glob {
            TypeGlob::Any => true,
            TypeGlob::Prefix(prefix) => type_name.starts_with(prefix),
            TypeGlob::Exact(exact) => type_name == exact,
        }
    }

    fn predicates_match(&self, vars: &BTreeMap<String, JsonValue>) -> bool {
        self.predicates
            .iter()
            .all(|pred| predicate_matches(pred, vars))
    }
}

/// evaluate a predicate against the object's flattened var namespace, with
/// scalar-comparison and existence semantics.
fn predicate_matches(pred: &Predicate, vars: &BTreeMap<String, JsonValue>) -> bool {
    let field = vars.get(&pred.field);
    match pred.op {
        PredicateOp::Exists => matches!(field, Some(value) if !value.is_null()),
        PredicateOp::NotExists => match field {
            Some(value) => value.is_null(),
            None => true,
        },
        PredicateOp::Eq => {
            matches!(field.and_then(json_scalar), Some(rendered) if rendered == pred.value)
        }
        PredicateOp::Ne => {
            matches!(field.and_then(json_scalar), Some(rendered) if rendered != pred.value)
        }
    }
}

/// render a scalar json value to the text a predicate compares against; mirrors
/// `predicate` scalar rules for the json value model.
fn json_scalar(value: &JsonValue) -> Option<String> {
    crate::render::scalar_string(value)
}

/// load a map spec from a yaml file. the spec remembers the file's directory so
/// `transforms.file` and starlark `load()` paths resolve relative to it.
pub fn load_map_spec(path: impl AsRef<Path>) -> Result<MapSpec> {
    let path = path.as_ref();
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("read map spec: {}", path.display()))?;
    let mut spec: MapSpec = serde_yaml::from_str(&raw)
        .with_context(|| format!("parse map spec: {}", path.display()))?;
    spec.base_dir = path.parent().map(Path::to_path_buf);
    Ok(spec)
}

/// build the transform registry for a spec: empty (built-ins only) without a
/// `transforms:` block, otherwise the compiled starlark module. starlark is
/// compiled once here, not per template.
fn transform_registry(spec: &MapSpec) -> Result<TransformRegistry> {
    let Some(transforms) = &spec.transforms else {
        return Ok(TransformRegistry::EMPTY);
    };
    #[cfg(not(feature = "starlark"))]
    {
        let _ = transforms;
        Err(anyhow!(
            "map spec has a transforms block but alembic-engine was built without the starlark feature"
        ))
    }
    #[cfg(feature = "starlark")]
    {
        let (source, filename) = match (&transforms.file, &transforms.inline) {
            (Some(_), Some(_)) | (None, None) => {
                return Err(anyhow!(
                    "map spec transforms: requires exactly one of file or inline"
                ));
            }
            (Some(file), None) => {
                let path = match &spec.base_dir {
                    Some(base) if file.is_relative() => base.join(file),
                    _ => file.clone(),
                };
                let source = std::fs::read_to_string(&path)
                    .with_context(|| format!("read transforms file: {}", path.display()))?;
                (source, path.display().to_string())
            }
            (None, Some(inline)) => (inline.clone(), "transforms".to_string()),
        };
        let user = crate::starlark_transforms::StarlarkTransforms::compile(
            &source,
            &filename,
            spec.base_dir.as_deref(),
        )?;
        Ok(TransformRegistry::with_user(user))
    }
}

/// evaluate a single named transform from a map spec against a json value,
/// consulting the spec's user transforms first, then the built-ins. backs
/// `alembic map transform`, the iteration loop for writing transforms: one
/// value in, the typed result out, no inventory or backend involved.
pub fn eval_map_transform(
    spec: &MapSpec,
    name: &str,
    value: &JsonValue,
    args: &[JsonValue],
) -> Result<JsonValue> {
    let registry = transform_registry(spec)?;
    crate::render::apply_single_transform(&registry, name, value, args)
}

/// per-run immutables shared by every emit: the compiled transform registry and
/// the source-object index used to resolve reference lookups.
struct MapRun<'a> {
    transforms: TransformRegistry,
    index: BTreeMap<Uid, &'a Object>,
}

/// an emitted object plus whether its uid was derived from its key (the
/// default), which decides whether the key-ref rewrite pass re-derives it
/// after rewiring the key.
struct Emitted {
    object: Object,
    uid_from_key: bool,
}

/// a multi-emit assigns identity explicitly: fan-out has no single source
/// identity to inherit, and a one-element list stays in the explicit regime so
/// reshaping `emit:` into a list cannot silently change identity.
fn validate_emit_identity(spec: &MapSpec) -> Result<()> {
    for rule in &spec.rules {
        let EmitSpec::Multi(emits) = &rule.emit else {
            continue;
        };
        for (index, emit) in emits.iter().enumerate() {
            if emit.uid.is_none() {
                return Err(anyhow!(
                    "rule {}: a multi-emit assigns identity explicitly, and emit {} ({}) has no uid:; \
                     write `uid: \"${{uid}}\"` on the continuing object and an explicit expression \
                     (or `uid: target`) on the others",
                    rule.name,
                    index + 1,
                    emit.type_name
                ));
            }
        }
    }
    Ok(())
}

/// transform an ir inventory into another ir inventory under the target schema.
pub fn compile_map(input: &Inventory, spec: &MapSpec) -> Result<Inventory> {
    validate_emit_identity(spec)?;
    let run = MapRun {
        transforms: transform_registry(spec)?,
        // source uid -> object, for resolving reference lookups.
        index: input.objects.iter().map(|o| (o.uid, o)).collect(),
    };
    let mut emitted: Vec<Emitted> = Vec::new();
    // source uid -> emitted uid, used to re-derive ref values in pass 2.
    let mut remap: BTreeMap<Uid, Uid> = BTreeMap::new();
    // source objects a non-passthrough rule matched, each with the rule's name
    // and whether it was a single emit. passthrough only covers the rest, so a
    // `match: "*"` catch-all cannot collide with a specific rule; a source
    // reshaped by two single-emit rules would carry one identity into two
    // objects, so that is refused naming both rules.
    let mut claimed: BTreeMap<Uid, (String, bool)> = BTreeMap::new();
    // types emitted by passthrough, whose source schema is carried into the output.
    let mut passthrough_types: BTreeSet<String> = BTreeSet::new();

    // pass 1: rules that reshape. these claim every source object they match.
    for rule in &spec.rules {
        let emits = match &rule.emit {
            EmitSpec::Single(emit) => std::slice::from_ref(emit),
            EmitSpec::Multi(emits) => emits.as_slice(),
            // passthrough is handled in pass 2, once all claims are known.
            EmitSpec::Keyword(EmitKeyword::Passthrough) => continue,
        };
        let matcher = Matcher::parse(&rule.r#match)
            .with_context(|| format!("rule {}: invalid match selector", rule.name))?;
        match &rule.group_by {
            None => {
                // a rule emitting exactly one object per source is a 1:1 rename,
                // so we record source->target for automatic ref-rewiring. a
                // multi-emit rule is a restructure where auto-rewiring would be
                // ambiguous, so its cross-object refs are wired explicitly via
                // named `uids` instead.
                let remap_each = emits.len() == 1;
                for src in input.objects.iter() {
                    if !matcher.type_matches(src.type_name.as_str()) {
                        continue;
                    }
                    let vars = object_vars(src);
                    if !matcher.predicates_match(&vars) {
                        continue;
                    }
                    if let Some((first_rule, first_single)) = claimed.get(&src.uid) {
                        if *first_single || remap_each {
                            return Err(anyhow!(
                                "source object {} is reshaped by both rule {} and rule {}; \
                                 a source has one identity, so reshape it in one rule \
                                 (fan out with a multi-emit if it becomes several objects)",
                                src.uid,
                                first_rule,
                                rule.name
                            ));
                        }
                    } else {
                        claimed.insert(src.uid, (rule.name.clone(), remap_each));
                    }
                    let remap_source = remap_each.then_some(src.uid);
                    emit_objects(
                        rule,
                        emits,
                        vars,
                        &run,
                        remap_source,
                        &mut emitted,
                        &mut remap,
                    )?;
                }
            }
            // aggregation: bucket matched sources by the rendered key, emit once
            // per group. N->1, so no auto ref-rewiring (cross-object refs use
            // named `uids`). a BTreeMap keys groups deterministically; members
            // stay in input order.
            Some(group_expr) => {
                let mut groups: BTreeMap<String, Vec<&Object>> = BTreeMap::new();
                for src in input.objects.iter() {
                    if !matcher.type_matches(src.type_name.as_str()) {
                        continue;
                    }
                    let vars = object_vars(src);
                    if !matcher.predicates_match(&vars) {
                        continue;
                    }
                    claimed
                        .entry(src.uid)
                        .or_insert_with(|| (rule.name.clone(), false));
                    let group_key = render_template(
                        group_expr,
                        &RenderCtx {
                            vars: &vars,
                            transforms: &run.transforms,
                            rule: &rule.name,
                        },
                        "group_by",
                    )?;
                    groups.entry(group_key).or_default().push(src);
                }
                for (group_key, members) in &groups {
                    let vars = group_vars(group_key, members);
                    emit_objects(rule, emits, vars, &run, None, &mut emitted, &mut remap)?;
                }
            }
        }
    }

    // pass 2: passthrough rules copy every matched source no other rule claimed,
    // unchanged -- key, attrs, and uid alike, so a passed-through object keeps
    // its identity and nothing pointing at it moves.
    for rule in &spec.rules {
        if !matches!(rule.emit, EmitSpec::Keyword(EmitKeyword::Passthrough)) {
            continue;
        }
        if rule.group_by.is_some() {
            return Err(anyhow!(
                "rule {}: `emit: passthrough` cannot be combined with group_by",
                rule.name
            ));
        }
        let matcher = Matcher::parse(&rule.r#match)
            .with_context(|| format!("rule {}: invalid match selector", rule.name))?;
        for src in input.objects.iter() {
            if claimed.contains_key(&src.uid) || !matcher.type_matches(src.type_name.as_str()) {
                continue;
            }
            if !matcher.predicates_match(&object_vars(src)) {
                continue;
            }
            claimed.insert(src.uid, (rule.name.clone(), false));
            passthrough_types.insert(src.type_name.as_str().to_string());
            emitted.push(Emitted {
                object: Object::new(
                    src.uid,
                    src.type_name.clone(),
                    src.key.clone(),
                    src.attrs.clone(),
                )?,
                uid_from_key: false,
            });
        }
    }

    // the output schema is the target schema, plus the source schema for every
    // passed-through type not already declared, so the spec need only spell out
    // the types it reshapes.
    let mut out_schema = spec.schema.clone();
    for type_name in &passthrough_types {
        if !out_schema.types.contains_key(type_name) {
            if let Some(type_schema) = input.schema.types.get(type_name) {
                out_schema
                    .types
                    .insert(type_name.clone(), type_schema.clone());
            }
        }
    }

    rewrite_key_refs(&mut emitted, &out_schema, &mut remap)?;
    let mut objects: Vec<Object> = emitted.into_iter().map(|e| e.object).collect();
    rewrite_refs(&mut objects, &out_schema, &remap);

    objects.sort_by_cached_key(|o| (o.type_name.as_str().to_string(), key_string(&o.key)));

    let inventory = Inventory {
        schema: out_schema,
        // carried verbatim: a spec that renames a scoped type fails the output
        // validation below (scope must name declared types), loudly not silently.
        scope: input.scope.clone(),
        objects,
    };
    crate::report_to_result(crate::validate(&inventory))?;
    Ok(inventory)
}

/// compute named uids and run a rule's emits against `vars`, pushing the
/// resulting objects. `remap_source`, when set, records source->target uid for
/// the automatic ref-rewiring pass (1:1 rules only).
fn emit_objects(
    rule: &MapRule,
    emits: &[MapEmit],
    mut vars: BTreeMap<String, JsonValue>,
    run: &MapRun,
    remap_source: Option<Uid>,
    objects: &mut Vec<Emitted>,
    remap: &mut BTreeMap<Uid, Uid>,
) -> Result<()> {
    // resolve reference lookups first, so named uids and emits can use them.
    // bound under `lookup.<name>`, mirroring `uids.<name>`, so a lookup can never
    // shadow the object's own vars (`uid`, `key.*`, `attrs.*`, ...).
    for (name, lookup) in &rule.lookups {
        let ctx = RenderCtx {
            vars: &vars,
            transforms: &run.transforms,
            rule: &rule.name,
        };
        let value = resolve_lookup(name, lookup, &ctx, &run.index)?;
        vars.insert(format!("lookup.{name}"), value);
    }
    // compute named uids once, exposed as `uids.name` to every emit.
    for (name, uid_spec) in &rule.uids {
        let context = format!("uids.{name}");
        let ctx = RenderCtx {
            vars: &vars,
            transforms: &run.transforms,
            rule: &rule.name,
        };
        let uid = resolve_uid_spec(uid_spec, &ctx, &context)?;
        vars.insert(context, JsonValue::String(uid.to_string()));
    }
    let ctx = RenderCtx {
        vars: &vars,
        transforms: &run.transforms,
        rule: &rule.name,
    };
    for emit in emits {
        let key = render_key(&emit.key, &ctx)?;
        let type_name = TypeName::new(render_template(&emit.type_name, &ctx, "type")?);
        let uid = resolve_emit_uid(&emit.uid, &ctx, type_name.as_str(), &key, remap_source)?;
        let attrs = render_attrs(&emit.attrs, &ctx, "attrs")?;
        let attrs = JsonMap::from(attrs.into_iter().collect::<BTreeMap<_, _>>());

        if let Some(source) = remap_source {
            if source != uid {
                remap.insert(source, uid);
            }
        }
        // only a value-identified uid tracks its key through the rewrite pass:
        // the group-emit default and the explicit `uid: target`.
        let uid_from_key = match &emit.uid {
            None => remap_source.is_none(),
            Some(EmitUid::Target) => true,
            Some(_) => false,
        };
        objects.push(Emitted {
            object: Object::new(uid, type_name, key, attrs)?,
            uid_from_key,
        });
    }
    Ok(())
}

/// resolve a reference lookup: render `ref` to a uid, find that object in the
/// input index, and read its `get` field path. strict: a ref that is not a uuid,
/// a uid not in the input, or a missing field is an error.
fn resolve_lookup(
    name: &str,
    lookup: &Lookup,
    ctx: &RenderCtx,
    index: &BTreeMap<Uid, &Object>,
) -> Result<JsonValue> {
    let rule = ctx.rule;
    let context = format!("lookups.{name}");
    let rendered = render_template(&lookup.r#ref, ctx, &context)?;
    let uid = Uuid::parse_str(&rendered).with_context(|| {
        format!("rule {rule}: lookup {name} ref is not a valid uuid: {rendered}")
    })?;
    let referent = index
        .get(&uid)
        .ok_or_else(|| anyhow!("rule {rule}: lookup {name} ref {uid} is not in the input"))?;
    object_vars(referent)
        .get(&lookup.get)
        .cloned()
        .ok_or_else(|| {
            anyhow!(
                "rule {rule}: lookup {name} field {} is absent on {uid}",
                lookup.get
            )
        })
}

/// build the template vars for an aggregation group: `group.key`, `group.count`,
/// and every member field collected into a list under `group.items.<path>`
/// (present, non-missing values in member order).
fn group_vars(group_key: &str, members: &[&Object]) -> BTreeMap<String, JsonValue> {
    let mut vars = BTreeMap::new();
    vars.insert(
        "group.key".to_string(),
        JsonValue::String(group_key.to_string()),
    );
    vars.insert(
        "group.count".to_string(),
        JsonValue::Number(members.len().into()),
    );

    let per_member: Vec<BTreeMap<String, JsonValue>> =
        members.iter().map(|member| object_vars(member)).collect();
    let mut paths: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for member in &per_member {
        paths.extend(member.keys().cloned());
    }
    for path in paths {
        let values: Vec<JsonValue> = per_member
            .iter()
            .filter_map(|member| member.get(&path).filter(|v| !v.is_null()).cloned())
            .collect();
        vars.insert(format!("group.items.{path}"), JsonValue::Array(values));
    }
    vars
}

/// build the template vars for a source object: `uid`, `type`, and every key /
/// attr field flattened to a dotted path (so `${attrs.model.fabric}` works).
fn object_vars(obj: &Object) -> BTreeMap<String, JsonValue> {
    let mut vars = BTreeMap::new();
    vars.insert("uid".to_string(), JsonValue::String(obj.uid.to_string()));
    vars.insert(
        "type".to_string(),
        JsonValue::String(obj.type_name.as_str().to_string()),
    );
    for (field, value) in obj.key.iter() {
        flatten(&format!("key.{field}"), value, &mut vars);
    }
    for (field, value) in obj.attrs.iter() {
        flatten(&format!("attrs.{field}"), value, &mut vars);
    }
    vars
}

/// insert `value` at `prefix`, recursing into objects so both the whole value
/// (`attrs.model`) and its leaves (`attrs.model.fabric`) are addressable.
fn flatten(prefix: &str, value: &JsonValue, out: &mut BTreeMap<String, JsonValue>) {
    out.insert(prefix.to_string(), value.clone());
    if let JsonValue::Object(map) = value {
        for (field, child) in map {
            flatten(&format!("{prefix}.{field}"), child, out);
        }
    }
}

/// resolve an emit's uid. absent, a one-to-one emit inherits the source uid
/// (the emitted object is the same logical object in another vocabulary) and a
/// group emit keeps value identity, minted from the rendered target `(type,
/// key)`. `uid: target` spells that mint explicitly; any other override defers
/// to `resolve_uid_spec`.
fn resolve_emit_uid(
    uid: &Option<EmitUid>,
    ctx: &RenderCtx,
    type_name: &str,
    key: &Key,
    inherit: Option<Uid>,
) -> Result<Uid> {
    match uid {
        None => Ok(inherit.unwrap_or_else(|| uid_v5(type_name, &key_string(key)))),
        Some(EmitUid::Target) => Ok(uid_v5(type_name, &key_string(key))),
        Some(spec) => resolve_uid_spec(spec, ctx, "uid"),
    }
}

/// resolve an explicit uid spec -- a `v5: {type, stable}` pair or a uuid-string
/// template -- against the current vars. shared by emit uids and named `uids`.
fn resolve_uid_spec(spec: &EmitUid, ctx: &RenderCtx, context: &str) -> Result<Uid> {
    let rule = ctx.rule;
    match spec {
        // `target` mints from an emit's own rendered (type, key), which named
        // uids do not have.
        EmitUid::Target => Err(anyhow!(
            "rule {rule}: `target` is only meaningful as an emit's uid:, not in {context}"
        )),
        EmitUid::Template(template) => {
            let rendered = render_template(template, ctx, context)?;
            Uuid::parse_str(&rendered).with_context(|| {
                format!("rule {rule}: uid template is not a valid uuid: {rendered}")
            })
        }
        EmitUid::V5 { v5 } => {
            let kind = render_template(&v5.type_name, ctx, context)?;
            let stable = render_template(&v5.stable, ctx, context)?;
            crate::render::derive_v5_uid(&kind, &stable, rule)
        }
    }
}

/// rewire ref-typed key fields through the remap and re-derive each key-derived
/// uid from the rewritten key, so `uid == uid_v5(type, key)` holds on the
/// output key. objects are processed in key-ref dependency order (kahn over key
/// refs only), so a chain -- an interface keyed by a port keyed by a node --
/// cascades regardless of rule order in the spec. each re-derivation adds a
/// stale-uid -> new-uid entry to the remap, and the table is compacted to its
/// fixpoint afterwards, so both attr refs and other keys pointing at the object
/// (by source uid or by its stale pass-1 uid) follow with a single lookup in
/// `rewrite_refs`. a cycle among key-derived key refs has no converging uid
/// assignment (core validation does not reject it, the planner merely tolerates
/// ref cycles), so it is reported as an error naming the cycle.
fn rewrite_key_refs(
    emitted: &mut [Emitted],
    schema: &Schema,
    remap: &mut BTreeMap<Uid, Uid>,
) -> Result<()> {
    // the ref-typed key fields per object, per the output schema. key fields
    // are schema-validated scalar, so `ref` is the only ref-bearing key type
    // (no list_ref, list, or map in a key).
    let ref_fields: Vec<Vec<String>> = emitted
        .iter()
        .map(|e| match schema.types.get(e.object.type_name.as_str()) {
            Some(type_schema) => type_schema
                .key
                .iter()
                .filter(|(_, field_schema)| matches!(field_schema.r#type, FieldType::Ref { .. }))
                .map(|(field, _)| field.clone())
                .collect(),
            None => Vec::new(),
        })
        .collect();
    if ref_fields.iter().all(|fields| fields.is_empty()) {
        return Ok(());
    }

    // pass-1 uid -> object index; the remap chases from any key ref value into
    // this table while the target still carries its pass-1 uid.
    let by_uid: BTreeMap<Uid, usize> = emitted
        .iter()
        .enumerate()
        .map(|(idx, e)| (e.object.uid, idx))
        .collect();

    // dependency edges over key refs only: i depends on j when a ref-typed key
    // field of i points at j and j's uid can still change (key-derived and
    // itself carrying key refs). an explicit uid never changes, so it breaks
    // any chain running through it.
    let mut deps: Vec<Vec<usize>> = vec![Vec::new(); emitted.len()];
    let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); emitted.len()];
    let mut pending: Vec<usize> = vec![0; emitted.len()];
    for (i, e) in emitted.iter().enumerate() {
        for field in &ref_fields[i] {
            let Some(JsonValue::String(raw)) = e.object.key.get(field) else {
                continue;
            };
            let Ok(value) = Uuid::parse_str(raw) else {
                continue;
            };
            let Some(&j) = by_uid.get(&chase(remap, value)) else {
                continue;
            };
            if emitted[j].uid_from_key && !ref_fields[j].is_empty() {
                deps[i].push(j);
                dependents[j].push(i);
                pending[i] += 1;
            }
        }
    }

    let mut queue: Vec<usize> = (0..emitted.len()).filter(|&i| pending[i] == 0).collect();
    let mut processed = vec![false; emitted.len()];
    let mut done = 0;
    while let Some(i) = queue.pop() {
        processed[i] = true;
        done += 1;
        // rewire the key refs; every dependency is final, so the chase lands
        // on the final uid.
        for field in &ref_fields[i] {
            let Some(value) = emitted[i].object.key.get_mut(field) else {
                continue;
            };
            let JsonValue::String(raw) = value else {
                continue;
            };
            let Ok(old) = Uuid::parse_str(raw) else {
                continue;
            };
            let new = chase(remap, old);
            if new != old {
                *value = JsonValue::String(new.to_string());
            }
        }
        // a key-derived uid follows its rewritten key; the stale uid is left in
        // the remap so anything pointing at it follows too. an explicit uid is
        // pinned by the rule and stays.
        if emitted[i].uid_from_key {
            let old_uid = emitted[i].object.uid;
            let new_uid = uid_v5(
                emitted[i].object.type_name.as_str(),
                &key_string(&emitted[i].object.key),
            );
            if new_uid != old_uid {
                emitted[i].object.uid = new_uid;
                remap.insert(old_uid, new_uid);
            }
        }
        for &dependent in &dependents[i] {
            pending[dependent] -= 1;
            if pending[dependent] == 0 {
                queue.push(dependent);
            }
        }
    }

    if done < emitted.len() {
        return Err(key_ref_cycle_error(emitted, &deps, &processed));
    }

    // compact chains (source uid -> pass-1 uid -> final uid) so the attr
    // rewrite in `rewrite_refs` stays a single lookup.
    let compacted: Vec<(Uid, Uid)> = remap
        .iter()
        .map(|(&from, &to)| (from, chase(remap, to)))
        .collect();
    remap.extend(compacted);
    Ok(())
}

/// name the key-ref cycle that stopped the kahn pass: walk unresolved
/// dependencies from any unprocessed object until one repeats, and report the
/// closed loop with each member's type and key.
fn key_ref_cycle_error(
    emitted: &[Emitted],
    deps: &[Vec<usize>],
    processed: &[bool],
) -> anyhow::Error {
    let label = |i: usize| {
        format!(
            "{}{}",
            emitted[i].object.type_name,
            key_string(&emitted[i].object.key)
        )
    };
    let mut path: Vec<usize> = Vec::new();
    let mut current = match (0..emitted.len()).find(|&i| !processed[i]) {
        Some(start) => start,
        // unreachable: the caller only reports a cycle when objects remain.
        None => return anyhow!("key refs form a cycle"),
    };
    loop {
        if let Some(pos) = path.iter().position(|&seen| seen == current) {
            let names: Vec<String> = path[pos..].iter().map(|&i| label(i)).collect();
            return anyhow!(
                "key refs form a cycle, so no uid assignment can converge: {} -> {}; break it with an explicit `uid:` on one of the emits",
                names.join(" -> "),
                names[0]
            );
        }
        path.push(current);
        // every unprocessed object kept at least one unprocessed dependency,
        // so the walk always continues until it closes a loop.
        match deps[current].iter().copied().find(|&j| !processed[j]) {
            Some(next) => current = next,
            None => return anyhow!("key refs form a cycle"),
        }
    }
}

/// follow a uid through the remap to its fixpoint. pass 1 fills the table with
/// source -> emitted entries and the key-ref pass adds stale -> re-derived
/// entries on top, so a value can need more than one hop. identity entries (an
/// already-converged mapping of its own output) and the hop cap keep a
/// pathological table from looping.
fn chase(remap: &BTreeMap<Uid, Uid>, uid: Uid) -> Uid {
    let mut current = uid;
    for _ in 0..=remap.len() {
        match remap.get(&current) {
            Some(&next) if next != current => current = next,
            _ => break,
        }
    }
    current
}

/// rewrite the references in each object's attrs through the source->target uid
/// remap, using the target schema to find which fields hold them.
fn rewrite_refs(objects: &mut [Object], schema: &Schema, remap: &BTreeMap<Uid, Uid>) {
    for obj in objects.iter_mut() {
        let Some(type_schema) = schema.types.get(obj.type_name.as_str()) else {
            continue;
        };
        for (field, field_schema) in &type_schema.fields {
            if let Some(value) = obj.attrs.get_mut(field) {
                rewrite_refs_in_value(&field_schema.r#type, value, remap);
            }
        }
    }
}

/// rewrite the ref uids a value carries, recursing through list and map
/// containers so nested refs are remapped too.
fn rewrite_refs_in_value(
    field_type: &FieldType,
    value: &mut JsonValue,
    remap: &BTreeMap<Uid, Uid>,
) {
    match field_type {
        FieldType::Ref { .. } => rewrite_ref_value(value, remap),
        FieldType::ListRef { .. } => {
            if let JsonValue::Array(items) = value {
                for item in items {
                    rewrite_ref_value(item, remap);
                }
            }
        }
        FieldType::List { item } => {
            if let JsonValue::Array(items) = value {
                for item_value in items {
                    rewrite_refs_in_value(item, item_value, remap);
                }
            }
        }
        FieldType::Map { value: inner } => {
            if let JsonValue::Object(map) = value {
                for entry in map.values_mut() {
                    rewrite_refs_in_value(inner, entry, remap);
                }
            }
        }
        // scalar leaves carry no refs to remap; enumerated explicitly so a new
        // ref-bearing FieldType variant forces a compile error here.
        FieldType::String
        | FieldType::Text
        | FieldType::Int
        | FieldType::Float
        | FieldType::Bool
        | FieldType::Uuid
        | FieldType::Date
        | FieldType::Datetime
        | FieldType::Time
        | FieldType::Json
        | FieldType::IpAddress
        | FieldType::Cidr
        | FieldType::Prefix
        | FieldType::Mac
        | FieldType::Slug
        | FieldType::Enum { .. } => {}
    }
}

fn rewrite_ref_value(value: &mut JsonValue, remap: &BTreeMap<Uid, Uid>) {
    let JsonValue::String(raw) = value else {
        return;
    };
    let Ok(old) = Uuid::parse_str(raw) else {
        return;
    };
    if let Some(new) = remap.get(&old) {
        *value = JsonValue::String(new.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn input_inventory(objects: JsonValue) -> Inventory {
        serde_json::from_value(json!({ "schema": { "types": {} }, "objects": objects })).unwrap()
    }

    fn spec(yaml: &str) -> MapSpec {
        serde_yaml::from_str(yaml).unwrap()
    }

    fn spec_err(yaml: &str) -> String {
        serde_yaml::from_str::<MapSpec>(yaml)
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn rejects_an_unknown_key_in_a_map_spec() {
        // an unknown key must not silently drop the ruleset.
        let err = spec_err("schema:\n  types: {}\nrulez: []\n");
        assert!(err.contains("unknown field `rulez`"), "{}", err);
    }

    #[test]
    fn a_typo_inside_an_emit_names_the_field() {
        // the emit shape is dispatched before the variant is deserialized, so the
        // error is the emit's own and not `did not match any variant`.
        let err = spec_err(
            r#"
rules:
  - name: sites
    match: "dcim.site"
    emit:
      type: location.site
      key: { slug: "${key.slug}" }
      atrrs: { label: "${attrs.name}" }
"#,
        );
        assert!(err.contains("unknown field `atrrs`"), "{}", err);
        assert!(!err.contains("any variant"), "{}", err);
    }

    #[test]
    fn a_typo_inside_a_list_emit_names_the_field() {
        let err = spec_err(
            r#"
rules:
  - name: sites
    match: "dcim.site"
    emit:
      - type: location.site
        key: { slug: "${key.slug}" }
      - type: location.region
        key: { slug: "${key.slug}" }
        atrrs: { label: "${attrs.name}" }
"#,
        );
        assert!(err.contains("unknown field `atrrs`"), "{}", err);
        assert!(!err.contains("any variant"), "{}", err);
    }

    #[test]
    fn a_typo_inside_an_emit_uid_names_the_field() {
        let err = spec_err(
            r#"
rules:
  - name: sites
    match: "dcim.site"
    emit:
      type: location.site
      key: { slug: "${key.slug}" }
      uid:
        v5: { type: location.site, stabel: "${key.slug}" }
"#,
        );
        assert!(err.contains("unknown field `stabel`"), "{}", err);
        assert!(!err.contains("any variant"), "{}", err);

        let err = spec_err(
            r#"
rules:
  - name: sites
    match: "dcim.site"
    emit:
      type: location.site
      key: { slug: "${key.slug}" }
      uid: { v6: { type: location.site, stable: "${key.slug}" } }
"#,
        );
        assert!(err.contains("unknown field `v6`"), "{}", err);
    }

    #[test]
    fn rejects_an_unknown_key_in_a_rule_lookup_or_transforms_block() {
        let err = spec_err("rules:\n  - name: n\n    matsh: \"dcim.site\"\n");
        assert!(err.contains("unknown field `matsh`"), "{}", err);

        let err = spec_err(
            r#"
rules:
  - name: sites
    match: "dcim.site"
    lookups:
      site: { ref: "${attrs.site}", gett: attrs.name }
    emit:
      type: location.site
      key: { slug: "${key.slug}" }
"#,
        );
        assert!(err.contains("unknown field `gett`"), "{}", err);

        let err = spec_err("transforms:\n  fil: ./transforms.star\n");
        assert!(err.contains("unknown field `fil`"), "{}", err);
    }

    #[test]
    fn keeps_the_emit_shapes_the_untagged_enum_accepted() {
        // shape dispatch has to still accept every form: the bare keyword, a
        // single mapping, a list, the `kind` alias, and both uid forms.
        let parsed = spec(
            r#"
rules:
  - name: rest
    match: "*"
    emit: passthrough
  - name: sites
    match: "dcim.site"
    uids:
      pinned: "11111111-1111-1111-1111-111111111111"
      derived: { v5: { kind: location.site, stable: "${key.slug}" } }
    emit:
      - kind: location.site
        key: { slug: "${key.slug}" }
        uid: "${uids.pinned}"
      - type: location.region
        key: { slug: "${key.slug}" }
        uid: { v5: { type: location.region, stable: "${key.slug}" } }
"#,
        );
        assert!(matches!(
            parsed.rules[0].emit,
            EmitSpec::Keyword(EmitKeyword::Passthrough)
        ));
        assert!(matches!(parsed.rules[1].emit, EmitSpec::Multi(ref e) if e.len() == 2));
        assert!(matches!(
            parsed.rules[1].uids["pinned"],
            EmitUid::Template(_)
        ));
        assert!(matches!(
            parsed.rules[1].uids["derived"],
            EmitUid::V5 { .. }
        ));
    }

    #[test]
    fn renames_type_and_field_carrying_key() {
        let input = input_inventory(json!([
            { "uid": Uuid::from_u128(1).to_string(), "type": "dcim.site",
              "key": { "site": "fra1" }, "attrs": { "name": "FRA1" } }
        ]));
        let out = compile_map(
            &input,
            &spec(
                r#"
schema:
  types:
    location.site:
      key:
        slug: { type: slug }
      fields:
        label: { type: string }
rules:
  - name: sites
    match: "dcim.site"
    emit:
      type: location.site
      key:
        slug: "${key.site}"
      attrs:
        label: "${attrs.name}"
"#,
            ),
        )
        .unwrap();

        assert_eq!(out.objects.len(), 1);
        let obj = &out.objects[0];
        assert_eq!(obj.type_name.as_str(), "location.site");
        assert_eq!(obj.key.get("slug").unwrap(), &json!("fra1"));
        assert_eq!(obj.attrs.get("label").unwrap(), &json!("FRA1"));
        // a one-to-one emit is the same logical object in another vocabulary,
        // so it inherits the source uid, type change included.
        assert_eq!(obj.uid, Uuid::from_u128(1));
    }

    #[test]
    fn drops_unmapped_fields_and_derives_via_transform() {
        let input = input_inventory(json!([
            { "uid": Uuid::from_u128(1).to_string(), "type": "dcim.site",
              "key": { "site": "fra1" }, "attrs": { "name": "frankfurt", "secret": "drop me" } }
        ]));
        let out = compile_map(
            &input,
            &spec(
                r#"
schema:
  types:
    location.site:
      key:
        slug: { type: slug }
      fields:
        name: { type: string }
rules:
  - name: sites
    match: "dcim.site"
    emit:
      type: location.site
      key:
        slug: "${key.site}"
      attrs:
        name: "${attrs.name|upper}"
"#,
            ),
        )
        .unwrap();

        let attrs = &out.objects[0].attrs;
        assert_eq!(attrs.get("name").unwrap(), &json!("FRANKFURT"));
        assert!(attrs.get("secret").is_none());
    }

    #[test]
    fn refs_stay_valid_across_a_rename() {
        let site_src = Uuid::from_u128(1).to_string();
        let input = input_inventory(json!([
            { "uid": site_src, "type": "dcim.site",
              "key": { "site": "fra1" }, "attrs": { "name": "FRA1" } },
            { "uid": Uuid::from_u128(2).to_string(), "type": "dcim.device",
              "key": { "device": "leaf01" }, "attrs": { "name": "leaf01", "site": site_src } }
        ]));
        let out = compile_map(
            &input,
            &spec(
                r#"
schema:
  types:
    location.site:
      key:
        slug: { type: slug }
      fields:
        name: { type: string }
    dcim.device:
      key:
        device: { type: slug }
      fields:
        name: { type: string }
        site: { type: ref, target: location.site }
rules:
  - name: sites
    match: "dcim.site"
    emit:
      type: location.site
      key:
        slug: "${key.site}"
      attrs:
        name: "${attrs.name}"
  - name: devices
    match: "dcim.device"
    emit:
      type: dcim.device
      key:
        device: "${key.device}"
      attrs:
        name: "${attrs.name}"
        site: "${attrs.site}"
"#,
            ),
        )
        .unwrap();

        let site = out
            .objects
            .iter()
            .find(|o| o.type_name.as_str() == "location.site")
            .unwrap();
        let device = out
            .objects
            .iter()
            .find(|o| o.type_name.as_str() == "dcim.device")
            .unwrap();
        // the renamed site inherits its identity, so the device's ref is valid
        // without any rewiring: it still names the same logical object.
        assert_eq!(site.uid.to_string(), site_src);
        assert_eq!(
            device.attrs.get("site").unwrap(),
            &json!(site.uid.to_string())
        );
    }

    #[test]
    fn rewires_refs_nested_in_a_list_field() {
        let a_src = Uuid::from_u128(1).to_string();
        let b_src = Uuid::from_u128(2).to_string();
        let input = input_inventory(json!([
            { "uid": a_src, "type": "dcim.device",
              "key": { "device": "leaf01" },
              "attrs": { "name": "leaf01", "peers": [b_src] } },
            { "uid": b_src, "type": "dcim.device",
              "key": { "device": "leaf02" },
              "attrs": { "name": "leaf02", "peers": [a_src] } }
        ]));
        let out = compile_map(
            &input,
            &spec(
                r#"
schema:
  types:
    net.node:
      key:
        name: { type: slug }
      fields:
        name: { type: slug }
        peers: { type: list, item: { type: ref, target: net.node } }
rules:
  - name: nodes
    match: "dcim.device"
    emit:
      type: net.node
      key:
        name: "${key.device}"
      attrs:
        name: "${attrs.name}"
        peers: "${attrs.peers}"
"#,
            ),
        )
        .unwrap();

        let node = |name: &str| {
            out.objects
                .iter()
                .find(|o| o.key.get("name").unwrap() == &json!(name))
                .unwrap()
        };
        let a = node("leaf01");
        let b = node("leaf02");
        // the peer refs nested in the `list` field now point at the new uids.
        assert_eq!(a.attrs.get("peers").unwrap(), &json!([b.uid.to_string()]));
        assert_eq!(b.attrs.get("peers").unwrap(), &json!([a.uid.to_string()]));
    }

    #[test]
    fn rewires_refs_in_a_list_ref_field() {
        let a_src = Uuid::from_u128(1).to_string();
        let b_src = Uuid::from_u128(2).to_string();
        let input = input_inventory(json!([
            { "uid": a_src, "type": "dcim.device",
              "key": { "device": "leaf01" },
              "attrs": { "name": "leaf01", "peers": [b_src] } },
            { "uid": b_src, "type": "dcim.device",
              "key": { "device": "leaf02" },
              "attrs": { "name": "leaf02", "peers": [a_src] } }
        ]));
        let out = compile_map(
            &input,
            &spec(
                r#"
schema:
  types:
    net.node:
      key:
        name: { type: slug }
      fields:
        name: { type: slug }
        peers: { type: list_ref, target: net.node }
rules:
  - name: nodes
    match: "dcim.device"
    emit:
      type: net.node
      key:
        name: "${key.device}"
      attrs:
        name: "${attrs.name}"
        peers: "${attrs.peers}"
"#,
            ),
        )
        .unwrap();

        let node = |name: &str| {
            out.objects
                .iter()
                .find(|o| o.key.get("name").unwrap() == &json!(name))
                .unwrap()
        };
        let a = node("leaf01");
        let b = node("leaf02");
        // the peer refs in the `list_ref` field now point at the new uids.
        assert_eq!(a.attrs.get("peers").unwrap(), &json!([b.uid.to_string()]));
        assert_eq!(b.attrs.get("peers").unwrap(), &json!([a.uid.to_string()]));
    }

    #[test]
    fn rewires_refs_nested_in_a_map_field() {
        let a_src = Uuid::from_u128(1).to_string();
        let b_src = Uuid::from_u128(2).to_string();
        let input = input_inventory(json!([
            { "uid": a_src, "type": "dcim.device",
              "key": { "device": "leaf01" },
              "attrs": { "name": "leaf01", "peers": { "primary": b_src } } },
            { "uid": b_src, "type": "dcim.device",
              "key": { "device": "leaf02" },
              "attrs": { "name": "leaf02", "peers": { "primary": a_src } } }
        ]));
        let out = compile_map(
            &input,
            &spec(
                r#"
schema:
  types:
    net.node:
      key:
        name: { type: slug }
      fields:
        name: { type: slug }
        peers: { type: map, value: { type: ref, target: net.node } }
rules:
  - name: nodes
    match: "dcim.device"
    emit:
      type: net.node
      key:
        name: "${key.device}"
      attrs:
        name: "${attrs.name}"
        peers: "${attrs.peers}"
"#,
            ),
        )
        .unwrap();

        let node = |name: &str| {
            out.objects
                .iter()
                .find(|o| o.key.get("name").unwrap() == &json!(name))
                .unwrap()
        };
        let a = node("leaf01");
        let b = node("leaf02");
        // the peer refs nested in the `map` field now point at the new uids.
        assert_eq!(
            a.attrs.get("peers").unwrap(),
            &json!({ "primary": b.uid.to_string() })
        );
        assert_eq!(
            b.attrs.get("peers").unwrap(),
            &json!({ "primary": a.uid.to_string() })
        );
    }

    /// the issue #225 repro: a ref-typed key field is rewired through the
    /// remap like an attr ref, and the referrer's key-derived uid follows the
    /// rewritten key, so the output validates instead of dangling.
    #[test]
    fn rewires_refs_inside_a_key_and_rederives_the_uid() {
        let node_src = Uuid::from_u128(1).to_string();
        let input = input_inventory(json!([
            { "uid": node_src, "type": "src.node",
              "key": { "name": "leaf01" }, "attrs": {} },
            { "uid": Uuid::from_u128(2).to_string(), "type": "src.port",
              "key": { "device": node_src, "name": "eth0" }, "attrs": {} }
        ]));
        let out = compile_map(
            &input,
            &spec(
                r#"
schema:
  types:
    net.node:
      key:
        name: { type: slug }
    net.port:
      key:
        device: { type: ref, target: net.node }
        name: { type: slug }
rules:
  - name: nodes
    match: "src.node"
    emit:
      type: net.node
      key:
        name: "${key.name}"
      uid: target
  - name: ports
    match: "src.port"
    emit:
      type: net.port
      key:
        device: "${key.device}"
        name: "${key.name}"
      uid: target
"#,
            ),
        )
        .unwrap();

        let node = out
            .objects
            .iter()
            .find(|o| o.type_name.as_str() == "net.node")
            .unwrap();
        let port = out
            .objects
            .iter()
            .find(|o| o.type_name.as_str() == "net.port")
            .unwrap();
        // under `uid: target` the key ref follows the node's minted uid.
        assert_eq!(
            port.key.get("device").unwrap(),
            &json!(node.uid.to_string())
        );
        assert_ne!(port.key.get("device").unwrap(), &json!(node_src));
        // the port's uid is derived from the *rewritten* key.
        assert_eq!(port.uid, uid_v5("net.port", &key_string(&port.key)));
    }

    /// the two-hop chain spec used by the rule-order test: iface keyed by
    /// port keyed by node, all under `uid: target` (value identity is what
    /// makes a key rewire cascade), with the three rules in the given order.
    fn chain_spec(rule_order: [&str; 3]) -> String {
        let rule = |name: &str| match name {
            "nodes" => {
                r#"
  - name: nodes
    match: "src.node"
    emit:
      type: net.node
      key:
        name: "${key.name}"
      uid: target
"#
            }
            "ports" => {
                r#"
  - name: ports
    match: "src.port"
    emit:
      type: net.port
      key:
        device: "${key.device}"
        name: "${key.name}"
      uid: target
"#
            }
            _ => {
                r#"
  - name: ifaces
    match: "src.iface"
    emit:
      type: net.iface
      key:
        port: "${key.port}"
        name: "${key.name}"
      uid: target
"#
            }
        };
        format!(
            r#"
schema:
  types:
    net.node:
      key:
        name: {{ type: slug }}
    net.port:
      key:
        device: {{ type: ref, target: net.node }}
        name: {{ type: slug }}
    net.iface:
      key:
        port: {{ type: ref, target: net.port }}
        name: {{ type: slug }}
rules:{}{}{}"#,
            rule(rule_order[0]),
            rule(rule_order[1]),
            rule(rule_order[2])
        )
    }

    /// remapping the node cascades through the port's re-derived uid into the
    /// interface's key and uid, and the final rewrite pass is ordered by key
    /// refs, not by rule order in the spec.
    #[test]
    fn key_ref_chains_cascade_in_both_rule_orders() {
        let node_src = Uuid::from_u128(1).to_string();
        let port_src = Uuid::from_u128(2).to_string();
        let input = input_inventory(json!([
            { "uid": node_src, "type": "src.node",
              "key": { "name": "leaf01" }, "attrs": {} },
            { "uid": port_src, "type": "src.port",
              "key": { "device": node_src, "name": "eth0" }, "attrs": {} },
            { "uid": Uuid::from_u128(3).to_string(), "type": "src.iface",
              "key": { "port": port_src, "name": "vlan10" }, "attrs": {} }
        ]));
        let forward =
            compile_map(&input, &spec(&chain_spec(["nodes", "ports", "ifaces"]))).unwrap();
        let reverse =
            compile_map(&input, &spec(&chain_spec(["ifaces", "ports", "nodes"]))).unwrap();
        assert_eq!(forward.objects, reverse.objects);

        let by_type = |type_name: &str| {
            forward
                .objects
                .iter()
                .find(|o| o.type_name.as_str() == type_name)
                .unwrap()
        };
        let node = by_type("net.node");
        let port = by_type("net.port");
        let iface = by_type("net.iface");
        assert_eq!(
            port.key.get("device").unwrap(),
            &json!(node.uid.to_string())
        );
        assert_eq!(port.uid, uid_v5("net.port", &key_string(&port.key)));
        assert_eq!(iface.key.get("port").unwrap(), &json!(port.uid.to_string()));
        assert_eq!(iface.uid, uid_v5("net.iface", &key_string(&iface.key)));
    }

    /// an explicit `uid:` is pinned by the rule: the ref inside the key is
    /// still rewired, but the uid is not re-derived from the rewritten key.
    #[test]
    fn explicit_uid_survives_a_key_rewire() {
        let node_src = Uuid::from_u128(1).to_string();
        let pinned = Uuid::from_u128(77);
        let input = input_inventory(json!([
            { "uid": node_src, "type": "src.node",
              "key": { "name": "leaf01" }, "attrs": {} },
            { "uid": Uuid::from_u128(2).to_string(), "type": "src.port",
              "key": { "device": node_src, "name": "eth0" }, "attrs": {} }
        ]));
        let out = compile_map(
            &input,
            &spec(&format!(
                r#"
schema:
  types:
    net.node:
      key:
        name: {{ type: slug }}
    net.port:
      key:
        device: {{ type: ref, target: net.node }}
        name: {{ type: slug }}
rules:
  - name: nodes
    match: "src.node"
    emit:
      type: net.node
      key:
        name: "${{key.name}}"
      uid: target
  - name: ports
    match: "src.port"
    emit:
      type: net.port
      key:
        device: "${{key.device}}"
        name: "${{key.name}}"
      uid: "{pinned}"
"#
            )),
        )
        .unwrap();

        let node = out
            .objects
            .iter()
            .find(|o| o.type_name.as_str() == "net.node")
            .unwrap();
        let port = out
            .objects
            .iter()
            .find(|o| o.type_name.as_str() == "net.port")
            .unwrap();
        assert_eq!(
            port.key.get("device").unwrap(),
            &json!(node.uid.to_string())
        );
        assert_eq!(port.uid, pinned);
    }

    /// an attr ref elsewhere pointing at an object whose uid was re-derived
    /// from its rewritten key follows the final uid, not the stale pass-1 one.
    #[test]
    fn attr_refs_follow_a_rederived_key_uid() {
        let node_src = Uuid::from_u128(1).to_string();
        let port_src = Uuid::from_u128(2).to_string();
        let input = input_inventory(json!([
            { "uid": node_src, "type": "src.node",
              "key": { "name": "leaf01" }, "attrs": {} },
            { "uid": port_src, "type": "src.port",
              "key": { "device": node_src, "name": "eth0" }, "attrs": {} },
            { "uid": Uuid::from_u128(3).to_string(), "type": "src.link",
              "key": { "name": "uplink" }, "attrs": { "port": port_src } }
        ]));
        let out = compile_map(
            &input,
            &spec(
                r#"
schema:
  types:
    net.node:
      key:
        name: { type: slug }
    net.port:
      key:
        device: { type: ref, target: net.node }
        name: { type: slug }
    net.link:
      key:
        name: { type: slug }
      fields:
        port: { type: ref, target: net.port }
rules:
  - name: nodes
    match: "src.node"
    emit:
      type: net.node
      key:
        name: "${key.name}"
      uid: target
  - name: ports
    match: "src.port"
    emit:
      type: net.port
      key:
        device: "${key.device}"
        name: "${key.name}"
      uid: target
  - name: links
    match: "src.link"
    emit:
      type: net.link
      key:
        name: "${key.name}"
      attrs:
        port: "${attrs.port}"
"#,
            ),
        )
        .unwrap();

        let port = out
            .objects
            .iter()
            .find(|o| o.type_name.as_str() == "net.port")
            .unwrap();
        let link = out
            .objects
            .iter()
            .find(|o| o.type_name.as_str() == "net.link")
            .unwrap();
        assert_eq!(
            link.attrs.get("port").unwrap(),
            &json!(port.uid.to_string())
        );
    }

    /// mapping the output of a key-rewiring map through identity rules is a
    /// no-op: keys, uids, and refs are already at their fixpoint.
    #[test]
    fn mapping_the_output_again_is_a_fixpoint() {
        let node_src = Uuid::from_u128(1).to_string();
        let input = input_inventory(json!([
            { "uid": node_src, "type": "src.node",
              "key": { "name": "leaf01" }, "attrs": {} },
            { "uid": Uuid::from_u128(2).to_string(), "type": "src.port",
              "key": { "device": node_src, "name": "eth0" }, "attrs": {} }
        ]));
        let reshape = r#"
schema:
  types:
    net.node:
      key:
        name: { type: slug }
    net.port:
      key:
        device: { type: ref, target: net.node }
        name: { type: slug }
rules:
  - name: nodes
    match: "src.node"
    emit:
      type: net.node
      key:
        name: "${key.name}"
  - name: ports
    match: "src.port"
    emit:
      type: net.port
      key:
        device: "${key.device}"
        name: "${key.name}"
"#;
        let identity = r#"
schema:
  types:
    net.node:
      key:
        name: { type: slug }
    net.port:
      key:
        device: { type: ref, target: net.node }
        name: { type: slug }
rules:
  - name: nodes
    match: "net.node"
    emit:
      type: net.node
      key:
        name: "${key.name}"
  - name: ports
    match: "net.port"
    emit:
      type: net.port
      key:
        device: "${key.device}"
        name: "${key.name}"
"#;
        let first = compile_map(&input, &spec(reshape)).unwrap();
        let second = compile_map(&first, &spec(identity)).unwrap();
        assert_eq!(first.objects, second.objects);
    }

    /// a key-ref cycle is an error naming the cycle (see rewrite_key_refs).
    #[test]
    fn key_ref_cycle_is_a_clear_error() {
        let a_src = Uuid::from_u128(1).to_string();
        let b_src = Uuid::from_u128(2).to_string();
        let input = input_inventory(json!([
            { "uid": a_src, "type": "src.thing",
              "key": { "peer": b_src, "name": "a" }, "attrs": {} },
            { "uid": b_src, "type": "src.thing",
              "key": { "peer": a_src, "name": "b" }, "attrs": {} }
        ]));
        let err = compile_map(
            &input,
            &spec(
                r#"
schema:
  types:
    net.thing:
      key:
        peer: { type: ref, target: net.thing }
        name: { type: slug }
rules:
  - name: things
    match: "src.thing"
    emit:
      type: net.thing
      key:
        peer: "${key.peer}"
        name: "${key.name}"
      uid: target
"#,
            ),
        )
        .unwrap_err();
        assert!(err.to_string().contains("key refs form a cycle"), "{err:#}");
        assert!(err.to_string().contains("net.thing"), "{err:#}");
    }

    #[test]
    fn is_deterministic_across_runs() {
        let input = input_inventory(json!([
            { "uid": Uuid::from_u128(1).to_string(), "type": "dcim.site",
              "key": { "site": "fra1" }, "attrs": { "name": "FRA1" } },
            { "uid": Uuid::from_u128(2).to_string(), "type": "dcim.site",
              "key": { "site": "ams1" }, "attrs": { "name": "AMS1" } }
        ]));
        let yaml = r#"
schema:
  types:
    location.site:
      key:
        slug: { type: slug }
      fields:
        name: { type: string }
rules:
  - name: sites
    match: "dcim.site"
    emit:
      type: location.site
      key:
        slug: "${key.site}"
      attrs:
        name: "${attrs.name}"
"#;
        let first = compile_map(&input, &spec(yaml)).unwrap();
        let second = compile_map(&input, &spec(yaml)).unwrap();
        assert_eq!(first.objects, second.objects);
    }

    #[test]
    fn type_glob_matches_a_prefix() {
        let input = input_inventory(json!([
            { "uid": Uuid::from_u128(1).to_string(), "type": "dcim.site",
              "key": { "k": "a" }, "attrs": {} },
            { "uid": Uuid::from_u128(2).to_string(), "type": "dcim.device",
              "key": { "k": "b" }, "attrs": {} },
            { "uid": Uuid::from_u128(3).to_string(), "type": "ipam.prefix",
              "key": { "k": "c" }, "attrs": {} }
        ]));
        let out = compile_map(
            &input,
            &spec(
                r#"
schema:
  types:
    thing:
      key:
        k: { type: string }
rules:
  - name: dcim-only
    match: "dcim.*"
    emit:
      type: thing
      key:
        k: "${key.k}"
"#,
            ),
        )
        .unwrap();
        // both dcim.* objects map to `thing`; the ipam.prefix is left out.
        assert_eq!(out.objects.len(), 2);
        assert!(out.objects.iter().all(|o| o.type_name.as_str() == "thing"));
        let keys: Vec<&str> = out
            .objects
            .iter()
            .map(|o| o.key.get("k").unwrap().as_str().unwrap())
            .collect();
        assert_eq!(keys, vec!["a", "b"]);
    }

    #[test]
    fn passthrough_preserves_identity_and_carries_unmatched_types() {
        let input: Inventory = serde_json::from_value(json!({
            "schema": { "types": {
                "dcim.interface": { "key": { "name": {"type":"slug"} },
                                    "fields": { "name": {"type":"slug"} } },
                "ipam.ip_address": { "key": { "address": {"type":"string"} },
                                     "fields": { "address": {"type":"string"},
                                                 "assigned_interface": {"type":"ref","target":"dcim.interface"} } }
            }},
            "objects": [
                { "uid": Uuid::from_u128(1).to_string(), "type": "dcim.interface",
                  "key": {"name":"eth0"}, "attrs": {"name":"eth0"} },
                { "uid": Uuid::from_u128(2).to_string(), "type": "ipam.ip_address",
                  "key": {"address":"10.0.0.10/24"},
                  "attrs": {"address":"10.0.0.10/24", "assigned_interface": Uuid::from_u128(1).to_string()} }
            ]
        }))
        .unwrap();
        let out = compile_map(
            &input,
            &spec(
                r#"
schema:
  types:
    ipam.ip_address:
      key: { address: { type: string } }
      fields:
        address: { type: string }
        assigned_object: { type: ref, target: dcim.interface }
rules:
  - name: rename-assignment
    match: ipam.ip_address
    emit:
      type: ipam.ip_address
      key: { address: "${key.address}" }
      attrs: { address: "${attrs.address}", assigned_object: "${attrs.assigned_interface}" }
  - name: rest
    match: "*"
    emit: passthrough
"#,
            ),
        )
        .unwrap();
        // the interface passed through, and its source schema was carried in.
        assert!(out.schema.types.contains_key("dcim.interface"));
        let iface = out
            .objects
            .iter()
            .find(|o| o.type_name.as_str() == "dcim.interface")
            .unwrap();
        // passthrough is genuinely unchanged: the authored uid survives.
        assert_eq!(iface.uid, Uuid::from_u128(1));
        // the ip's ref was renamed to assigned_object and still names it.
        let ip = out
            .objects
            .iter()
            .find(|o| o.type_name.as_str() == "ipam.ip_address")
            .unwrap();
        assert_eq!(
            ip.attrs.get("assigned_object").unwrap().as_str().unwrap(),
            iface.uid.to_string()
        );
        assert!(ip.attrs.get("assigned_interface").is_none());
    }

    #[test]
    fn passthrough_skips_objects_another_rule_claimed() {
        let input: Inventory = serde_json::from_value(json!({
            "schema": { "types": { "dcim.site": { "key": {"slug":{"type":"slug"}},
                                                  "fields": {"name":{"type":"string"}} } } },
            "objects": [
                { "uid": Uuid::from_u128(1).to_string(), "type": "dcim.site",
                  "key": {"slug":"fra1"}, "attrs": {"name":"fra1"} }
            ]
        }))
        .unwrap();
        let out = compile_map(
            &input,
            &spec(
                r#"
schema:
  types:
    dcim.site:
      key: { slug: { type: slug } }
      fields: { name: { type: string } }
rules:
  - name: sites
    match: dcim.site
    emit:
      type: dcim.site
      key: { slug: "${key.slug}" }
      attrs: { name: "${attrs.name|upper}" }
  - name: rest
    match: "*"
    emit: passthrough
"#,
            ),
        )
        .unwrap();
        // reshaped by the specific rule; passthrough did not re-emit it.
        assert_eq!(out.objects.len(), 1);
        assert_eq!(
            out.objects[0].attrs.get("name").unwrap().as_str().unwrap(),
            "FRA1"
        );
    }

    #[test]
    fn passthrough_with_group_by_is_an_error() {
        let input = input_inventory(json!([
            { "uid": Uuid::from_u128(1).to_string(), "type": "dcim.site",
              "key": {"k":"a"}, "attrs": {} }
        ]));
        let err = compile_map(
            &input,
            &spec(
                r#"
schema:
  types: {}
rules:
  - name: bad
    match: "*"
    group_by: "${type}"
    emit: passthrough
"#,
            ),
        )
        .unwrap_err();
        assert!(err.to_string().contains("passthrough"), "{err}");
    }

    #[test]
    fn predicate_filters_matched_objects() {
        let input = input_inventory(json!([
            { "uid": Uuid::from_u128(1).to_string(), "type": "dcim.device",
              "key": { "device": "leaf01" }, "attrs": { "role": "leaf" } },
            { "uid": Uuid::from_u128(2).to_string(), "type": "dcim.device",
              "key": { "device": "spine01" }, "attrs": { "role": "spine" } },
            { "uid": Uuid::from_u128(3).to_string(), "type": "dcim.device",
              "key": { "device": "leaf02" }, "attrs": { "role": "leaf" } }
        ]));
        let out = compile_map(
            &input,
            &spec(
                r#"
schema:
  types:
    fabric.leaf:
      key:
        name: { type: slug }
rules:
  - name: leaves
    match: "dcim.device[attrs.role=leaf]"
    emit:
      type: fabric.leaf
      key:
        name: "${key.device}"
"#,
            ),
        )
        .unwrap();
        // only the two leaves survive the predicate; the spine is filtered out.
        let names: Vec<&str> = out
            .objects
            .iter()
            .map(|o| o.key.get("name").unwrap().as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["leaf01", "leaf02"]);
    }

    #[test]
    fn multi_predicate_selector_ands_predicates() {
        let input = input_inventory(json!([
            { "uid": Uuid::from_u128(1).to_string(), "type": "dcim.device",
              "key": { "device": "leaf-cisco" }, "attrs": { "role": "leaf", "vendor": "cisco" } },
            { "uid": Uuid::from_u128(2).to_string(), "type": "dcim.device",
              "key": { "device": "leaf-arista" }, "attrs": { "role": "leaf", "vendor": "arista" } },
            { "uid": Uuid::from_u128(3).to_string(), "type": "dcim.device",
              "key": { "device": "spine-cisco" }, "attrs": { "role": "spine", "vendor": "cisco" } }
        ]));
        let out = compile_map(
            &input,
            &spec(
                r#"
schema:
  types:
    fabric.leaf:
      key:
        name: { type: slug }
rules:
  - name: cisco-leaves
    match: "dcim.device[attrs.role=leaf][attrs.vendor=cisco]"
    emit:
      type: fabric.leaf
      key:
        name: "${key.device}"
"#,
            ),
        )
        .unwrap();
        let names: Vec<&str> = out
            .objects
            .iter()
            .map(|o| o.key.get("name").unwrap().as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["leaf-cisco"]);
    }

    #[test]
    fn existence_predicates_filter_on_presence() {
        let input = input_inventory(json!([
            { "uid": Uuid::from_u128(1).to_string(), "type": "dcim.device",
              "key": { "device": "leaf01" }, "attrs": { "primary_ip": "10.0.0.1" } },
            { "uid": Uuid::from_u128(2).to_string(), "type": "dcim.device",
              "key": { "device": "leaf02" }, "attrs": {} },
            { "uid": Uuid::from_u128(3).to_string(), "type": "dcim.device",
              "key": { "device": "leaf03" }, "attrs": { "primary_ip": null } }
        ]));
        let template = r#"
schema:
  types:
    fabric.leaf:
      key:
        name: { type: slug }
rules:
  - name: leaves
    match: "SELECTOR"
    emit:
      type: fabric.leaf
      key:
        name: "${key.device}"
"#;
        let names = |selector: &str| -> Vec<String> {
            compile_map(&input, &spec(&template.replace("SELECTOR", selector)))
                .unwrap()
                .objects
                .iter()
                .map(|o| o.key.get("name").unwrap().as_str().unwrap().to_string())
                .collect()
        };
        // `[field]` keeps present, non-null values; `[!field]` is its complement,
        // with both absent and null counting as missing.
        assert_eq!(names("dcim.device[attrs.primary_ip]"), vec!["leaf01"]);
        assert_eq!(
            names("dcim.device[!attrs.primary_ip]"),
            vec!["leaf02", "leaf03"]
        );
    }

    #[test]
    fn value_predicates_need_a_present_scalar() {
        let input = input_inventory(json!([
            { "uid": Uuid::from_u128(1).to_string(), "type": "dcim.device",
              "key": { "device": "leaf01" }, "attrs": { "role": "leaf", "tags": ["a"] } },
            { "uid": Uuid::from_u128(2).to_string(), "type": "dcim.device",
              "key": { "device": "spine01" }, "attrs": { "role": "spine", "tags": ["a"] } },
            { "uid": Uuid::from_u128(3).to_string(), "type": "dcim.device",
              "key": { "device": "norole" }, "attrs": { "tags": ["a"] } },
            { "uid": Uuid::from_u128(4).to_string(), "type": "dcim.device",
              "key": { "device": "nullrole" }, "attrs": { "role": null, "tags": ["a"] } }
        ]));
        let template = r#"
schema:
  types:
    fabric.node:
      key:
        name: { type: slug }
rules:
  - name: nodes
    match: "SELECTOR"
    emit:
      type: fabric.node
      key:
        name: "${key.device}"
"#;
        let names = |selector: &str| -> Vec<String> {
            compile_map(&input, &spec(&template.replace("SELECTOR", selector)))
                .unwrap()
                .objects
                .iter()
                .map(|o| o.key.get("name").unwrap().as_str().unwrap().to_string())
                .collect()
        };
        // `=` and `!=` both need a present, scalar field, so they do not partition
        // the way `[field]`/`[!field]` does: `norole` and `nullrole` fall out of both.
        assert_eq!(names("dcim.device[attrs.role=leaf]"), vec!["leaf01"]);
        assert_eq!(names("dcim.device[attrs.role!=leaf]"), vec!["spine01"]);
        assert_eq!(
            names("dcim.device[!attrs.role]"),
            vec!["norole", "nullrole"]
        );
        // a null under `nullable: true` validates and reaches the predicate, but
        // renders no scalar either, so `=null` matches nothing.
        assert!(names("dcim.device[attrs.role=null]").is_empty());
        // a list has no scalar rendering, so it matches neither operator, while
        // existence still sees it.
        assert!(names("dcim.device[attrs.tags=a]").is_empty());
        assert!(names("dcim.device[attrs.tags!=a]").is_empty());
        assert_eq!(
            names("dcim.device[attrs.tags]"),
            vec!["leaf01", "norole", "nullrole", "spine01"]
        );
    }

    #[test]
    fn predicate_coerces_numeric_and_bool_values() {
        let input = input_inventory(json!([
            { "uid": Uuid::from_u128(1).to_string(), "type": "ipam.vlan",
              "key": { "vid": 10 }, "attrs": { "enabled": true, "label": "v10" } },
            { "uid": Uuid::from_u128(2).to_string(), "type": "ipam.vlan",
              "key": { "vid": 20 }, "attrs": { "enabled": false, "label": "v20" } },
            { "uid": Uuid::from_u128(3).to_string(), "type": "ipam.vlan",
              "key": { "vid": 30 }, "attrs": { "enabled": true, "label": "v30" } }
        ]));
        let template = r#"
schema:
  types:
    fabric.vlan:
      key:
        name: { type: slug }
rules:
  - name: vlans
    match: "SELECTOR"
    emit:
      type: fabric.vlan
      key:
        name: "${attrs.label}"
"#;
        let names = |selector: &str| -> Vec<String> {
            compile_map(&input, &spec(&template.replace("SELECTOR", selector)))
                .unwrap()
                .objects
                .iter()
                .map(|o| o.key.get("name").unwrap().as_str().unwrap().to_string())
                .collect()
        };
        assert_eq!(names("ipam.vlan[key.vid=10]"), vec!["v10"]);
        assert_eq!(names("ipam.vlan[attrs.enabled=true]"), vec!["v10", "v30"]);
    }

    #[test]
    fn multi_emit_fans_out_with_named_uid_reference() {
        // one source fabric fans out into a site and a vrf; the vrf references
        // the site via a named uid (auto ref-rewiring does not apply to
        // multi-emit, so the relationship is wired explicitly through `uids`).
        let input = input_inventory(json!([
            { "uid": Uuid::from_u128(1).to_string(), "type": "net.fabric",
              "key": { "fabric": "fra" }, "attrs": { "site": "fra1", "vrf": "blue" } }
        ]));
        let out = compile_map(
            &input,
            &spec(
                r#"
schema:
  types:
    location.site:
      key:
        slug: { type: slug }
    net.vrf:
      key:
        name: { type: slug }
      fields:
        site: { type: ref, target: location.site }
rules:
  - name: fabric
    match: "net.fabric"
    uids:
      site:
        v5:
          type: "location.site"
          stable: "slug=${attrs.site}"
    emit:
      - type: location.site
        key:
          slug: "${attrs.site}"
        uid: "${uids.site}"
      - type: net.vrf
        key:
          name: "${attrs.vrf}"
        uid:
          v5:
            type: "net.vrf"
            stable: "${uid}#vrf"
        attrs:
          site: "${uids.site}"
"#,
            ),
        )
        .unwrap();

        assert_eq!(out.objects.len(), 2);
        let site = out
            .objects
            .iter()
            .find(|o| o.type_name.as_str() == "location.site")
            .unwrap();
        let vrf = out
            .objects
            .iter()
            .find(|o| o.type_name.as_str() == "net.vrf")
            .unwrap();
        // the named uid pins the site identity and the vrf ref resolves to it,
        // passing reference-integrity validation.
        assert_eq!(site.uid, uid_v5("location.site", "slug=fra1"));
        assert_eq!(vrf.attrs.get("site").unwrap(), &json!(site.uid.to_string()));
    }

    #[test]
    fn group_by_aggregates_members_into_list_fields() {
        // many vlans collapse into one vrf per group; the members' vids are
        // collected into the vrf's `vlans` list.
        let input = input_inventory(json!([
            { "uid": Uuid::from_u128(1).to_string(), "type": "ipam.vlan",
              "key": { "vid": 10 }, "attrs": { "vrf": "blue" } },
            { "uid": Uuid::from_u128(2).to_string(), "type": "ipam.vlan",
              "key": { "vid": 20 }, "attrs": { "vrf": "blue" } },
            { "uid": Uuid::from_u128(3).to_string(), "type": "ipam.vlan",
              "key": { "vid": 30 }, "attrs": { "vrf": "red" } }
        ]));
        let out = compile_map(
            &input,
            &spec(
                r#"
schema:
  types:
    ipam.vrf:
      key:
        name: { type: slug }
      fields:
        vlans:
          type: list
          item: { type: int }
rules:
  - name: vrfs
    match: "ipam.vlan"
    group_by: "${attrs.vrf}"
    emit:
      type: ipam.vrf
      key:
        name: "${group.key}"
      attrs:
        vlans: "${group.items.key.vid}"
"#,
            ),
        )
        .unwrap();

        // two groups (blue, red), sorted by key; members keep input order.
        assert_eq!(out.objects.len(), 2);
        let blue = out
            .objects
            .iter()
            .find(|o| o.key.get("name").unwrap() == &json!("blue"))
            .unwrap();
        let red = out
            .objects
            .iter()
            .find(|o| o.key.get("name").unwrap() == &json!("red"))
            .unwrap();
        assert_eq!(blue.attrs.get("vlans").unwrap(), &json!([10, 20]));
        assert_eq!(red.attrs.get("vlans").unwrap(), &json!([30]));
    }

    #[test]
    fn group_by_excludes_null_member_values() {
        // a member whose value at the path is present-but-null is dropped from
        // the `group.items` list, matching the "non-missing values only" contract
        // (and the null-is-missing convention the predicates use).
        let input = input_inventory(json!([
            { "uid": Uuid::from_u128(1).to_string(), "type": "ipam.vlan",
              "key": { "vid": 10 }, "attrs": { "vrf": "blue", "name": "core" } },
            { "uid": Uuid::from_u128(2).to_string(), "type": "ipam.vlan",
              "key": { "vid": 20 }, "attrs": { "vrf": "blue", "name": null } }
        ]));
        let out = compile_map(
            &input,
            &spec(
                r#"
schema:
  types:
    ipam.vrf:
      key:
        name: { type: slug }
      fields:
        names: { type: json }
rules:
  - name: vrfs
    match: "ipam.vlan"
    group_by: "${attrs.vrf}"
    emit:
      type: ipam.vrf
      key:
        name: "${group.key}"
      attrs:
        names: "${group.items.attrs.name}"
"#,
            ),
        )
        .unwrap();

        assert_eq!(out.objects.len(), 1);
        assert_eq!(out.objects[0].attrs.get("names").unwrap(), &json!(["core"]));
    }

    #[test]
    fn lookup_reads_a_field_from_a_referenced_object() {
        // the device's `status` is a ref to a status object; the lookup follows
        // it and reads the referent's label, turning a ref into a string.
        let status_uid = Uuid::from_u128(9).to_string();
        let input = input_inventory(json!([
            { "uid": status_uid, "type": "extras.status",
              "key": { "name": "active" }, "attrs": { "label": "Active" } },
            { "uid": Uuid::from_u128(1).to_string(), "type": "dcim.device",
              "key": { "name": "leaf01" }, "attrs": { "status": status_uid } }
        ]));
        let out = compile_map(
            &input,
            &spec(
                r#"
schema:
  types:
    dcim.device:
      key:
        name: { type: slug }
      fields:
        status: { type: string }
rules:
  - name: devices
    match: "dcim.device"
    lookups:
      status_label:
        ref: "${attrs.status}"
        get: "attrs.label"
    emit:
      type: dcim.device
      key:
        name: "${key.name}"
      attrs:
        status: "${lookup.status_label}"
"#,
            ),
        )
        .unwrap();

        assert_eq!(out.objects.len(), 1);
        assert_eq!(
            out.objects[0].attrs.get("status").unwrap(),
            &json!("Active")
        );
    }

    /// the spec used by the three `resolve_lookup` failure tests: a single
    /// device rule following `attrs.status` and reading `attrs.label` off it.
    const LOOKUP_SPEC: &str = r#"
schema:
  types:
    dcim.device:
      key:
        name: { type: slug }
      fields:
        status: { type: string }
rules:
  - name: devices
    match: "dcim.device"
    lookups:
      status_label:
        ref: "${attrs.status}"
        get: "attrs.label"
    emit:
      type: dcim.device
      key:
        name: "${key.name}"
      attrs:
        status: "${lookup.status_label}"
"#;

    #[test]
    fn lookup_ref_must_be_a_valid_uuid() {
        let input = input_inventory(json!([
            { "uid": Uuid::from_u128(1).to_string(), "type": "dcim.device",
              "key": { "name": "leaf01" }, "attrs": { "status": "not-a-uuid" } }
        ]));
        let err = compile_map(&input, &spec(LOOKUP_SPEC)).unwrap_err();
        assert!(
            err.to_string().contains("ref is not a valid uuid"),
            "{err:#}"
        );
    }

    #[test]
    fn lookup_ref_must_be_present_in_the_input() {
        let absent = Uuid::from_u128(99).to_string();
        let input = input_inventory(json!([
            { "uid": Uuid::from_u128(1).to_string(), "type": "dcim.device",
              "key": { "name": "leaf01" }, "attrs": { "status": absent } }
        ]));
        let err = compile_map(&input, &spec(LOOKUP_SPEC)).unwrap_err();
        assert!(err.to_string().contains("is not in the input"), "{err:#}");
    }

    #[test]
    fn lookup_get_field_must_exist_on_the_referent() {
        let status_uid = Uuid::from_u128(9).to_string();
        let input = input_inventory(json!([
            { "uid": status_uid, "type": "extras.status",
              "key": { "name": "active" }, "attrs": {} },
            { "uid": Uuid::from_u128(1).to_string(), "type": "dcim.device",
              "key": { "name": "leaf01" }, "attrs": { "status": status_uid } }
        ]));
        let err = compile_map(&input, &spec(LOOKUP_SPEC)).unwrap_err();
        assert!(err.to_string().contains("is absent on"), "{err:#}");
    }

    #[test]
    fn conflicting_one_to_one_rules_on_one_source_error() {
        // two 1:1 rules match the same source; a source has one identity, so
        // reshaping it twice is refused naming both rules.
        let input = input_inventory(json!([
            { "uid": Uuid::from_u128(1).to_string(), "type": "dcim.device",
              "key": { "name": "leaf01" }, "attrs": {} }
        ]));
        let err = compile_map(
            &input,
            &spec(
                r#"
schema:
  types:
    a.node:
      key:
        name: { type: slug }
    b.node:
      key:
        name: { type: slug }
rules:
  - name: as
    match: "dcim.device"
    emit:
      type: a.node
      key:
        name: "${key.name}"
  - name: bs
    match: "dcim.device"
    emit:
      type: b.node
      key:
        name: "${key.name}"
"#,
            ),
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("is reshaped by both rule as and rule bs"),
            "{err:#}"
        );
    }

    #[test]
    fn emit_uid_template_must_be_a_valid_uuid() {
        let input = input_inventory(json!([
            { "uid": Uuid::from_u128(1).to_string(), "type": "dcim.device",
              "key": { "name": "leaf01" }, "attrs": {} }
        ]));
        let err = compile_map(
            &input,
            &spec(
                r#"
schema:
  types:
    lab.node:
      key:
        name: { type: slug }
rules:
  - name: nodes
    match: "dcim.device"
    emit:
      type: lab.node
      key:
        name: "${key.name}"
      uid: "not-a-uuid"
"#,
            ),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("uid template is not a valid uuid"),
            "{err:#}"
        );
    }

    #[test]
    fn emit_uid_v5_rejects_empty_components() {
        // the `stable` component renders empty, tripping the shared
        // `derive_v5_uid` non-empty guard.
        let input = input_inventory(json!([
            { "uid": Uuid::from_u128(1).to_string(), "type": "dcim.device",
              "key": { "name": "leaf01" }, "attrs": { "blank": "" } }
        ]));
        let err = compile_map(
            &input,
            &spec(
                r#"
schema:
  types:
    lab.node:
      key:
        name: { type: slug }
rules:
  - name: nodes
    match: "dcim.device"
    emit:
      type: lab.node
      key:
        name: "${key.name}"
      uid:
        v5:
          type: "lab.node"
          stable: "${attrs.blank}"
"#,
            ),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("non-empty type and stable"),
            "{err:#}"
        );
    }

    #[test]
    fn eval_map_transform_without_a_transforms_block_runs_builtins() {
        // no `transforms:` block -> the built-ins-only EMPTY registry; a
        // built-in still resolves, and an unknown name has no user fns to fall
        // through to, so it errors as an unknown transform.
        let map_spec = spec("{}");
        let result = eval_map_transform(&map_spec, "upper", &json!("ab"), &[]).unwrap();
        assert_eq!(result, json!("AB"));
        let err = eval_map_transform(&map_spec, "nope", &json!("x"), &[]).unwrap_err();
        assert!(
            err.to_string().contains("unknown transform nope"),
            "{err:#}"
        );
    }

    #[cfg(not(feature = "starlark"))]
    #[test]
    fn transforms_block_errors_without_the_feature() {
        let input = input_inventory(json!([]));
        let err = compile_map(
            &input,
            &spec(
                r#"
transforms:
  inline: |
    def f(v):
        return v
"#,
            ),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("without the starlark feature"),
            "{err:#}"
        );
    }

    #[cfg(feature = "starlark")]
    mod starlark {
        use super::*;

        /// the motivating example: a netbox-shaped address with a cidr suffix
        /// denormalised into a connectable `ansible_host`.
        #[test]
        fn inline_transform_derives_attr_end_to_end() {
            let input = input_inventory(json!([
                { "uid": Uuid::from_u128(1).to_string(), "type": "dcim.device",
                  "key": { "name": "leaf01" },
                  "attrs": { "address": "198.51.100.1/24", "platform": "nxos" } }
            ]));
            let out = compile_map(
                &input,
                &spec(
                    r#"
transforms:
  inline: |
    ANSIBLE_OS = {"nxos": "cisco.nxos.nxos", "eos": "arista.eos.eos"}

    def cidr_host(v):
        return v.split("/")[0]

    def ansible_os(platform):
        if platform not in ANSIBLE_OS:
            fail("no ansible_network_os mapping for platform: " + platform)
        return ANSIBLE_OS[platform]
schema:
  types:
    ansible.host:
      key:
        name: { type: string }
      fields:
        ansible_host: { type: string }
        ansible_network_os: { type: string }
rules:
  - name: hosts
    match: "dcim.device"
    emit:
      type: ansible.host
      key:
        name: "${key.name}"
      attrs:
        ansible_host: "${attrs.address|cidr_host}"
        ansible_network_os: "${attrs.platform|ansible_os}"
"#,
                ),
            )
            .unwrap();
            assert_eq!(out.objects.len(), 1);
            let attrs = &out.objects[0].attrs;
            assert_eq!(attrs.get("ansible_host").unwrap(), &json!("198.51.100.1"));
            assert_eq!(
                attrs.get("ansible_network_os").unwrap(),
                &json!("cisco.nxos.nxos")
            );
        }

        /// a transform returning a dict fills a `json`-typed attr with the dict
        /// preserved, and passes schema validation.
        #[test]
        fn typed_dict_return_fills_a_json_attr() {
            let input = input_inventory(json!([
                { "uid": Uuid::from_u128(1).to_string(), "type": "dcim.device",
                  "key": { "name": "leaf01" }, "attrs": { "platform": "eos" } }
            ]));
            let out = compile_map(
                &input,
                &spec(
                    r#"
transforms:
  inline: |
    def profile(platform):
        return {"os": platform, "ports": [22, 830]}
schema:
  types:
    lab.node:
      key:
        name: { type: string }
      fields:
        profile: { type: json }
rules:
  - name: nodes
    match: "dcim.device"
    emit:
      type: lab.node
      key:
        name: "${key.name}"
      attrs:
        profile: "${attrs.platform|profile}"
"#,
                ),
            )
            .unwrap();
            assert_eq!(
                out.objects[0].attrs.get("profile").unwrap(),
                &json!({"os": "eos", "ports": [22, 830]})
            );
        }

        /// `key:` templates feed uid derivation, so a transformed value there is
        /// coerced to a string; a collection return is rejected.
        #[test]
        fn key_context_coerces_scalars_and_rejects_collections() {
            let input = input_inventory(json!([
                { "uid": Uuid::from_u128(1).to_string(), "type": "dcim.device",
                  "key": { "name": "leaf01" }, "attrs": {} }
            ]));
            let scalar_spec = r#"
transforms:
  inline: |
    def n(v):
        return 42
schema:
  types:
    lab.node:
      key:
        name: { type: string }
rules:
  - name: nodes
    match: "dcim.device"
    emit:
      type: lab.node
      key:
        name: "${key.name|n}"
"#;
            let out = compile_map(&input, &spec(scalar_spec)).unwrap();
            assert_eq!(out.objects[0].key.get("name").unwrap(), &json!("42"));

            let collection_spec = scalar_spec.replace("return 42", "return [v]");
            let err = compile_map(&input, &spec(&collection_spec)).unwrap_err();
            assert!(err.to_string().contains("must be a scalar"), "{err:#}");
        }

        #[test]
        fn transforms_block_requires_exactly_one_source() {
            let input = input_inventory(json!([]));
            for block in [
                "transforms: {}",
                "transforms:\n  file: a.star\n  inline: \"x = 1\"",
            ] {
                let err = compile_map(&input, &spec(block)).unwrap_err();
                assert!(
                    err.to_string()
                        .contains("requires exactly one of file or inline"),
                    "{err:#}"
                );
            }
        }

        /// a file-based transforms block whose script has a `load()` dependency,
        /// loaded the way the cli does: `load_map_spec` captures the spec
        /// directory, and both the transforms file and its `load()` target
        /// resolve against it.
        #[test]
        fn file_transforms_with_load_resolve_against_the_spec_dir() {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(
                dir.path().join("lib.star"),
                "def shout(v):\n    return v.upper()\n",
            )
            .unwrap();
            std::fs::write(
                dir.path().join("transforms.star"),
                "load(\"lib.star\", \"shout\")\n\ndef loud_host(v):\n    return shout(v.split(\"/\")[0])\n",
            )
            .unwrap();
            std::fs::write(
                dir.path().join("map.yaml"),
                r#"
transforms:
  file: ./transforms.star
schema:
  types:
    lab.node:
      key:
        name: { type: string }
rules:
  - name: nodes
    match: "dcim.device"
    emit:
      type: lab.node
      key:
        name: "${attrs.address|loud_host}"
"#,
            )
            .unwrap();
            let map_spec = load_map_spec(dir.path().join("map.yaml")).unwrap();
            let input = input_inventory(json!([
                { "uid": Uuid::from_u128(1).to_string(), "type": "dcim.device",
                  "key": { "name": "leaf01" }, "attrs": { "address": "leaf01/24" } }
            ]));
            let out = compile_map(&input, &map_spec).unwrap();
            assert_eq!(out.objects[0].key.get("name").unwrap(), &json!("LEAF01"));
        }

        #[test]
        fn missing_transforms_file_surfaces() {
            // a `file:` transforms block pointing at a nonexistent path surfaces
            // the read error (with its context) rather than silently yielding an
            // empty transform set.
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(
                dir.path().join("map.yaml"),
                "transforms:\n  file: ./missing.star\n",
            )
            .unwrap();
            let map_spec = load_map_spec(dir.path().join("map.yaml")).unwrap();
            let err = eval_map_transform(&map_spec, "f", &json!("x"), &[]).unwrap_err();
            assert!(err.to_string().contains("read transforms file"), "{err:#}");
        }

        #[test]
        fn eval_map_transform_runs_user_builtin_and_errors() {
            let map_spec = spec(
                r#"
transforms:
  inline: |
    def pad(v, width, fill):
        return fill * (width - len(v)) + v

    def reject(v):
        fail("rejected: " + v)
"#,
            );
            let result =
                eval_map_transform(&map_spec, "pad", &json!("7"), &[json!(3), json!("0")]).unwrap();
            assert_eq!(result, json!("007"));

            let result = eval_map_transform(&map_spec, "upper", &json!("q"), &[]).unwrap();
            assert_eq!(result, json!("Q"));

            let err = eval_map_transform(&map_spec, "reject", &json!("v"), &[]).unwrap_err();
            assert!(err.to_string().contains("rejected: v"), "{err:#}");

            let err = eval_map_transform(&map_spec, "nope", &json!("v"), &[]).unwrap_err();
            assert!(
                err.to_string().contains("unknown transform nope"),
                "{err:#}"
            );
        }
    }

    /// identity law: a one-to-one emit inherits the source uid, so renaming a
    /// key changes the key alone. the same source under two keys keeps one uid.
    #[test]
    fn one_to_one_emit_inherits_the_source_uid_across_a_key_rename() {
        let yaml = r#"
schema:
  types:
    dcim.site:
      key:
        slug: { type: slug }
      fields:
        slug: { type: slug }
rules:
  - name: sites
    match: "src.site"
    emit:
      type: dcim.site
      key:
        slug: "${key.slug}"
      attrs:
        slug: "${key.slug}"
"#;
        let object = |slug: &str| {
            input_inventory(json!([
                { "uid": Uuid::from_u128(9).to_string(), "type": "src.site",
                  "key": { "slug": slug }, "attrs": {} }
            ]))
        };
        let before = compile_map(&object("fra1"), &spec(yaml)).unwrap();
        let after = compile_map(&object("fra2"), &spec(yaml)).unwrap();
        assert_eq!(before.objects[0].uid, Uuid::from_u128(9));
        assert_eq!(after.objects[0].uid, Uuid::from_u128(9));
        assert_eq!(after.objects[0].key.get("slug").unwrap(), &json!("fra2"));
    }

    /// identity law: renaming a ref-keyed parent re-mints nothing; the
    /// dependent's key still names the parent's (unchanged) uid.
    #[test]
    fn ref_keyed_parent_rename_leaves_dependent_identity_untouched() {
        let yaml = r#"
schema:
  types:
    net.node:
      key:
        name: { type: slug }
    net.port:
      key:
        device: { type: ref, target: net.node }
        name: { type: slug }
rules:
  - name: nodes
    match: "src.node"
    emit:
      type: net.node
      key:
        name: "${key.name}"
  - name: ports
    match: "src.port"
    emit:
      type: net.port
      key:
        device: "${key.device}"
        name: "${key.name}"
"#;
        let node_src = Uuid::from_u128(1).to_string();
        let chain = |node_name: &str| {
            input_inventory(json!([
                { "uid": node_src, "type": "src.node",
                  "key": { "name": node_name }, "attrs": {} },
                { "uid": Uuid::from_u128(2).to_string(), "type": "src.port",
                  "key": { "device": node_src, "name": "eth0" }, "attrs": {} }
            ]))
        };
        let before = compile_map(&chain("leaf01"), &spec(yaml)).unwrap();
        let after = compile_map(&chain("leaf09"), &spec(yaml)).unwrap();
        let uids = |inv: &Inventory| {
            inv.objects
                .iter()
                .map(|o| (o.type_name.as_str().to_string(), o.uid))
                .collect::<BTreeMap<_, _>>()
        };
        assert_eq!(uids(&before), uids(&after));
        let port = after
            .objects
            .iter()
            .find(|o| o.type_name.as_str() == "net.port")
            .unwrap();
        assert_eq!(port.key.get("device").unwrap(), &json!(node_src));
    }

    /// `uid: target` is the explicit spelling of the value-identity mint.
    #[test]
    fn uid_target_mints_from_the_rendered_target() {
        let input = input_inventory(json!([
            { "uid": Uuid::from_u128(7).to_string(), "type": "src.site",
              "key": { "slug": "fra1" }, "attrs": {} }
        ]));
        let out = compile_map(
            &input,
            &spec(
                r#"
schema:
  types:
    dcim.site:
      key:
        slug: { type: slug }
rules:
  - name: sites
    match: "src.site"
    emit:
      type: dcim.site
      key:
        slug: "${key.slug}"
      uid: target
"#,
            ),
        )
        .unwrap();
        let obj = &out.objects[0];
        assert_eq!(obj.uid, uid_v5("dcim.site", &key_string(&obj.key)));
        assert_ne!(obj.uid, Uuid::from_u128(7));
    }

    /// a multi-emit assigns identity explicitly, a one-element list included,
    /// so reshaping `emit:` into a list cannot silently change identity.
    #[test]
    fn multi_emit_without_a_uid_is_a_spec_error() {
        let input = input_inventory(json!([
            { "uid": Uuid::from_u128(1).to_string(), "type": "src.site",
              "key": { "slug": "fra1" }, "attrs": {} }
        ]));
        let multi = r#"
schema:
  types:
    dcim.site:
      key:
        slug: { type: slug }
    net.zone:
      key:
        name: { type: slug }
rules:
  - name: sites
    match: "src.site"
    emit:
      - type: dcim.site
        key:
          slug: "${key.slug}"
        uid: "${uid}"
      - type: net.zone
        key:
          name: "z-${key.slug}"
"#;
        let err = compile_map(&input, &spec(multi)).unwrap_err();
        assert!(
            err.to_string()
                .contains("rule sites: a multi-emit assigns identity explicitly"),
            "{err:#}"
        );
        assert!(err.to_string().contains("emit 2 (net.zone)"), "{err:#}");

        let one_element = r#"
schema:
  types:
    dcim.site:
      key:
        slug: { type: slug }
rules:
  - name: sites
    match: "src.site"
    emit:
      - type: dcim.site
        key:
          slug: "${key.slug}"
"#;
        let err = compile_map(&input, &spec(one_element)).unwrap_err();
        assert!(err.to_string().contains("emit 1 (dcim.site)"), "{err:#}");
    }

    /// `uid: "${uid}"` marks the continuing object of a fan-out; siblings use
    /// explicit expressions, and reordering or inserting emits moves nothing.
    #[test]
    fn explicit_multi_identities_survive_reorder_and_insertion() {
        let input = input_inventory(json!([
            { "uid": Uuid::from_u128(5).to_string(), "type": "net.fabric",
              "key": { "name": "fab1" }, "attrs": { "site": "fra1" } }
        ]));
        let schema = r#"
schema:
  types:
    net.fabric:
      key:
        name: { type: slug }
    location.site:
      key:
        slug: { type: slug }
    net.zone:
      key:
        name: { type: slug }
"#;
        let fabric = r#"      - type: net.fabric
        key:
          name: "${key.name}"
        uid: "${uid}"
"#;
        let site = r#"      - type: location.site
        key:
          slug: "${attrs.site}"
        uid:
          v5:
            type: "location.site"
            stable: "${uid}#site"
"#;
        let zone = r#"      - type: net.zone
        key:
          name: "z-${attrs.site}"
        uid:
          v5:
            type: "net.zone"
            stable: "${uid}#zone"
"#;
        let build = |emits: &[&str]| {
            format!(
                "{schema}rules:\n  - name: fabric\n    match: \"net.fabric\"\n    emit:\n{}",
                emits.concat()
            )
        };
        let uids = |inv: &Inventory| {
            inv.objects
                .iter()
                .map(|o| (o.type_name.as_str().to_string(), o.uid))
                .collect::<BTreeMap<_, _>>()
        };
        let ab = compile_map(&input, &spec(&build(&[fabric, site]))).unwrap();
        let ba = compile_map(&input, &spec(&build(&[site, fabric]))).unwrap();
        let abc = compile_map(&input, &spec(&build(&[zone, fabric, site]))).unwrap();
        assert_eq!(uids(&ab), uids(&ba));
        for (type_name, uid) in uids(&ab) {
            assert_eq!(uids(&abc).get(&type_name), Some(&uid));
        }
        assert_eq!(
            uids(&ab).get("net.fabric").copied(),
            Some(Uuid::from_u128(5))
        );
    }

    /// identity flows through composed one-to-one maps unchanged, so a
    /// pipeline refactored into stages keeps every uid.
    #[test]
    fn composed_one_to_one_maps_preserve_identity() {
        let input = input_inventory(json!([
            { "uid": Uuid::from_u128(3).to_string(), "type": "src.site",
              "key": { "slug": "fra1" }, "attrs": {} }
        ]));
        let stage = |from: &str, to: &str| {
            format!(
                r#"
schema:
  types:
    {to}:
      key:
        slug: {{ type: slug }}
rules:
  - name: s
    match: "{from}"
    emit:
      type: {to}
      key:
        slug: "${{key.slug}}"
"#
            )
        };
        let mid = compile_map(&input, &spec(&stage("src.site", "mid.site"))).unwrap();
        let via = compile_map(&mid, &spec(&stage("mid.site", "dcim.site"))).unwrap();
        let direct = compile_map(&input, &spec(&stage("src.site", "dcim.site"))).unwrap();
        assert_eq!(via.objects[0].uid, direct.objects[0].uid);
        assert_eq!(via.objects[0].uid, Uuid::from_u128(3));
    }

    /// a group keeps value identity: membership changes leave the aggregate's
    /// uid alone, and only the group value names it.
    #[test]
    fn group_by_membership_changes_keep_the_aggregate_uid() {
        let yaml = r#"
schema:
  types:
    ipam.vrf:
      key:
        name: { type: slug }
      fields:
        vlans: { type: int }
rules:
  - name: vrfs
    match: "ipam.vlan"
    group_by: "${attrs.vrf}"
    emit:
      type: ipam.vrf
      key:
        name: "${group.key}"
      attrs:
        vlans: "${group.count}"
"#;
        let vlans = |third_vrf: &str| {
            input_inventory(json!([
                { "uid": Uuid::from_u128(1).to_string(), "type": "ipam.vlan",
                  "key": { "vid": 10 }, "attrs": { "vrf": "blue" } },
                { "uid": Uuid::from_u128(2).to_string(), "type": "ipam.vlan",
                  "key": { "vid": 20 }, "attrs": { "vrf": "blue" } },
                { "uid": Uuid::from_u128(3).to_string(), "type": "ipam.vlan",
                  "key": { "vid": 30 }, "attrs": { "vrf": third_vrf } }
            ]))
        };
        let uid_of = |inv: &Inventory, name: &str| {
            inv.objects
                .iter()
                .find(|o| o.key.get("name").unwrap() == &json!(name))
                .map(|o| o.uid)
        };
        let before = compile_map(&vlans("red"), &spec(yaml)).unwrap();
        let moved = compile_map(&vlans("blue"), &spec(yaml)).unwrap();
        assert_eq!(uid_of(&before, "blue"), uid_of(&moved, "blue"));
    }

    /// grouping by a ref anchors the aggregate on the referenced object's uid,
    /// so renaming the referent moves nothing.
    #[test]
    fn group_by_a_ref_survives_the_referent_rename() {
        let vrf_src = Uuid::from_u128(4).to_string();
        let yaml = r#"
schema:
  types:
    src.vrf:
      key:
        name: { type: slug }
    ipam.vrf:
      key:
        anchor: { type: string }
      fields:
        vlans: { type: int }
rules:
  - name: vrfs-through
    match: "src.vrf"
    emit:
      type: src.vrf
      key:
        name: "${key.name}"
  - name: vrfs
    match: "ipam.vlan"
    group_by: "${attrs.vrf}"
    emit:
      type: ipam.vrf
      key:
        anchor: "${group.key}"
      attrs:
        vlans: "${group.count}"
"#;
        let world = |vrf_name: &str| {
            input_inventory(json!([
                { "uid": vrf_src, "type": "src.vrf",
                  "key": { "name": vrf_name }, "attrs": {} },
                { "uid": Uuid::from_u128(1).to_string(), "type": "ipam.vlan",
                  "key": { "vid": 10 }, "attrs": { "vrf": vrf_src } }
            ]))
        };
        let aggregate = |inv: &Inventory| {
            inv.objects
                .iter()
                .find(|o| o.type_name.as_str() == "ipam.vrf")
                .map(|o| o.uid)
                .unwrap()
        };
        let before = compile_map(&world("blue"), &spec(yaml)).unwrap();
        let after = compile_map(&world("azure"), &spec(yaml)).unwrap();
        assert_eq!(aggregate(&before), aggregate(&after));
    }

    /// `target` names an emit's own rendered (type, key); a named uid has
    /// neither, so it is refused there.
    #[test]
    fn uid_target_in_a_named_uid_is_an_error() {
        let input = input_inventory(json!([
            { "uid": Uuid::from_u128(1).to_string(), "type": "src.site",
              "key": { "slug": "fra1" }, "attrs": {} }
        ]));
        let err = compile_map(
            &input,
            &spec(
                r#"
schema:
  types:
    dcim.site:
      key:
        slug: { type: slug }
rules:
  - name: sites
    match: "src.site"
    uids:
      pin: target
    emit:
      type: dcim.site
      key:
        slug: "${key.slug}"
"#,
            ),
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("`target` is only meaningful as an emit's uid:"),
            "{err:#}"
        );
    }
}
