//! user-defined map template transforms, written in starlark.
//!
//! a map spec's `transforms:` block names starlark source (a file or inline)
//! whose top-level `def`s become template transforms: `${var|cidr_host}` calls
//! `cidr_host` with the piped value, `${var|f(2)}` passes literal arguments
//! after it. the source is compiled and frozen once per map run; each transform
//! call evaluates the frozen function on a fresh module.
//!
//! hermetic by construction: the standard globals expose no i/o, the language
//! has no `while`, and recursion is rejected at evaluation time, so transforms
//! terminate and map runs stay deterministic. `load()` is supported but
//! restricted to relative paths, which all resolve against the map spec's
//! directory (including transitive loads).

use anyhow::{anyhow, Context, Result};
use serde_json::Value as JsonValue;
use starlark::environment::{FrozenModule, Globals, Module};
use starlark::eval::{Evaluator, ReturnFileLoader};
use starlark::syntax::{AstModule, Dialect};
use starlark::values::dict::AllocDict;
use starlark::values::list::AllocList;
use starlark::values::{Heap, Value};
use std::collections::HashMap;
use std::path::Path;

/// a compiled set of user transforms: the frozen module holding the top-level
/// `def`s from the map spec's `transforms:` source.
#[derive(Debug)]
pub(crate) struct StarlarkTransforms {
    module: FrozenModule,
}

impl StarlarkTransforms {
    /// compile starlark source into a frozen module. `filename` labels parse
    /// and evaluation errors; `base_dir` is the map spec's directory, against
    /// which `load()` paths resolve (`None` for specs parsed from strings,
    /// where `load()` is an error).
    pub(crate) fn compile(source: &str, filename: &str, base_dir: Option<&Path>) -> Result<Self> {
        let mut visiting = Vec::new();
        let module = compile_module(source.to_owned(), filename, base_dir, &mut visiting)?;
        Ok(StarlarkTransforms { module })
    }

    /// call a user transform: the piped value first, then the literal
    /// arguments. `Ok(None)` means no module-level binding named `name` exists
    /// (the caller falls through to the built-ins); any defined binding counts,
    /// so calling a non-callable surfaces starlark's own error.
    pub(crate) fn call(
        &self,
        name: &str,
        value: &JsonValue,
        args: &[JsonValue],
    ) -> Result<Option<JsonValue>> {
        let Some(function) = self.module.get_option(name)? else {
            return Ok(None);
        };
        Module::with_temp_heap(|module| {
            let heap = module.heap();
            let function = heap.access_owned_frozen_value(&function);
            let mut positional = Vec::with_capacity(args.len() + 1);
            positional.push(json_to_starlark(heap, value)?);
            for arg in args {
                positional.push(json_to_starlark(heap, arg)?);
            }
            let mut eval = Evaluator::new(&module);
            let result = eval
                .eval_function(function, &positional, &[])
                .map_err(starlark::Error::into_anyhow)?;
            let json = result
                .to_json_value()
                .map_err(|err| anyhow!("returned a value with no json representation: {err:#}"))?;
            Ok(Some(json))
        })
    }
}

/// parse and evaluate one starlark module, recursively pre-loading its
/// `load()` dependencies (the `ReturnFileLoader` pattern). `visiting` carries
/// the in-progress load chain for cycle detection.
fn compile_module(
    source: String,
    filename: &str,
    base_dir: Option<&Path>,
    visiting: &mut Vec<String>,
) -> Result<FrozenModule> {
    let ast = AstModule::parse(filename, source, &Dialect::Standard)
        .map_err(starlark::Error::into_anyhow)
        .with_context(|| format!("parse starlark: {filename}"))?;

    let load_ids: Vec<String> = ast
        .loads()
        .iter()
        .map(|load| load.module_id.to_string())
        .collect();
    let mut loaded: Vec<(String, FrozenModule)> = Vec::new();
    for module_id in load_ids {
        let Some(base) = base_dir else {
            return Err(anyhow!(
                "load(\"{module_id}\") in {filename}: load() requires a map spec loaded from a file"
            ));
        };
        let load_path = Path::new(&module_id);
        if load_path.is_absolute() {
            return Err(anyhow!(
                "load(\"{module_id}\") in {filename}: absolute paths are not allowed; \
                 paths resolve relative to the spec file"
            ));
        }
        if visiting.iter().any(|seen| seen == &module_id) {
            return Err(anyhow!(
                "load() cycle: {} -> {module_id}",
                visiting.join(" -> ")
            ));
        }
        let resolved = base.join(load_path);
        let loaded_source = std::fs::read_to_string(&resolved)
            .with_context(|| format!("read starlark module: {}", resolved.display()))?;
        visiting.push(module_id.clone());
        let frozen = compile_module(loaded_source, &module_id, base_dir, visiting)?;
        visiting.pop();
        loaded.push((module_id, frozen));
    }

    let modules: HashMap<&str, &FrozenModule> = loaded
        .iter()
        .map(|(id, frozen)| (id.as_str(), frozen))
        .collect();
    Module::with_temp_heap(|module| {
        let loader = ReturnFileLoader { modules: &modules };
        {
            let mut eval = Evaluator::new(&module);
            eval.set_loader(&loader);
            eval.eval_module(ast, &Globals::standard())
                .map_err(starlark::Error::into_anyhow)
                .with_context(|| format!("evaluate starlark: {filename}"))?;
        }
        module
            .freeze()
            .map_err(|err| anyhow!("freeze starlark module {filename}: {err:?}"))
    })
}

/// allocate a json value on a starlark heap. arrays become lists, objects
/// become dicts (keys are strings by construction), numbers prefer ints
/// (starlark ints are arbitrary precision) and fall back to floats.
fn json_to_starlark<'v>(heap: Heap<'v>, value: &JsonValue) -> Result<Value<'v>> {
    Ok(match value {
        JsonValue::Null => Value::new_none(),
        JsonValue::Bool(boolean) => Value::new_bool(*boolean),
        JsonValue::Number(number) => {
            if let Some(int) = number.as_i64() {
                heap.alloc(int)
            } else if let Some(int) = number.as_u64() {
                heap.alloc(int)
            } else if let Some(float) = number.as_f64() {
                heap.alloc(float)
            } else {
                return Err(anyhow!("number {number} has no starlark representation"));
            }
        }
        JsonValue::String(string) => heap.alloc(string.as_str()),
        JsonValue::Array(items) => {
            let mut values = Vec::with_capacity(items.len());
            for item in items {
                values.push(json_to_starlark(heap, item)?);
            }
            heap.alloc(AllocList(values))
        }
        JsonValue::Object(map) => {
            let mut pairs = Vec::with_capacity(map.len());
            for (key, value) in map {
                pairs.push((heap.alloc(key.as_str()), json_to_starlark(heap, value)?));
            }
            heap.alloc(AllocDict(pairs))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transforms(source: &str) -> StarlarkTransforms {
        StarlarkTransforms::compile(source, "test", None).unwrap()
    }

    #[test]
    fn calls_a_simple_transform() {
        let user = transforms("def cidr_host(v):\n    return v.split(\"/\")[0]\n");
        let result = user
            .call("cidr_host", &serde_json::json!("10.0.0.1/24"), &[])
            .unwrap();
        assert_eq!(result, Some(serde_json::json!("10.0.0.1")));
    }

    #[test]
    fn unknown_name_is_none() {
        let user = transforms("def f(v):\n    return v\n");
        assert_eq!(user.call("g", &serde_json::json!(1), &[]).unwrap(), None);
    }

    #[test]
    fn passes_literal_arguments() {
        let user = transforms("def pad(v, width, fill):\n    return fill * (width - len(v)) + v\n");
        let result = user
            .call(
                "pad",
                &serde_json::json!("7"),
                &[serde_json::json!(3), serde_json::json!("0")],
            )
            .unwrap();
        assert_eq!(result, Some(serde_json::json!("007")));
    }

    #[test]
    fn fail_surfaces_as_error() {
        let user = transforms("def f(v):\n    fail(\"no mapping for \" + v)\n");
        let err = user.call("f", &serde_json::json!("vyos"), &[]).unwrap_err();
        assert!(err.to_string().contains("no mapping for vyos"), "{err:#}");
    }

    #[test]
    fn non_callable_binding_errors() {
        let user = transforms("TABLE = {\"a\": 1}\n");
        let err = user
            .call("TABLE", &serde_json::json!("a"), &[])
            .unwrap_err();
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn round_trips_types() {
        let user = transforms("def id(v):\n    return v\n");
        for value in [
            serde_json::json!(null),
            serde_json::json!(true),
            serde_json::json!(i64::MIN),
            serde_json::json!(i64::MAX),
            serde_json::json!(u64::MAX),
            serde_json::json!(2.5),
            serde_json::json!("s"),
            serde_json::json!([1, "a", [true]]),
            serde_json::json!({"k": {"nested": [1, 2]}}),
        ] {
            let result = user.call("id", &value, &[]).unwrap();
            assert_eq!(result, Some(value));
        }
    }

    #[test]
    fn typed_returns_map_to_json() {
        let user = transforms(
            "def profile(platform):\n    return {\"os\": platform, \"ports\": [1, 2]}\n",
        );
        let result = user
            .call("profile", &serde_json::json!("eos"), &[])
            .unwrap();
        assert_eq!(
            result,
            Some(serde_json::json!({"os": "eos", "ports": [1, 2]}))
        );
    }

    #[test]
    fn function_return_has_no_json_representation() {
        let user = transforms("def f(v):\n    return lambda: v\n");
        let err = user.call("f", &serde_json::json!(1), &[]).unwrap_err();
        assert!(
            err.to_string().contains("no json representation"),
            "{err:#}"
        );
    }

    #[test]
    fn recursion_is_rejected() {
        let user = transforms("def f(v):\n    return f(v)\n");
        let err = user.call("f", &serde_json::json!(1), &[]).unwrap_err();
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn load_requires_a_base_dir() {
        let err =
            StarlarkTransforms::compile("load(\"lib.star\", \"f\")\n", "test", None).unwrap_err();
        assert!(
            err.to_string()
                .contains("load() requires a map spec loaded from a file"),
            "{err:#}"
        );
    }

    #[test]
    fn load_rejects_absolute_paths() {
        let dir = tempfile::tempdir().unwrap();
        let err = StarlarkTransforms::compile(
            "load(\"/etc/lib.star\", \"f\")\n",
            "test",
            Some(dir.path()),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("absolute paths are not allowed"),
            "{err:#}"
        );
    }

    #[test]
    fn load_resolves_relative_to_base_dir() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("lib.star"),
            "def shout(v):\n    return v.upper()\n",
        )
        .unwrap();
        let user = StarlarkTransforms::compile(
            "load(\"lib.star\", \"shout\")\n\ndef f(v):\n    return shout(v)\n",
            "test",
            Some(dir.path()),
        )
        .unwrap();
        let result = user.call("f", &serde_json::json!("quiet"), &[]).unwrap();
        assert_eq!(result, Some(serde_json::json!("QUIET")));
    }

    #[test]
    fn load_detects_cycles() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("a.star"),
            "load(\"b.star\", \"g\")\n\ndef f(v):\n    return g(v)\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("b.star"),
            "load(\"a.star\", \"f\")\n\ndef g(v):\n    return f(v)\n",
        )
        .unwrap();
        let err =
            StarlarkTransforms::compile("load(\"a.star\", \"f\")\n", "test", Some(dir.path()))
                .unwrap_err();
        assert!(err.to_string().contains("load() cycle"), "{err:#}");
    }
}
