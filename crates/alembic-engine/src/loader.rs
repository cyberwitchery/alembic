//! inventory file loading with include/import support.

use crate::{
    explain_parse_error, report_to_result_with_sources, validate, DocumentKind, InputError,
};
use alembic_core::{Inventory, Schema, Scope, SourceLocation, Uid};
use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

/// raw on-disk representation for a inventory file.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InventoryFile {
    #[serde(default)]
    include: Vec<String>,
    #[serde(default)]
    imports: Vec<String>,
    #[serde(default)]
    schema: Option<Schema>,
    #[serde(default)]
    scope: Option<Scope>,
    #[serde(default)]
    objects: Vec<alembic_core::Object>,
}

/// load a inventory file (yaml or json) and merge any includes.
pub fn load_inventory(path: impl AsRef<Path>) -> Result<Inventory> {
    let inventory = load_inventory_unvalidated(path)?;
    report_to_result_with_sources(validate(&inventory), &inventory.objects)?;
    Ok(inventory)
}

/// load and merge an inventory without validating it, for a caller that reports
/// the validation errors itself rather than failing on the load.
pub fn load_inventory_unvalidated(path: impl AsRef<Path>) -> Result<Inventory> {
    let mut visited = BTreeSet::new();
    let mut objects = Vec::new();
    let mut schema: Option<Schema> = None;
    let mut scope: Option<Scope> = None;
    let path = path.as_ref();
    load_recursive(path, &mut visited, &mut objects, &mut schema, &mut scope)?;
    let schema = schema
        .ok_or_else(|| InputError::Invalid("inventory is missing a schema block".to_string()))?;
    Ok(Inventory {
        schema,
        scope,
        objects,
    })
}

/// recursive loader with cycle-safe include handling.
fn load_recursive(
    path: &Path,
    visited: &mut BTreeSet<PathBuf>,
    objects: &mut Vec<alembic_core::Object>,
    schema: &mut Option<Schema>,
    scope: &mut Option<Scope>,
) -> Result<()> {
    let canonical = fs::canonicalize(path)
        .with_context(|| InputError::Usage(format!("load inventory: {}", path.display())))?;
    if !visited.insert(canonical.clone()) {
        return Ok(());
    }

    let content = fs::read_to_string(&canonical)
        .with_context(|| InputError::Usage(format!("read inventory: {}", canonical.display())))?;
    let json = canonical.extension().and_then(|s| s.to_str()) == Some("json");
    let parsed: Result<InventoryFile> = if json {
        serde_json::from_str(&content).map_err(Into::into)
    } else {
        crate::yaml::from_str(&content).map_err(Into::into)
    };
    let inventory = parsed
        .map_err(|err| explain_parse_error(DocumentKind::Inventory, &content, err))
        .with_context(|| {
            let format = if json { "json" } else { "yaml" };
            InputError::Invalid(format!("parse {format}: {}", canonical.display()))
        })?;

    let base = canonical
        .parent()
        .ok_or_else(|| anyhow!("missing parent dir for {}", canonical.display()))?;

    let mut includes = inventory.include;
    includes.extend(inventory.imports);

    for entry in includes {
        let include_path = base.join(entry);
        load_recursive(&include_path, visited, objects, schema, scope)?;
    }

    merge_schema(schema, inventory.schema)?;
    merge_scope(scope, inventory.scope)?;

    // set source location on each object from this file, with line numbers.
    // the uid->line index is built once per file (one pass), not rescanned per
    // object, so loading is linear in file size rather than objects x lines.
    let uid_lines = index_uid_lines(&content);
    for object in inventory.objects {
        let source = match uid_lines.get(&object.uid) {
            Some(&n) => SourceLocation::file_line(&canonical, n),
            None => SourceLocation::file(&canonical),
        };
        objects.push(object.with_source(source));
    }

    Ok(())
}

/// map each object's `uid` value to the 1-indexed line where it is defined (its
/// own `uid:` key), keeping the first occurrence. built in a single pass so the
/// caller can resolve every object's source line without rescanning the file.
fn index_uid_lines(content: &str) -> HashMap<Uid, usize> {
    let mut index = HashMap::new();
    for (idx, line) in content.lines().enumerate() {
        if let Some(uid) = uid_key_value(line).and_then(|value| Uid::parse_str(value).ok()) {
            index.entry(uid).or_insert(idx + 1);
        }
    }
    index
}

/// if `line`'s key is `uid`, the declared value with surrounding quotes and a
/// trailing json comma stripped; otherwise `None`. handles yaml `uid:` /
/// `- uid:` and json `"uid":`.
fn uid_key_value(line: &str) -> Option<&str> {
    let rest = line.trim_start();
    let rest = rest.strip_prefix('-').map_or(rest, str::trim_start);
    let rest = rest.strip_prefix('"').unwrap_or(rest);
    let after = rest.strip_prefix("uid")?.trim_start();
    let after = after.strip_prefix('"').unwrap_or(after).trim_start();
    let value = after.strip_prefix(':')?.trim().trim_end_matches(',').trim();
    let value = value
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .unwrap_or(value);
    Some(value)
}

/// merge an included file's scope the way schemas merge: disjoint types
/// combine, a type scoped twice is an error rather than a silent winner.
fn merge_scope(current: &mut Option<Scope>, incoming: Option<Scope>) -> Result<()> {
    let Some(incoming) = incoming else {
        return Ok(());
    };
    match current {
        Some(existing) => {
            for (name, entry) in incoming.0 {
                if existing.0.contains_key(&name) {
                    return Err(InputError::Invalid(format!("duplicate scope type {name}")).into());
                }
                existing.0.insert(name, entry);
            }
        }
        None => {
            *current = Some(incoming);
        }
    }
    Ok(())
}

fn merge_schema(current: &mut Option<Schema>, incoming: Option<Schema>) -> Result<()> {
    let Some(incoming) = incoming else {
        return Ok(());
    };
    match current {
        Some(existing) => {
            for (name, schema) in incoming.types {
                if existing.types.contains_key(&name) {
                    return Err(InputError::Invalid(format!("duplicate schema type {name}")).into());
                }
                existing.types.insert(name, schema);
            }
        }
        None => {
            *current = Some(incoming);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{index_uid_lines, load_inventory, merge_schema};
    use alembic_core::{Inventory, Schema, TypeSchema, Uid};
    use anyhow::Result;
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    #[test]
    fn a_loaded_scope_survives_and_merges_across_includes() {
        let dir = tempdir().unwrap();
        let root = dir.path().join("inv.yaml");
        std::fs::write(
            dir.path().join("other.yaml"),
            "schema: { types: { dcim.device: { key: { name: { type: slug } }, fields: {} } } }\nscope: { dcim.device: {} }\n",
        )
        .unwrap();
        std::fs::write(
            &root,
            "include: [other.yaml]\nschema: { types: { dcim.site: { key: { slug: { type: slug } }, fields: {} } } }\nscope: { dcim.site: { slug: fra1 } }\n",
        )
        .unwrap();
        let inventory = load_inventory(&root).unwrap();
        let scope = inventory.scope.expect("scope loaded");
        assert!(scope.entry("dcim.site").is_some());
        assert!(scope.entry("dcim.device").is_some());
    }

    #[test]
    fn a_type_scoped_twice_across_includes_is_an_error() {
        let dir = tempdir().unwrap();
        let root = dir.path().join("inv.yaml");
        std::fs::write(dir.path().join("other.yaml"), "scope: { dcim.site: {} }\n").unwrap();
        std::fs::write(
            &root,
            "include: [other.yaml]\nschema: { types: { dcim.site: { key: { slug: { type: slug } }, fields: {} } } }\nscope: { dcim.site: { slug: fra1 } }\n",
        )
        .unwrap();
        let err = format!("{:#}", load_inventory(&root).unwrap_err());
        assert!(err.contains("duplicate scope type dcim.site"), "{}", err);
    }

    #[test]
    fn rejects_an_unknown_key_in_an_inventory_file() {
        // a typo'd `includes` must be rejected, not load nothing and report ok.
        let dir = tempdir().unwrap();
        let path = dir.path().join("inv.yaml");
        std::fs::write(&path, "includes: [other.yaml]\nschema: { types: {} }\n").unwrap();
        let err = format!("{:#}", load_inventory(&path).unwrap_err());
        assert!(err.contains("unknown field `includes`"), "{}", err);
    }

    /// write `schema: { types }` once as a yaml and once as a json inventory and load each.
    fn load_types_both_ways(types: serde_json::Value) -> Vec<(&'static str, Result<Inventory>)> {
        let doc = serde_json::json!({ "schema": { "types": types } });
        ["yaml", "json"]
            .into_iter()
            .map(|ext| {
                let dir = tempdir().unwrap();
                let path = dir.path().join(format!("inv.{ext}"));
                let content = match ext {
                    "json" => serde_json::to_string(&doc).unwrap(),
                    _ => crate::yaml::to_string(&doc).unwrap(),
                };
                std::fs::write(&path, content).unwrap();
                (ext, load_inventory(&path))
            })
            .collect()
    }

    fn load_error_both_ways(types: serde_json::Value) -> Vec<(&'static str, String)> {
        load_types_both_ways(types)
            .into_iter()
            .map(|(ext, loaded)| (ext, format!("{:#}", loaded.unwrap_err())))
            .collect()
    }

    #[test]
    fn a_key_list_takes_each_schema_from_fields() {
        let interface_fields = serde_json::json!({
            "device": { "type": "ref", "target": "dcim.device" },
            "name": { "type": "string" },
            "mac": { "type": "mac" },
        });
        let types = serde_json::json!({
            "dcim.device": { "key": ["name"], "fields": { "name": { "type": "string" } } },
            "dcim.interface": { "key": ["device", "name"], "fields": interface_fields },
        });
        for (ext, loaded) in load_types_both_ways(types) {
            let inventory = loaded.unwrap_or_else(|err| panic!("{ext}: {err:#}"));
            let interface = &inventory.schema.types["dcim.interface"];
            let fields = &interface.fields;
            let expected: BTreeMap<_, _> = ["device", "name"]
                .into_iter()
                .map(|name| (name.to_string(), fields[name].clone()))
                .collect();
            assert_eq!(interface.key, expected, "{ext}");
            assert_eq!(fields.len(), 3, "{ext}");
        }
    }

    #[test]
    fn a_key_list_refuses_a_name_not_declared_in_fields() {
        let types = serde_json::json!({
            "dcim.interface": { "key": ["name", "mac"], "fields": { "name": { "type": "string" } } },
        });
        for (ext, err) in load_error_both_ways(types) {
            assert!(
                err.contains(
                    "key list of dcim.interface names `mac`, which is not declared in fields"
                ),
                "{ext}: {err}"
            );
        }
    }

    #[test]
    fn a_key_list_refuses_a_repeated_name() {
        let types = serde_json::json!({
            "dcim.interface": { "key": ["name", "name"], "fields": { "name": { "type": "string" } } },
        });
        for (ext, err) in load_error_both_ways(types) {
            assert!(
                err.contains("key list of dcim.interface names `name` twice"),
                "{ext}: {err}"
            );
        }
    }

    #[test]
    fn a_key_list_refuses_a_non_string_entry() {
        let types = serde_json::json!({
            "dcim.interface": { "key": ["name", 3], "fields": { "name": { "type": "string" } } },
        });
        for (ext, err) in load_error_both_ways(types) {
            assert!(
                err.contains("key list of dcim.interface holds `3`, which is not a field name"),
                "{ext}: {err}"
            );
        }
    }

    #[test]
    fn a_key_list_still_obeys_the_key_field_rules() {
        let nullable = serde_json::json!({
            "dcim.site": { "key": ["slug"], "fields": { "slug": { "type": "slug", "nullable": true } } },
        });
        for (ext, err) in load_error_both_ways(nullable) {
            assert!(
                err.contains("nullable key field dcim.site.key.slug"),
                "{ext}: {err}"
            );
        }
        let composite = serde_json::json!({
            "dcim.site": { "key": ["tags"], "fields": { "tags": { "type": "list", "item": "string" } } },
        });
        for (ext, err) in load_error_both_ways(composite) {
            assert!(
                err.contains("non-scalar key field dcim.site.key.tags"),
                "{ext}: {err}"
            );
        }
    }

    #[test]
    fn a_malformed_key_map_keeps_its_precise_error() {
        let types = serde_json::json!({
            "dcim.site": { "key": { "slug": { "type": "slug", "requried": true } } },
        });
        for (ext, err) in load_error_both_ways(types) {
            assert!(err.contains("unknown field `requried`"), "{ext}: {err}");
            assert!(!err.contains("did not match any variant"), "{ext}: {err}");
        }
    }

    #[test]
    fn locates_uid_definition_not_an_earlier_reference() {
        let content = r#"objects:
  - uid: "11111111-1111-1111-1111-111111111111"
    attrs:
      site: "22222222-2222-2222-2222-222222222222"
  - uid: "22222222-2222-2222-2222-222222222222"
"#;
        let index = index_uid_lines(content);
        let dev = Uid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let site = Uid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
        // site resolves to its own `uid:` line (5), not the attr line (4) above it.
        assert_eq!(index.get(&site).copied(), Some(5));
        assert_eq!(index.get(&dev).copied(), Some(2));
    }

    #[test]
    fn locates_uid_key_in_json() {
        let content = r#"{
  "objects": [
    {
      "uid": "11111111-1111-1111-1111-111111111111",
      "attrs": { "site": "22222222-2222-2222-2222-222222222222" }
    },
    {
      "uid": "22222222-2222-2222-2222-222222222222"
    }
  ]
}
"#;
        let index = index_uid_lines(content);
        let site = Uid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
        assert_eq!(index.get(&site).copied(), Some(8));
    }

    #[test]
    fn skips_a_non_uuid_uid_value() {
        // keyed by parsed uid, a `uid:` line whose value is not a uuid is not indexed.
        let content = r#"objects:
  - uid: "not-a-uuid"
"#;
        assert!(index_uid_lines(content).is_empty());
    }

    #[test]
    fn non_canonical_uid_keeps_its_error_line() {
        // an uppercase uid is legal but not canonical; its validation errors must
        // still carry its definition line, like the lowercase control.
        let dir = tempdir().unwrap();
        let path = dir.path().join("inv.yaml");
        let content = r#"schema:
  types:
    dcim.site:
      key:
        site:
          type: slug
      fields:
        name:
          type: string
    dcim.rack:
      key:
        rack:
          type: slug
      fields:
        name:
          type: string
objects:
  - uid: "3F2504E0-4F89-11D3-9A0C-0305E82C3301"
    type: dcim.site
    key:
      site: "fra1"
    attrs:
      name: "FRA1"
      extra: "boom"
  - uid: "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
    type: dcim.rack
    key:
      rack: "r1"
    attrs:
      name: "R1"
      extra: "boom"
"#;
        std::fs::write(&path, content).unwrap();

        let err = load_inventory(&path).unwrap_err().to_string();
        // uppercase uid on line 18, lowercase control on line 25.
        assert!(err.contains(":18:"), "uppercase uid lost its line: {err}");
        assert!(err.contains(":25:"), "lowercase uid lost its line: {err}");
    }

    fn schema_with_type(name: &str) -> Schema {
        let mut schema = Schema::default();
        schema.types.insert(
            name.to_string(),
            TypeSchema {
                key: BTreeMap::new(),
                fields: BTreeMap::new(),
            },
        );
        schema
    }

    #[test]
    fn merge_schema_combines_disjoint_types() {
        let mut current = Some(schema_with_type("dcim.site"));
        merge_schema(&mut current, Some(schema_with_type("dcim.device"))).unwrap();
        let types = current.unwrap().types;
        assert!(types.contains_key("dcim.site"));
        assert!(types.contains_key("dcim.device"));
    }

    #[test]
    fn merge_schema_rejects_duplicate_type() {
        let mut current = Some(schema_with_type("dcim.site"));
        let err = merge_schema(&mut current, Some(schema_with_type("dcim.site"))).unwrap_err();
        assert_eq!(err.to_string(), "duplicate schema type dcim.site");
    }
}
