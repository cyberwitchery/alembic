//! inventory file loading with include/import support.

use crate::{report_to_result_with_sources, validate};
use alembic_core::{Inventory, Schema, SourceLocation};
use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

/// raw on-disk representation for a inventory file.
#[derive(Debug, Deserialize)]
struct InventoryFile {
    #[serde(default)]
    include: Vec<String>,
    #[serde(default)]
    imports: Vec<String>,
    #[serde(default)]
    schema: Option<Schema>,
    #[serde(default)]
    objects: Vec<alembic_core::Object>,
}

/// load a inventory file (yaml or json) and merge any includes.
pub fn load_inventory(path: impl AsRef<Path>) -> Result<Inventory> {
    let mut visited = BTreeSet::new();
    let mut objects = Vec::new();
    let mut schema: Option<Schema> = None;
    let path = path.as_ref();
    load_recursive(path, &mut visited, &mut objects, &mut schema)?;
    let schema = schema.ok_or_else(|| anyhow!("inventory is missing a schema block"))?;
    let inventory = Inventory { schema, objects };
    report_to_result_with_sources(validate(&inventory), &inventory.objects)?;
    Ok(inventory)
}

/// recursive loader with cycle-safe include handling.
fn load_recursive(
    path: &Path,
    visited: &mut BTreeSet<PathBuf>,
    objects: &mut Vec<alembic_core::Object>,
    schema: &mut Option<Schema>,
) -> Result<()> {
    let canonical =
        fs::canonicalize(path).with_context(|| format!("load inventory: {}", path.display()))?;
    if !visited.insert(canonical.clone()) {
        return Ok(());
    }

    let content = fs::read_to_string(&canonical)
        .with_context(|| format!("read inventory: {}", canonical.display()))?;
    let inventory: InventoryFile = if canonical.extension().and_then(|s| s.to_str()) == Some("json")
    {
        serde_json::from_str(&content)
            .with_context(|| format!("parse json: {}", canonical.display()))?
    } else {
        serde_yaml::from_str(&content)
            .with_context(|| format!("parse yaml: {}", canonical.display()))?
    };

    let base = canonical
        .parent()
        .ok_or_else(|| anyhow!("missing parent dir for {}", canonical.display()))?;

    let mut includes = inventory.include;
    includes.extend(inventory.imports);

    for entry in includes {
        let include_path = base.join(entry);
        load_recursive(&include_path, visited, objects, schema)?;
    }

    merge_schema(schema, inventory.schema)?;

    // set source location on each object from this file, with line numbers.
    // the uid->line index is built once per file (one pass), not rescanned per
    // object, so loading is linear in file size rather than objects x lines.
    let uid_lines = index_uid_lines(&content);
    for object in inventory.objects {
        let source = match uid_lines.get(object.uid.to_string().as_str()) {
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
fn index_uid_lines(content: &str) -> HashMap<&str, usize> {
    let mut index = HashMap::new();
    for (idx, line) in content.lines().enumerate() {
        if let Some(uid) = uid_key_value(line) {
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

fn merge_schema(current: &mut Option<Schema>, incoming: Option<Schema>) -> Result<()> {
    let Some(incoming) = incoming else {
        return Ok(());
    };
    match current {
        Some(existing) => {
            for (name, schema) in incoming.types {
                if existing.types.contains_key(&name) {
                    return Err(anyhow!("duplicate schema type {name}"));
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
    use super::{index_uid_lines, merge_schema};
    use alembic_core::{Schema, TypeSchema};
    use std::collections::BTreeMap;

    #[test]
    fn locates_uid_definition_not_an_earlier_reference() {
        let content = r#"objects:
  - uid: "dev-1"
    attrs:
      site: "site-1"
  - uid: "site-1"
"#;
        let index = index_uid_lines(content);
        assert_eq!(index.get("site-1").copied(), Some(5));
        assert_eq!(index.get("dev-1").copied(), Some(2));
    }

    #[test]
    fn locates_uid_key_in_json() {
        let content = r#"{
  "objects": [
    {
      "uid": "dev-1",
      "attrs": { "site": "site-1" }
    },
    {
      "uid": "site-1"
    }
  ]
}
"#;
        let index = index_uid_lines(content);
        assert_eq!(index.get("site-1").copied(), Some(8));
    }

    #[test]
    fn distinguishes_a_uid_that_is_a_prefix_of_another() {
        // "dev-1" resolves to its own line, not the earlier "dev-10" line that
        // contains it as a substring (the pre-index scan matched on `contains`).
        let content = r#"objects:
  - uid: "dev-10"
  - uid: "dev-1"
"#;
        let index = index_uid_lines(content);
        assert_eq!(index.get("dev-1").copied(), Some(3));
        assert_eq!(index.get("dev-10").copied(), Some(2));
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
