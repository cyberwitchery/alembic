//! inventory file loading with include/import support.

use crate::{report_to_result_with_sources, validate};
use alembic_core::{Inventory, Schema, SourceLocation};
use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::collections::BTreeSet;
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

    // set source location on each object from this file, with line numbers
    for object in inventory.objects {
        let line = find_uid_line(&content, &object.uid.to_string());
        let source = match line {
            Some(n) => SourceLocation::file_line(&canonical, n),
            None => SourceLocation::file(&canonical),
        };
        objects.push(object.with_source(source));
    }

    Ok(())
}

/// the 1-indexed line where `uid` is defined (its own `uid:` key), not a line
/// that merely references it as an attr value.
fn find_uid_line(content: &str, uid: &str) -> Option<usize> {
    content
        .lines()
        .position(|line| line.contains(uid) && is_uid_key_line(line))
        .map(|idx| idx + 1)
}

/// whether `line`'s key is `uid` (yaml `uid:` / `- uid:`, json `"uid":`).
fn is_uid_key_line(line: &str) -> bool {
    let rest = line.trim_start();
    let rest = rest.strip_prefix('-').map_or(rest, str::trim_start);
    let rest = rest.strip_prefix('"').unwrap_or(rest);
    rest.strip_prefix("uid")
        .is_some_and(|after| after.starts_with([':', '"', ' ']))
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
    use super::find_uid_line;

    #[test]
    fn locates_uid_definition_not_an_earlier_reference() {
        let content = r#"objects:
  - uid: "dev-1"
    attrs:
      site: "site-1"
  - uid: "site-1"
"#;
        assert_eq!(find_uid_line(content, "site-1"), Some(5));
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
        assert_eq!(find_uid_line(content, "site-1"), Some(8));
    }
}
