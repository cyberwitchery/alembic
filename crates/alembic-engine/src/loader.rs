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

    // Set source location on each object from this file, with line numbers
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

/// Find the line number (1-indexed) where a UID appears in the content.
fn find_uid_line(content: &str, uid: &str) -> Option<usize> {
    for (idx, line) in content.lines().enumerate() {
        if line.contains(uid) {
            return Some(idx + 1); // 1-indexed line numbers
        }
    }
    None
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
