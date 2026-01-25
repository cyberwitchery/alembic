//! brew file loading with include/import support.

use crate::validate;
use alembic_core::{Inventory, Schema};
use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

/// raw on-disk representation for a brew file.
#[derive(Debug, Deserialize)]
struct BrewFile {
    #[serde(default)]
    include: Vec<String>,
    #[serde(default)]
    imports: Vec<String>,
    #[serde(default)]
    schema: Option<Schema>,
    #[serde(default)]
    objects: Vec<alembic_core::Object>,
}

/// load a brew file (yaml or json) and merge any includes.
pub fn load_brew(path: impl AsRef<Path>) -> Result<Inventory> {
    let mut visited = BTreeSet::new();
    let mut objects = Vec::new();
    let mut schema: Option<Schema> = None;
    let path = path.as_ref();
    load_recursive(path, &mut visited, &mut objects, &mut schema)?;
    let schema = schema.ok_or_else(|| anyhow!("brew is missing a schema block"))?;
    let inventory = Inventory { schema, objects };
    crate::report_to_result(validate(&inventory))?;
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
        fs::canonicalize(path).with_context(|| format!("load brew: {}", path.display()))?;
    if !visited.insert(canonical.clone()) {
        return Ok(());
    }

    let content = fs::read_to_string(&canonical)
        .with_context(|| format!("read brew: {}", canonical.display()))?;
    let brew: BrewFile = if canonical.extension().and_then(|s| s.to_str()) == Some("json") {
        serde_json::from_str(&content)
            .with_context(|| format!("parse json: {}", canonical.display()))?
    } else {
        serde_yaml::from_str(&content)
            .with_context(|| format!("parse yaml: {}", canonical.display()))?
    };

    let base = canonical
        .parent()
        .ok_or_else(|| anyhow!("missing parent dir for {}", canonical.display()))?;

    let mut includes = brew.include;
    includes.extend(brew.imports);

    for entry in includes {
        let include_path = base.join(entry);
        load_recursive(&include_path, visited, objects, schema)?;
    }

    merge_schema(schema, brew.schema)?;
    objects.extend(brew.objects);
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
