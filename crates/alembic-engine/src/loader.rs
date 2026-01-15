//! brew file loading with include/import support.

use alembic_core::Inventory;
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
    objects: Vec<alembic_core::Object>,
}

/// load a brew file (yaml or json) and merge any includes.
pub fn load_brew(path: impl AsRef<Path>) -> Result<Inventory> {
    let mut visited = BTreeSet::new();
    let mut objects = Vec::new();
    let path = path.as_ref();
    load_recursive(path, &mut visited, &mut objects)?;
    Ok(Inventory { objects })
}

/// recursive loader with cycle-safe include handling.
fn load_recursive(
    path: &Path,
    visited: &mut BTreeSet<PathBuf>,
    objects: &mut Vec<alembic_core::Object>,
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
        load_recursive(&include_path, visited, objects)?;
    }

    objects.extend(brew.objects);
    Ok(())
}
