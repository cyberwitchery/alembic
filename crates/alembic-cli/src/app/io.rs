use alembic_engine::Plan;
use anyhow::{Context, Result};
use std::fs;
use std::path::Path;

pub(super) fn format_validation_errors(
    report: alembic_core::ValidationReport,
    objects: &[alembic_core::Object],
) -> Vec<String> {
    report
        .with_sources(objects)
        .into_iter()
        .map(|error| format!("error: {error}"))
        .collect()
}

pub(super) fn write_plan(path: &Path, plan: &Plan) -> Result<()> {
    let raw = serde_json::to_string_pretty(plan)?;
    fs::write(path, raw).with_context(|| format!("write plan: {}", path.display()))
}

pub(super) fn write_inventory(path: &Path, inventory: &alembic_core::Inventory) -> Result<()> {
    let raw = serde_json::to_string_pretty(inventory)?;
    fs::write(path, raw).with_context(|| format!("write ir: {}", path.display()))
}

pub(super) fn read_plan(path: &Path) -> Result<Plan> {
    let raw = fs::read_to_string(path).with_context(|| format!("read plan: {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("parse plan: {}", path.display()))
}

/// load a canonical inventory (ir) file, merging includes and validating it.
pub(super) fn load_inventory(path: &Path) -> Result<alembic_core::Inventory> {
    alembic_engine::load_inventory(path)
}
