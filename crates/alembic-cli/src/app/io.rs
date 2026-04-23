use super::diag::warn;
use alembic_adapter_containerlab::Topology;
use alembic_engine::{compile_retort, is_brew_format, load_brew, load_raw_yaml, load_retort, Plan};
use anyhow::{anyhow, Context, Result};
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

pub(super) fn write_topology(path: &Path, topology: &Topology) -> Result<()> {
    let raw = serde_yaml::to_string(topology)?;
    fs::write(path, raw).with_context(|| format!("write topology: {}", path.display()))
}

pub(super) fn read_plan(path: &Path) -> Result<Plan> {
    let raw = fs::read_to_string(path).with_context(|| format!("read plan: {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("parse plan: {}", path.display()))
}

pub(super) fn load_inventory(
    path: &Path,
    retort: Option<&Path>,
) -> Result<alembic_core::Inventory> {
    let raw = load_raw_yaml(path)?;
    if is_brew_format(&raw) {
        if retort.is_some() {
            warn("io", "retort ignored for brew input");
        }
        return load_brew(path);
    }

    let retort_path =
        retort.ok_or_else(|| anyhow!("raw yaml requires --retort to compile inventory"))?;
    let retort = load_retort(retort_path)?;
    let mut inventory = compile_retort(&raw, &retort)?;
    let source = alembic_core::SourceLocation::file(path);
    inventory.objects = inventory
        .objects
        .into_iter()
        .map(|object| {
            if object.source.is_some() {
                object
            } else {
                object.with_source(source.clone())
            }
        })
        .collect();
    Ok(inventory)
}

pub(super) fn load_brew_only(path: &Path) -> Result<alembic_core::Inventory> {
    let raw = load_raw_yaml(path)?;
    if !is_brew_format(&raw) {
        return Err(anyhow!(
            "cast requires a brew/ir yaml file with objects; run distill first"
        ));
    }
    load_brew(path)
}
