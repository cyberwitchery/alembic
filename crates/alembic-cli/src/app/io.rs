use alembic_engine::{ApplyReport, DriftReport, Plan};
use anyhow::{Context, Result};
use std::fs;
use std::path::Path;

pub(super) fn ensure_parent_dir(path: &Path) -> Result<()> {
    match path.parent() {
        // a bare filename has an empty parent; create_dir_all("") is not portable
        Some(parent) if !parent.as_os_str().is_empty() => fs::create_dir_all(parent)
            .with_context(|| format!("create output directory: {}", parent.display())),
        _ => Ok(()),
    }
}

pub(super) fn write_plan(path: &Path, plan: &Plan) -> Result<()> {
    ensure_parent_dir(path)?;
    let raw = serde_json::to_string_pretty(plan)?;
    fs::write(path, raw).with_context(|| format!("write plan: {}", path.display()))
}

pub(super) fn write_apply_report(path: &Path, report: &ApplyReport) -> Result<()> {
    ensure_parent_dir(path)?;
    let raw = serde_json::to_string_pretty(report)?;
    fs::write(path, raw).with_context(|| format!("write apply report: {}", path.display()))
}

pub(super) fn write_drift_report(path: &Path, report: &DriftReport) -> Result<()> {
    ensure_parent_dir(path)?;
    let raw = serde_json::to_string_pretty(report)?;
    fs::write(path, raw).with_context(|| format!("write drift report: {}", path.display()))
}

pub(super) fn write_validation_report(
    path: &Path,
    report: &alembic_core::LocatedReport,
) -> Result<()> {
    ensure_parent_dir(path)?;
    let raw = serde_json::to_string_pretty(report)?;
    fs::write(path, raw).with_context(|| format!("write validation report: {}", path.display()))
}

pub(super) fn write_inventory(path: &Path, inventory: &alembic_core::Inventory) -> Result<()> {
    ensure_parent_dir(path)?;
    let raw = serde_json::to_string_pretty(inventory)?;
    fs::write(path, raw).with_context(|| format!("write ir: {}", path.display()))
}

pub(super) fn read_plan(path: &Path) -> Result<Plan> {
    let raw = fs::read_to_string(path).with_context(|| format!("read plan: {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("parse plan: {}", path.display()))
}

/// when an always-JSON output path carries a `.yaml`/`.yml` extension, return a
/// warning that the file is written as JSON despite its name; `None` otherwise
/// (any other extension, or none).
///
/// pure: it returns the message instead of printing, so callers choose the sink
/// (stderr) and it stays unit-testable. this is a gentle nudge, never an error:
/// the file is still written and existing workflows keep working.
pub(super) fn warn_misleading_output_extension(path: &Path) -> Option<String> {
    let ext = path.extension().and_then(|s| s.to_str())?;
    if ext.eq_ignore_ascii_case("yaml") || ext.eq_ignore_ascii_case("yml") {
        Some(format!(
            "warning: --output `{}` is written as JSON despite the .{} extension",
            path.display(),
            ext
        ))
    } else {
        None
    }
}
