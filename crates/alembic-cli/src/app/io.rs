use alembic_engine::{ApplyReport, DriftReport, Plan};
use anyhow::{anyhow, Context, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) fn ensure_parent_dir(path: &Path) -> Result<()> {
    match path.parent() {
        // a bare filename has an empty parent; create_dir_all("") is not portable
        Some(parent) if !parent.as_os_str().is_empty() => fs::create_dir_all(parent)
            .with_context(|| format!("create output directory: {}", parent.display())),
        _ => Ok(()),
    }
}

/// check that `path` can really be written, so a bad `-o` fails before the run
/// pays for a backend observation and only trips at the write.
///
/// `ensure_parent_dir` creates the parent and nothing else, which leaves the
/// three ordinary bad values: a path that is itself a directory, a parent that
/// exists but rejects writes, and an existing file that rejects writes. the last
/// is probed by opening the target itself, the other two by writing a real file
/// beside it, which also answers what mode bits cannot (a read-only mount, an
/// acl).
///
/// side-effect-free: the target probe never truncates, and the sibling probe
/// removes itself and every directory it had to create, deepest first, so a run
/// that dies later leaves the filesystem as it found it. the write path still
/// calls `ensure_parent_dir` to recreate them.
pub(super) fn preflight_output_path(path: &Path) -> Result<()> {
    if path.is_dir() {
        return Err(anyhow!("write output: {}: is a directory", path.display()));
    }
    if let Some(result) = probe_existing_target(path) {
        return result;
    }
    let created = missing_ancestors(path);
    let result = ensure_parent_dir(path).and_then(|()| probe_writable(path));
    for dir in &created {
        // best effort: a directory something else populated meanwhile is not
        // ours to take away, and remove_dir refuses a non-empty one anyway.
        let _ = fs::remove_dir(dir);
    }
    result
}

/// the directories `ensure_parent_dir` would have to create for `path`, deepest
/// first, which is the order they have to be removed in.
fn missing_ancestors(path: &Path) -> Vec<PathBuf> {
    let mut missing = Vec::new();
    let mut current = path.parent();
    while let Some(dir) = current {
        if dir.as_os_str().is_empty() || dir.exists() {
            break;
        }
        missing.push(dir.to_path_buf());
        current = dir.parent();
    }
    missing
}

/// an existing regular target answers for itself, which the sibling probe cannot:
/// a file that denies writes sits under a parent that accepts them. `None` for
/// anything else, since opening a fifo for write blocks until a reader attaches.
fn probe_existing_target(path: &Path) -> Option<Result<()>> {
    if !path.is_file() {
        return None;
    }
    // write(true) alone, no truncate and no create: the open asks whether the
    // write will be allowed, and the contents survive it either way.
    match fs::OpenOptions::new().write(true).open(path) {
        Ok(_) => Some(Ok(())),
        // gone between the two calls; the sibling probe answers instead
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(err) => Some(Err(err).with_context(|| format!("write output: {}", path.display()))),
    }
}

fn probe_writable(path: &Path) -> Result<()> {
    let probe = probe_path(path);
    let result =
        fs::write(&probe, b"").with_context(|| format!("write output: {}", path.display()));
    let _ = fs::remove_file(&probe);
    result
}

/// a probe name that cannot collide with the target or with a concurrent run.
fn probe_path(path: &Path) -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let name = format!(
        ".alembic-write-probe-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    );
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join(name),
        _ => PathBuf::from(name),
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
