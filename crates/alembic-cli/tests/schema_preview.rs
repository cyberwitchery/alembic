//! drives the real `alembic` binary to prove `plan` reports a failing schema
//! preview as a failure, not as a capability gap. needs a subprocess because the
//! report goes to stderr via `eprintln!`, which libtest's capture can't intercept.

use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::tempdir;

/// llvm-cov runs `--tests` in its own target dir, which never gets examples.
fn example_binary(name: &str) -> PathBuf {
    let mut path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .find(|p| p.join("target").exists())
        .expect("workspace target dir")
        .join("target");
    if std::env::var("CI").is_ok() {
        path.push("ci");
    }
    path.push("debug");
    path.push("examples");
    path.push(name);
    path
}

#[test]
fn plan_reports_a_preview_error_as_a_failure() {
    let dir = tempdir().expect("create temp dir");
    let inventory = dir.path().join("inventory.yaml");
    std::fs::write(&inventory, "schema:\n  types: {}\nobjects: []\n").expect("write inventory");
    let config = dir.path().join("backend.yaml");
    std::fs::write(
        &config,
        format!(
            "backend: external\ncommand: \"{}\"\ntimeout-seconds: 5\n",
            example_binary("preview_error_adapter").display()
        ),
    )
    .expect("write backend config");

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_alembic"));
    cmd.env("ALEMBIC_STATE_PATH", dir.path().join("state.json"));
    cmd.arg("plan")
        .arg("-f")
        .arg(&inventory)
        .arg("-o")
        .arg(dir.path().join("plan.json"))
        .arg("--backend")
        .arg("external")
        .arg("--backend-config")
        .arg(&config);
    let output = cmd.output().expect("run alembic plan");
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        stderr.contains("schema preview failed:") && stderr.contains("preview failed for test"),
        "expected the preview error reported as a failure; stderr:\n{stderr}"
    );
    // the bug: an Err used the phrase the docs reserve for Ok(None).
    assert!(
        !stderr.contains("unavailable for this backend"),
        "a preview error must not be reported as a capability gap; stderr:\n{stderr}"
    );
}
