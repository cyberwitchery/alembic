//! drives the real `alembic` binary over `plan`'s schema preview: a failing
//! preview reads as a failure and a backend that provisions nothing as nothing
//! to provision, neither as a capability gap. needs a subprocess because the
//! report goes to stderr via `eprintln!`, which libtest's capture can't intercept.

use std::process::Command;
use tempfile::tempdir;

mod support;

use support::{bin_path, example_binary};

#[test]
fn plan_reports_a_preview_error_as_a_failure() {
    let dir = tempdir().expect("create temp dir");
    let inventory = dir.path().join("inventory.yaml");
    std::fs::write(&inventory, "schema:\n  types: {}\nobjects: []\n").expect("write inventory");
    let config = dir.path().join("backend.yaml");
    std::fs::write(
        &config,
        format!(
            "backend: external\ncommand: \"{}\"\ntimeout_seconds: 5\n",
            example_binary("preview_error_adapter").display()
        ),
    )
    .expect("write backend config");

    let mut cmd = Command::new(bin_path());
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
    // an Err must not reuse the phrase the docs reserve for Ok(None).
    assert!(
        !stderr.contains("unavailable for this backend"),
        "a preview error must not be reported as a capability gap; stderr:\n{stderr}"
    );
}

/// django is the built-in write-only backend the plain `plan` reaches. it
/// provisions nothing, which is an empty report, not a preview it cannot give.
#[test]
fn plan_over_a_backend_that_provisions_nothing_reports_no_capability_gap() {
    let dir = tempdir().expect("create temp dir");
    let inventory = dir.path().join("inventory.yaml");
    std::fs::write(&inventory, "schema:\n  types: {}\nobjects: []\n").expect("write inventory");
    let config = dir.path().join("backend.yaml");
    std::fs::write(
        &config,
        format!(
            "backend: django\noutput: \"{}\"\n",
            dir.path().join("out").display()
        ),
    )
    .expect("write backend config");

    let mut cmd = Command::new(bin_path());
    cmd.env("ALEMBIC_STATE_PATH", dir.path().join("state.json"));
    cmd.arg("plan")
        .arg("-f")
        .arg(&inventory)
        .arg("--dry-run")
        .arg("--backend-config")
        .arg(&config);
    let output = cmd.output().expect("run alembic plan");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(output.status.success(), "plan failed; stderr:\n{stderr}");
    assert!(
        !stderr.contains("unavailable for this backend"),
        "nothing to provision must not be reported as a capability gap; stderr:\n{stderr}"
    );
    // the machine-readable half of the same answer: an empty report, not a null.
    let plan: serde_json::Value = serde_json::from_str(&stdout).expect("plan json");
    assert_eq!(plan["schema_preview"], serde_json::json!({}), "{stdout}");
}
