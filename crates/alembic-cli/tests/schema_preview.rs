//! drives the real `alembic` binary over `plan`'s schema preview: a failing
//! preview reads as a failure and a backend that provisions nothing as nothing
//! to provision, neither as a capability gap. a preview it cannot give is named
//! on the provisioning path too, where it means the --allow-delete gate did not
//! run. needs a subprocess because the report goes to stderr via `eprintln!`,
//! which libtest's capture can't intercept.

use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::tempdir;

mod support;

use support::{bin_path, example_binary};

fn external_backend(dir: &Path, adapter: &str) -> PathBuf {
    let config = dir.join("backend.yaml");
    std::fs::write(
        &config,
        format!(
            "backend: external\ncommand: \"{}\"\ntimeout_seconds: 5\n",
            example_binary(adapter).display()
        ),
    )
    .expect("write backend config");
    config
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

fn plan_provision(adapter: &str, allow_delete: bool) -> (bool, String, String) {
    let dir = tempdir().expect("create temp dir");
    let inventory = dir.path().join("inventory.yaml");
    std::fs::write(&inventory, "schema:\n  types: {}\nobjects: []\n").expect("write inventory");
    let config = external_backend(dir.path(), adapter);

    let mut cmd = Command::new(bin_path());
    cmd.env("ALEMBIC_STATE_PATH", dir.path().join("state.json"));
    cmd.args(["plan", "--backend", "external", "--provision"])
        .arg("-f")
        .arg(&inventory)
        .arg("-o")
        .arg(dir.path().join("plan.json"))
        .arg("--backend-config")
        .arg(&config);
    if allow_delete {
        cmd.arg("--allow-delete");
    }
    let output = cmd.output().expect("run alembic plan");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// the sdk's unsafe pairing: an adapter that overrides `ensure_schema` to delete
/// and inherits the `None` preview. the gate has nothing to gate on, so the run
/// names the skip before provisioning rather than reporting the delete after it.
#[test]
fn plan_provision_names_the_gate_it_could_not_run() {
    let (ok, stdout, stderr) = plan_provision("unpreviewable_emitter_adapter", false);

    assert!(ok, "plan failed; stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stderr.contains("schema preview: unavailable for this backend")
            && stderr.contains("not gated by --allow-delete"),
        "expected the skipped gate named; stderr:\n{stderr}"
    );
    // saying so is the whole change: the provisioning still happens.
    assert!(
        stdout.contains("  deleted dcim.fossil"),
        "stdout:\n{stdout}"
    );
}

#[test]
fn plan_provision_says_nothing_when_the_gate_ran_or_was_waived() {
    // the adapter previews, so the gate ran on what it reported.
    let (ok, stdout, stderr) = plan_provision("provisioning_emitter_adapter", false);
    assert!(ok, "plan failed; stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        !stderr.contains("unavailable for this backend"),
        "stderr:\n{stderr}"
    );

    // --allow-delete waives the gate by design, so a skipped gate is not news.
    let (ok, stdout, stderr) = plan_provision("unpreviewable_emitter_adapter", true);
    assert!(ok, "plan failed; stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        !stderr.contains("unavailable for this backend"),
        "stderr:\n{stderr}"
    );
}
