//! drives the real `alembic` binary over `plan`'s schema preview: a failing
//! preview reads as a failure and a backend that provisions nothing as nothing
//! to provision, neither as a capability gap. on the provisioning path a preview
//! it cannot give refuses the run, since the --allow-delete gate cannot run.
//! needs a subprocess because the report goes to stderr via `eprintln!`, which
//! libtest's capture can't intercept.

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

/// an adapter that deletes schema and declares it cannot preview: the gate has
/// nothing to gate on, so the run is refused rather than provisioned blind, and
/// the message names both ways out.
#[test]
fn plan_provision_refuses_an_adapter_that_cannot_preview() {
    let (ok, stdout, stderr) = plan_provision("unpreviewable_emitter_adapter", false);

    assert!(!ok, "plan succeeded; stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stderr.contains("cannot preview schema")
            && stderr.contains("implement preview_schema")
            && stderr.contains("--allow-delete"),
        "stderr:\n{stderr}"
    );
    // refused before the write, so the delete it would have made never happened.
    assert!(!stdout.contains("deleted dcim.fossil"), "stdout:\n{stdout}");
}

/// an adapter overriding neither provisioning method, which is what every
/// emit-only adapter ships: it provisions nothing, previews that, and passes.
#[test]
fn plan_provision_passes_an_adapter_that_overrides_neither_method() {
    let (ok, stdout, stderr) = plan_provision("emitter_role_adapter", false);
    assert!(ok, "plan failed; stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        !stderr.contains("cannot preview schema"),
        "stderr:\n{stderr}"
    );
}

#[test]
fn plan_provision_still_refuses_a_previewed_delete_and_allow_delete_waives_both() {
    // the adapter previews a delete, so the gate runs on it and refuses as before.
    let (ok, stdout, stderr) = plan_provision("converging_emitter_adapter", false);
    assert!(!ok, "plan succeeded; stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stderr.contains("provisioning would delete schema"),
        "stderr:\n{stderr}"
    );

    // --allow-delete short-circuits the gate, so both refusals lift.
    for adapter in [
        "unpreviewable_emitter_adapter",
        "converging_emitter_adapter",
        "emitter_role_adapter",
    ] {
        let (ok, stdout, stderr) = plan_provision(adapter, true);
        assert!(ok, "{adapter} failed; stdout:\n{stdout}\nstderr:\n{stderr}");
    }
}
