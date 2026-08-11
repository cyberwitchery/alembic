//! drives the real `alembic` binary over the provisioning summary: the three
//! sites that print it name what a run wrote to schema it did not create, and the
//! `--allow-delete` gate names what it is refusing over. needs a subprocess
//! because the summary goes to process stdout/stderr, which libtest cannot
//! intercept from an in-process `run()`.

use std::process::Command;
use tempfile::tempdir;

mod support;

use support::{bin_path, example_binary, fixture_path};

/// write a backend config pointing at the example adapter that provisions a
/// create, an update, a deprecation and two deletes.
fn converging_backend(dir: &std::path::Path) -> std::path::PathBuf {
    let config = dir.join("backend.yaml");
    std::fs::write(
        &config,
        format!(
            "backend: external\ncommand: \"{}\"\ntimeout_seconds: 5\n",
            example_binary("converging_emitter_adapter").display()
        ),
    )
    .expect("write backend config");
    config
}

fn run(cmd: &mut Command) -> (bool, String, String) {
    let output = cmd.output().expect("run alembic");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

#[test]
fn apply_names_what_it_updated_deprecated_and_deleted() {
    let dir = tempdir().expect("create temp dir");
    let config = converging_backend(dir.path());

    let mut cmd = Command::new(bin_path());
    cmd.env("ALEMBIC_STATE_PATH", dir.path().join("state.json"));
    cmd.args(["apply", "--backend", "external", "--allow-delete"])
        .arg("--backend-config")
        .arg(&config)
        .arg("--plan")
        .arg(fixture_path("minimal_plan.json"));
    let (ok, stdout, stderr) = run(&mut cmd);

    assert!(ok, "apply failed; stdout:\n{stdout}\nstderr:\n{stderr}");
    for line in [
        "  updated dcim.gadget.color",
        "  deprecated dcim.relic",
        "  deleted dcim.fossil",
        "  deleted dcim.fossil.age",
    ] {
        assert!(
            stdout.contains(line),
            "expected `{line}`; stdout:\n{stdout}"
        );
    }
    // a create is new, so the count is the whole story: it stays unnamed.
    assert!(
        !stdout.contains("  created dcim.widget"),
        "stdout:\n{stdout}"
    );
    assert!(
        stdout.contains("1 object types created"),
        "stdout:\n{stdout}"
    );
}

#[test]
fn the_delete_gate_names_what_it_refuses_over() {
    let dir = tempdir().expect("create temp dir");
    let config = converging_backend(dir.path());

    let mut cmd = Command::new(bin_path());
    cmd.env("ALEMBIC_STATE_PATH", dir.path().join("state.json"));
    cmd.args(["apply", "--backend", "external"])
        .arg("--backend-config")
        .arg(&config)
        .arg("--plan")
        .arg(fixture_path("minimal_plan.json"));
    let (ok, stdout, stderr) = run(&mut cmd);

    assert!(!ok, "the gate must refuse; stdout:\n{stdout}");
    for line in [
        "re-run with --allow-delete",
        "- type dcim.fossil",
        "- field dcim.fossil.age",
    ] {
        assert!(
            stderr.contains(line),
            "expected `{line}`; stderr:\n{stderr}"
        );
    }
}

#[test]
fn plan_provision_names_what_it_provisioned() {
    let dir = tempdir().expect("create temp dir");
    let config = converging_backend(dir.path());
    let inventory = dir.path().join("inventory.yaml");
    std::fs::write(&inventory, "schema:\n  types: {}\nobjects: []\n").expect("write inventory");

    let mut cmd = Command::new(bin_path());
    cmd.env("ALEMBIC_STATE_PATH", dir.path().join("state.json"));
    cmd.args([
        "plan",
        "--backend",
        "external",
        "--provision",
        "--allow-delete",
    ])
    .arg("-f")
    .arg(&inventory)
    .arg("-o")
    .arg(dir.path().join("plan.json"))
    .arg("--backend-config")
    .arg(&config);
    let (ok, stdout, stderr) = run(&mut cmd);

    assert!(ok, "plan failed; stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stdout.contains("  deleted dcim.fossil.age"),
        "provisioning happened, so past tense; stdout:\n{stdout}"
    );
}

#[test]
fn plan_previews_what_provisioning_would_delete() {
    let dir = tempdir().expect("create temp dir");
    let config = converging_backend(dir.path());
    let inventory = dir.path().join("inventory.yaml");
    std::fs::write(&inventory, "schema:\n  types: {}\nobjects: []\n").expect("write inventory");

    let mut cmd = Command::new(bin_path());
    cmd.env("ALEMBIC_STATE_PATH", dir.path().join("state.json"));
    cmd.args(["plan", "--backend", "external"])
        .arg("-f")
        .arg(&inventory)
        .arg("-o")
        .arg(dir.path().join("plan.json"))
        .arg("--backend-config")
        .arg(&config);
    let (ok, stdout, stderr) = run(&mut cmd);

    assert!(ok, "plan failed; stdout:\n{stdout}\nstderr:\n{stderr}");
    // the read-only preview: nothing was written, and it is not the plan's stdout.
    for line in [
        "  would update dcim.gadget.color",
        "  would deprecate dcim.relic",
        "  would delete dcim.fossil",
    ] {
        assert!(
            stderr.contains(line),
            "expected `{line}`; stderr:\n{stderr}"
        );
    }
    assert!(!stdout.contains("would delete"), "stdout:\n{stdout}");
}
