//! drives the real `alembic` binary over an external adapter that misspells a
//! key on the wire. needs a subprocess: the sdk serializes a typed answer, so
//! the misspelling only exists in hand-written bytes.

use std::process::Command;
use tempfile::tempdir;

mod support;

use support::{bin_path, example_binary};

const PLAN: &str =
    r#"{"schema":{"types":{}},"ops":[],"summary":{"create":0,"update":0,"delete":0}}"#;

/// apply an empty plan over the misspelling adapter, `env` picking its spellings.
fn apply(env: &[(&str, &str)]) -> (bool, String, String) {
    let dir = tempdir().expect("create temp dir");
    let plan = dir.path().join("plan.json");
    std::fs::write(&plan, PLAN).expect("write plan");
    let config = dir.path().join("backend.yaml");
    std::fs::write(
        &config,
        format!(
            "backend: external\ncommand: \"{}\"\ntimeout_seconds: 5\n",
            example_binary("misspelled_key_adapter").display()
        ),
    )
    .expect("write backend config");

    let mut cmd = Command::new(bin_path());
    cmd.env("ALEMBIC_STATE_PATH", dir.path().join("state.json"));
    for (key, value) in env {
        cmd.env(key, value);
    }
    cmd.arg("apply")
        .arg("--plan")
        .arg(&plan)
        .arg("--backend-config")
        .arg(&config);
    let output = cmd.output().expect("run alembic apply");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

#[test]
fn a_correctly_spelled_schema_delete_is_refused() {
    // the control: the same adapter one letter apart from both tests below, so a
    // refusal there is the misspelling and not the harness.
    let (ok, stdout, stderr) = apply(&[]);
    assert!(!ok, "the delete gate passed the apply; stdout:\n{stdout}");
    assert!(
        stderr.contains("provisioning would delete schema"),
        "{stderr}"
    );
}

#[test]
fn a_misspelled_provision_category_fails_the_apply() {
    let (ok, stdout, stderr) = apply(&[("ADAPTER_DELETED_KEY", "deleted_obejct_types")]);
    assert!(!ok, "a misspelled category was applied; stdout:\n{stdout}");
    assert!(stderr.contains("deleted_obejct_types"), "{stderr}");
}

#[test]
fn a_misspelled_result_key_fails_the_apply() {
    // an absent result carries a meaning of its own, so a typo'd `result` has to
    // be an error rather than that meaning.
    let (ok, stdout, stderr) = apply(&[("ADAPTER_RESULT_KEY", "reslt")]);
    assert!(
        !ok,
        "a misspelled result key was applied; stdout:\n{stdout}"
    );
    assert!(stderr.contains("reslt"), "{stderr}");
    assert!(!stdout.contains("deleted dcim.site"), "{stdout}");
}
