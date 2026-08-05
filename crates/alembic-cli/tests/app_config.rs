//! drives the real `alembic` binary to prove `alembic.yaml` rejects a typo'd key
//! without swallowing the `ALEMBIC_*` variables it shares its prefix with. needs a
//! subprocess because the config comes from the process environment and the cwd.

use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::tempdir;

fn fixture_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("fixtures")
        .join(name)
}

fn inventory(dir: &Path) -> PathBuf {
    let path = dir.join("inventory.yaml");
    std::fs::write(&path, "schema:\n  types: {}\nobjects: []\n").expect("write inventory");
    path
}

/// runs in `dir`, so an `alembic.yaml` written there is the one that gets loaded.
fn run_validate(dir: &Path, envs: &[(&str, &str)]) -> (bool, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_alembic"));
    cmd.current_dir(dir).arg("validate").arg("-f");
    cmd.arg(inventory(dir));
    for (key, value) in envs {
        cmd.env(key, value);
    }
    let output = cmd.output().expect("run alembic validate");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

#[test]
fn an_unknown_key_in_the_config_file_is_rejected_by_name() {
    let dir = tempdir().expect("create temp dir");
    std::fs::write(
        dir.path().join("alembic.yaml"),
        "plugin_dir: \"./elsewhere\"\n",
    )
    .expect("write config");

    let (ok, stderr) = run_validate(dir.path(), &[]);

    assert!(!ok, "expected a typo'd key to fail; stderr:\n{stderr}");
    assert!(
        stderr.contains("plugin_dir"),
        "expected the error to name the unknown key; stderr:\n{stderr}"
    );
}

#[test]
fn the_state_variables_survive_a_config_file() {
    let dir = tempdir().expect("create temp dir");
    std::fs::write(
        dir.path().join("alembic.yaml"),
        "plugins_dir: \"./plugins\"\n",
    )
    .expect("write config");
    let state = dir.path().join("state.json");

    let (ok, stderr) = run_validate(
        dir.path(),
        &[
            ("ALEMBIC_STATE_BACKEND", "local"),
            ("ALEMBIC_STATE_PATH", state.to_str().expect("utf-8 path")),
        ],
    );

    assert!(
        ok,
        "expected ALEMBIC_STATE_* to stay out of the config file's key space; stderr:\n{stderr}"
    );
}

#[test]
fn the_plugins_dir_variable_still_reaches_the_config() {
    let dir = tempdir().expect("create temp dir");
    let plugins = dir.path().join("elsewhere");

    // `apply` logs the plugin dir it looked in; the read-only backend stops the
    // run before it touches anything.
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_alembic"));
    cmd.current_dir(dir.path())
        .env("RUST_LOG", "debug")
        .env("ALEMBIC_PLUGINS_DIR", &plugins)
        .arg("apply")
        .arg("--backend")
        .arg("peeringdb")
        .arg("--plan")
        .arg(fixture_path("minimal_plan.json"));
    let output = cmd.output().expect("run alembic apply");
    // tracing writes to stdout; the read-only refusal goes to stderr.
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        stdout.contains(&format!("plugin dir '{}' not found", plugins.display())),
        "expected ALEMBIC_PLUGINS_DIR to set plugins_dir; stdout:\n{stdout}"
    );
}
