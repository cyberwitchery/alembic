//! drives the real `alembic` binary to prove `apply` against a read-only backend
//! fails fast without prompting. needs a subprocess because `confirm` writes the
//! prompt straight to process stdout, which libtest's capture can't intercept.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use tempfile::tempdir;

fn fixture_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("fixtures")
        .join(name)
}

/// run `apply --backend peeringdb [--interactive]` over a create-only plan,
/// returning (success, stdout, stderr).
fn run_apply_peeringdb(interactive: bool) -> (bool, String, String) {
    let out = tempdir().expect("create temp dir");
    let plan = fixture_path("minimal_plan.json");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_alembic"));
    cmd.env("ALEMBIC_STATE_PATH", out.path().join("state.json"));
    cmd.arg("apply").arg("--backend").arg("peeringdb");
    if interactive {
        cmd.arg("--interactive");
    }
    cmd.arg("--plan").arg(&plan);
    // null stdin: if the prompt loop ever runs, `confirm` reads EOF and declines
    // instead of hanging the test.
    cmd.stdin(Stdio::null());
    let output = cmd.output().expect("run alembic apply");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

#[test]
fn apply_read_only_interactive_prints_no_prompt() {
    let (ok, stdout, stderr) = run_apply_peeringdb(true);
    assert!(
        !ok,
        "apply to a read-only backend must fail; stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("backend is read-only; it cannot apply changes"),
        "expected the read-only capability error; stderr:\n{stderr}"
    );
    // the bug: interactive apply used to prompt for every op before failing.
    assert!(
        !stdout.contains("[y/N]"),
        "interactive apply must not prompt before failing fast; stdout:\n{stdout}"
    );
}

#[test]
fn apply_read_only_non_interactive_errors() {
    let (ok, stdout, stderr) = run_apply_peeringdb(false);
    assert!(
        !ok,
        "apply to a read-only backend must fail; stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("backend is read-only; it cannot apply changes"),
        "expected the read-only capability error; stderr:\n{stderr}"
    );
}
