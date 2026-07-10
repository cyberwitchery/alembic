//! end-to-end proof that `apply` against a read-only (observer) backend fails
//! fast, without first prompting for every op under `--interactive`.
//!
//! driving the real binary is the only faithful way to observe the prompt:
//! `confirm` writes `create ...? [y/N]` straight to the process stdout, which
//! libtest's in-process capture does not intercept. the in-process companion in
//! `src/app/tests.rs` (`run_apply_read_only_backend_fails_before_prompting`)
//! proves the same ordering via the error message instead.

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

/// run `alembic apply --backend peeringdb [--interactive] --plan <fixture>` with
/// no stdin, against an isolated temp state path, and return (success, stdout,
/// stderr). `minimal_plan.json` holds only create ops, so nothing else
/// short-circuits the interactive prompt loop.
fn run_apply_peeringdb(interactive: bool) -> (bool, String, String) {
    let out = tempdir().expect("create temp dir");
    let plan = fixture_path("minimal_plan.json");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_alembic"));
    // isolate state per run so nothing races on (or pollutes) the source tree.
    cmd.env("ALEMBIC_STATE_PATH", out.path().join("state.json"));
    cmd.arg("apply").arg("--backend").arg("peeringdb");
    if interactive {
        cmd.arg("--interactive");
    }
    cmd.arg("--plan").arg(&plan);
    // a null stdin means that if the (buggy) prompt loop ever runs, `confirm`
    // reads EOF and declines rather than hanging the test forever.
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
    // the bug this guards: before failing fast, interactive apply prompted
    // `create ...? [y/N]` for every op, wasting the user's answers.
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
