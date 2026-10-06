//! drives the real binary through the stdin plan path added for #442.
//! this needs subprocesses because the plan producer's stdout is the apply
//! consumer's stdin.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use tempfile::tempdir;

mod support;

use support::{bin_path, example_binary, fixture_path};

fn backend_config(dir: &Path) -> PathBuf {
    let config = dir.join("backend.yaml");
    std::fs::write(
        &config,
        format!(
            r#"backend: external
command: "{}"
timeout_seconds: 5
"#,
            example_binary("applied_ops_adapter").display()
        ),
    )
    .expect("write backend config");
    config
}

#[test]
fn plan_dry_run_pipes_into_apply_stdin() {
    let dir = tempdir().expect("create temp dir");
    let config = backend_config(dir.path());

    let mut plan = Command::new(bin_path());
    plan.env("ALEMBIC_STATE_PATH", dir.path().join("plan-state.json"))
        .args(["plan", "--dry-run", "--backend", "external"])
        .arg("--backend-config")
        .arg(&config)
        .arg("-f")
        .arg(fixture_path("agent/base.yaml"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut plan = plan.spawn().expect("run alembic plan --dry-run");
    let plan_stdout = plan.stdout.take().expect("plan stdout pipe");

    let mut apply = Command::new(bin_path());
    apply
        .env("ALEMBIC_STATE_PATH", dir.path().join("apply-state.json"))
        .args(["apply", "--backend", "external"])
        .arg("--backend-config")
        .arg(&config)
        .args(["--plan", "-"])
        .stdin(Stdio::from(plan_stdout))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let apply_output = apply.output().expect("run alembic apply from stdin");
    let plan_output = plan.wait_with_output().expect("wait for alembic plan");

    assert!(
        plan_output.status.success(),
        "plan failed; stderr:\n{}",
        String::from_utf8_lossy(&plan_output.stderr)
    );
    let apply_stdout = String::from_utf8_lossy(&apply_output.stdout);
    let apply_stderr = String::from_utf8_lossy(&apply_output.stderr);
    assert!(
        apply_output.status.success(),
        "apply failed; stdout:\n{apply_stdout}\nstderr:\n{apply_stderr}"
    );
    assert!(
        apply_stdout.contains("applied 2 operations"),
        "stdin plan did not reach the backend; stdout:\n{apply_stdout}"
    );
}

#[test]
fn interactive_apply_rejects_a_stdin_plan_before_backend_setup() {
    let output = Command::new(bin_path())
        .args(["apply", "--plan", "-", "--interactive"])
        .stdin(Stdio::null())
        .output()
        .expect("run alembic apply");
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success(), "the conflicting inputs must fail");
    assert!(
        stderr.contains("--interactive cannot be used with --plan -"),
        "expected the direct stdin/interactive conflict; stderr:\n{stderr}"
    );
}
