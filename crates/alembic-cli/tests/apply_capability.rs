//! drives the real `alembic` binary to prove `apply` refuses before prompting
//! against a read-only backend, and that `--interactive` reads its answers from
//! stdin. needs a subprocess because `confirm` prompts on process stdout and
//! reads process stdin, neither of which libtest can stand in for.

use std::io::Write;
use std::process::{Command, Stdio};
use tempfile::tempdir;

mod support;

use support::{bin_path, example_binary, fixture_path};

/// run `apply --backend peeringdb [--interactive]` over a create-only plan,
/// returning (success, stdout, stderr).
fn run_apply_peeringdb(interactive: bool) -> (bool, String, String) {
    let out = tempdir().expect("create temp dir");
    let plan = fixture_path("minimal_plan.json");
    let mut cmd = Command::new(bin_path());
    cmd.env("ALEMBIC_STATE_PATH", out.path().join("state.json"));
    cmd.arg("apply").arg("--backend").arg("peeringdb");
    if interactive {
        cmd.arg("--interactive");
    }
    cmd.arg("--plan").arg(&plan);
    // null stdin: if the prompt loop ever runs, `confirm` reads EOF and errors
    // instead of hanging the test.
    cmd.stdin(Stdio::null());
    let output = cmd.output().expect("run alembic apply");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// run `apply --interactive` over the five-create fixture against the example
/// external adapter, which reports every op it receives as applied. `answers`
/// is fed on stdin and then closed; `None` supplies no stdin at all.
fn run_apply_interactive(answers: Option<&str>) -> (bool, String, String) {
    let out = tempdir().expect("create temp dir");
    let config = out.path().join("backend.yaml");
    std::fs::write(
        &config,
        format!(
            "backend: external\ncommand: \"{}\"\ntimeout_seconds: 5\n",
            example_binary("applied_ops_adapter").display()
        ),
    )
    .expect("write backend config");
    let mut cmd = Command::new(bin_path());
    cmd.env("ALEMBIC_STATE_PATH", out.path().join("state.json"));
    cmd.arg("apply")
        .arg("--backend")
        .arg("external")
        .arg("--backend-config")
        .arg(&config)
        .arg("--interactive")
        .arg("--plan")
        .arg(fixture_path("minimal_plan.json"));
    cmd.stdin(match answers {
        Some(_) => Stdio::piped(),
        None => Stdio::null(),
    });
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().expect("run alembic apply");
    if let Some(answers) = answers {
        child
            .stdin
            .take()
            .expect("child stdin")
            .write_all(answers.as_bytes())
            .expect("write answers");
    }
    let output = child.wait_with_output().expect("wait for alembic apply");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// the prompts a run printed, in order, without their `? [y/N] ` suffix.
fn prompts(stdout: &str) -> Vec<&str> {
    let mut parts: Vec<&str> = stdout.split("? [y/N] ").collect();
    // whatever trails the last prompt is the report, not a prompt
    parts.pop();
    parts
}

/// a redirected stdin is what ci, `nohup` and any wrapper script supply. before
/// this fix eof answered every prompt with a decline nobody gave: the run
/// provisioned schema anyway and exited 0 with `applied 0 operations`.
#[test]
fn apply_interactive_without_an_answer_refuses() {
    let (ok, stdout, stderr) = run_apply_interactive(None);
    assert!(
        !ok,
        "interactive apply with no answers must fail; stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    let prompted = prompts(&stdout);
    assert_eq!(
        prompted.len(),
        1,
        "the run must stop at the first unanswered prompt; stdout:\n{stdout}"
    );
    assert!(
        stderr.contains(&format!(
            "stdin ended before `{}` was answered",
            prompted[0]
        )),
        "the error must name the op it could not get an answer for; stderr:\n{stderr}"
    );
    assert!(
        !stdout.contains("applied"),
        "a refused run must not report an apply; stdout:\n{stdout}"
    );
}

/// scripted approval: answers by pipe select ops the same way a terminal does.
#[test]
fn apply_interactive_applies_the_approved_ops() {
    let (ok, stdout, stderr) = run_apply_interactive(Some("y\nn\ny\nn\ny\n"));
    assert!(
        ok,
        "scripted answers must apply; stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert_eq!(
        prompts(&stdout).len(),
        5,
        "every op in the plan must be prompted for; stdout:\n{stdout}"
    );
    // the report counts what reached the backend, so three is the selection
    // having taken effect rather than the whole plan or none of it.
    assert!(
        stdout.contains("applied 3 operations"),
        "expected only the approved ops applied; stdout:\n{stdout}"
    );
}

/// answers that run out mid-plan are the same bug one step in: the rest must not
/// be declined on the operator's behalf.
#[test]
fn apply_interactive_with_too_few_answers_refuses() {
    let (ok, stdout, stderr) = run_apply_interactive(Some("y\nn\n"));
    assert!(
        !ok,
        "answers running out must fail; stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    let prompted = prompts(&stdout);
    assert_eq!(
        prompted.len(),
        3,
        "the run must stop at the first unanswered prompt; stdout:\n{stdout}"
    );
    assert!(
        stderr.contains(&format!(
            "stdin ended before `{}` was answered",
            prompted[2]
        )),
        "the error must name the op it could not get an answer for; stderr:\n{stderr}"
    );
    // the two answered ops must not be applied on their own either.
    assert!(
        !stdout.contains("applied"),
        "a refused run must not report an apply; stdout:\n{stdout}"
    );
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
    // interactive apply must not prompt before the capability check fails.
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
