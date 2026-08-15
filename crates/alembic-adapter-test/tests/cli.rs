//! end-to-end tests that drive the built `alembic-adapter-test` binary, so the
//! cli surface an adapter author actually runs (argument parsing, the report, and
//! the 0/1/2 exit codes) is exercised, not just the library.

use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::tempdir;

const BIN: &str = env!("CARGO_BIN_EXE_alembic-adapter-test");

fn manifest(rel: &str) -> String {
    format!("{}/{}", env!("CARGO_MANIFEST_DIR"), rel)
}

/// cargo exports `CARGO_BIN_EXE_<name>` for bins but nothing for examples, so the
/// path comes from `CARGO_TARGET_DIR` instead (an absolute value replaces the root).
fn example_binary(name: &str) -> PathBuf {
    let target_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join(std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".to_string()));
    let path = target_dir.join("debug").join("examples").join(name);
    assert!(
        path.exists(),
        "example `{name}` is not built at {}: selecting a single test target does not build examples, so run `cargo test -p alembic-adapter-test` or `cargo build --examples` first",
        path.display()
    );
    path
}

/// run the built-in checks against an example adapter, returning (exit code, stdout).
fn run_builtin_against(example: &str) -> (Option<i32>, String) {
    run_builtin_against_with(example, &[])
}

/// the same, with extra flags before the `--`.
fn run_builtin_against_with(example: &str, flags: &[&str]) -> (Option<i32>, String) {
    let out = Command::new(BIN)
        .args(flags)
        .arg("--")
        .arg(example_binary(example))
        .output()
        .expect("run binary");
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
    )
}

fn python3_available() -> bool {
    Command::new("python3").arg("--version").output().is_ok()
}

#[test]
fn passes_against_the_python_example() {
    if !python3_available() {
        eprintln!("skipping passes_against_the_python_example: python3 not found");
        return;
    }
    let out = Command::new(BIN)
        .args(["--cases", &manifest("examples/cases"), "--", "python3"])
        .arg(manifest("examples/adapter.py"))
        .output()
        .expect("run binary");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "expected exit 0, got {}\n{stdout}",
        out.status
    );
    assert!(stdout.contains("protocol/read-empty"), "{stdout}");
    assert!(stdout.contains("passed"), "{stdout}");
}

#[test]
fn an_sdk_emitter_passes_every_built_in_check() {
    // the sdk rejects an unsupported version before setup and before dispatch, so
    // an emitter built on it answers the version probe whichever method it rides.
    // this is the check that the org's emit-only adapters stay green.
    let (code, stdout) = run_builtin_against("sdk_emitter");
    assert_eq!(code, Some(0), "{stdout}");
    assert!(stdout.contains("protocol/write-empty"), "{stdout}");
    assert!(
        !stdout.contains("FAILED"),
        "an sdk emitter must pass every built-in check; {stdout}"
    );
}

#[test]
fn an_emitter_that_ignores_the_version_fails_the_version_probe() {
    // the probe used to be sent as a `read`, which a declared emitter refuses for
    // role reasons -- so this adapter passed it without ever reading `version`.
    let (code, stdout) = run_builtin_against("version_blind_emitter");
    assert_eq!(code, Some(1), "{stdout}");
    let mismatch = stdout
        .lines()
        .find(|line| line.contains("protocol/version-mismatch"))
        .expect("the version-mismatch check must run");
    assert!(
        mismatch.contains("FAILED"),
        "a version-blind emitter must fail the version probe; {stdout}"
    );
    // and only that check: the rest of the suite still certifies it.
    assert!(stdout.contains("6 passed, 1 failed"), "{stdout}");
}

#[test]
fn reports_a_failing_adapter_and_exits_1() {
    // a crashing adapter that still writes to both streams fails every built-in
    // check and exercises the failure diagnostics (status, stdout, stderr).
    let out = Command::new(BIN)
        .args(["--", "sh", "-c", "echo noise; echo boom >&2; exit 1"])
        .output()
        .expect("run binary");
    assert_eq!(out.status.code(), Some(1));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("FAILED"), "{stdout}");
    assert!(stdout.contains("noise"), "{stdout}");
    assert!(stdout.contains("failed"), "{stdout}");
}

#[test]
fn unreadable_cases_path_exits_2() {
    let out = Command::new(BIN)
        .args(["--cases", "/no/such/cases", "--", "sh", "-c", "true"])
        .output()
        .expect("run binary");
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("error:"), "{stderr}");
}

#[test]
fn a_cases_path_resolving_to_no_cases_exits_2() {
    // the ci snippet in docs/external-adapters.md points `--cases` at a path, so a
    // renamed or moved case directory would otherwise certify only the built-ins.
    let dir = tempdir().expect("create case dir");

    let out = Command::new(BIN)
        .args(["--cases"])
        .arg(dir.path())
        .args(["--", "sh", "-c", "true"])
        .output()
        .expect("run binary");

    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("no cases"), "{stderr}");
}

#[test]
fn missing_adapter_argument_exits_2() {
    // with no `-- adapter`, clap rejects the usage and exits 2.
    let out = Command::new(BIN).output().expect("run binary");
    assert_eq!(out.status.code(), Some(2));
}

#[test]
fn a_case_whose_expectation_key_is_misspelled_exits_2() {
    // the case is copied from examples/cases/delete-unsupported.json and pins an
    // error the adapter never returns. spelled `error` the case fails, so the
    // one letter used to be the difference between a real assertion and a green
    // run that compared nothing -- the gate must refuse the fixture instead.
    let dir = tempdir().expect("create case dir");
    let case = std::fs::read_to_string(manifest("examples/cases/delete-unsupported.json"))
        .expect("read fixture")
        .replace(
            r#""ok": false"#,
            r#""ok": false, "errror": "this message is definitely wrong""#,
        );
    std::fs::write(dir.path().join("misspelled.json"), case).expect("write case");

    let out = Command::new(BIN)
        .args(["--cases"])
        .arg(dir.path())
        .args(["--", "sh", "-c", "true"])
        .output()
        .expect("run binary");

    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("errror"), "{stderr}");
}

/// the provisioning built-in is opt-out: it runs unless it is turned off, since a
/// check that does not run certifies nothing.
#[test]
fn the_provisioning_check_runs_by_default() {
    let (code, stdout) = run_builtin_against("sdk_emitter");
    assert_eq!(code, Some(0), "{stdout}");
    let line = stdout
        .lines()
        .find(|line| line.contains("protocol/ensure-schema-empty"))
        .expect("the provisioning check must run without a flag");
    assert!(line.contains("ok"), "{stdout}");
    assert!(!stdout.contains("skipped"), "{stdout}");
}

/// turning it off reports the check as skipped rather than dropping it, so a
/// reader cannot mistake a suite that never sent `ensure_schema` for one that
/// certified it.
#[test]
fn turning_the_provisioning_check_off_reports_it_as_skipped() {
    let (code, stdout) = run_builtin_against_with("sdk_emitter", &["--no-provisioning-check"]);
    assert_eq!(code, Some(0), "a skipped check is not a failure; {stdout}");
    let line = stdout
        .lines()
        .find(|line| line.contains("protocol/ensure-schema-empty"))
        .expect("the skipped check must still be listed");
    assert!(line.contains("skipped"), "{stdout}");
    assert!(
        !line.contains("ok"),
        "a skipped check must not read as a pass; {stdout}"
    );
    assert!(
        stdout.contains("1 skipped"),
        "the summary must count it apart from passes; {stdout}"
    );
}
