//! end-to-end tests that drive the built `alembic-adapter-test` binary, so the
//! cli surface an adapter author actually runs (argument parsing, the report, and
//! the 0/1/2 exit codes) is exercised, not just the library.

use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_alembic-adapter-test");

fn manifest(rel: &str) -> String {
    format!("{}/{}", env!("CARGO_MANIFEST_DIR"), rel)
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
    let dir = std::env::temp_dir().join("alembic-adapter-test-misspelled-expect");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create case dir");
    let case = std::fs::read_to_string(manifest("examples/cases/delete-unsupported.json"))
        .expect("read fixture")
        .replace(
            r#""ok": false"#,
            r#""ok": false, "errror": "this message is definitely wrong""#,
        );
    std::fs::write(dir.join("misspelled.json"), case).expect("write case");

    let out = Command::new(BIN)
        .args(["--cases"])
        .arg(&dir)
        .args(["--", "sh", "-c", "true"])
        .output()
        .expect("run binary");
    let _ = std::fs::remove_dir_all(&dir);

    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("errror"), "{stderr}");
}
