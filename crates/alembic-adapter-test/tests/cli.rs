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
    let (code, stdout) = run_builtin_against_with("sdk_emitter", &["--write-checks"]);
    assert_eq!(code, Some(0), "{stdout}");
    assert!(stdout.contains("protocol/write-empty"), "{stdout}");
    assert!(
        !stdout.contains("FAILED"),
        "an sdk emitter must pass every built-in check; {stdout}"
    );
}

#[test]
fn an_emitter_that_ignores_the_version_fails_the_version_probe() {
    // the probe rides the emitter's `preview_schema`, so no flag is needed to catch
    // this adapter and asking for the writing checks does not change the verdict.
    for flags in [&[][..], &["--write-checks"][..]] {
        let (code, stdout) = run_builtin_against_with("version_blind_emitter", flags);
        assert_eq!(code, Some(1), "{stdout}");
        let mismatch = stdout
            .lines()
            .find(|line| line.contains("protocol/version-mismatch"))
            .expect("the version-mismatch check must run");
        assert!(
            mismatch.contains("FAILED"),
            "a version-blind emitter must fail the version probe; {stdout}"
        );
    }
    // and only that check: the rest of the suite still certifies it.
    let (_, stdout) = run_builtin_against("version_blind_emitter");
    assert!(stdout.contains("4 passed, 2 skipped, 1 failed"), "{stdout}");
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
    assert!(
        stderr.contains("subdirectories"),
        "the message has to name the layout, or someone who grouped cases per backend cannot tell what happened; {stderr}"
    );
}

#[test]
fn cases_in_a_subdirectory_exit_2_even_when_others_loaded() {
    // one file at the top level is the only difference from the total miss above.
    let dir = tempdir().expect("create case dir");
    std::fs::copy(
        manifest("examples/cases/write-create.json"),
        dir.path().join("write-create.json"),
    )
    .expect("copy fixture");
    let nested = dir.path().join("netbox");
    std::fs::create_dir(&nested).expect("create subdirectory");
    std::fs::copy(
        manifest("examples/cases/read-empty.json"),
        nested.join("read-empty.json"),
    )
    .expect("copy fixture");

    let out = Command::new(BIN)
        .args(["--cases"])
        .arg(dir.path())
        .arg("--")
        .arg(example_binary("sdk_emitter"))
        .output()
        .expect("run binary");

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(2), "{stdout}");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("netbox"), "{stderr}");
    assert!(!stdout.contains("passed"), "{stdout}");
}

#[test]
fn cases_two_directories_down_are_named_too() {
    let dir = tempdir().expect("create case dir");
    std::fs::copy(
        manifest("examples/cases/write-create.json"),
        dir.path().join("write-create.json"),
    )
    .expect("copy fixture");
    let nested = dir.path().join("backends").join("netbox");
    std::fs::create_dir_all(&nested).expect("create subdirectories");
    std::fs::copy(
        manifest("examples/cases/read-empty.json"),
        nested.join("read-empty.json"),
    )
    .expect("copy fixture");

    let out = Command::new(BIN)
        .args(["--cases"])
        .arg(dir.path())
        .arg("--")
        .arg(example_binary("sdk_emitter"))
        .output()
        .expect("run binary");

    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("backends"), "{stderr}");
}

#[test]
fn a_subdirectory_holding_no_cases_is_not_a_fixtures_error() {
    let dir = tempdir().expect("create case dir");
    std::fs::copy(
        manifest("examples/cases/write-create.json"),
        dir.path().join("write-create.json"),
    )
    .expect("copy fixture");
    let notes = dir.path().join("notes");
    std::fs::create_dir(&notes).expect("create subdirectory");
    std::fs::write(notes.join("README.md"), "how these cases were captured\n").expect("write file");

    let out = Command::new(BIN)
        .args(["--cases"])
        .arg(dir.path())
        .arg("--")
        .arg(example_binary("sdk_emitter"))
        .output()
        .expect("run binary");

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{stdout}{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(stdout.contains("case/write a create op"), "{stdout}");
}

#[test]
fn a_subdirectory_holding_a_json_that_is_not_a_case_is_not_a_fixtures_error() {
    // the boundary the check draws is case, not `.json`: a directory kept for
    // notes or editor settings holds neither.
    let dir = tempdir().expect("create case dir");
    std::fs::copy(
        manifest("examples/cases/write-create.json"),
        dir.path().join("write-create.json"),
    )
    .expect("copy fixture");
    let vscode = dir.path().join(".vscode");
    std::fs::create_dir(&vscode).expect("create subdirectory");
    std::fs::write(vscode.join("settings.json"), r#"{"editor.tabSize": 2}"#).expect("write file");

    let out = Command::new(BIN)
        .args(["--cases"])
        .arg(dir.path())
        .arg("--")
        .arg(example_binary("sdk_emitter"))
        .output()
        .expect("run binary");

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{stdout}{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(stdout.contains("case/write a create op"), "{stdout}");
}

/// the narrowing check against the four ways an adapter can answer the hint.
/// only the union is correct, and the check has to fail each half on its own:
/// one arm names the objects through `keys`, the other through `backend_ids`.
#[test]
fn a_wrongly_narrowing_adapter_fails_the_case() {
    if !python3_available() {
        eprintln!("skipping a_wrongly_narrowing_adapter_fails_the_case: python3 not found");
        return;
    }
    let dir = tempdir().expect("create case dir");
    // no `expect.result`: the unscoped run is the same for every mode, so only
    // the narrowing arms discriminate.
    std::fs::write(
        dir.path().join("read.json"),
        r#"{
          "name": "read sites",
          "request": {
            "version": 1, "setup": {}, "method": "read",
            "schema": { "types": { "dcim.site": {
              "key": { "site": { "type": "slug", "required": false, "nullable": false } },
              "fields": { "name": { "type": "string", "required": false, "nullable": false } } } } },
            "types": ["dcim.site"],
            "state": { "mappings": {} }
          },
          "expect": { "ok": true }
        }"#,
    )
    .expect("write case");

    for (mode, code, dropped) in [
        ("ignore", 0, ""),
        ("union", 0, ""),
        // drops what only `keys` names: an object state has never bound.
        ("ids", 1, "narrowed on keys"),
        // drops what only `backend_ids` names: an object whose key drifted.
        ("keys", 1, "narrowed on backend ids"),
    ] {
        let out = Command::new(BIN)
            .args(["--cases"])
            .arg(dir.path())
            .args(["--", "python3"])
            .arg(manifest("examples/narrowing_adapter.py"))
            .arg(mode)
            .output()
            .expect("run binary");
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert_eq!(out.status.code(), Some(code), "{mode}: {stdout}");
        assert!(stdout.contains("case/read sites"), "{mode}: {stdout}");
        if !dropped.is_empty() {
            assert!(
                stdout.contains(dropped) && stdout.contains("dropped objects the scope names"),
                "{mode}: {stdout}"
            );
        }
    }
}

/// a ref-keyed type is in `unnarrowed` and in neither map, so an adapter honoring
/// the union and nothing else answers with none of it and its every plan creates
/// over live objects. the arm driving the engine's own scope is what catches it.
#[test]
fn an_adapter_dropping_a_held_out_type_fails_the_case() {
    if !python3_available() {
        eprintln!("skipping an_adapter_dropping_a_held_out_type_fails_the_case: no python3");
        return;
    }
    let dir = tempdir().expect("create case dir");
    // the interface's key names its device by the uid the host mints, so the
    // device has to be read too for the ref to resolve.
    std::fs::write(
        dir.path().join("read.json"),
        r#"{
          "name": "read a chain",
          "request": {
            "version": 1, "setup": {}, "method": "read",
            "schema": { "types": {
              "dcim.site": {
                "key": { "site": { "type": "slug", "required": false, "nullable": false } },
                "fields": { "name": { "type": "string", "required": false, "nullable": false } } },
              "dcim.device": {
                "key": { "name": { "type": "string", "required": false, "nullable": false } },
                "fields": { "name": { "type": "string", "required": false, "nullable": false } } },
              "dcim.interface": {
                "key": {
                  "device": { "type": "ref", "target": "dcim.device", "required": false, "nullable": false },
                  "name": { "type": "string", "required": false, "nullable": false } },
                "fields": { "name": { "type": "string", "required": false, "nullable": false } } } } },
            "types": ["dcim.site", "dcim.device", "dcim.interface"],
            "state": { "mappings": {} }
          },
          "expect": { "ok": true }
        }"#,
    )
    .expect("write case");

    for (mode, code) in [("ignore", 0), ("unnarrowed", 0), ("union", 1)] {
        let out = Command::new(BIN)
            .args(["--cases"])
            .arg(dir.path())
            .args(["--", "python3"])
            .arg(manifest("examples/narrowing_adapter.py"))
            .arg(mode)
            .output()
            .expect("run binary");
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert_eq!(out.status.code(), Some(code), "{mode}: {stdout}");
        assert!(
            stdout.contains("case/read a chain narrowed on unnarrowed"),
            "{mode}: {stdout}"
        );
        if code == 1 {
            assert!(
                stdout.contains("dropped objects the scope names: dcim.interface"),
                "{mode}: {stdout}"
            );
        }
    }
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

/// the writing built-ins are opt-in, and reported as skipped rather than dropped.
/// the version probe is not one of them: it rides a method that writes nothing.
#[test]
fn the_writing_checks_are_off_by_default() {
    let (code, stdout) = run_builtin_against("sdk_emitter");
    assert_eq!(code, Some(0), "a skipped check is not a failure; {stdout}");
    for name in ["protocol/write-empty", "protocol/ensure-schema-empty"] {
        let line = stdout
            .lines()
            .find(|line| line.contains(name))
            .unwrap_or_else(|| panic!("{name} must still be listed; {stdout}"));
        assert!(line.contains("skipped"), "{stdout}");
        assert!(
            !line.contains("   ok"),
            "a skipped check must not read as a pass; {stdout}"
        );
    }
    assert!(stdout.contains("writes are opt-in"), "{stdout}");
    let probe = stdout
        .lines()
        .find(|line| line.contains("protocol/version-mismatch"))
        .expect("the version probe must run by default");
    assert!(probe.contains("ok"), "{stdout}");
    assert!(
        stdout.contains("5 passed, 2 skipped"),
        "the summary must count them apart from passes; {stdout}"
    );
}

/// asked for, both run.
#[test]
fn write_checks_runs_them() {
    let (code, stdout) = run_builtin_against_with("sdk_emitter", &["--write-checks"]);
    assert_eq!(code, Some(0), "{stdout}");
    assert!(!stdout.contains("skipped"), "{stdout}");
    assert!(stdout.contains("7 passed"), "{stdout}");
}

/// the old spelling is the default now: it still parses and still means no writing
/// check ran, but it says so rather than reading as a flag that did something.
#[test]
fn no_provisioning_check_warns_and_keeps_its_meaning() {
    let out = Command::new(BIN)
        .args(["--no-provisioning-check", "--"])
        .arg(example_binary("sdk_emitter"))
        .output()
        .expect("run binary");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(0), "{stdout}");
    assert!(stderr.contains("--no-provisioning-check"), "{stderr}");
    assert!(stdout.contains("5 passed, 2 skipped"), "{stdout}");

    // and it cannot be quietly overruled: asking for both is a usage error.
    let out = Command::new(BIN)
        .args(["--no-provisioning-check", "--write-checks", "--"])
        .arg(example_binary("sdk_emitter"))
        .output()
        .expect("run binary");
    assert_eq!(out.status.code(), Some(2));
}
