//! drives the real `alembic` binary to prove `-o` adds a document to `validate`
//! without changing what it tells the operator. needs a subprocess because the
//! human half is the error anyhow prints on the way out of `main`.

mod support;

use std::path::Path;
use std::process::Command;
use support::bin_path;
use tempfile::tempdir;

/// an inventory declaring `dcim.site`, with `attrs` extended by `extra`.
fn fixture(dir: &Path, extra: &str) -> std::path::PathBuf {
    let path = dir.join("inventory.yaml");
    std::fs::write(
        &path,
        format!(
            r#"schema:
  types:
    dcim.site:
      key:
        site:
          type: slug
      fields:
        name:
          type: string
objects:
  - uid: "00000000-0000-0000-0000-000000000001"
    type: dcim.site
    key:
      site: "fra1"
    attrs:
      name: "FRA1"
{extra}"#
        ),
    )
    .unwrap();
    path
}

/// run `validate -f <inventory> [-o <output>]`, returning (success, stdout, stderr).
fn run_validate(inventory: &Path, output: Option<&Path>) -> (bool, String, String) {
    let mut cmd = Command::new(bin_path());
    cmd.arg("validate").arg("-f").arg(inventory);
    if let Some(output) = output {
        cmd.arg("-o").arg(output);
    }
    let out = cmd.output().expect("run alembic validate");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn validate_output_leaves_the_human_report_untouched() {
    let dir = tempdir().unwrap();
    let inventory = fixture(dir.path(), "      bogus: \"nope\"\n");
    let report = dir.path().join("validation.json");

    let (bare_ok, _, bare_stderr) = run_validate(&inventory, None);
    let (with_ok, _, with_stderr) = run_validate(&inventory, Some(&report));

    assert!(
        !bare_ok && !with_ok,
        "a failing inventory must exit non-zero"
    );
    assert!(
        bare_stderr.contains("extra attr field dcim.site.bogus"),
        "stderr:\n{bare_stderr}"
    );
    assert_eq!(
        bare_stderr, with_stderr,
        "-o is an additional document, not a different command"
    );
    assert!(report.exists(), "the report is written on the failure path");
}

#[test]
fn validate_output_is_written_when_the_inventory_passes() {
    let dir = tempdir().unwrap();
    let inventory = fixture(dir.path(), "");
    let report = dir.path().join("validation.json");

    let (bare_ok, bare_stdout, bare_stderr) = run_validate(&inventory, None);
    let (with_ok, with_stdout, with_stderr) = run_validate(&inventory, Some(&report));

    assert!(bare_ok && with_ok, "stderr:\n{with_stderr}");
    assert_eq!(bare_stderr, with_stderr);
    assert!(bare_stdout.starts_with("ok"), "stdout:\n{bare_stdout}");
    // `ok` is the last thing printed: it reports the whole command, the write
    // included, not the inventory alone
    assert!(
        with_stdout.starts_with("validation report written to") && with_stdout.ends_with("ok\n"),
        "stdout:\n{with_stdout}"
    );

    let raw = std::fs::read_to_string(&report).unwrap();
    let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(value["errors"], serde_json::json!([]), "{raw}");
}

/// drive both inventories through `-o <output>` and assert each is refused with
/// `expected` and no verdict: the passing one has none to print, and the failing
/// one has a validation error the write must not displace. an inventory that
/// cannot be loaded at all is refused the same way, which is what pins the
/// refusal to *before* the read rather than merely instead of the verdict.
///
/// every bad shape is refused here: `preflight_output_path` runs ahead of the
/// whole command, so one that fails only on permissions is caught with the rest
/// rather than at the write.
fn assert_refused_before_the_verdict(dir: &Path, output: &Path, expected: &str) {
    for extra in ["", "      bogus: \"nope\"\n"] {
        assert_refused(&fixture(dir, extra), output, expected);
    }
    assert_refused(&dir.join("no-such-inventory.yaml"), output, expected);
}

fn assert_refused(inventory: &Path, output: &Path, expected: &str) {
    let (ok, stdout, stderr) = run_validate(inventory, Some(output));
    assert!(!ok, "an -o that cannot be written fails the run");
    assert!(
        stdout.is_empty(),
        "no verdict for a run that cannot deliver its document: {stdout}"
    );
    assert!(stderr.contains(expected), "{stderr}");
}

#[test]
fn validate_warns_about_a_yaml_output_and_still_writes_it() {
    // the warning rides with the write rather than being copied to each `-o`
    // site, so a site that has one has the other.
    let dir = tempdir().unwrap();
    let report = dir.path().join("validation.yaml");

    let (ok, stdout, stderr) = run_validate(&fixture(dir.path(), ""), Some(&report));

    assert!(
        ok,
        "a misleading extension is a nudge, not an error: {stderr}"
    );
    assert!(
        stderr.contains(&format!(
            "warning: --output `{}` is written as JSON despite the .yaml extension",
            report.display()
        )),
        "stderr:\n{stderr}"
    );
    assert!(
        stdout.starts_with("validation report written to"),
        "stdout:\n{stdout}"
    );
    let raw = std::fs::read_to_string(&report).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&raw).unwrap()["errors"],
        serde_json::json!([]),
        "{raw}"
    );
}

#[test]
fn validate_refuses_an_output_parent_it_cannot_create_before_it_has_a_verdict() {
    let dir = tempdir().unwrap();
    // a file where the report's parent would go: create_dir_all cannot pass it
    let blocker = dir.path().join("blocker");
    std::fs::write(&blocker, "").unwrap();

    assert_refused_before_the_verdict(
        dir.path(),
        &blocker.join("sub/validation.json"),
        "create output directory",
    );
}

#[cfg(unix)]
#[test]
fn validate_refuses_an_unwritable_existing_output_before_it_has_a_verdict() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempdir().unwrap();

    // root ignores the mode bits, so ask the os on a separate file rather than
    // skipping, and assert whichever answer it gives
    let sentinel = dir.path().join("sentinel");
    std::fs::write(&sentinel, b"x").unwrap();
    std::fs::set_permissions(&sentinel, std::fs::Permissions::from_mode(0o444)).unwrap();
    let denied = std::fs::write(&sentinel, b"y").is_err();

    // the parent accepts writes and the target is not a directory, so only the
    // write probe refuses this one
    let target = dir.path().join("validation.json");
    std::fs::write(&target, "previous").unwrap();
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o444)).unwrap();

    if denied {
        assert_refused_before_the_verdict(dir.path(), &target, "write output");
        // before the *read*, not merely before the verdict: loading a missing
        // inventory is the side effect that would otherwise report first
        let missing = dir.path().join("no-such-inventory.yaml");
        let (_, _, stderr) = run_validate(&missing, Some(&target));
        assert!(
            !stderr.contains("load inventory"),
            "the inventory must not be read: {stderr}"
        );
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            "previous",
            "and the probe must not touch the contents"
        );
    } else {
        let (ok, _, stderr) = run_validate(&fixture(dir.path(), ""), Some(&target));
        assert!(ok, "a target this user can write to is an output: {stderr}");
    }
}

#[test]
fn validate_refuses_an_output_path_that_is_a_directory_before_it_has_a_verdict() {
    let dir = tempdir().unwrap();
    let target = dir.path().join("adir");
    std::fs::create_dir(&target).unwrap();

    // creating the parent proves nothing about the target: without the is_dir
    // check the write reports `Is a directory` in place of the verdict
    assert_refused_before_the_verdict(
        dir.path(),
        &target,
        &format!("write output: {}: is a directory", target.display()),
    );
}

#[cfg(unix)]
#[test]
fn validate_accepts_a_character_device_output() {
    // `-o /dev/null` is how a run is observed and its document thrown away. the
    // preflight asks the target, not `/dev`, which takes no new file.
    let dir = tempdir().unwrap();
    let (ok, stdout, stderr) = run_validate(&fixture(dir.path(), ""), Some(Path::new("/dev/null")));

    assert!(ok, "stderr:\n{stderr}");
    assert!(
        stdout.starts_with("validation report written to") && stdout.ends_with("ok\n"),
        "the run reaches its work and its write: {stdout}"
    );
}

#[test]
fn validate_leaves_no_output_directory_behind_when_the_run_fails() {
    // the other half of dropping the inline `ensure_parent_dir`: it ran before
    // the load, so a run that died on the load left the report's directory
    // behind. the probe takes back what it creates.
    let dir = tempdir().unwrap();
    let nested = dir.path().join("reports");

    let (ok, _, stderr) = run_validate(
        &dir.path().join("no-such-inventory.yaml"),
        Some(&nested.join("validation.json")),
    );

    assert!(!ok, "a missing inventory fails the run: {stderr}");
    assert!(
        !nested.exists(),
        "a run that delivered no report leaves no directory for it"
    );
}
