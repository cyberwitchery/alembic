//! drives the real `alembic` binary to prove `-o` adds a document to `validate`
//! without changing what it tells the operator. needs a subprocess because the
//! human half is the error anyhow prints on the way out of `main`.

use std::path::Path;
use std::process::Command;
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
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_alembic"));
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
    assert!(with_stdout.starts_with("ok"), "stdout:\n{with_stdout}");

    let raw = std::fs::read_to_string(&report).unwrap();
    let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(value["errors"], serde_json::json!([]), "{raw}");
}
