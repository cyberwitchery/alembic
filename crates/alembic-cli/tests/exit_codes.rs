//! docs/cli.md, exit codes: what each kind of failure exits with, through the
//! binary, since main is where an error becomes a code.

mod support;

use std::fs;
use std::process::{Command, Output};
use support::bin_path;
use tempfile::tempdir;

fn alembic(args: &[&str], dir: &std::path::Path) -> Output {
    Command::new(bin_path())
        .current_dir(dir)
        .args(args)
        .output()
        .expect("run alembic")
}

fn assert_code(output: &Output, code: i32) {
    assert_eq!(
        output.status.code(),
        Some(code),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn help_and_version_succeed() {
    let dir = tempdir().unwrap();
    assert_code(&alembic(&["--help"], dir.path()), 0);
    assert_code(&alembic(&["--version"], dir.path()), 0);
}

#[test]
fn a_bad_flag_or_an_unreadable_file_is_a_usage_error() {
    let dir = tempdir().unwrap();
    assert_code(&alembic(&["plan", "--no-such-flag"], dir.path()), 64);
    assert_code(
        &alembic(&["validate", "-f", "missing.yaml"], dir.path()),
        64,
    );
    assert_code(
        &alembic(&["apply", "-p", "missing.json", "-i"], dir.path()),
        64,
    );
}

#[test]
fn an_input_that_does_not_parse_or_validate_is_invalid() {
    let dir = tempdir().unwrap();
    // a dangling ref: parses, does not validate
    fs::write(
        dir.path().join("invalid.yaml"),
        "schema:\n  types:\n    dcim.device:\n      key: {name: {type: string}}\n      fields:\n        name: {type: string}\n        site: {type: ref, target: dcim.site}\nobjects:\n  - uid: 00000000-0000-0000-0000-000000000001\n    type: dcim.device\n    key: {name: leaf1}\n    attrs: {name: leaf1, site: 00000000-0000-0000-0000-000000000002}\n",
    )
    .unwrap();
    assert_code(
        &alembic(&["validate", "-f", "invalid.yaml"], dir.path()),
        65,
    );

    // a plan passed where an inventory belongs
    fs::write(
        dir.path().join("plan.json"),
        r#"{"schema":{"types":{}},"ops":[]}"#,
    )
    .unwrap();
    assert_code(&alembic(&["validate", "-f", "plan.json"], dir.path()), 65);
}
