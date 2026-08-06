// every integration test binary compiles this module separately, so a helper
// only one of them needs reads as dead code in the others.
#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::tempdir;

pub fn fixture_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("fixtures")
        .join(name)
}

pub fn python_path() -> String {
    std::env::var("ALEMBIC_DJANGO_PYTHON").unwrap_or_else(|_| "python3".to_string())
}

pub fn bin_path() -> PathBuf {
    let env_keys = ["CARGO_BIN_EXE_alembic"];
    for key in env_keys {
        if let Ok(value) = std::env::var(key) {
            return PathBuf::from(value);
        }
    }
    let target_dir = std::env::var("CARGO_TARGET_DIR")
        .ok()
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("..")
                .join("..")
                .join("target")
        });
    target_dir.join("debug").join("alembic")
}

/// cargo exports `CARGO_BIN_EXE_<name>` for bins but nothing for examples, so the
/// path comes from `CARGO_TARGET_DIR` instead (an absolute value replaces the root).
pub fn example_binary(name: &str) -> PathBuf {
    let target_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join(std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".to_string()));
    let path = target_dir.join("debug").join("examples").join(name);
    assert!(
        path.exists(),
        "example `{name}` is not built at {}: selecting a single test target does not build examples, so run `cargo test -p alembic-cli` or `cargo build --examples` first",
        path.display()
    );
    path
}

pub fn run_command(command: Command, context: &str) {
    run_command_capture(command, context);
}

/// `run_command`, returning stdout so a test can print what the command reported.
pub fn run_command_capture(mut command: Command, context: &str) -> String {
    let output = command.output().unwrap_or_else(|err| {
        panic!("{context}: failed to start command: {err}");
    });
    if !output.status.success() {
        panic!(
            "{context}: command failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// whether the django e2e tests can run. a developer box without django skips
/// them, but a run that names its interpreter (ALEMBIC_DJANGO_PYTHON, which CI
/// sets) means django is expected: skipping there would report green for tests
/// that never ran.
pub fn django_available(python: &str) -> bool {
    modules_available(
        python,
        "django, rest_framework",
        "django + djangorestframework",
    )
}

/// whether the optional packages that unlock the generated app's full path
/// (filtering, the openapi schema, the docs route) are importable. same
/// discipline as `django_available`: under ALEMBIC_DJANGO_PYTHON a missing
/// package fails loudly, because skipping would report green for the very path
/// the test exists to exercise.
pub fn django_full_stack_available(python: &str) -> bool {
    django_available(python)
        && modules_available(
            python,
            "django_filters, drf_spectacular",
            "django-filter + drf-spectacular",
        )
}

fn modules_available(python: &str, imports: &str, label: &str) -> bool {
    let available = Command::new(python)
        .args(["-c", &format!("import {imports}")])
        .output()
        .is_ok_and(|result| result.status.success());
    if !available && std::env::var_os("ALEMBIC_DJANGO_PYTHON").is_some() {
        panic!("ALEMBIC_DJANGO_PYTHON is set to '{python}', but {label} are not importable there");
    }
    available
}

/// the directory the documented example walkthroughs live in.
pub fn walkthroughs_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("examples")
        .join("walkthroughs")
}

/// path to a documented example walkthrough inventory (examples/walkthroughs/<name>).
pub fn walkthrough_path(name: &str) -> PathBuf {
    walkthroughs_dir().join(name)
}

/// write a django backend config into `dir`, emitting the generated app under `output`.
pub fn write_django_config(dir: &Path, output: &Path) -> PathBuf {
    let path = dir.join("django.yaml");
    // the config's interpreter has to be the one the availability check probed,
    // or a run names one python and generates against another.
    let data = format!(
        r"backend: django
output: {}
project: alembic_project
app: alembic_app
python: {}
no_migrate: false
no_admin: false",
        output.to_str().unwrap(),
        python_path(),
    );
    fs::write(&path, data).expect("write django config to temp dir");
    path
}

pub fn run_apply_django(fixture: &str) {
    let python = python_path();
    if !django_available(&python) {
        eprintln!("skipping django e2e; django + djangorestframework not available for {python}");
        return;
    }

    let out = tempdir().expect("create temp dir");
    let config_file_path = write_django_config(out.path(), out.path());

    let mut cmd = Command::new(bin_path());
    // isolate state per test: the default `./.alembic/state.json` is cwd-relative,
    // so parallel runs race on a shared path (and pollute the source tree).
    cmd.env("ALEMBIC_STATE_PATH", out.path().join("state.json"));
    cmd.args([
        "apply",
        "--backend-config",
        config_file_path.to_str().unwrap(),
        "--plan",
        fixture_path(fixture).to_str().unwrap(),
    ]);
    run_command(cmd, &format!("apply django ({fixture})"));
}
