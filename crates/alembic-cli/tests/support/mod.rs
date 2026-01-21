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
    std::env::var("ALEMBIC_CAST_PYTHON").unwrap_or_else(|_| "python3".to_string())
}

pub fn bin_path() -> PathBuf {
    let env_keys = ["CARGO_BIN_EXE_alembic_cli", "CARGO_BIN_EXE_alembic-cli"];
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
    target_dir.join("debug").join("alembic-cli")
}

pub fn run_command(mut command: Command, context: &str) {
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
}

pub fn django_available(python: &str) -> bool {
    let output = Command::new(python)
        .args(["-c", "import django, rest_framework"])
        .output();
    match output {
        Ok(result) => result.status.success(),
        Err(_) => false,
    }
}

pub fn run_cast(fixture: &str) {
    let python = python_path();
    if !django_available(&python) {
        eprintln!(
            "skipping cast django e2e; django + djangorestframework not available for {python}"
        );
        return;
    }

    let bin = bin_path();
    let out = tempdir().expect("create temp dir");

    let mut cmd = Command::new(&bin);
    cmd.args([
        "cast",
        "django",
        "-f",
        fixture_path(fixture).to_str().unwrap(),
        "-o",
        out.path().to_str().unwrap(),
        "--project",
        "alembic_project",
        "--app",
        "alembic_app",
        "--python",
        &python,
    ]);
    run_command(cmd, &format!("cast django ({fixture})"));
}
