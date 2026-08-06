use super::Runner;
use anyhow::Result;
use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Call {
    pub(crate) program: String,
    pub(crate) args: Vec<String>,
    pub(crate) cwd: Option<PathBuf>,
}

pub(crate) struct MockRunner {
    calls: RefCell<Vec<Call>>,
}

impl MockRunner {
    pub(crate) fn new() -> Self {
        Self {
            calls: RefCell::new(Vec::new()),
        }
    }

    pub(crate) fn calls(&self) -> Vec<Call> {
        self.calls.borrow().clone()
    }
}

impl Runner for MockRunner {
    fn run(&self, program: &str, args: &[&str], cwd: Option<&Path>) -> Result<()> {
        self.calls.borrow_mut().push(Call {
            program: program.to_string(),
            args: args.iter().map(|arg| arg.to_string()).collect(),
            cwd: cwd.map(|dir| dir.to_path_buf()),
        });
        Ok(())
    }
}

pub(crate) struct FixtureRunner {
    calls: RefCell<Vec<Call>>,
    output_dir: PathBuf,
}

impl FixtureRunner {
    pub(crate) fn new(output_dir: PathBuf) -> Self {
        Self {
            calls: RefCell::new(Vec::new()),
            output_dir,
        }
    }

    pub(crate) fn calls(&self) -> Vec<Call> {
        self.calls.borrow().clone()
    }

    pub(crate) fn write_settings(&self, project_name: &str) -> PathBuf {
        let project_dir = self.output_dir.join(project_name);
        std::fs::create_dir_all(&project_dir).unwrap();
        let settings = project_dir.join("settings.py");
        std::fs::write(
            &settings,
            "INSTALLED_APPS = [\n    \"django.contrib.admin\",\n]\n",
        )
        .unwrap();
        std::fs::write(
            project_dir.join("urls.py"),
            "from django.contrib import admin\nfrom django.urls import path\n\nurlpatterns = [\n    path('admin/', admin.site.urls),\n]\n",
        )
        .unwrap();
        settings
    }

    pub(crate) fn write_startapp_files(&self, app_name: &str) {
        let app_dir = self.output_dir.join(app_name);
        std::fs::create_dir_all(app_dir.join("migrations")).unwrap();
        std::fs::write(
            app_dir.join("models.py"),
            "from django.db import models\n\n# Create your models here.\n",
        )
        .unwrap();
        std::fs::write(
            app_dir.join("admin.py"),
            "from django.contrib import admin\n\n# Register your models here.\n",
        )
        .unwrap();
        std::fs::write(
            app_dir.join("apps.py"),
            "from django.apps import AppConfig\n",
        )
        .unwrap();
        std::fs::write(app_dir.join("migrations/__init__.py"), "").unwrap();
    }
}

impl Runner for FixtureRunner {
    fn run(&self, program: &str, args: &[&str], cwd: Option<&Path>) -> Result<()> {
        self.calls.borrow_mut().push(Call {
            program: program.to_string(),
            args: args.iter().map(|arg| arg.to_string()).collect(),
            cwd: cwd.map(|dir| dir.to_path_buf()),
        });
        if program == "django-admin" && args.len() >= 3 && args[0] == "startproject" {
            let project_name = args[1];
            std::fs::create_dir_all(&self.output_dir).unwrap();
            std::fs::write(self.output_dir.join("manage.py"), "").unwrap();
            self.write_settings(project_name);
        }
        if program.ends_with("python3")
            && args.len() >= 3
            && args[0] == "manage.py"
            && args[1] == "startapp"
        {
            let app_name = args[2];
            self.write_startapp_files(app_name);
        }
        Ok(())
    }
}

/// cargo exports `CARGO_BIN_EXE_<name>` for bins but nothing for examples, so the
/// path comes from `CARGO_TARGET_DIR` instead (an absolute value replaces the root).
pub(crate) fn example_binary(name: &str) -> PathBuf {
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

pub(crate) fn write_minimal_inventory(dir: &Path) -> PathBuf {
    let inventory = dir.join("inventory.yaml");
    std::fs::write(&inventory, "schema:\n  types: {}\nobjects: []\n").unwrap();
    inventory
}

pub(crate) fn write_settings(output_dir: &Path, project_name: &str) -> PathBuf {
    let project_dir = output_dir.join(project_name);
    std::fs::create_dir_all(&project_dir).unwrap();
    let settings = project_dir.join("settings.py");
    std::fs::write(
        &settings,
        "INSTALLED_APPS = [\n    \"django.contrib.admin\",\n]\n",
    )
    .unwrap();
    std::fs::write(
        project_dir.join("urls.py"),
        "from django.contrib import admin\nfrom django.urls import path\n\nurlpatterns = [\n    path('admin/', admin.site.urls),\n]\n",
    )
    .unwrap();
    settings
}

pub(crate) fn write_site_inventory(dir: &Path) -> PathBuf {
    let inventory = dir.join("inventory.yaml");
    std::fs::write(
        &inventory,
        r#"
schema:
  types:
    dcim.site:
      key:
        site:
          type: slug
      fields:
        name:
          type: string
        slug:
          type: slug
objects:
  - uid: "00000000-0000-0000-0000-000000000001"
    type: dcim.site
    key:
      site: "fra1"
    attrs:
      name: "FRA1"
      slug: "fra1"
"#,
    )
    .unwrap();
    inventory
}

/// serializes tests that read or mutate the process environment. it is a
/// `tokio::sync::Mutex` (acquired through [`EnvVarGuard`]) so the async
/// full-path tests can hold the guard across `.await` without tripping
/// `clippy::await_holding_lock`.
pub(crate) fn env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

pub(crate) fn cwd_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// RAII guard that pins a set of environment variables for the lifetime of a
/// test, restoring their prior values on drop.
///
/// construction takes [`env_lock`], so env-mutating tests (the synchronous
/// `resolve_state_backend_*` tests) and env-reading tests (the async full-path
/// tests that resolve a state backend) serialize on the same lock and never
/// race on the process environment. use [`EnvVarGuard::acquire`] from `#[test]`
/// functions and [`EnvVarGuard::acquire_async`] from `#[tokio::test]` functions.
///
/// each override is `(name, value)`: `Some(v)` sets the variable to `v` and
/// `None` removes it. the prior value of every named variable is restored when
/// the guard drops, even on panic.
pub(crate) struct EnvVarGuard {
    _lock: tokio::sync::MutexGuard<'static, ()>,
    saved: Vec<(String, Option<String>)>,
}

impl EnvVarGuard {
    /// acquire the env lock synchronously (for non-async tests) and apply
    /// `overrides`.
    pub(crate) fn acquire(overrides: &[(&str, Option<&str>)]) -> Self {
        Self::with_lock(env_lock().blocking_lock(), overrides)
    }

    /// acquire the env lock from an async context (for `#[tokio::test]` tests)
    /// and apply `overrides`. the async-aware guard is safe to hold across
    /// `.await`, unlike a `std::sync::MutexGuard`.
    pub(crate) async fn acquire_async(overrides: &[(&str, Option<&str>)]) -> Self {
        Self::with_lock(env_lock().lock().await, overrides)
    }

    fn with_lock(
        lock: tokio::sync::MutexGuard<'static, ()>,
        overrides: &[(&str, Option<&str>)],
    ) -> Self {
        let mut saved = Vec::with_capacity(overrides.len());
        for (name, value) in overrides {
            saved.push(((*name).to_string(), std::env::var(name).ok()));
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
        Self { _lock: lock, saved }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        // restore in reverse insertion order so a name listed twice still ends
        // up with its original value.
        for (name, value) in self.saved.iter().rev() {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
    }
}
