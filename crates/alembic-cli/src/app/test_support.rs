use super::Runner;
use anyhow::Result;
use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

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

pub(crate) fn write_minimal_brew(dir: &Path) -> PathBuf {
    let brew = dir.join("brew.yaml");
    std::fs::write(&brew, "objects: []\n").unwrap();
    brew
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

pub(crate) fn write_site_brew(dir: &Path) -> PathBuf {
    let brew = dir.join("brew.yaml");
    std::fs::write(
        &brew,
        r#"
objects:
  - uid: "00000000-0000-0000-0000-000000000001"
    kind: dcim.site
    key: "site=fra1"
    attrs:
      name: "FRA1"
      slug: "fra1"
"#,
    )
    .unwrap();
    brew
}

pub(crate) fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

pub(crate) fn cwd_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}
