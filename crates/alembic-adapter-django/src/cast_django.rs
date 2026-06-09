#![allow(dead_code)]

use crate::DjangoEmitOptions;
use alembic_engine::load_inventory;
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;

pub trait Runner {
    fn run(&self, program: &str, args: &[&str], cwd: Option<&Path>) -> Result<()>;
}

pub struct CommandRunner;

impl CommandRunner {
    pub fn new() -> Self {
        Self
    }

    fn run_command(&self, program: &str, args: &[&str], cwd: Option<&Path>) -> Result<()> {
        let mut command = ProcessCommand::new(program);
        command.args(args);
        if let Some(dir) = cwd {
            command.current_dir(dir);
        }
        let output = command.output().with_context(|| {
            if program == "django-admin" {
                "failed to run django-admin; is Django installed? (pip install django)"
            } else {
                "failed to run command"
            }
        })?;
        if output.status.success() {
            return Ok(());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(anyhow!(
            "command failed: {program} {args_str}\nstdout:\n{stdout}\nstderr:\n{stderr}",
            program = program,
            args_str = args.join(" "),
            stdout = stdout,
            stderr = stderr,
        ))
    }
}

impl Runner for CommandRunner {
    fn run(&self, program: &str, args: &[&str], cwd: Option<&Path>) -> Result<()> {
        self.run_command(program, args, cwd)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DjangoConfig {
    pub file: PathBuf,
    pub output: PathBuf,
    pub project: Option<String>,
    pub app: Option<String>,
    pub python: String,
    pub no_migrate: bool,
    pub no_admin: bool,
}

pub fn run_cast_django(runner: &dyn Runner, config: &DjangoConfig) -> Result<()> {
    let inventory = load_inventory(&config.file)?;
    let project_name = config.project.as_deref().unwrap_or("alembic_project");
    let app_name = config.app.as_deref().unwrap_or("alembic_app");
    validate_python_identifier(project_name, "project")?;
    validate_python_identifier(app_name, "app")?;
    let output_dir = &config.output;
    fs::create_dir_all(output_dir).with_context(|| format!("create {}", output_dir.display()))?;

    ensure_django_project(runner, output_dir, project_name)?;
    ensure_python_has_django(runner, &config.python)?;
    ensure_python_has_drf(runner, &config.python)?;
    ensure_django_app(runner, output_dir, app_name, &config.python)?;

    let app_dir = output_dir.join(app_name);
    let options = DjangoEmitOptions {
        emit_admin: !config.no_admin,
    };
    crate::emit_django_app(&app_dir, &inventory, options)?;
    ensure_installed_apps_entries(output_dir, project_name, &["rest_framework", app_name])?;
    ensure_project_urls(output_dir, project_name, app_name)?;
    run_manage_check(runner, output_dir, &config.python)?;
    run_manage_makemigrations(runner, output_dir, &config.python)?;
    if !config.no_migrate {
        run_manage_migrate(runner, output_dir, &config.python)?;
    }

    println!(
        "django app generated at {} (project {}, app {})",
        output_dir.display(),
        project_name,
        app_name
    );
    Ok(())
}

fn ensure_django_project(runner: &dyn Runner, output_dir: &Path, project_name: &str) -> Result<()> {
    let manage_py = output_dir.join("manage.py");
    let project_dir = output_dir.join(project_name);
    if manage_py.exists() && project_dir.exists() {
        return Ok(());
    }
    runner.run(
        "django-admin",
        &[
            "startproject",
            project_name,
            &output_dir.display().to_string(),
        ],
        None,
    )
}

fn ensure_django_app(
    runner: &dyn Runner,
    output_dir: &Path,
    app_name: &str,
    python: &str,
) -> Result<()> {
    let app_dir = output_dir.join(app_name);
    if app_dir.join("apps.py").exists() {
        return Ok(());
    }
    ensure_app_name_available(runner, output_dir, app_name, python)?;
    runner.run(
        python,
        &["manage.py", "startapp", app_name],
        Some(output_dir),
    )
}

fn ensure_python_has_django(runner: &dyn Runner, python: &str) -> Result<()> {
    match runner.run(python, &["-c", "import django"], None) {
        Ok(()) => Ok(()),
        Err(_) => Err(anyhow!(
            "django is not available for {}; install it (pip install django)",
            python
        )),
    }
}

fn ensure_python_has_drf(runner: &dyn Runner, python: &str) -> Result<()> {
    match runner.run(python, &["-c", "import rest_framework"], None) {
        Ok(()) => Ok(()),
        Err(_) => Err(anyhow!(
            " djangorestframework is not available for {}; install it (pip install djangorestframework)",
            python
        )),
    }
}

fn ensure_app_name_available(
    runner: &dyn Runner,
    output_dir: &Path,
    app_name: &str,
    python: &str,
) -> Result<()> {
    let check = format!(
        "import importlib.util, sys; sys.exit(0 if importlib.util.find_spec({name:?}) is None else 1)",
        name = app_name
    );
    match runner.run(python, &["-c", &check], Some(output_dir)) {
        Ok(()) => Ok(()),
        Err(_) => Err(anyhow!(
            "app name '{}' conflicts with an existing Python module; pick a different --app name",
            app_name
        )),
    }
}

fn validate_python_identifier(name: &str, label: &str) -> Result<()> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err(anyhow!("{label} name is empty"));
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(anyhow!(
            "invalid {label} name '{name}': must start with a letter or underscore"
        ));
    }
    for ch in chars {
        if !(ch.is_ascii_alphanumeric() || ch == '_') {
            return Err(anyhow!(
                "invalid {label} name '{name}': only letters, digits, and underscores are allowed"
            ));
        }
    }
    Ok(())
}

fn ensure_installed_apps_entries(
    output_dir: &Path,
    project_name: &str,
    entries: &[&str],
) -> Result<()> {
    let settings_path = output_dir.join(project_name).join("settings.py");
    let mut contents = fs::read_to_string(&settings_path)
        .with_context(|| format!("read {}", settings_path.display()))?;

    let start = contents
        .find("INSTALLED_APPS")
        .ok_or_else(|| anyhow!("settings.py missing INSTALLED_APPS"))?;
    for entry in entries {
        let quoted = format!("\"{}\"", entry);
        let single_quoted = format!("'{}'", entry);
        if contents.contains(&quoted) || contents.contains(&single_quoted) {
            continue;
        }
        let end = contents[start..]
            .find(']')
            .ok_or_else(|| anyhow!("settings.py missing INSTALLED_APPS closing bracket"))?
            + start;
        contents.insert_str(end, &format!("    \"{}\",\n", entry));
    }
    fs::write(&settings_path, contents)
        .with_context(|| format!("write {}", settings_path.display()))?;
    Ok(())
}

fn ensure_project_urls(output_dir: &Path, project_name: &str, app_name: &str) -> Result<()> {
    let urls_path = output_dir.join(project_name).join("urls.py");
    let mut contents =
        fs::read_to_string(&urls_path).with_context(|| format!("read {}", urls_path.display()))?;

    if contents.contains("include(") && contents.contains(&format!("{}.urls", app_name)) {
        return Ok(());
    }

    let mut import_fixed = false;
    for line in contents.lines() {
        if line.trim_start().starts_with("from django.urls import") {
            if line.contains("include") {
                import_fixed = true;
                break;
            }
            let updated = line.replace("import", "import include,");
            contents = contents.replace(line, &updated);
            import_fixed = true;
            break;
        }
    }
    if !import_fixed {
        contents = format!("from django.urls import include, path\n{}", contents);
    }

    if !contents.contains(&format!("include(\"{}.urls\")", app_name))
        && !contents.contains(&format!("include('{}.urls')", app_name))
    {
        if let Some(pos) = contents.find("urlpatterns = [") {
            let insert_pos = contents[pos..]
                .find(']')
                .ok_or_else(|| anyhow!("urls.py missing urlpatterns closing bracket"))?
                + pos;
            contents.insert_str(
                insert_pos,
                &format!("    path(\"api/\", include(\"{}.urls\")),\n", app_name),
            );
        }
    }

    fs::write(&urls_path, contents).with_context(|| format!("write {}", urls_path.display()))?;
    Ok(())
}

fn run_manage_check(runner: &dyn Runner, output_dir: &Path, python: &str) -> Result<()> {
    runner.run(python, &["manage.py", "check"], Some(output_dir))
}

fn run_manage_makemigrations(runner: &dyn Runner, output_dir: &Path, python: &str) -> Result<()> {
    runner.run(python, &["manage.py", "makemigrations"], Some(output_dir))
}

fn run_manage_migrate(runner: &dyn Runner, output_dir: &Path, python: &str) -> Result<()> {
    runner.run(python, &["manage.py", "migrate"], Some(output_dir))
}
