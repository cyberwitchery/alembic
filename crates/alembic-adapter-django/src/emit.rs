use crate::DjangoEmitOptions;
use alembic_core::Inventory;
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

impl Default for CommandRunner {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DjangoConfig {
    pub output: PathBuf,
    #[serde(default)]
    pub project: Option<String>,
    #[serde(default)]
    pub app: Option<String>,
    #[serde(default = "default_python")]
    pub python: String,
    #[serde(default)]
    pub no_migrate: bool,
    #[serde(default)]
    pub no_admin: bool,
}

fn default_python() -> String {
    "python3".to_string()
}

impl Default for DjangoConfig {
    fn default() -> Self {
        DjangoConfig {
            output: "./out".into(),
            project: None,
            app: None,
            python: "python3".to_string(),
            no_migrate: false,
            no_admin: false,
        }
    }
}

pub fn run_emit(runner: &dyn Runner, inventory: &Inventory, config: &DjangoConfig) -> Result<()> {
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

    // django-filter is optional: with it the viewsets get per-field filtering,
    // without it they only declare what the built-in backends can honour.
    let filter_backend = python_has_module(runner, &config.python, "django_filters");
    // the openapi schema and docs page come from drf-spectacular when it is there.
    let schema_view = python_has_module(runner, &config.python, "drf_spectacular");

    let app_dir = output_dir.join(app_name);
    let options = DjangoEmitOptions {
        emit_admin: !config.no_admin,
        filter_backend,
        schema_view,
    };
    crate::emit_django_app(&app_dir, inventory, options)?;

    let mut installed = vec!["rest_framework"];
    if filter_backend {
        installed.push("django_filters");
    }
    if schema_view {
        installed.push("drf_spectacular");
    }
    installed.push(app_name);
    ensure_installed_apps_entries(output_dir, project_name, &installed)?;
    ensure_rest_framework_settings(
        output_dir,
        project_name,
        app_name,
        filter_backend,
        schema_view,
    )?;
    ensure_project_urls(output_dir, project_name, app_name)?;
    run_manage_check(runner, output_dir, &config.python)?;
    run_manage_makemigrations(runner, output_dir, &config.python)?;

    let mut loaded = 0;
    if !config.no_migrate {
        run_manage_migrate(runner, output_dir, &config.python)?;
        if !inventory.objects.is_empty() {
            run_manage_loaddata(runner, output_dir, &config.python)?;
            loaded = inventory.objects.len();
        }
    }

    // no_migrate needs the default level: `applied N operations` is otherwise the whole story,
    // and nothing in it says the database was left alone.
    if config.no_migrate {
        tracing::warn!(
            "django app generated at {} (project {}, app {}); {} objects written to the fixture, not loaded (no_migrate)",
            output_dir.display(),
            project_name,
            app_name,
            inventory.objects.len()
        );
    } else {
        tracing::info!(
            "django app generated at {} (project {}, app {}); {loaded} objects loaded",
            output_dir.display(),
            project_name,
            app_name
        );
    }
    Ok(())
}

fn ensure_django_project(runner: &dyn Runner, output_dir: &Path, project_name: &str) -> Result<()> {
    let manage_py = output_dir.join("manage.py");
    let project_dir = output_dir.join(project_name);
    let scaffolded = manage_py
        .try_exists()
        .with_context(|| format!("check {}", manage_py.display()))?
        && project_dir
            .try_exists()
            .with_context(|| format!("check {}", project_dir.display()))?;
    if scaffolded {
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
    let apps_py = output_dir.join(app_name).join("apps.py");
    if apps_py
        .try_exists()
        .with_context(|| format!("check {}", apps_py.display()))?
    {
        return Ok(());
    }
    ensure_app_name_available(runner, output_dir, app_name, python)?;
    runner.run(
        python,
        &["manage.py", "startapp", app_name],
        Some(output_dir),
    )
}

fn python_has_module(runner: &dyn Runner, python: &str, module: &str) -> bool {
    runner
        .run(python, &["-c", &format!("import {module}")], None)
        .is_ok()
}

fn ensure_python_has_django(runner: &dyn Runner, python: &str) -> Result<()> {
    if python_has_module(runner, python, "django") {
        return Ok(());
    }
    Err(anyhow!(
        "django is not available for python version '{}'; install it (pip install django)",
        python
    ))
}

fn ensure_python_has_drf(runner: &dyn Runner, python: &str) -> Result<()> {
    if python_has_module(runner, python, "rest_framework") {
        return Ok(());
    }
    Err(anyhow!(
        "djangorestframework is not available for {}; install it (pip install djangorestframework)",
        python
    ))
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

/// the generated viewsets declare filtering, search, and ordering; drf ignores
/// all three unless a backend is configured, so the settings say so explicitly.
fn ensure_rest_framework_settings(
    output_dir: &Path,
    project_name: &str,
    app_name: &str,
    filter_backend: bool,
    schema_view: bool,
) -> Result<()> {
    let settings_path = output_dir.join(project_name).join("settings.py");
    let contents = fs::read_to_string(&settings_path)
        .with_context(|| format!("read {}", settings_path.display()))?;
    if contents.contains("REST_FRAMEWORK") {
        return Ok(());
    }

    let mut backends = Vec::new();
    if filter_backend {
        backends.push("        \"django_filters.rest_framework.DjangoFilterBackend\",");
    }
    backends.push("        \"rest_framework.filters.SearchFilter\",");
    backends.push("        \"rest_framework.filters.OrderingFilter\",");

    let (schema_class, spectacular) = if schema_view {
        (
            "    \"DEFAULT_SCHEMA_CLASS\": \"drf_spectacular.openapi.AutoSchema\",\n".to_string(),
            format!("\nSPECTACULAR_SETTINGS = {{\n    \"TITLE\": \"{app_name} API\",\n}}\n"),
        )
    } else {
        (String::new(), String::new())
    };
    let block = format!(
        "\nREST_FRAMEWORK = {{\n    \"DEFAULT_FILTER_BACKENDS\": [\n{}\n    ],\n    \
         \"DEFAULT_PAGINATION_CLASS\": \"rest_framework.pagination.PageNumberPagination\",\n    \
         \"PAGE_SIZE\": 50,\n{schema_class}}}\n{spectacular}",
        backends.join("\n")
    );
    let separator = if contents.ends_with('\n') { "" } else { "\n" };
    fs::write(&settings_path, format!("{contents}{separator}{block}"))
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
        let pos = contents
            .find("urlpatterns = [")
            .ok_or_else(|| anyhow!("urls.py missing urlpatterns list"))?;
        let insert_pos = contents[pos..]
            .find(']')
            .ok_or_else(|| anyhow!("urls.py missing urlpatterns closing bracket"))?
            + pos;
        contents.insert_str(
            insert_pos,
            &format!("    path(\"api/\", include(\"{}.urls\")),\n", app_name),
        );
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

/// loaddata keys on the primary key, and the primary key is the ir uid, so
/// re-running converges the app's rows instead of duplicating them.
fn run_manage_loaddata(runner: &dyn Runner, output_dir: &Path, python: &str) -> Result<()> {
    runner.run(
        python,
        &["manage.py", "loaddata", crate::FIXTURE_LABEL],
        Some(output_dir),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use alembic_core::{FieldSchema, FieldType, Key, Object, Schema, TypeName, TypeSchema};
    use serde_json::{json, Value};
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    /// records what run_emit would have shelled out to, and reports every module
    /// as importable so the preflight checks pass without django installed.
    #[derive(Default)]
    struct FakeRunner {
        calls: RefCell<Vec<String>>,
    }

    impl FakeRunner {
        fn calls(&self) -> Vec<String> {
            self.calls.borrow().clone()
        }

        fn ran(&self, needle: &str) -> bool {
            self.calls().iter().any(|call| call.contains(needle))
        }
    }

    impl Runner for FakeRunner {
        fn run(&self, program: &str, args: &[&str], _cwd: Option<&Path>) -> Result<()> {
            self.calls
                .borrow_mut()
                .push(format!("{program} {}", args.join(" ")));
            Ok(())
        }
    }

    /// a project skeleton as django-admin would leave it, so run_emit's
    /// "already there" checks hold and the fake runner never has to create files.
    fn scaffold_project(dir: &Path, project: &str, app: &str) {
        fs::create_dir_all(dir.join(project)).unwrap();
        fs::create_dir_all(dir.join(app)).unwrap();
        fs::write(dir.join("manage.py"), "# manage\n").unwrap();
        fs::write(dir.join(app).join("apps.py"), "# apps\n").unwrap();
        fs::write(
            dir.join(project).join("settings.py"),
            "INSTALLED_APPS = [\n    \"django.contrib.admin\",\n]\n",
        )
        .unwrap();
        fs::write(
            dir.join(project).join("urls.py"),
            "from django.urls import path\n\nurlpatterns = [\n]\n",
        )
        .unwrap();
    }

    fn one_type_inventory(objects: Vec<Object>) -> Inventory {
        let field = |r#type: FieldType| FieldSchema {
            r#type,
            required: true,
            nullable: false,
            description: None,
            format: None,
            pattern: None,
        };
        let mut types = BTreeMap::new();
        types.insert(
            "dcim.site".to_string(),
            TypeSchema {
                key: BTreeMap::from([("slug".to_string(), field(FieldType::Slug))]),
                fields: BTreeMap::from([("name".to_string(), field(FieldType::String))]),
            },
        );
        Inventory {
            scope: None,
            schema: Schema { types },
            objects,
        }
    }

    fn site_object() -> Object {
        Object::new(
            uuid::Uuid::from_u128(1),
            TypeName::new("dcim.site"),
            Key::from(BTreeMap::from([(
                "slug".to_string(),
                Value::String("fra1".to_string()),
            )])),
            alembic_core::JsonMap::from(BTreeMap::from([("name".to_string(), json!("FRA1"))])),
        )
        .unwrap()
    }

    fn config_for(dir: &Path) -> DjangoConfig {
        DjangoConfig {
            output: dir.to_path_buf(),
            project: Some("proj".to_string()),
            app: Some("app".to_string()),
            ..DjangoConfig::default()
        }
    }

    #[test]
    fn run_emit_loads_the_objects_it_emitted() {
        let dir = tempdir().unwrap();
        scaffold_project(dir.path(), "proj", "app");
        let runner = FakeRunner::default();

        run_emit(
            &runner,
            &one_type_inventory(vec![site_object()]),
            &config_for(dir.path()),
        )
        .expect("emit the app");

        assert!(runner.ran("manage.py check"), "{:?}", runner.calls());
        assert!(
            runner.ran("manage.py makemigrations"),
            "{:?}",
            runner.calls()
        );
        assert!(runner.ran("manage.py migrate"), "{:?}", runner.calls());
        // the objects are only in the app once loaddata has run.
        assert!(
            runner.ran("manage.py loaddata alembic"),
            "{:?}",
            runner.calls()
        );

        let settings = fs::read_to_string(dir.path().join("proj").join("settings.py")).unwrap();
        assert!(settings.contains("\"app\","), "{settings}");
        assert!(settings.contains("\"rest_framework\","), "{settings}");
        // the fake runner reports django_filters as importable, so it is wired up.
        assert!(settings.contains("\"django_filters\","), "{settings}");
        assert!(
            settings.contains("DEFAULT_FILTER_BACKENDS"),
            "drf ignores the generated filtering unless a backend is configured: {settings}"
        );
    }

    #[test]
    fn run_emit_skips_loaddata_without_objects() {
        let dir = tempdir().unwrap();
        scaffold_project(dir.path(), "proj", "app");
        let runner = FakeRunner::default();

        run_emit(
            &runner,
            &one_type_inventory(vec![]),
            &config_for(dir.path()),
        )
        .expect("emit the app");

        assert!(!runner.ran("loaddata"), "{:?}", runner.calls());
    }

    #[test]
    fn run_emit_does_not_migrate_when_told_not_to() {
        let dir = tempdir().unwrap();
        scaffold_project(dir.path(), "proj", "app");
        let runner = FakeRunner::default();
        let config = DjangoConfig {
            no_migrate: true,
            ..config_for(dir.path())
        };

        run_emit(&runner, &one_type_inventory(vec![site_object()]), &config).expect("emit the app");

        assert!(!runner.ran("manage.py migrate"), "{:?}", runner.calls());
        assert!(!runner.ran("loaddata"), "{:?}", runner.calls());
    }

    fn setup_project(urls_py: &str) -> (tempfile::TempDir, String) {
        let dir = tempdir().unwrap();
        let project_name = "proj".to_string();
        let project_dir = dir.path().join(&project_name);
        fs::create_dir_all(&project_dir).unwrap();
        fs::write(project_dir.join("urls.py"), urls_py).unwrap();
        (dir, project_name)
    }

    #[test]
    fn errors_when_urlpatterns_list_missing() {
        // a tuple-form urlpatterns lacks the exact `urlpatterns = [` landmark, so
        // the api route cannot be wired in and the step must error rather than
        // write a routeless file and report success.
        let urls_py =
            "from django.urls import path\n\nurlpatterns = (\n    path('admin/', admin.site.urls),\n)\n";
        let (dir, project) = setup_project(urls_py);

        let result = ensure_project_urls(dir.path(), &project, "api_app");

        assert!(
            result.is_err(),
            "expected an error when urls.py has no urlpatterns list to wire the api route into"
        );
    }

    #[test]
    fn wires_route_into_urlpatterns_list() {
        let urls_py =
            "from django.contrib import admin\nfrom django.urls import path\n\nurlpatterns = [\n    path('admin/', admin.site.urls),\n]\n";
        let (dir, project) = setup_project(urls_py);

        ensure_project_urls(dir.path(), &project, "api_app").expect("wiring the api route");

        let written = fs::read_to_string(dir.path().join(&project).join("urls.py")).unwrap();
        assert!(
            written.contains("path(\"api/\", include(\"api_app.urls\"))"),
            "expected the api route to be inserted, got:\n{written}"
        );
    }

    /// a typo'd `no_migrate` must not fall back to the default: that migrates and
    /// loads the inventory into a database the user meant to leave untouched, and
    /// reports success either way.
    #[test]
    fn unknown_config_key_is_rejected() {
        let err =
            serde_json::from_value::<DjangoConfig>(json!({"output": "./out", "no_migrat": true}))
                .expect_err("a typo'd key must be rejected");
        assert!(
            err.to_string().contains("unknown field"),
            "expected an unknown-field error, got: {err}"
        );
    }

    #[test]
    fn known_config_keys_still_parse() {
        let config: DjangoConfig = serde_json::from_value(
            json!({"output": "./out", "no_migrate": true, "python": "python3.12"}),
        )
        .unwrap();
        assert!(config.no_migrate);
        assert_eq!(config.python, "python3.12");
    }

    /// a parent that is a regular file makes the stat fail with `NotADirectory`
    /// rather than answering absent, and scaffolding on that answer would run
    /// `startproject` over a project that is already there.
    #[test]
    fn ensure_django_project_reports_a_stat_it_could_not_make() {
        let dir = tempdir().unwrap();
        let blocker = dir.path().join("blocker");
        fs::write(&blocker, "regular file").unwrap();
        let runner = FakeRunner::default();

        let err = ensure_django_project(&runner, &blocker, "proj").unwrap_err();

        assert!(!runner.ran("startproject"), "must not scaffold on a guess");
        assert!(
            format!("{err:#}").contains("manage.py"),
            "the error must name the path: {err:#}"
        );
    }

    #[test]
    fn ensure_django_app_reports_a_stat_it_could_not_make() {
        let dir = tempdir().unwrap();
        let blocker = dir.path().join("blocker");
        fs::write(&blocker, "regular file").unwrap();
        let runner = FakeRunner::default();

        let err = ensure_django_app(&runner, &blocker, "app", "python3").unwrap_err();

        assert!(!runner.ran("startapp"), "must not scaffold on a guess");
        assert!(
            format!("{err:#}").contains("apps.py"),
            "the error must name the path: {err:#}"
        );
    }

    /// the absent case still scaffolds: `ENOENT` stays `Ok(false)`.
    #[test]
    fn ensure_django_project_and_app_still_scaffold_when_absent() {
        let dir = tempdir().unwrap();
        let runner = FakeRunner::default();

        ensure_django_project(&runner, dir.path(), "proj").unwrap();
        ensure_django_app(&runner, dir.path(), "app", "python3").unwrap();

        assert!(runner.ran("startproject"));
        assert!(runner.ran("startapp"));
    }
}
