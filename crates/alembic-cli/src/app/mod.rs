//! cli entrypoint for alembic.

use alembic_adapter_netbox::NetBoxAdapter;
use alembic_engine::Adapter;
use alembic_engine::{
    apply_plan, apply_projection, build_plan_with_projection, compile_retort, is_brew_format,
    lint_specs, load_brew, load_projection, load_raw_yaml, load_retort, missing_custom_fields,
    missing_tags, plan, DjangoEmitOptions, ExtractReport, Plan, ProjectedInventory, ProjectionSpec,
    Retort, StateStore,
};
use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use serde_json;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;

#[cfg(test)]
mod test_support;

/// top-level cli definition.
#[derive(Parser)]
#[command(name = "alembic")]
#[command(about = "Data-model-first converger + loader for NetBox")]
pub(crate) struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// cli subcommands.
#[derive(Subcommand)]
enum Command {
    Validate {
        #[arg(short = 'f', long)]
        file: PathBuf,
        #[arg(long)]
        retort: Option<PathBuf>,
        #[arg(long)]
        projection: Option<PathBuf>,
    },
    Lint {
        #[arg(long)]
        retort: Option<PathBuf>,
        #[arg(long)]
        projection: Option<PathBuf>,
    },
    Plan {
        #[arg(short = 'f', long)]
        file: PathBuf,
        #[arg(long)]
        retort: Option<PathBuf>,
        #[arg(long)]
        projection: Option<PathBuf>,
        #[arg(long, default_value_t = true)]
        projection_strict: bool,
        #[arg(long, default_value_t = false)]
        projection_propose: bool,
        #[arg(short = 'o', long)]
        output: PathBuf,
        #[arg(long)]
        netbox_url: Option<String>,
        #[arg(long)]
        netbox_token: Option<String>,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value_t = false)]
        allow_delete: bool,
    },
    Apply {
        #[arg(short = 'p', long)]
        plan: PathBuf,
        #[arg(long)]
        netbox_url: Option<String>,
        #[arg(long)]
        netbox_token: Option<String>,
        #[arg(long, default_value_t = false)]
        allow_delete: bool,
    },
    Distill {
        #[arg(short = 'f', long)]
        file: PathBuf,
        #[arg(long)]
        retort: PathBuf,
        #[arg(short = 'o', long)]
        output: PathBuf,
    },
    Project {
        #[arg(short = 'f', long)]
        file: PathBuf,
        #[arg(long)]
        retort: Option<PathBuf>,
        #[arg(long)]
        projection: PathBuf,
        #[arg(short = 'o', long)]
        output: PathBuf,
    },
    Extract {
        #[arg(short = 'o', long)]
        output: PathBuf,
        #[arg(long)]
        retort: Option<PathBuf>,
        #[arg(long)]
        projection: Option<PathBuf>,
        #[arg(long)]
        netbox_url: Option<String>,
        #[arg(long)]
        netbox_token: Option<String>,
    },
    Cast {
        #[command(subcommand)]
        target: CastTarget,
    },
}

/// cast subcommands.
#[derive(Subcommand)]
enum CastTarget {
    Django {
        #[arg(short = 'f', long)]
        file: PathBuf,
        #[arg(short = 'o', long)]
        output: PathBuf,
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        app: Option<String>,
        #[arg(long, default_value = "python3")]
        python: String,
        #[arg(long, default_value_t = false)]
        no_migrate: bool,
        #[arg(long, default_value_t = false)]
        no_admin: bool,
    },
}

pub(crate) async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Validate {
            file,
            retort,
            projection,
        } => {
            let inventory = load_inventory(&file, retort.as_deref())?;
            if let Some(spec) = load_projection_optional(projection.as_deref())? {
                let _ = apply_projection(&spec, &inventory.objects)?;
            }
            alembic_engine::validate(&inventory)?;
            println!("ok");
        }
        Command::Lint { retort, projection } => {
            let retort = load_retort_optional(retort.as_deref())?;
            let projection = load_projection_optional(projection.as_deref())?;
            if retort.is_none() && projection.is_none() {
                return Err(anyhow!("lint requires --retort and/or --projection"));
            }
            let report = lint_specs(retort.as_ref(), projection.as_ref());
            for warning in &report.warnings {
                eprintln!("warning: {warning}");
            }
            for error in &report.errors {
                eprintln!("error: {error}");
            }
            if !report.is_ok() {
                return Err(anyhow!("lint failed"));
            }
            if report.warnings.is_empty() {
                println!("ok");
            } else {
                println!("ok (with warnings)");
            }
        }
        Command::Plan {
            file,
            retort,
            projection,
            projection_strict,
            projection_propose,
            output,
            netbox_url,
            netbox_token,
            dry_run,
            allow_delete,
        } => {
            let inventory = load_inventory(&file, retort.as_deref())?;
            let mut state = load_state()?;
            let projection = load_projection_optional(projection.as_deref())?;
            let (url, token) = netbox_credentials(netbox_url, netbox_token)?;
            let adapter = NetBoxAdapter::new(&url, &token, state.clone())?;
            let plan = if projection_propose {
                build_plan_with_proposal(
                    &adapter,
                    &inventory,
                    &mut state,
                    allow_delete,
                    projection.as_ref(),
                    projection_strict,
                )
                .await?
            } else {
                build_plan_with_projection(
                    &adapter,
                    &inventory,
                    &mut state,
                    allow_delete,
                    projection.as_ref(),
                    projection_strict,
                )
                .await?
            };
            if dry_run {
                let raw = serde_json::to_string_pretty(&plan)?;
                println!("{raw}");
            } else {
                write_plan(&output, &plan)?;
                state.save()?;
                println!("plan written to {}", output.display());
            }
        }
        Command::Apply {
            plan,
            netbox_url,
            netbox_token,
            allow_delete,
        } => {
            let mut state = load_state()?;
            let (url, token) = netbox_credentials(netbox_url, netbox_token)?;
            let adapter = NetBoxAdapter::new(&url, &token, state.clone())?;
            let plan = read_plan(&plan)?;
            let report = apply_plan(&adapter, &plan, &mut state, allow_delete).await?;
            state.save()?;
            println!("applied {} operations", report.applied.len());
        }
        Command::Distill {
            file,
            retort,
            output,
        } => {
            let raw = load_raw_yaml(&file)?;
            if is_brew_format(&raw) {
                return Err(anyhow!("distill expects raw yaml without objects"));
            }
            let retort = load_retort(&retort)?;
            let inventory = compile_retort(&raw, &retort)?;
            write_inventory(&output, &inventory)?;
            println!("ir written to {}", output.display());
        }
        Command::Project {
            file,
            retort,
            projection,
            output,
        } => {
            let inventory = load_inventory(&file, retort.as_deref())?;
            let projection = load_projection(&projection)?;
            let projected = apply_projection(&projection, &inventory.objects)?;
            write_projected(&output, &projected)?;
            println!("projected ir written to {}", output.display());
        }
        Command::Extract {
            output,
            retort,
            projection,
            netbox_url,
            netbox_token,
        } => {
            if let Some(retort_path) = retort.as_deref() {
                let _ = load_retort(retort_path)?;
                eprintln!("warning: retort inversion is not implemented; ignoring --retort");
            }
            let projection = load_projection_optional(projection.as_deref())?;
            let (url, token) = netbox_credentials(netbox_url, netbox_token)?;
            let adapter = NetBoxAdapter::new(&url, &token, load_state()?)?;
            let ExtractReport {
                inventory,
                warnings,
            } = alembic_engine::extract_inventory(&adapter, projection.as_ref()).await?;
            for warning in warnings {
                eprintln!("warning: {warning}");
            }
            write_inventory(&output, &inventory)?;
            println!("inventory written to {}", output.display());
        }
        Command::Cast { target } => match target {
            CastTarget::Django {
                file,
                output,
                project,
                app,
                python,
                no_migrate,
                no_admin,
            } => {
                let runner = CommandRunner::new();
                let config = CastDjangoConfig {
                    file,
                    output,
                    project,
                    app,
                    python,
                    no_migrate,
                    no_admin,
                };
                run_cast_django(&runner, config)?;
            }
        },
    }

    Ok(())
}

/// load state from the default path.
fn load_state() -> Result<StateStore> {
    let path = state_path(Path::new("."));
    StateStore::load(path)
}

/// build the default state store path under the workspace root.
fn state_path(root: &Path) -> PathBuf {
    root.join(".alembic").join("state.json")
}

/// write a plan file to disk.
fn write_plan(path: &Path, plan: &Plan) -> Result<()> {
    let raw = serde_json::to_string_pretty(plan)?;
    fs::write(path, raw).with_context(|| format!("write plan: {}", path.display()))
}

fn write_inventory(path: &Path, inventory: &alembic_core::Inventory) -> Result<()> {
    let raw = serde_json::to_string_pretty(inventory)?;
    fs::write(path, raw).with_context(|| format!("write ir: {}", path.display()))
}

fn write_projected(path: &Path, projected: &ProjectedInventory) -> Result<()> {
    let raw = serde_json::to_string_pretty(projected)?;
    fs::write(path, raw).with_context(|| format!("write projected ir: {}", path.display()))
}

/// read a plan file from disk.
fn read_plan(path: &Path) -> Result<Plan> {
    let raw = fs::read_to_string(path).with_context(|| format!("read plan: {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("parse plan: {}", path.display()))
}

/// resolve netbox credentials from flags or environment.
fn netbox_credentials(url: Option<String>, token: Option<String>) -> Result<(String, String)> {
    let url = url
        .or_else(|| std::env::var("NETBOX_URL").ok())
        .ok_or_else(|| anyhow!("missing --netbox-url or NETBOX_URL"))?;
    let token = token
        .or_else(|| std::env::var("NETBOX_TOKEN").ok())
        .ok_or_else(|| anyhow!("missing --netbox-token or NETBOX_TOKEN"))?;
    Ok((url, token))
}

fn load_inventory(path: &Path, retort: Option<&Path>) -> Result<alembic_core::Inventory> {
    let raw = load_raw_yaml(path)?;
    if is_brew_format(&raw) {
        if retort.is_some() {
            eprintln!("warning: retort ignored for brew input");
        }
        return load_brew(path);
    }

    let retort_path =
        retort.ok_or_else(|| anyhow!("raw yaml requires --retort to compile inventory"))?;
    let retort = load_retort(retort_path)?;
    compile_retort(&raw, &retort)
}

fn load_brew_only(path: &Path) -> Result<alembic_core::Inventory> {
    let raw = load_raw_yaml(path)?;
    if !is_brew_format(&raw) {
        return Err(anyhow!(
            "cast requires a brew/ir yaml file with objects; run distill first"
        ));
    }
    load_brew(path)
}

fn load_projection_optional(path: Option<&Path>) -> Result<Option<ProjectionSpec>> {
    match path {
        Some(path) => Ok(Some(load_projection(path)?)),
        None => Ok(None),
    }
}

fn load_retort_optional(path: Option<&Path>) -> Result<Option<Retort>> {
    match path {
        Some(path) => Ok(Some(load_retort(path)?)),
        None => Ok(None),
    }
}

trait Runner {
    fn run(&self, program: &str, args: &[&str], cwd: Option<&Path>) -> Result<()>;
}

struct CommandRunner;

impl CommandRunner {
    fn new() -> Self {
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
            "command failed: {program} {}\nstdout:\n{stdout}\nstderr:\n{stderr}",
            args.join(" ")
        ))
    }
}

impl Runner for CommandRunner {
    fn run(&self, program: &str, args: &[&str], cwd: Option<&Path>) -> Result<()> {
        self.run_command(program, args, cwd)
    }
}

struct CastDjangoConfig {
    file: PathBuf,
    output: PathBuf,
    project: Option<String>,
    app: Option<String>,
    python: String,
    no_migrate: bool,
    no_admin: bool,
}

fn run_cast_django(runner: &dyn Runner, config: CastDjangoConfig) -> Result<()> {
    let inventory = load_brew_only(&config.file)?;
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
    alembic_engine::emit_django_app(&app_dir, &inventory, options)?;
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
            "djangorestframework is not available for {}; install it (pip install djangorestframework)",
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
        let quoted = format!("\"{entry}\"");
        let single_quoted = format!("'{entry}'");
        if contents.contains(&quoted) || contents.contains(&single_quoted) {
            continue;
        }
        let end = contents[start..]
            .find(']')
            .ok_or_else(|| anyhow!("settings.py missing INSTALLED_APPS closing bracket"))?
            + start;
        contents.insert_str(end, &format!("    \"{entry}\",\n"));
    }
    fs::write(&settings_path, contents)
        .with_context(|| format!("write {}", settings_path.display()))?;
    Ok(())
}

fn ensure_project_urls(output_dir: &Path, project_name: &str, app_name: &str) -> Result<()> {
    let urls_path = output_dir.join(project_name).join("urls.py");
    let mut contents =
        fs::read_to_string(&urls_path).with_context(|| format!("read {}", urls_path.display()))?;

    if contents.contains("include(") && contents.contains(&format!("{app_name}.urls")) {
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
        contents = format!("from django.urls import include, path\n{contents}");
    }

    if !contents.contains(&format!("include(\"{app_name}.urls\")"))
        && !contents.contains(&format!("include('{app_name}.urls')"))
    {
        if let Some(pos) = contents.find("urlpatterns = [") {
            let insert_pos = contents[pos..]
                .find(']')
                .ok_or_else(|| anyhow!("urls.py missing urlpatterns closing bracket"))?
                + pos;
            contents.insert_str(
                insert_pos,
                &format!("    path(\"api/\", include(\"{app_name}.urls\")),\n"),
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

async fn build_plan_with_proposal(
    adapter: &NetBoxAdapter,
    inventory: &alembic_core::Inventory,
    state: &mut StateStore,
    allow_delete: bool,
    projection: Option<&ProjectionSpec>,
    projection_strict: bool,
) -> Result<Plan> {
    let Some(spec) = projection else {
        return build_plan_with_projection(
            adapter,
            inventory,
            state,
            allow_delete,
            None,
            projection_strict,
        )
        .await;
    };
    let projected = apply_projection(spec, &inventory.objects)?;
    let kinds: Vec<_> = projected
        .objects
        .iter()
        .map(|o| o.base.kind.clone())
        .collect();
    let mut observed = adapter.observe(&kinds).await?;
    let missing = missing_custom_fields(spec, &inventory.objects, &observed.capabilities)?;
    if !missing.is_empty() {
        eprintln!("projection proposal: missing custom fields");
        for entry in &missing {
            eprintln!(
                "- rule {} (kind {}, x {}) -> field {}",
                entry.rule, entry.kind, entry.x_key, entry.field
            );
        }
        eprintln!();
        if confirm("create missing custom fields in netbox? [y/N] ")? {
            adapter.create_custom_fields(&missing).await?;
            for entry in &missing {
                observed
                    .capabilities
                    .custom_fields_by_kind
                    .entry(entry.kind.clone())
                    .or_default()
                    .insert(entry.field.clone());
            }
        }
    }
    let missing_tags = missing_tags(spec, &inventory.objects, &observed.capabilities)?;
    if !missing_tags.is_empty() {
        eprintln!("projection proposal: missing tags");
        for entry in &missing_tags {
            eprintln!(
                "- rule {} (kind {}, x {}) -> tag {}",
                entry.rule, entry.kind, entry.x_key, entry.tag
            );
        }
        eprintln!();
        if confirm("create missing tags in netbox? [y/N] ")? {
            let tags: Vec<String> = missing_tags.iter().map(|entry| entry.tag.clone()).collect();
            adapter.create_tags(&tags).await?;
            for tag in tags {
                observed.capabilities.tags.insert(tag);
            }
        }
    }
    if projection_strict {
        alembic_engine::validate_projection_strict(
            spec,
            &inventory.objects,
            &observed.capabilities,
        )?;
    }
    Ok(plan(&projected, &observed, state, allow_delete))
}

fn confirm(prompt: &str) -> Result<bool> {
    use std::io::{self, Write};
    let mut stdout = io::stdout();
    stdout.write_all(prompt.as_bytes())?;
    stdout.flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(matches!(input.trim().to_lowercase().as_str(), "y" | "yes"))
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;
    use alembic_engine::Op;
    use tempfile::tempdir;

    #[test]
    fn state_path_uses_dot_alembic() {
        let root = Path::new("/tmp/example");
        let path = state_path(root);
        assert!(path.ends_with(".alembic/state.json"));
    }

    #[test]
    fn netbox_credentials_prefers_args() {
        let creds = netbox_credentials(
            Some("http://example".to_string()),
            Some("token".to_string()),
        )
        .unwrap();
        assert_eq!(creds.0, "http://example");
        assert_eq!(creds.1, "token");
    }

    #[test]
    fn netbox_credentials_from_env() {
        let _guard = env_lock().lock().unwrap();
        let old_url = std::env::var("NETBOX_URL").ok();
        let old_token = std::env::var("NETBOX_TOKEN").ok();
        std::env::set_var("NETBOX_URL", "http://env");
        std::env::set_var("NETBOX_TOKEN", "envtoken");

        let result = std::panic::catch_unwind(|| {
            let creds = netbox_credentials(None, None).unwrap();
            assert_eq!(creds.0, "http://env");
            assert_eq!(creds.1, "envtoken");
        });

        if let Some(value) = old_url {
            std::env::set_var("NETBOX_URL", value);
        } else {
            std::env::remove_var("NETBOX_URL");
        }
        if let Some(value) = old_token {
            std::env::set_var("NETBOX_TOKEN", value);
        } else {
            std::env::remove_var("NETBOX_TOKEN");
        }

        assert!(result.is_ok());
    }

    #[test]
    fn plan_roundtrip_io() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("plan.json");
        let plan = Plan {
            ops: vec![Op::Delete {
                uid: uuid::Uuid::from_u128(1),
                kind: alembic_core::Kind::DcimSite,
                key: "site=fra1".to_string(),
                backend_id: Some(1),
            }],
        };

        write_plan(&path, &plan).unwrap();
        let loaded = read_plan(&path).unwrap();
        assert_eq!(loaded.ops.len(), 1);
    }

    #[test]
    fn netbox_credentials_missing_is_error() {
        let _guard = env_lock().lock().unwrap();
        let old_url = std::env::var("NETBOX_URL").ok();
        let old_token = std::env::var("NETBOX_TOKEN").ok();
        std::env::remove_var("NETBOX_URL");
        std::env::remove_var("NETBOX_TOKEN");

        let result = netbox_credentials(None, None);
        assert!(result.is_err());

        if let Some(value) = old_url {
            std::env::set_var("NETBOX_URL", value);
        }
        if let Some(value) = old_token {
            std::env::set_var("NETBOX_TOKEN", value);
        }
    }

    #[test]
    fn cast_django_runs_migrations_by_default() {
        let dir = tempdir().unwrap();
        let output = dir.path().join("out");
        std::fs::create_dir_all(&output).unwrap();
        std::fs::write(output.join("manage.py"), "").unwrap();
        write_settings(&output, "alembic_project");
        let brew = write_minimal_brew(dir.path());

        let runner = MockRunner::new();
        run_cast_django(
            &runner,
            CastDjangoConfig {
                file: brew,
                output: output.clone(),
                project: Some("alembic_project".to_string()),
                app: Some("alembic_app".to_string()),
                python: "python3".to_string(),
                no_migrate: false,
                no_admin: false,
            },
        )
        .unwrap();

        let calls = runner.calls();
        let called: Vec<(String, Vec<String>)> = calls
            .into_iter()
            .map(|call| (call.program, call.args))
            .collect();

        assert!(called
            .iter()
            .any(|call| { call.1 == vec!["-c".to_string(), "import django".to_string()] }));
        assert!(called
            .iter()
            .any(|call| { call.1 == vec!["-c".to_string(), "import rest_framework".to_string()] }));
        assert!(called
            .iter()
            .any(|call| call.1.iter().any(|arg| arg.contains("importlib.util"))));
        assert!(called.iter().any(|call| {
            call.1
                == vec![
                    "manage.py".to_string(),
                    "startapp".to_string(),
                    "alembic_app".to_string(),
                ]
        }));
        assert!(called
            .iter()
            .any(|call| { call.1 == vec!["manage.py".to_string(), "check".to_string()] }));
        assert!(called
            .iter()
            .any(|call| { call.1 == vec!["manage.py".to_string(), "makemigrations".to_string()] }));
        assert!(called
            .iter()
            .any(|call| call.1 == vec!["manage.py".to_string(), "migrate".to_string()]));

        let settings = std::fs::read_to_string(output.join("alembic_project/settings.py")).unwrap();
        assert!(settings.contains("\"alembic_app\""));
        assert!(settings.contains("\"rest_framework\""));
        let urls = std::fs::read_to_string(output.join("alembic_project/urls.py")).unwrap();
        assert!(urls.contains("include(\"alembic_app.urls\")"));
    }

    #[test]
    fn cast_django_skips_migrate_with_flag() {
        let dir = tempdir().unwrap();
        let output = dir.path().join("out");
        std::fs::create_dir_all(&output).unwrap();
        std::fs::write(output.join("manage.py"), "").unwrap();
        write_settings(&output, "alembic_project");
        let brew = write_minimal_brew(dir.path());

        let runner = MockRunner::new();
        run_cast_django(
            &runner,
            CastDjangoConfig {
                file: brew,
                output: output.clone(),
                project: Some("alembic_project".to_string()),
                app: Some("alembic_app".to_string()),
                python: "python3".to_string(),
                no_migrate: true,
                no_admin: false,
            },
        )
        .unwrap();

        let calls = runner.calls();
        assert!(calls.iter().any(|call| {
            call.args == vec!["manage.py".to_string(), "makemigrations".to_string()]
        }));
        assert!(!calls
            .iter()
            .any(|call| call.args == vec!["manage.py".to_string(), "migrate".to_string()]));
    }

    #[test]
    fn cast_django_integration_writes_generated_files() {
        let dir = tempdir().unwrap();
        let output = dir.path().join("out");
        let brew = write_site_brew(dir.path());
        let runner = FixtureRunner::new(output.clone());

        run_cast_django(
            &runner,
            CastDjangoConfig {
                file: brew,
                output: output.clone(),
                project: Some("alembic_project".to_string()),
                app: Some("alembic_app".to_string()),
                python: "python3".to_string(),
                no_migrate: true,
                no_admin: false,
            },
        )
        .unwrap();

        let app_dir = output.join("alembic_app");
        assert!(app_dir.join("generated_models.py").exists());
        assert!(app_dir.join("generated_admin.py").exists());
        assert!(app_dir.join("generated_serializers.py").exists());
        assert!(app_dir.join("generated_views.py").exists());
        assert!(app_dir.join("generated_urls.py").exists());
        let models = std::fs::read_to_string(app_dir.join("models.py")).unwrap();
        assert!(models.contains("generated_models"));
        let admin = std::fs::read_to_string(app_dir.join("admin.py")).unwrap();
        assert!(admin.contains("generated_admin"));
        let views = std::fs::read_to_string(app_dir.join("views.py")).unwrap();
        assert!(views.contains("generated_views"));
        let urls = std::fs::read_to_string(app_dir.join("urls.py")).unwrap();
        assert!(urls.contains("generated_urls"));

        let settings = std::fs::read_to_string(output.join("alembic_project/settings.py")).unwrap();
        assert!(settings.contains("\"alembic_app\""));
        assert!(settings.contains("\"rest_framework\""));
        let urls = std::fs::read_to_string(output.join("alembic_project/urls.py")).unwrap();
        assert!(urls.contains("include(\"alembic_app.urls\")"));

        let calls = runner.calls();
        assert!(calls.iter().any(|call| {
            call.args == vec!["manage.py".to_string(), "makemigrations".to_string()]
        }));
    }

    #[test]
    fn read_plan_invalid_json_errors() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("plan.json");
        std::fs::write(&path, "not-json").unwrap();
        assert!(read_plan(&path).is_err());
    }

    #[test]
    fn load_inventory_brew_ignores_retort() {
        let dir = tempdir().unwrap();
        let brew = dir.path().join("brew.yaml");
        let retort = dir.path().join("retort.yaml");
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
        std::fs::write(
            &retort,
            r#"
version: 1
rules: []
"#,
        )
        .unwrap();

        let inventory = load_inventory(&brew, Some(&retort)).unwrap();
        assert_eq!(inventory.objects.len(), 1);
    }

    #[test]
    fn load_inventory_raw_requires_retort() {
        let dir = tempdir().unwrap();
        let raw = dir.path().join("raw.yaml");
        std::fs::write(
            &raw,
            r#"
sites:
  - slug: fra1
    name: FRA1
"#,
        )
        .unwrap();
        let err = load_inventory(&raw, None).unwrap_err();
        assert!(err.to_string().contains("raw yaml requires --retort"));
    }

    #[test]
    fn load_inventory_raw_with_retort() {
        let dir = tempdir().unwrap();
        let raw = dir.path().join("raw.yaml");
        let retort = dir.path().join("retort.yaml");
        std::fs::write(
            &raw,
            r#"
sites:
  - slug: fra1
    name: FRA1
"#,
        )
        .unwrap();
        std::fs::write(
            &retort,
            r#"
version: 1
rules:
  - name: sites
    select: /sites/*
    emit:
      kind: dcim.site
      key: "site=${slug}"
      vars:
        slug: { from: .slug, required: true }
        name: { from: .name, required: true }
      attrs:
        name: ${name}
        slug: ${slug}
"#,
        )
        .unwrap();

        let inventory = load_inventory(&raw, Some(&retort)).unwrap();
        assert_eq!(inventory.objects.len(), 1);
        assert_eq!(inventory.objects[0].kind.as_string(), "dcim.site");
    }

    #[test]
    fn write_inventory_and_projected() {
        let dir = tempdir().unwrap();
        let inv_path = dir.path().join("ir.json");
        let proj_path = dir.path().join("projected.json");
        let inventory = alembic_core::Inventory {
            objects: Vec::new(),
        };
        let projected = ProjectedInventory {
            objects: Vec::new(),
        };
        write_inventory(&inv_path, &inventory).unwrap();
        write_projected(&proj_path, &projected).unwrap();
        let inv = std::fs::read_to_string(inv_path).unwrap();
        let proj = std::fs::read_to_string(proj_path).unwrap();
        assert!(inv.contains("\"objects\""));
        assert!(proj.contains("\"objects\""));
    }

    #[tokio::test]
    async fn run_validate_brew() {
        let dir = tempdir().unwrap();
        let brew = dir.path().join("brew.yaml");
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

        let cli = Cli {
            command: Command::Validate {
                file: brew,
                retort: None,
                projection: None,
            },
        };
        run(cli).await.unwrap();
    }

    #[tokio::test]
    async fn run_distill_raw() {
        let _guard = cwd_lock().lock().await;
        let dir = tempdir().unwrap();
        let raw = dir.path().join("raw.yaml");
        let retort = dir.path().join("retort.yaml");
        let out = dir.path().join("ir.json");
        std::fs::write(
            &raw,
            r#"
sites:
  - slug: fra1
    name: FRA1
"#,
        )
        .unwrap();
        std::fs::write(
            &retort,
            r#"
version: 1
rules:
  - name: sites
    select: /sites/*
    emit:
      kind: dcim.site
      key: "site=${slug}"
      vars:
        slug: { from: .slug, required: true }
        name: { from: .name, required: true }
      attrs:
        name: ${name}
        slug: ${slug}
"#,
        )
        .unwrap();
        let cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir.path()).unwrap();

        let cli = Cli {
            command: Command::Distill {
                file: raw,
                retort,
                output: out.clone(),
            },
        };
        run(cli).await.unwrap();
        let raw = std::fs::read_to_string(out).unwrap();
        assert!(raw.contains("\"objects\""));
        std::env::set_current_dir(cwd).unwrap();
    }

    #[tokio::test]
    async fn run_project_raw() {
        let _guard = cwd_lock().lock().await;
        let dir = tempdir().unwrap();
        let raw = dir.path().join("raw.yaml");
        let retort = dir.path().join("retort.yaml");
        let projection = dir.path().join("projection.yaml");
        let out = dir.path().join("projected.json");
        std::fs::write(
            &raw,
            r#"
sites:
  - slug: fra1
    name: FRA1
"#,
        )
        .unwrap();
        std::fs::write(
            &retort,
            r#"
version: 1
rules:
  - name: sites
    select: /sites/*
    emit:
      kind: dcim.site
      key: "site=${slug}"
      vars:
        slug: { from: .slug, required: true }
        name: { from: .name, required: true }
      attrs:
        name: ${name}
        slug: ${slug}
"#,
        )
        .unwrap();
        std::fs::write(
            &projection,
            r#"
version: 1
backend: netbox
rules: []
"#,
        )
        .unwrap();
        let cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir.path()).unwrap();

        let cli = Cli {
            command: Command::Project {
                file: raw,
                retort: Some(retort),
                projection,
                output: out.clone(),
            },
        };
        run(cli).await.unwrap();
        let raw = std::fs::read_to_string(out).unwrap();
        assert!(raw.contains("\"objects\""));
        std::env::set_current_dir(cwd).unwrap();
    }

    #[tokio::test]
    async fn run_plan_missing_credentials_errors() {
        let _guard = cwd_lock().lock().await;
        let dir = tempdir().unwrap();
        let brew = dir.path().join("brew.yaml");
        let out = dir.path().join("plan.json");
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
        let cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir.path()).unwrap();

        let cli = Cli {
            command: Command::Plan {
                file: brew,
                retort: None,
                projection: None,
                projection_strict: true,
                projection_propose: false,
                output: out,
                netbox_url: None,
                netbox_token: None,
                dry_run: false,
                allow_delete: false,
            },
        };
        let err = run(cli).await.unwrap_err();
        assert!(err.to_string().contains("missing --netbox-url"));
        std::env::set_current_dir(cwd).unwrap();
    }

    #[tokio::test]
    async fn run_apply_missing_credentials_errors() {
        let _guard = cwd_lock().lock().await;
        let dir = tempdir().unwrap();
        let plan_path = dir.path().join("plan.json");
        std::fs::write(&plan_path, r#"{ "ops": [] }"#).unwrap();
        let cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir.path()).unwrap();

        let cli = Cli {
            command: Command::Apply {
                plan: plan_path,
                netbox_url: None,
                netbox_token: None,
                allow_delete: false,
            },
        };
        let err = run(cli).await.unwrap_err();
        assert!(err.to_string().contains("missing --netbox-url"));
        std::env::set_current_dir(cwd).unwrap();
    }

    #[tokio::test]
    async fn run_extract_missing_credentials_errors() {
        let _guard = cwd_lock().lock().await;
        let dir = tempdir().unwrap();
        let out = dir.path().join("inventory.yaml");
        let cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir.path()).unwrap();

        let cli = Cli {
            command: Command::Extract {
                output: out,
                retort: None,
                projection: None,
                netbox_url: None,
                netbox_token: None,
            },
        };
        let err = run(cli).await.unwrap_err();
        assert!(err.to_string().contains("missing --netbox-url"));
        std::env::set_current_dir(cwd).unwrap();
    }
}
