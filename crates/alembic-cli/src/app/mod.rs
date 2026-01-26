//! cli entrypoint for alembic.

use alembic_adapter_nautobot::NautobotAdapter;
use alembic_adapter_netbox::NetBoxAdapter;
use alembic_core::ValidationError;
use alembic_engine::Adapter;
use alembic_engine::{
    apply_plan, apply_projection, build_plan_with_projection, compile_retort, is_brew_format,
    lint_specs, load_brew, load_projection, load_raw_yaml, load_retort, missing_custom_fields,
    missing_tags, plan, DjangoEmitOptions, ExtractReport, Plan, ProjectedInventory, ProjectionSpec,
    Retort, StateStore,
};
use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;

#[cfg(test)]
mod test_support;

/// top-level cli definition.
#[derive(Parser)]
#[command(name = "alembic")]
#[command(about = "Data-model-first converger + loader for DCIM/IPAM")]
pub(crate) struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Backend {
    Netbox,
    Nautobot,
    Generic,
    Peeringdb,
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
        #[arg(long, default_value = "netbox")]
        backend: Backend,
        #[arg(long)]
        netbox_url: Option<String>,
        #[arg(long)]
        netbox_token: Option<String>,
        #[arg(long)]
        nautobot_url: Option<String>,
        #[arg(long)]
        nautobot_token: Option<String>,
        #[arg(long)]
        generic_config: Option<PathBuf>,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value_t = false)]
        allow_delete: bool,
    },
    Apply {
        #[arg(short = 'p', long)]
        plan: PathBuf,
        #[arg(long, default_value = "netbox")]
        backend: Backend,
        #[arg(long)]
        netbox_url: Option<String>,
        #[arg(long)]
        netbox_token: Option<String>,
        #[arg(long)]
        nautobot_url: Option<String>,
        #[arg(long)]
        nautobot_token: Option<String>,
        #[arg(long)]
        generic_config: Option<PathBuf>,
        #[arg(long, default_value_t = false)]
        allow_delete: bool,
        #[arg(short = 'i', long, default_value_t = false)]
        interactive: bool,
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
        #[arg(long, default_value = "netbox")]
        backend: Backend,
        #[arg(long)]
        netbox_url: Option<String>,
        #[arg(long)]
        netbox_token: Option<String>,
        #[arg(long)]
        nautobot_url: Option<String>,
        #[arg(long)]
        nautobot_token: Option<String>,
        #[arg(long)]
        generic_config: Option<PathBuf>,
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
            let report = alembic_engine::validate(&inventory);
            if report.is_ok() {
                println!("ok");
            } else {
                let content = fs::read_to_string(&file).ok();
                for error in report.errors {
                    let line_info = if let Some(c) = &content {
                        find_error_location(c, &error)
                    } else {
                        None
                    };

                    match line_info {
                        Some(line) => eprintln!("{}:{}: error: {}", file.display(), line, error),
                        None => eprintln!("{}: error: {}", file.display(), error),
                    }
                }
                return Err(anyhow!("validation failed"));
            }
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
            backend,
            netbox_url,
            netbox_token,
            nautobot_url,
            nautobot_token,
            generic_config,
            dry_run,
            allow_delete,
        } => {
            let inventory = load_inventory(&file, retort.as_deref())?;
            let mut state = load_state()?;
            let projection = load_projection_optional(projection.as_deref())?;

            let adapter = create_adapter(
                backend,
                netbox_url.clone(),
                netbox_token.clone(),
                nautobot_url,
                nautobot_token,
                generic_config,
                state.clone(),
            )?;

            let plan = if projection_propose {
                build_plan_with_proposal(
                    adapter.as_ref(),
                    &inventory,
                    &mut state,
                    allow_delete,
                    projection.as_ref(),
                    projection_strict,
                    backend,
                )
                .await?
            } else {
                build_plan_with_projection(
                    adapter.as_ref(),
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
                if let Some(s) = &plan.summary {
                    println!(
                        "plan: {} to create, {} to update, {} to delete",
                        s.create, s.update, s.delete
                    );
                }
                println!("plan written to {}", output.display());
            }
        }
        Command::Apply {
            plan,
            backend,
            netbox_url,
            netbox_token,
            nautobot_url,
            nautobot_token,
            generic_config,
            allow_delete,
            interactive,
        } => {
            let mut state = load_state()?;
            let adapter = create_adapter(
                backend,
                netbox_url,
                netbox_token,
                nautobot_url,
                nautobot_token,
                generic_config,
                state.clone(),
            )?;
            let plan = read_plan(&plan)?;

            if interactive {
                let ordered = alembic_engine::sort_ops_for_apply(&plan.ops);
                let mut applied_count = 0;
                for op in ordered {
                    let prompt = match &op {
                        alembic_engine::Op::Create {
                            type_name, desired, ..
                        } => format!(
                            "create {} {}? [y/N] ",
                            type_name,
                            alembic_core::key_string(&desired.base.key)
                        ),
                        alembic_engine::Op::Update {
                            type_name, desired, ..
                        } => format!(
                            "update {} {}? [y/N] ",
                            type_name,
                            alembic_core::key_string(&desired.base.key)
                        ),
                        alembic_engine::Op::Delete { type_name, key, .. } => format!(
                            "delete {} {}? [y/N] ",
                            type_name,
                            alembic_core::key_string(key)
                        ),
                    };
                    if confirm(&prompt)? {
                        let report = adapter.apply(&plan.schema, &[op]).await?;
                        for applied in &report.applied {
                            if let Some(backend_id) = &applied.backend_id {
                                state.set_backend_id(
                                    applied.type_name.clone(),
                                    applied.uid,
                                    backend_id.clone(),
                                );
                            } else {
                                state.remove_backend_id(applied.type_name.clone(), applied.uid);
                            }
                        }
                        applied_count += report.applied.len();
                    }
                }
                state.save()?;
                println!("applied {} operations", applied_count);
            } else {
                let report = apply_plan(adapter.as_ref(), &plan, &mut state, allow_delete).await?;
                state.save()?;
                println!("applied {} operations", report.applied.len());
            }
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
            backend,
            netbox_url,
            netbox_token,
            nautobot_url,
            nautobot_token,
            generic_config,
        } => {
            let retort_path = retort
                .as_deref()
                .ok_or_else(|| anyhow!("extract requires a retort with schema"))?;
            let retort = load_retort(retort_path)?;
            eprintln!("warning: retort inversion is not implemented; using schema only");
            let projection = load_projection_optional(projection.as_deref())?;
            let adapter = create_adapter(
                backend,
                netbox_url,
                netbox_token,
                nautobot_url,
                nautobot_token,
                generic_config,
                load_state()?,
            )?;
            let ExtractReport {
                inventory,
                warnings,
            } = alembic_engine::extract_inventory(
                adapter.as_ref(),
                &retort.schema,
                projection.as_ref(),
            )
            .await?;
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

fn create_adapter(
    backend: Backend,
    netbox_url: Option<String>,
    netbox_token: Option<String>,
    nautobot_url: Option<String>,
    nautobot_token: Option<String>,
    generic_config: Option<PathBuf>,
    state: StateStore,
) -> Result<Box<dyn Adapter>> {
    match backend {
        Backend::Netbox => {
            let (url, token) = resolve_credentials("NETBOX", netbox_url, netbox_token)?;
            Ok(Box::new(NetBoxAdapter::new(&url, &token, state)?))
        }
        Backend::Nautobot => {
            let (url, token) = resolve_credentials("NAUTOBOT", nautobot_url, nautobot_token)?;
            Ok(Box::new(NautobotAdapter::new(&url, &token, state)?))
        }
        Backend::Generic => {
            let path = generic_config
                .ok_or_else(|| anyhow!("--generic-config required for generic backend"))?;
            let content = fs::read_to_string(&path)
                .with_context(|| format!("read generic config: {}", path.display()))?;
            let config: alembic_adapter_generic::GenericConfig = serde_yaml::from_str(&content)
                .with_context(|| format!("parse generic config: {}", path.display()))?;
            Ok(Box::new(alembic_adapter_generic::GenericAdapter::new(
                config, state,
            )?))
        }
        Backend::Peeringdb => {
            // API key is read from PEERINGDB_API_KEY env var by the peeringdb-rs crate
            Ok(Box::new(alembic_adapter_peeringdb::PeeringDBAdapter::new()))
        }
    }
}

fn resolve_credentials(
    prefix: &str,
    url: Option<String>,
    token: Option<String>,
) -> Result<(String, String)> {
    let env_url = format!("{}_URL", prefix);
    let env_token = format!("{}_TOKEN", prefix);
    let url = url
        .or_else(|| std::env::var(&env_url).ok())
        .ok_or_else(|| anyhow!("missing --{}-url or {}", prefix.to_lowercase(), env_url))?;
    let token = token
        .or_else(|| std::env::var(&env_token).ok())
        .ok_or_else(|| anyhow!("missing --{}-token or {}", prefix.to_lowercase(), env_token))?;
    Ok((url, token))
}

fn load_state() -> Result<StateStore> {
    let path = state_path(Path::new("."));
    StateStore::load(path)
}

fn state_path(root: &Path) -> PathBuf {
    root.join(".alembic").join("state.json")
}

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

fn read_plan(path: &Path) -> Result<Plan> {
    let raw = fs::read_to_string(path).with_context(|| format!("read plan: {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("parse plan: {}", path.display()))
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
        && !contents.contains(&format!("include('{}'.urls')", app_name))
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

async fn build_plan_with_proposal(
    adapter: &dyn Adapter,
    inventory: &alembic_core::Inventory,
    state: &mut StateStore,
    allow_delete: bool,
    projection: Option<&ProjectionSpec>,
    projection_strict: bool,
    backend: Backend,
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
    let types: Vec<_> = projected
        .objects
        .iter()
        .map(|o| o.base.type_name.clone())
        .collect();
    let mut observed = adapter.observe(&inventory.schema, &types).await?;
    let missing = missing_custom_fields(spec, &inventory.objects, &observed.capabilities)?;
    if !missing.is_empty() {
        eprintln!("projection proposal: missing custom fields");
        for entry in &missing {
            eprintln!(
                "- rule {} (type {}, attr {}) -> field {}",
                entry.rule, entry.type_name, entry.attr_key, entry.field
            );
        }
        eprintln!();
        let prompt = format!("create missing custom fields in {:?}? [y/N] ", backend);
        if confirm(&prompt)? {
            adapter.create_custom_fields(&missing).await?;
            for entry in &missing {
                observed
                    .capabilities
                    .custom_fields_by_type
                    .entry(entry.type_name.clone())
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
                "- rule {} (type {}, attr {}) -> tag {}",
                entry.rule, entry.type_name, entry.attr_key, entry.tag
            );
        }
        eprintln!();
        let prompt = format!("create missing tags in {:?}? [y/N] ", backend);
        if confirm(&prompt)? {
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
    Ok(plan(
        &projected,
        &observed,
        state,
        &inventory.schema,
        allow_delete,
    ))
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

fn find_error_location(content: &str, error: &ValidationError) -> Option<usize> {
    if let Some(uid) = error.uid() {
        let pattern = uid.to_string();
        for (i, line) in content.lines().enumerate() {
            if line.contains(&pattern) {
                return Some(i + 1);
            }
        }
    }
    if let Some(hint) = error.key_hint() {
        for (i, line) in content.lines().enumerate() {
            if line.contains(&hint) {
                return Some(i + 1);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;
    use alembic_engine::Op;
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    fn key_str(raw: &str) -> alembic_core::Key {
        let mut map = BTreeMap::new();
        for segment in raw.split('/') {
            let (field, value) = segment
                .split_once('=')
                .unwrap_or_else(|| panic!("invalid key segment: {segment}"));
            map.insert(
                field.to_string(),
                serde_json::Value::String(value.to_string()),
            );
        }
        alembic_core::Key::from(map)
    }

    #[test]
    fn state_path_uses_dot_alembic() {
        let root = Path::new("/tmp/example");
        let path = state_path(root);
        assert!(path.ends_with(".alembic/state.json"));
    }

    #[test]
    fn resolve_credentials_prefers_args() {
        let creds = resolve_credentials(
            "NETBOX",
            Some("http://example".to_string()),
            Some("token".to_string()),
        )
        .unwrap();
        assert_eq!(creds.0, "http://example");
        assert_eq!(creds.1, "token");
    }

    #[test]
    fn resolve_credentials_from_env() {
        let _guard = env_lock().lock().unwrap();
        let old_url = std::env::var("NETBOX_URL").ok();
        let old_token = std::env::var("NETBOX_TOKEN").ok();
        std::env::set_var("NETBOX_URL", "http://env");
        std::env::set_var("NETBOX_TOKEN", "envtoken");

        let result = std::panic::catch_unwind(|| {
            let creds = resolve_credentials("NETBOX", None, None).unwrap();
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
            schema: alembic_core::Schema {
                types: BTreeMap::new(),
            },
            ops: vec![Op::Delete {
                uid: uuid::Uuid::from_u128(1),
                type_name: alembic_core::TypeName::new("dcim.site"),
                key: key_str("site=fra1"),
                backend_id: Some(alembic_engine::BackendId::Int(1)),
            }],
            summary: None,
        };

        write_plan(&path, &plan).unwrap();
        let loaded = read_plan(&path).unwrap();
        assert_eq!(loaded.ops.len(), 1);
    }

    #[test]
    fn resolve_credentials_missing_is_error() {
        let _guard = env_lock().lock().unwrap();
        let old_url = std::env::var("NETBOX_URL").ok();
        let old_token = std::env::var("NETBOX_TOKEN").ok();
        std::env::remove_var("NETBOX_URL");
        std::env::remove_var("NETBOX_TOKEN");

        let result = resolve_credentials("NETBOX", None, None);
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
            r#"schema:
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
        std::fs::write(
            &retort,
            r#"version: 1
schema:
  types: {}
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
            r#"sites:
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
            r#"sites:
  - slug: fra1
    name: FRA1
"#,
        )
        .unwrap();
        std::fs::write(
            &retort,
            r#"version: 1
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
rules:
  - name: sites
    select: /sites/*
    emit:
      type: dcim.site
      key:
        site: "${slug}"
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
        assert_eq!(inventory.objects[0].type_name.as_str(), "dcim.site");
    }

    #[test]
    fn write_inventory_and_projected() {
        let dir = tempdir().unwrap();
        let inv_path = dir.path().join("ir.json");
        let proj_path = dir.path().join("projected.json");
        let inventory = alembic_core::Inventory {
            schema: alembic_core::Schema {
                types: BTreeMap::new(),
            },
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
            r#"schema:
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
            r#"sites:
  - slug: fra1
    name: FRA1
"#,
        )
        .unwrap();
        std::fs::write(
            &retort,
            r#"version: 1
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
rules:
  - name: sites
    select: /sites/*
    emit:
      type: dcim.site
      key:
        site: "${slug}"
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
            r#"sites:
  - slug: fra1
    name: FRA1
"#,
        )
        .unwrap();
        std::fs::write(
            &retort,
            r#"version: 1
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
rules:
  - name: sites
    select: /sites/*
    emit:
      type: dcim.site
      key:
        site: "${slug}"
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
            r#"version: 1
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
            r#"schema:
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
                backend: Backend::Netbox,
                netbox_url: None,
                netbox_token: None,
                nautobot_url: None,
                nautobot_token: None,
                generic_config: None,
                dry_run: false,
                allow_delete: false,
            },
        };
        let err = run(cli).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("missing --netbox-url or NETBOX_URL"));
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
                backend: Backend::Netbox,
                netbox_url: None,
                netbox_token: None,
                nautobot_url: None,
                nautobot_token: None,
                generic_config: None,
                allow_delete: false,
                interactive: false,
            },
        };
        let err = run(cli).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("missing --netbox-url or NETBOX_URL"));
        std::env::set_current_dir(cwd).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_plan_nautobot_backend() {
        use httpmock::Method::GET;
        use httpmock::MockServer;
        use serde_json::json;

        let _guard = cwd_lock().lock().await;
        let server = MockServer::start();
        let dir = tempdir().unwrap();
        let brew = dir.path().join("brew.yaml");
        let out = dir.path().join("plan.json");
        std::fs::write(
            &brew,
            r#"
schema:
  types:
    dcim.device:
      key:
        name:
          type: string
      fields:
        name:
          type: string
objects:
  - uid: "00000000-0000-0000-0000-000000000001"
    type: dcim.device
    key:
      name: "leaf01"
    attrs:
      name: "leaf01"
"#,
        )
        .unwrap();

        let _content_types = server.mock(|when, then| {
            when.method(GET)
                .path("/api/extras/content-types/")
                .query_param("limit", "200")
                .query_param("offset", "0");
            then.status(200).json_body(json!({
                "count": 1,
                "next": null,
                "previous": null,
                "results": [{
                    "app_label": "dcim",
                    "model": "device",
                    "display": "Device"
                }]
            }));
        });

        let _custom_fields = server.mock(|when, then| {
            when.method(GET).path("/api/extras/custom-fields/");
            then.status(200).json_body(json!({
                "count": 0,
                "next": null,
                "previous": null,
                "results": []
            }));
        });

        let _tags = server.mock(|when, then| {
            when.method(GET).path("/api/extras/tags/");
            then.status(200).json_body(json!({
                "count": 0,
                "next": null,
                "previous": null,
                "results": []
            }));
        });

        let _devices = server.mock(|when, then| {
            when.method(GET)
                .path("/api/dcim/devices/")
                .query_param("limit", "200")
                .query_param("offset", "0");
            then.status(200).json_body(json!({
                "count": 0,
                "next": null,
                "previous": null,
                "results": []
            }));
        });

        let cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir.path()).unwrap();

        let cli = Cli {
            command: Command::Plan {
                file: brew,
                retort: None,
                projection: None,
                projection_strict: true,
                projection_propose: false,
                output: out.clone(),
                backend: Backend::Nautobot,
                netbox_url: None,
                netbox_token: None,
                nautobot_url: Some(server.base_url()),
                nautobot_token: Some("token".to_string()),
                generic_config: None,
                dry_run: false,
                allow_delete: false,
            },
        };
        run(cli).await.unwrap();

        let raw = std::fs::read_to_string(&out).unwrap();
        assert!(raw.contains("\"op\": \"create\""));
        assert!(raw.contains("\"type_name\": \"dcim.device\""));

        std::env::set_current_dir(cwd).unwrap();
    }
}
