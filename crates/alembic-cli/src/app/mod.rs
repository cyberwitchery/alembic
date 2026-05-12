//! cli entrypoint for alembic.

mod cast_django;
pub mod config;
mod diag;
mod io;
mod state;

use alembic_adapter_registry::{create_adapter, Plugin};
use alembic_engine::{
    apply_plan, build_plan, compile_retort, is_brew_format, load_raw_yaml, load_retort, Plan,
};
use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use std::fs;
use std::path::PathBuf;

use self::cast_django::{run_cast_django, CastDjangoConfig, CommandRunner};
use self::diag::err;
use self::io::{format_validation_errors, load_inventory, read_plan, write_inventory, write_plan};
use self::state::load_state;
use alembic_core::TypeName;

#[cfg(test)]
use self::cast_django::Runner;
#[cfg(test)]
use self::state::{resolve_state_backend_config, state_path, StateBackendConfig};
use crate::app::config::AppConfig;
#[cfg(test)]
use alembic_engine::PostgresTlsMode;
#[cfg(test)]
use std::path::Path;

/// top-level cli definition.
#[derive(Parser)]
#[command(name = "alembic")]
#[command(about = "Data-model-first converger + loader for DCIM/IPAM")]
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
    },
    Plan {
        #[arg(short = 'f', long)]
        file: PathBuf,
        #[arg(long)]
        retort: Option<PathBuf>,
        #[arg(short = 'o', long)]
        output: PathBuf,
        #[arg(long)]
        backend: Option<String>,
        #[arg(long)]
        backend_config: Option<PathBuf>,
        #[arg(long, default_value_t = false)]
        provision: bool,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value_t = false)]
        allow_delete: bool,
    },
    Apply {
        #[arg(short = 'p', long)]
        plan: PathBuf,
        #[arg(long)]
        backend: Option<String>,
        #[arg(long)]
        backend_config: Option<PathBuf>,
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
    Import {
        #[arg(short = 'o', long)]
        output: PathBuf,
        #[arg(long)]
        retort: Option<PathBuf>,
        #[arg(long)]
        backend: Option<String>,
        #[arg(long)]
        backend_config: Option<PathBuf>,
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

fn confirm(prompt: &str) -> Result<bool> {
    use std::io::{self, Write};
    let mut stdout = io::stdout();
    stdout.write_all(prompt.as_bytes())?;
    stdout.flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(matches!(input.trim().to_lowercase().as_str(), "y" | "yes"))
}

pub(crate) async fn run(cli: Cli, config: AppConfig) -> Result<()> {
    match cli.command {
        Command::Validate { file, retort } => {
            let inventory = load_inventory(&file, retort.as_deref())?;
            let report = alembic_engine::validate(&inventory);
            if report.is_ok() {
                println!("ok");
            } else {
                for error in format_validation_errors(report, &inventory.objects) {
                    err("validate", &error);
                }
                return Err(anyhow!("validation failed"));
            }
        }
        Command::Plan {
            file,
            retort,
            output,
            backend,
            backend_config,
            provision,
            dry_run,
            allow_delete,
        } => {
            let inventory = load_inventory(&file, retort.as_deref())?;
            let mut state = load_state().await?;
            let plugins = search_for_plugins(&config);
            let adapter = create_adapter(&plugins, backend.as_deref(), backend_config)?;
            if provision {
                let provision_report = adapter.ensure_schema(&inventory.schema).await?;
                if !provision_report.is_empty() {
                    println!("provision: {provision_report}");
                }
            }

            let plan = build_plan(adapter.as_ref(), &inventory, &mut state, allow_delete).await?;
            if dry_run {
                let raw = serde_json::to_string_pretty(&plan)?;
                println!("{raw}");
            } else {
                write_plan(&output, &plan)?;
                state.save_async().await?;
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
            backend_config,
            allow_delete,
            interactive,
        } => {
            let mut state = load_state().await?;
            let plugins = search_for_plugins(&config);
            let adapter = create_adapter(&plugins, backend.as_deref(), backend_config)?;
            let plan = read_plan(&plan)?;

            if interactive {
                if !allow_delete
                    && plan
                        .ops
                        .iter()
                        .any(|op| matches!(op, alembic_engine::Op::Delete { .. }))
                {
                    return Err(anyhow!(
                        "plan contains delete operations; re-run with --allow-delete"
                    ));
                }
                let ordered = alembic_engine::sort_ops_for_apply(&plan.ops);
                let mut approved = Vec::new();
                for op in ordered {
                    let prompt = match &op {
                        alembic_engine::Op::Create {
                            type_name, desired, ..
                        } => format!(
                            "create {} {}? [y/N] ",
                            type_name,
                            alembic_core::key_string(&desired.key)
                        ),
                        alembic_engine::Op::Update {
                            type_name, desired, ..
                        } => format!(
                            "update {} {}? [y/N] ",
                            type_name,
                            alembic_core::key_string(&desired.key)
                        ),
                        alembic_engine::Op::Delete { type_name, key, .. } => format!(
                            "delete {} {}? [y/N] ",
                            type_name,
                            alembic_core::key_string(key)
                        ),
                    };
                    if confirm(&prompt)? {
                        approved.push(op);
                    }
                }
                let interactive_plan = Plan {
                    schema: plan.schema.clone(),
                    ops: approved,
                    summary: None,
                };
                let report = apply_plan(
                    adapter.as_ref(),
                    &interactive_plan,
                    &mut state,
                    allow_delete,
                )
                .await?;
                state.save_async().await?;
                if !report.provision.is_empty() {
                    println!("provision: {}", report.provision);
                }
                println!("applied {} operations", report.applied.len());
            } else {
                let report = apply_plan(adapter.as_ref(), &plan, &mut state, allow_delete).await?;
                state.save_async().await?;
                if !report.provision.is_empty() {
                    println!("provision: {}", report.provision);
                }
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
        Command::Import {
            output,
            retort,
            backend,
            backend_config,
        } => {
            let retort_path = retort
                .as_deref()
                .ok_or_else(|| anyhow!("import requires a retort with schema"))?;
            let retort = load_retort(retort_path)?;
            let plugins = search_for_plugins(&config);
            let adapter = create_adapter(&plugins, backend.as_deref(), backend_config)?;
            let state = load_state().await?;
            let types: Vec<TypeName> = retort.schema.types.keys().map(TypeName::new).collect();
            let report =
                alembic_engine::import_inventory(adapter.as_ref(), &retort.schema, &types, &state)
                    .await?;
            write_inventory(&output, &report.inventory)?;
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
                run_cast_django(
                    &runner,
                    CastDjangoConfig {
                        file,
                        output,
                        project,
                        app,
                        python,
                        no_migrate,
                        no_admin,
                    },
                )?;
            }
        },
    }

    Ok(())
}

fn search_for_plugins(config: &AppConfig) -> Vec<Plugin> {
    let Ok(dir_contents) = fs::read_dir(&config.plugins_dir) else {
        tracing::debug!("plugin dir '{}' not found", config.plugins_dir.display());
        return vec![];
    };

    dir_contents
        .flatten()
        .filter(|e| {
            e.path()
                .extension()
                .and_then(|s| s.to_str())
                .map(|s| s == "yaml")
                .unwrap_or(false)
        })
        .map(|e| Plugin {
            name: e
                .path()
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default()
                .to_string(),
            path: e.path(),
        })
        .collect()
}

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
