//! cli entrypoint for alembic.

pub mod config;
mod diag;
mod io;
mod state;

use alembic_adapter_registry::{create_adapter, Plugin};
use alembic_engine::{apply_plan, build_plan, load_inventory, DriftReport, Plan};
use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use std::fs;
use std::path::PathBuf;

use self::diag::err;
use self::io::{format_validation_errors, read_plan, write_inventory, write_plan};
use self::state::load_state;
use crate::app::config::AppConfig;
use alembic_core::TypeName;

#[cfg(test)]
use self::state::{resolve_state_backend_config, state_path, StateBackendConfig};
#[cfg(test)]
use alembic_adapter_django::cast_django::Runner;
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
    },
    Plan {
        #[arg(short = 'f', long)]
        file: PathBuf,
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
        /// print a read-only drift report (desired vs observed) and exit without
        /// writing a plan file or saving state. mutually exclusive with --dry-run.
        #[arg(long, default_value_t = false, conflicts_with = "dry_run")]
        report: bool,
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
    /// transform an ir inventory into another ir inventory (ir -> ir).
    Map {
        /// input ir inventory file.
        #[arg(short = 'f', long)]
        file: PathBuf,
        /// map specification (target schema + rules).
        #[arg(long)]
        spec: PathBuf,
        #[arg(short = 'o', long)]
        output: PathBuf,
    },
    /// observe a backend's live state into canonical ir.
    Import {
        #[arg(short = 'o', long)]
        output: PathBuf,
        /// inventory whose schema selects which types to observe.
        #[arg(short = 'f', long)]
        file: PathBuf,
        #[arg(long)]
        backend: Option<String>,
        #[arg(long)]
        backend_config: Option<PathBuf>,
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

/// whether the planner should emit delete ops (objects present on the backend
/// but not declared in intent).
///
/// `--report` never applies the plan, so it forces delete-detection on purely to
/// populate the drift report's `extra` category. without this, the documented
/// `plan ... --report` invocation would silently never surface unmanaged backend
/// objects, regardless of `--allow-delete`. non-report paths are unchanged and
/// remain governed solely by `--allow-delete`.
fn should_detect_deletes(allow_delete: bool, report: bool) -> bool {
    allow_delete || report
}

pub(crate) async fn run(cli: Cli, config: AppConfig) -> Result<()> {
    match cli.command {
        Command::Validate { file } => {
            let inventory = load_inventory(&file)?;
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
            output,
            backend,
            backend_config,
            provision,
            dry_run,
            report,
            allow_delete,
        } => {
            let inventory = load_inventory(&file)?;
            let mut state = load_state().await?;
            let plugins = search_for_plugins(&config);
            let adapter = create_adapter(&plugins, backend.as_deref(), backend_config)?;
            if provision {
                let provision_report = adapter.ensure_schema(&inventory.schema).await?;
                if !provision_report.is_empty() {
                    println!("provision: {provision_report}");
                }
            }

            let plan = build_plan(
                adapter.as_ref(),
                &inventory,
                &mut state,
                should_detect_deletes(allow_delete, report),
            )
            .await?;
            if report {
                // read-only: describe desired-vs-observed and exit without
                // writing a plan file or saving state.
                println!("{}", DriftReport::from_plan(&plan));
            } else if dry_run {
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
                let ordered = alembic_engine::sort_ops_for_apply(&plan.ops, &plan.schema);
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
        Command::Map { file, spec, output } => {
            let input = load_inventory(&file)?;
            let spec = alembic_engine::load_map_spec(&spec)?;
            let inventory = alembic_engine::compile_map(&input, &spec)?;
            write_inventory(&output, &inventory)?;
            println!("ir written to {}", output.display());
        }
        Command::Import {
            output,
            file,
            backend,
            backend_config,
        } => {
            // observe live backend state into ir; the inventory's schema selects
            // which types to observe.
            let inventory = load_inventory(&file)?;
            let plugins = search_for_plugins(&config);
            let adapter = create_adapter(&plugins, backend.as_deref(), backend_config)?;
            let state = load_state().await?;
            let types: Vec<TypeName> = inventory.schema.types.keys().map(TypeName::new).collect();
            let report = alembic_engine::import_inventory(
                adapter.as_ref(),
                &inventory.schema,
                &types,
                &state,
            )
            .await?;
            write_inventory(&output, &report.inventory)?;
            println!("inventory written to {}", output.display());
        }
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
                .map(|s| s.to_lowercase() == "yaml" || s.to_lowercase() == "yml")
                .unwrap_or(false)
        })
        .filter_map(|e| {
            let path = e.path();
            let name = path.file_stem().and_then(|s| s.to_str())?;
            if name.is_empty() {
                return None;
            }
            Some(Plugin {
                name: name.to_string().to_lowercase(),
                path: e.path(),
            })
        })
        .collect()
}

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
