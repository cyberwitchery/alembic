//! cli entrypoint for alembic.

mod adapter;
mod cast_django;
mod diag;
mod io;
mod proposal;
mod state;

use alembic_engine::{
    apply_plan, apply_projection, build_plan_with_projection, compile_retort, is_brew_format,
    lint_specs, load_projection, load_raw_yaml, load_retort, ExtractReport, Plan,
};
use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

use self::adapter::create_adapter;
use self::cast_django::{run_cast_django, CastDjangoConfig, CommandRunner};
use self::diag::{err, warn};
use self::io::{
    format_validation_errors, load_inventory, load_projection_optional, load_retort_optional,
    read_plan, write_inventory, write_plan, write_projected,
};
use self::proposal::{build_plan_with_proposal, confirm};
use self::state::load_state;

#[cfg(test)]
use self::adapter::resolve_credentials;
#[cfg(test)]
use self::cast_django::Runner;
#[cfg(test)]
use self::state::{resolve_state_backend_config, state_path, StateBackendConfig};
#[cfg(test)]
use alembic_engine::{PostgresTlsMode, ProjectedInventory};
#[cfg(test)]
use std::path::Path;

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
                for error in format_validation_errors(report, &inventory.objects) {
                    err("validate", &error);
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
                warn("lint", warning);
            }
            for error in &report.errors {
                err("lint", error);
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
            if dry_run && projection_propose {
                return Err(anyhow!(
                    "--dry-run cannot be used with --projection-propose; rerun without --dry-run to allow proposal actions"
                ));
            }
            let inventory = load_inventory(&file, retort.as_deref())?;
            let mut state = load_state().await?;
            let projection = load_projection_optional(projection.as_deref())?;

            let adapter = create_adapter(
                backend,
                netbox_url.clone(),
                netbox_token.clone(),
                nautobot_url,
                nautobot_token,
                generic_config,
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
            netbox_url,
            netbox_token,
            nautobot_url,
            nautobot_token,
            generic_config,
            allow_delete,
            interactive,
        } => {
            let mut state = load_state().await?;
            let adapter = create_adapter(
                backend,
                netbox_url,
                netbox_token,
                nautobot_url,
                nautobot_token,
                generic_config,
            )?;
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
                println!("applied {} operations", report.applied.len());
            } else {
                let report = apply_plan(adapter.as_ref(), &plan, &mut state, allow_delete).await?;
                state.save_async().await?;
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
            warn(
                "extract",
                "retort inversion is not implemented; using schema only",
            );
            let projection = load_projection_optional(projection.as_deref())?;
            let adapter = create_adapter(
                backend,
                netbox_url,
                netbox_token,
                nautobot_url,
                nautobot_token,
                generic_config,
            )?;
            let state = load_state().await?;
            let ExtractReport {
                inventory,
                warnings,
            } = alembic_engine::extract_inventory(
                adapter.as_ref(),
                &retort.schema,
                projection.as_ref(),
                &state,
            )
            .await?;
            for warning in warnings {
                warn("extract", &warning);
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

#[cfg(test)]
mod tests;
