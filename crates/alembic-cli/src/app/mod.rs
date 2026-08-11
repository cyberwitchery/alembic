//! cli entrypoint for alembic.

pub mod config;
mod io;
mod state;

use alembic_adapter_registry::{create_backend, Plugin};
use alembic_engine::{
    apply_plan, build_plan, guard_drift_report, guard_schema_deletes, load_inventory,
    load_inventory_unvalidated, plan_write_only, render_plan, ApplyReport, Backend, DriftReport,
    Plan, Tense,
};
use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use std::fs;
use std::path::{Path, PathBuf};

use self::io::{
    read_plan, write_apply_report, write_drift_report, write_inventory, write_plan,
    write_validation_report,
};
use self::state::load_state;
use crate::app::config::AppConfig;
use alembic_core::TypeName;

#[cfg(test)]
use self::io::warn_misleading_output_extension;
#[cfg(test)]
use self::state::{resolve_state_backend_config, state_path, StateBackendConfig};
#[cfg(test)]
use alembic_adapter_django::emit::Runner;
#[cfg(test)]
use alembic_engine::PostgresTlsMode;

/// top-level cli definition.
#[derive(Parser)]
#[command(name = "alembic", version)]
#[command(
    about = "Data-model-first converger + loader for DCIM/IPAM (YAML/JSON inventories in, JSON plans out)"
)]
#[command(long_about = "\
Data-model-first converger + loader for DCIM/IPAM.

File formats are chosen by file extension:
  - inventories (IR) are authored as YAML or JSON: a .json extension is parsed as
    JSON, anything else (.yaml, .yml, or no extension) is parsed as YAML. each
    inventory carries a schema block plus optional include/imports.
  - plans (plan --output), the validation report (validate --output), the drift
    report (plan --report --output), observed or transformed IR (import --output
    and map --output), and the apply report (apply --output) are always written
    as JSON, regardless of the path extension.
  - apply --plan consumes a JSON plan file as produced by alembic plan.")]
pub(crate) struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// cli subcommands.
#[derive(Subcommand)]
enum Command {
    /// validate an inventory against its schema, without touching a backend.
    Validate {
        /// inventory file to validate (yaml or json).
        #[arg(short = 'f', long)]
        file: PathBuf,
        /// where to write the json validation report; written on both outcomes,
        /// with an empty `errors` list when the inventory validates.
        #[arg(short = 'o', long)]
        output: Option<PathBuf>,
    },
    /// compute a deterministic plan (desired vs observed) and write it as json.
    Plan {
        /// inventory file to plan from (yaml or json).
        #[arg(short = 'f', long)]
        file: PathBuf,
        /// where to write the json plan, or the json drift report under
        /// --report; required unless --report or --dry-run, and rejected with
        /// --dry-run, which prints the plan instead of writing it.
        #[arg(
            short = 'o',
            long,
            required_unless_present_any = ["report", "dry_run"],
            conflicts_with = "dry_run"
        )]
        output: Option<PathBuf>,
        /// backend name (netbox, nautobot, infrahub, generic, peeringdb, django,
        /// external); credentials come from the environment.
        #[arg(long)]
        backend: Option<String>,
        /// path to a backend config file instead of --backend plus env vars.
        #[arg(long)]
        backend_config: Option<PathBuf>,
        /// run adapter schema provisioning (ensure_schema) now, before observing;
        /// gated by --allow-delete when it would delete schema.
        #[arg(long, default_value_t = false, conflicts_with = "dry_run")]
        provision: bool,
        /// print the raw json plan to stdout instead of writing it to a file.
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        /// print a read-only drift report (desired vs observed) and exit without
        /// writing a plan file or saving state; --output writes the same report
        /// as json. mutually exclusive with --dry-run.
        #[arg(long, default_value_t = false, conflicts_with = "dry_run")]
        report: bool,
        /// allow the plan to include deletes (objects, and destructive schema
        /// provisioning) for objects present on the backend but not declared.
        #[arg(long, default_value_t = false)]
        allow_delete: bool,
    },
    /// apply a json plan to a backend (the only command that writes).
    Apply {
        /// json plan file produced by `alembic plan`.
        #[arg(short = 'p', long)]
        plan: PathBuf,
        /// where to write the json apply report (uid -> backend id per applied
        /// op); written only when the apply succeeds.
        #[arg(short = 'o', long)]
        output: Option<PathBuf>,
        /// backend name; credentials come from the environment.
        #[arg(long)]
        backend: Option<String>,
        /// path to a backend config file instead of --backend plus env vars.
        #[arg(long)]
        backend_config: Option<PathBuf>,
        /// allow delete ops (object and destructive schema deletes) in the plan.
        #[arg(long, default_value_t = false)]
        allow_delete: bool,
        /// prompt for confirmation per operation, applying only approved ops.
        #[arg(short = 'i', long, default_value_t = false)]
        interactive: bool,
    },
    /// transform an ir inventory into another ir inventory (ir -> ir).
    // `transform` carries its own --spec and prints to stdout, so the inventory
    // flow's args have nowhere to go under it. rejected at parse time, like -o
    // with --dry-run, rather than by a hand-written check with its own exit code
    #[command(args_conflicts_with_subcommands = true)]
    Map {
        #[command(subcommand)]
        action: Option<MapAction>,
        /// input ir inventory file.
        #[arg(short = 'f', long)]
        file: Option<PathBuf>,
        /// map specification (target schema + rules).
        #[arg(long)]
        spec: Option<PathBuf>,
        /// where to write the transformed json inventory.
        #[arg(short = 'o', long)]
        output: Option<PathBuf>,
    },
    /// observe a backend's live state into the data model.
    Import {
        /// where to write the observed json inventory.
        #[arg(short = 'o', long)]
        output: PathBuf,
        /// inventory whose schema selects which types to observe.
        #[arg(short = 'f', long)]
        file: PathBuf,
        /// backend name; credentials come from the environment.
        #[arg(long)]
        backend: Option<String>,
        /// path to a backend config file instead of --backend plus env vars.
        #[arg(long)]
        backend_config: Option<PathBuf>,
    },
}

/// map subcommands.
#[derive(Subcommand)]
enum MapAction {
    /// evaluate a single transform against a json value, for iterating on a
    /// map spec's user-defined transforms without an inventory or backend.
    Transform {
        /// map specification carrying the transforms block.
        #[arg(long)]
        spec: PathBuf,
        /// transform name (user-defined or built-in).
        name: String,
        /// json-encoded input value, e.g. '"nxos"'.
        value: String,
        /// json-encoded extra literal arguments.
        args: Vec<String>,
    },
}

/// eof is not an answer: `read_line` returns `Ok(0)` leaving the line empty,
/// which would read as a decline nobody gave.
fn confirm(description: &str) -> Result<bool> {
    use std::io::{self, Write};
    let mut stdout = io::stdout();
    write!(stdout, "{description}? [y/N] ")?;
    stdout.flush()?;
    let mut input = String::new();
    if io::stdin().read_line(&mut input)? == 0 {
        return Err(anyhow!(
            "stdin ended before `{description}` was answered; drop --interactive \
             to apply the whole plan non-interactively"
        ));
    }
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

/// the `-o`/`--output` path a command will write, if any. the one place that
/// knows, so every write site is preflighted by construction; matching
/// exhaustively means a new *variant* has to answer this. a new `-o` on an
/// existing variant does not: `..` absorbs it silently, so it has to be bound
/// here by hand.
fn output_path(command: &Command) -> Option<&Path> {
    match command {
        Command::Validate { output, .. }
        | Command::Plan { output, .. }
        | Command::Apply { output, .. } => output.as_deref(),
        // the transform subcommand prints to stdout and writes no file
        Command::Map {
            action: Some(_), ..
        } => None,
        Command::Map { output, .. } => output.as_deref(),
        Command::Import { output, .. } => Some(output),
    }
}

pub(crate) async fn run(cli: Cli, config: AppConfig) -> Result<()> {
    // before anything expensive: a command that writes an output file pays for a
    // load, a backend observation or an apply first, so a bad -o must not surface
    // at the write. the write path recreates what the probe removed.
    if let Some(output) = output_path(&cli.command) {
        io::preflight_output_path(output)?;
    }
    match cli.command {
        Command::Validate { file, output } => {
            // the only command that loads without the loader's own validation
            // gate: it reports the errors rather than failing on the load, which
            // is what leaves a report to write.
            let inventory = load_inventory_unvalidated(&file)?;
            let report = alembic_engine::validate(&inventory);
            let located = report.clone().located(&inventory.objects);
            // written on both outcomes: a ci gate wants an artifact either way,
            // and an absent file would be indistinguishable from a crash.
            if let Some(output) = &output {
                write_validation_report(output, &located)?;
                println!("validation report written to {}", output.display());
            }
            // after the write, so `ok` means the whole command succeeded
            if located.errors.is_empty() {
                println!("ok");
            }
            // the human half stays the loader's own error, verbatim
            alembic_engine::report_to_result_with_sources(report, &inventory.objects)?;
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
            let plugins = search_for_plugins(&config)?;
            let backend = create_backend(&plugins, backend.as_deref(), backend_config)?;
            // a drift report asserts what the backend holds; one that observes
            // nothing would report every declared object absent, so refuse it
            // before provisioning or a backend read or write.
            if report {
                guard_drift_report(&backend)?;
            }
            // read-only schema preview: what apply's ensure_schema would provision,
            // writing nothing. skipped when --provision actually provisions now, and
            // for a read-only backend, which cannot provision schema at all. all
            // preview output goes to stderr so it never pollutes a --dry-run/--report
            // stdout; the machine-readable copy rides in the plan's schema_preview.
            let mut schema_preview = None;
            if provision {
                let emitter = backend.emitter()?;
                // provisioning can delete custom object types/fields the inventory
                // no longer declares; gate it behind --allow-delete like apply,
                // checking the read-only preview before writing schema.
                if !allow_delete {
                    if let Some(preview) = emitter.preview_schema(&inventory.schema).await? {
                        guard_schema_deletes(&preview, allow_delete)?;
                    }
                }
                let provision_report = emitter.ensure_schema(&inventory.schema).await?;
                if !provision_report.is_empty() {
                    println!("provision: {provision_report}");
                    for (label, name) in provision_report.named_changes(Tense::Past) {
                        println!("  {label} {name}");
                    }
                }
            } else if let Ok(emitter) = backend.emitter() {
                match emitter.preview_schema(&inventory.schema).await {
                    Ok(Some(report)) => {
                        if !report.is_empty() {
                            eprintln!("schema preview: {report}");
                            for (label, name) in report.named_changes(Tense::Would) {
                                eprintln!("  {label} {name}");
                            }
                        }
                        schema_preview = Some(report);
                    }
                    Ok(None) => eprintln!("schema preview: unavailable for this backend"),
                    // a preview hiccup must not sink the read-only plan; report and continue.
                    Err(err) => eprintln!("schema preview failed: {err:#}"),
                }
            }

            let mut plan = if matches!(&backend, Backend::Emitter(_)) {
                // write-only backend: it cannot observe existing state, so plan
                // every declared object as a create rather than failing to observe.
                plan_write_only(&inventory, &state)?
            } else {
                build_plan(
                    backend.observer()?,
                    &inventory,
                    &mut state,
                    should_detect_deletes(allow_delete, report),
                )
                .await?
            };
            plan.schema_preview = schema_preview;
            if report {
                // read-only: describe desired-vs-observed and exit without
                // writing a plan file or saving state.
                let drift = DriftReport::from_plan(&plan);
                println!("{drift}");
                // the machine-readable half of the same report: a drift document,
                // never a plan, and still no state save
                if let Some(output) = &output {
                    write_drift_report(output, &drift)?;
                    println!("\ndrift report written to {}", output.display());
                }
            } else if dry_run {
                let raw = serde_json::to_string_pretty(&plan)?;
                println!("{raw}");
            } else {
                let Some(output) = output else {
                    return Err(anyhow!("--output is required unless --report or --dry-run"));
                };
                write_plan(&output, &plan)?;
                state.save_async().await?;
                // human-readable, per-op view of what apply would do (see before
                // write); the machine-readable copy is the written plan file.
                println!("{}", render_plan(&plan));
                println!("\nplan written to {}", output.display());
            }
        }
        Command::Apply {
            plan,
            output,
            backend,
            backend_config,
            allow_delete,
            interactive,
        } => {
            let mut state = load_state().await?;
            let plugins = search_for_plugins(&config)?;
            let backend = create_backend(&plugins, backend.as_deref(), backend_config)?;
            // reject a backend that cannot apply before reading the plan or prompting
            backend.emitter()?;
            let plan = read_plan(&plan)?;

            let plan = if interactive {
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
                    let description = match &op {
                        alembic_engine::Op::Create {
                            type_name, desired, ..
                        } => format!(
                            "create {} {}",
                            type_name,
                            alembic_core::key_string(&desired.key)
                        ),
                        alembic_engine::Op::Update {
                            type_name, desired, ..
                        } => format!(
                            "update {} {}",
                            type_name,
                            alembic_core::key_string(&desired.key)
                        ),
                        alembic_engine::Op::Delete { type_name, key, .. } => {
                            format!("delete {} {}", type_name, alembic_core::key_string(key))
                        }
                    };
                    if confirm(&description)? {
                        approved.push(op);
                    }
                }
                Plan {
                    schema: plan.schema,
                    ops: approved,
                    summary: None,
                    schema_preview: None,
                }
            } else {
                plan
            };

            let report = apply_plan(&backend, &plan, &mut state, allow_delete).await?;
            state.save_async().await?;
            print_apply_report(&report);
            // machine-readable record of what this run wrote, on the success
            // path only: state.json is cumulative and the journal is gone by now.
            if let Some(output) = output {
                write_apply_report(&output, &report)?;
                println!("apply report written to {}", output.display());
            }
        }
        Command::Map {
            action,
            file,
            spec,
            output,
        } => match action {
            Some(MapAction::Transform {
                spec,
                name,
                value,
                args,
            }) => {
                // file/spec/output are rejected against this subcommand at parse
                // time (args_conflicts_with_subcommands), so they are None here
                let spec = alembic_engine::load_map_spec(&spec)?;
                let parse_json = |label: &str, raw: &str| -> Result<serde_json::Value> {
                    serde_json::from_str(raw).map_err(|err| {
                        anyhow!(
                            "{label} is not valid json: {err}\n\
                             hint: string values need json quoting, e.g. '\"{raw}\"'"
                        )
                    })
                };
                let value = parse_json("value", &value)?;
                let args = args
                    .iter()
                    .map(|arg| parse_json(&format!("argument {arg}"), arg))
                    .collect::<Result<Vec<serde_json::Value>>>()?;
                let result = alembic_engine::eval_map_transform(&spec, &name, &value, &args)?;
                println!("{}", serde_json::to_string(&result)?);
            }
            None => {
                let (Some(file), Some(spec), Some(output)) = (file, spec, output) else {
                    return Err(anyhow!(
                        "alembic map requires -f, --spec, and -o (or the transform subcommand)"
                    ));
                };
                let input = load_inventory(&file)?;
                let spec = alembic_engine::load_map_spec(&spec)?;
                let inventory = alembic_engine::compile_map(&input, &spec)?;
                write_inventory(&output, &inventory)?;
                println!("ir written to {}", output.display());
            }
        },
        Command::Import {
            output,
            file,
            backend,
            backend_config,
        } => {
            // observe live backend state into ir; the inventory's schema selects
            // which types to observe.
            let inventory = load_inventory(&file)?;
            let plugins = search_for_plugins(&config)?;
            let backend = create_backend(&plugins, backend.as_deref(), backend_config)?;
            let types: Vec<TypeName> = inventory.schema.types.keys().map(TypeName::new).collect();
            let report =
                alembic_engine::import_inventory(backend.observer()?, &inventory.schema, &types)
                    .await?;
            write_inventory(&output, &report.inventory)?;
            println!("inventory written to {}", output.display());
        }
    }

    Ok(())
}

fn print_apply_report(report: &ApplyReport) {
    if !report.provision.is_empty() {
        println!("provision: {}", report.provision);
        for (label, name) in report.provision.named_changes(Tense::Past) {
            println!("  {label} {name}");
        }
    }
    if let Some(previously_applied_count) = report.previously_applied_count {
        println!(
            "applied {} operations (after resuming, had previously applied {} operations)",
            report.applied.len(),
            previously_applied_count
        );
    } else {
        println!("applied {} operations", report.applied.len());
    }
}

fn search_for_plugins(config: &AppConfig) -> Result<Vec<Plugin>> {
    let dir_contents = match fs::read_dir(&config.plugins_dir) {
        Ok(contents) => contents,
        // the default `./plugins` is usually not there, which is not an error;
        // a directory that is there but unreadable is.
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            tracing::debug!("plugin dir '{}' not found", config.plugins_dir.display());
            return Ok(Vec::new());
        }
        Err(err) => {
            return Err(err)
                .with_context(|| format!("read plugin dir {}", config.plugins_dir.display()))
        }
    };

    // an entry that fails to yield would drop that plugin as silently as the
    // whole directory used to.
    let entries = dir_contents
        .collect::<std::io::Result<Vec<_>>>()
        .with_context(|| format!("read plugin dir {}", config.plugins_dir.display()))?;

    Ok(entries
        .into_iter()
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
        .collect())
}

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
