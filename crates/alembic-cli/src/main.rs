//! cli entrypoint for alembic.

use alembic_adapter_netbox::NetBoxAdapter;
use alembic_engine::{apply_plan, build_plan, load_brew, Plan, StateStore};
use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use std::fs;
use std::path::{Path, PathBuf};

/// top-level cli definition.
#[derive(Parser)]
#[command(name = "alembic")]
#[command(about = "Data-model-first converger + loader for NetBox")]
struct Cli {
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
}

/// main entrypoint for the async cli.
#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Validate { file } => {
            let inventory = load_brew(&file)?;
            alembic_engine::validate(&inventory)?;
            println!("ok");
        }
        Command::Plan {
            file,
            output,
            netbox_url,
            netbox_token,
            dry_run: _,
            allow_delete,
        } => {
            let inventory = load_brew(&file)?;
            let state = load_state()?;
            let (url, token) = netbox_credentials(netbox_url, netbox_token)?;
            let adapter = NetBoxAdapter::new(&url, &token, state.clone())?;
            let plan = build_plan(&adapter, &inventory, &state, allow_delete).await?;
            write_plan(&output, &plan)?;
            state.save()?;
            println!("plan written to {}", output.display());
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
