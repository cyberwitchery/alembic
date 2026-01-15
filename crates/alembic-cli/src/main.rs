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

#[cfg(test)]
mod tests {
    use super::*;
    use alembic_engine::Op;
    use std::sync::{Mutex, OnceLock};
    use tempfile::tempdir;

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

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
    fn read_plan_invalid_json_errors() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("plan.json");
        std::fs::write(&path, "not-json").unwrap();
        assert!(read_plan(&path).is_err());
    }
}
