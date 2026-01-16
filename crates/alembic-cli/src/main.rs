//! cli entrypoint for alembic.

use alembic_adapter_netbox::NetBoxAdapter;
use alembic_engine::Adapter;
use alembic_engine::{
    apply_plan, apply_projection, build_plan_with_projection, compile_retort, is_brew_format,
    load_brew, load_projection, load_raw_yaml, load_retort, missing_custom_fields, missing_tags,
    plan, Plan, ProjectedInventory, ProjectionSpec, StateStore,
};
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
}

/// main entrypoint for the async cli.
#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    run(cli).await
}

async fn run(cli: Cli) -> Result<()> {
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
        Command::Plan {
            file,
            retort,
            projection,
            projection_strict,
            projection_propose,
            output,
            netbox_url,
            netbox_token,
            dry_run: _,
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

fn load_projection_optional(path: Option<&Path>) -> Result<Option<ProjectionSpec>> {
    match path {
        Some(path) => Ok(Some(load_projection(path)?)),
        None => Ok(None),
    }
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
    use super::*;
    use alembic_engine::Op;
    use std::sync::{Mutex, OnceLock};
    use tempfile::tempdir;

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn cwd_lock() -> &'static tokio::sync::Mutex<()> {
        static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
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
}
