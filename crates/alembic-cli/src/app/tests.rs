use super::test_support::*;
use super::*;
use alembic_adapter_django::emit::{run_emit, DjangoConfig};
use alembic_adapter_registry::{AdapterConfig, ExternalConfig};
use alembic_core::{Inventory, Schema};
use alembic_engine::{Op, StateData, StateStore};
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
fn resolve_state_backend_defaults_to_local() {
    let _env = EnvVarGuard::acquire(&[
        ("ALEMBIC_STATE_BACKEND", None),
        ("ALEMBIC_STATE_PATH", None),
    ]);

    let root = Path::new("/tmp/example");
    let config = resolve_state_backend_config(root).unwrap();
    assert_eq!(
        config,
        StateBackendConfig::Local {
            path: root.join(".alembic/state.json")
        }
    );
}

#[test]
fn resolve_state_backend_uses_custom_local_path() {
    let _env = EnvVarGuard::acquire(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some("/tmp/custom-state.json")),
    ]);

    let config = resolve_state_backend_config(Path::new("/tmp/ignored")).unwrap();
    assert_eq!(
        config,
        StateBackendConfig::Local {
            path: PathBuf::from("/tmp/custom-state.json")
        }
    );
}

#[test]
fn resolve_state_backend_postgres_requires_url() {
    let _env = EnvVarGuard::acquire(&[
        ("ALEMBIC_STATE_BACKEND", Some("postgres")),
        ("ALEMBIC_STATE_POSTGRES_URL", None),
        ("ALEMBIC_STATE_POSTGRES_TLS", None),
    ]);

    let err = resolve_state_backend_config(Path::new("/tmp/ignored")).unwrap_err();
    assert!(err.to_string().contains("ALEMBIC_STATE_POSTGRES_URL"));
}

#[test]
fn resolve_state_backend_postgres_with_default_key() {
    let _env = EnvVarGuard::acquire(&[
        ("ALEMBIC_STATE_BACKEND", Some("postgres")),
        (
            "ALEMBIC_STATE_POSTGRES_URL",
            Some("postgres://user:pass@localhost:5432/alembic"),
        ),
        ("ALEMBIC_STATE_KEY", None),
        ("ALEMBIC_STATE_POSTGRES_TLS", None),
    ]);

    let config = resolve_state_backend_config(Path::new("/tmp/ignored")).unwrap();
    assert_eq!(
        config,
        StateBackendConfig::Postgres {
            url: "postgres://user:pass@localhost:5432/alembic".to_string(),
            key: "default".to_string(),
            tls_mode: PostgresTlsMode::Disable,
        }
    );
}

#[test]
fn resolve_state_backend_postgres_with_tls_require() {
    let _env = EnvVarGuard::acquire(&[
        ("ALEMBIC_STATE_BACKEND", Some("postgres")),
        (
            "ALEMBIC_STATE_POSTGRES_URL",
            Some("postgres://user:pass@localhost:5432/alembic"),
        ),
        ("ALEMBIC_STATE_KEY", Some("workspace-a")),
        ("ALEMBIC_STATE_POSTGRES_TLS", Some("require")),
    ]);

    let config = resolve_state_backend_config(Path::new("/tmp/ignored")).unwrap();
    assert_eq!(
        config,
        StateBackendConfig::Postgres {
            url: "postgres://user:pass@localhost:5432/alembic".to_string(),
            key: "workspace-a".to_string(),
            tls_mode: PostgresTlsMode::Require,
        }
    );
}

#[test]
fn resolve_state_backend_postgres_with_invalid_tls_mode_errors() {
    let _env = EnvVarGuard::acquire(&[
        ("ALEMBIC_STATE_BACKEND", Some("postgres")),
        (
            "ALEMBIC_STATE_POSTGRES_URL",
            Some("postgres://user:pass@localhost:5432/alembic"),
        ),
        ("ALEMBIC_STATE_POSTGRES_TLS", Some("weird")),
    ]);

    let err = resolve_state_backend_config(Path::new("/tmp/ignored")).unwrap_err();
    assert!(err.to_string().contains("ALEMBIC_STATE_POSTGRES_TLS"));
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
        schema_preview: None,
    };

    write_plan(&path, &plan).unwrap();
    let loaded = read_plan(&path).unwrap();
    assert_eq!(loaded.ops.len(), 1);
}

#[test]
fn write_plan_creates_missing_parent_dirs() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("nested/out/plan.json");
    let plan = Plan {
        schema: alembic_core::Schema {
            types: BTreeMap::new(),
        },
        ops: vec![],
        summary: None,
        schema_preview: None,
    };
    write_plan(&path, &plan).unwrap();
    assert!(read_plan(&path).is_ok());
}

#[test]
fn write_apply_report_creates_missing_parent_dirs() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("nested/out/report.json");
    write_apply_report(&path, &ApplyReport::default()).unwrap();
    assert!(path.exists());
}

#[test]
fn apply_report_json_carries_the_uid_to_backend_id_pairs() {
    // the uid to backend-id pairs are the point of the file; nothing else
    // records them per run.
    let dir = tempdir().unwrap();
    let path = dir.path().join("report.json");
    let report = ApplyReport {
        applied: vec![alembic_engine::AppliedOp {
            uid: uuid::Uuid::from_u128(1),
            type_name: alembic_core::TypeName::new("dcim.site"),
            backend_id: Some(alembic_engine::BackendId::Int(7)),
        }],
        ..Default::default()
    };
    write_apply_report(&path, &report).unwrap();

    let raw = std::fs::read_to_string(&path).unwrap();
    let loaded: ApplyReport = serde_json::from_str(&raw).unwrap();
    assert_eq!(loaded.applied.len(), 1);
    assert_eq!(loaded.applied[0].uid, uuid::Uuid::from_u128(1));
    assert_eq!(
        loaded.applied[0].backend_id,
        Some(alembic_engine::BackendId::Int(7))
    );
    // absent, not null, when the apply did not resume from a journal
    assert!(
        !raw.contains("previously_applied_count"),
        "a non-resumed apply must not report a resume count: {raw}"
    );
}

#[test]
fn write_inventory_creates_missing_parent_dirs() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("nested/out/ir.json");
    let inventory = Inventory {
        schema: Default::default(),
        objects: vec![],
    };
    write_inventory(&path, &inventory).unwrap();
    assert!(path.exists());
}

#[test]
fn django_emit_runs_migrations_by_default() {
    let dir = tempdir().unwrap();
    let output = dir.path().join("out");
    std::fs::create_dir_all(&output).unwrap();
    std::fs::write(output.join("manage.py"), "").unwrap();
    write_settings(&output, "alembic_project");
    let minimal_inventory = Inventory {
        schema: Default::default(),
        objects: vec![],
    };
    let _inventory = write_minimal_inventory(dir.path());

    let runner = MockRunner::new();
    run_emit(
        &runner,
        &minimal_inventory,
        &DjangoConfig {
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
fn django_emit_skips_migrate_with_flag() {
    let dir = tempdir().unwrap();
    let output = dir.path().join("out");
    std::fs::create_dir_all(&output).unwrap();
    std::fs::write(output.join("manage.py"), "").unwrap();
    write_settings(&output, "alembic_project");
    let minimal_inventory = Inventory {
        schema: Default::default(),
        objects: vec![],
    };
    let _inventory = write_minimal_inventory(dir.path());

    let runner = MockRunner::new();
    run_emit(
        &runner,
        &minimal_inventory,
        &DjangoConfig {
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
    assert!(calls
        .iter()
        .any(|call| { call.args == vec!["manage.py".to_string(), "makemigrations".to_string()] }));
    assert!(!calls
        .iter()
        .any(|call| call.args == vec!["manage.py".to_string(), "migrate".to_string()]));
}

#[test]
fn django_emit_integration_writes_generated_files() {
    let dir = tempdir().unwrap();
    let output = dir.path().join("out");
    let inventory = write_site_inventory(dir.path());
    let runner = FixtureRunner::new(output.clone());
    let site_inventory = load_inventory(inventory).unwrap();

    run_emit(
        &runner,
        &site_inventory,
        &DjangoConfig {
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
    assert!(calls
        .iter()
        .any(|call| { call.args == vec!["manage.py".to_string(), "makemigrations".to_string()] }));
}

#[test]
fn read_plan_invalid_json_errors() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("plan.json");
    std::fs::write(&path, "not-json").unwrap();
    assert!(read_plan(&path).is_err());
}

#[test]
fn warn_misleading_output_extension_flags_yaml() {
    // always-JSON outputs named like yaml get a (non-fatal) heads-up that mentions
    // the path and the actual format.
    let msg = warn_misleading_output_extension(Path::new("plan.yaml"))
        .expect("a .yaml output path should warn");
    assert!(msg.contains("plan.yaml"));
    assert!(msg.contains("JSON"));
    assert!(
        warn_misleading_output_extension(Path::new("out.yml")).is_some(),
        ".yml should warn too"
    );
    // detection is case-insensitive on the extension.
    assert!(warn_misleading_output_extension(Path::new("out.YAML")).is_some());
}

#[test]
fn warn_misleading_output_extension_allows_json() {
    assert!(warn_misleading_output_extension(Path::new("plan.json")).is_none());
}

#[test]
fn warn_misleading_output_extension_allows_no_extension() {
    assert!(warn_misleading_output_extension(Path::new("plan")).is_none());
}

#[test]
fn cli_command_definition_is_valid() {
    // clap's own assertions over the derived command: catches duplicate flags,
    // malformed help, broken conflicts, etc. at test time rather than at runtime.
    use clap::CommandFactory;
    Cli::command().debug_assert();
}

#[test]
fn format_validation_errors_prefers_source_locations() {
    let mut key = BTreeMap::new();
    key.insert("site".to_string(), serde_json::json!("fra1"));
    let key = alembic_core::Key::from(key);
    let attrs = alembic_core::JsonMap::default();
    let object = alembic_core::Object::new(
        uuid::Uuid::from_u128(1),
        alembic_core::TypeName::new("dcim.site"),
        key,
        attrs,
    )
    .unwrap()
    .with_source(alembic_core::SourceLocation::file_line(
        "inventory.yaml",
        42,
    ));

    let report = alembic_core::ValidationReport {
        errors: vec![alembic_core::ValidationError::DuplicateUid(
            uuid::Uuid::from_u128(1),
        )],
    };
    let errors = format_validation_errors(report, &[object]);

    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains("inventory.yaml:42"));
    assert!(errors[0].contains("duplicate uid"));
}

#[tokio::test]
async fn run_validate_inventory() {
    let dir = tempdir().unwrap();
    let inventory = dir.path().join("inventory.yaml");
    std::fs::write(
        &inventory,
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
        command: Command::Validate { file: inventory },
    };
    run(cli, AppConfig::load().unwrap()).await.unwrap();
}

#[tokio::test]
async fn run_map_ir() {
    let _guard = cwd_lock().lock().await;
    let dir = tempdir().unwrap();
    let input = dir.path().join("in.json");
    let spec = dir.path().join("map.yaml");
    let out = dir.path().join("out.json");
    // an ir inventory (dcim.site) to be renamed to location.site.
    std::fs::write(
        &input,
        r#"{
  "schema": {
    "types": {
      "dcim.site": {
        "key": { "site": { "type": "slug" } },
        "fields": { "name": { "type": "string" } }
      }
    }
  },
  "objects": [
    { "uid": "00000000-0000-0000-0000-000000000001", "type": "dcim.site",
      "key": { "site": "fra1" }, "attrs": { "name": "FRA1" } }
  ]
}"#,
    )
    .unwrap();
    std::fs::write(
        &spec,
        r#"schema:
  types:
    location.site:
      key:
        slug:
          type: slug
      fields:
        label:
          type: string
rules:
  - name: sites
    match: "dcim.site"
    emit:
      type: location.site
      key:
        slug: "${key.site}"
      attrs:
        label: "${attrs.name}"
"#,
    )
    .unwrap();
    let cwd = std::env::current_dir().unwrap();
    std::env::set_current_dir(dir.path()).unwrap();

    let cli = Cli {
        command: Command::Map {
            action: None,
            file: Some(input),
            spec: Some(spec),
            output: Some(out.clone()),
        },
    };
    run(cli, AppConfig::load().unwrap()).await.unwrap();
    let raw = std::fs::read_to_string(out).unwrap();
    assert!(raw.contains("location.site"));
    assert!(raw.contains("\"label\""));
    assert!(!raw.contains("dcim.site"));
    std::env::set_current_dir(cwd).unwrap();
}

#[tokio::test]
async fn run_map_transform_evaluates_a_transform() {
    let dir = tempdir().unwrap();
    let spec = dir.path().join("map.yaml");
    std::fs::write(
        &spec,
        "transforms:\n  inline: |\n    def cidr_host(v):\n        return v.split(\"/\")[0]\n",
    )
    .unwrap();
    let cli = Cli {
        command: Command::Map {
            action: Some(MapAction::Transform {
                spec,
                name: "cidr_host".to_string(),
                value: "\"10.0.0.1/24\"".to_string(),
                args: vec![],
            }),
            file: None,
            spec: None,
            output: None,
        },
    };
    run(cli, AppConfig::load().unwrap()).await.unwrap();
}

#[tokio::test]
async fn run_map_transform_surfaces_fail() {
    let dir = tempdir().unwrap();
    let spec = dir.path().join("map.yaml");
    std::fs::write(
        &spec,
        "transforms:\n  inline: |\n    def reject(v):\n        fail(\"bad: \" + v)\n",
    )
    .unwrap();
    let cli = Cli {
        command: Command::Map {
            action: Some(MapAction::Transform {
                spec,
                name: "reject".to_string(),
                value: "\"x\"".to_string(),
                args: vec![],
            }),
            file: None,
            spec: None,
            output: None,
        },
    };
    let err = run(cli, AppConfig::load().unwrap()).await.unwrap_err();
    assert!(
        err.to_string().contains("transform reject failed"),
        "{err:#}"
    );
}

#[tokio::test]
async fn run_map_transform_rejects_invalid_json_value() {
    let dir = tempdir().unwrap();
    let spec = dir.path().join("map.yaml");
    std::fs::write(
        &spec,
        "transforms:\n  inline: |\n    def f(v):\n        return v\n",
    )
    .unwrap();
    let cli = Cli {
        command: Command::Map {
            action: Some(MapAction::Transform {
                spec,
                name: "f".to_string(),
                value: "not-json".to_string(),
                args: vec![],
            }),
            file: None,
            spec: None,
            output: None,
        },
    };
    let err = run(cli, AppConfig::load().unwrap()).await.unwrap_err();
    assert!(
        err.to_string().contains("value is not valid json"),
        "{err:#}"
    );
}

#[tokio::test]
async fn run_map_without_flat_args_errors() {
    let cli = Cli {
        command: Command::Map {
            action: None,
            file: None,
            spec: None,
            output: None,
        },
    };
    let err = run(cli, AppConfig::load().unwrap()).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("alembic map requires -f, --spec, and -o"),
        "{err:#}"
    );
}

#[tokio::test]
async fn run_plan_missing_credentials_errors() {
    let _guard = cwd_lock().lock().await;
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    let inventory = dir.path().join("inventory.yaml");
    let out = dir.path().join("plan.json");
    std::fs::write(
        &inventory,
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
            file: inventory,
            output: Some(out),
            backend: Some("netbox".to_string()),
            backend_config: None,
            provision: false,
            dry_run: false,
            report: false,
            allow_delete: false,
        },
    };
    let err = run(cli, AppConfig::load().unwrap()).await.unwrap_err();
    assert!(err.to_string().contains("missing NETBOX_URL"));
    std::env::set_current_dir(cwd).unwrap();
}

#[tokio::test]
async fn run_apply_missing_credentials_errors() {
    let _guard = cwd_lock().lock().await;
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    let plan_path = dir.path().join("plan.json");
    std::fs::write(&plan_path, r#"{ "ops": [] }"#).unwrap();
    let cwd = std::env::current_dir().unwrap();
    std::env::set_current_dir(dir.path()).unwrap();

    let cli = Cli {
        command: Command::Apply {
            plan: plan_path,
            output: None,
            backend: Some("netbox".to_string()),
            backend_config: None,
            allow_delete: false,
            interactive: false,
        },
    };
    let err = run(cli, AppConfig::load().unwrap()).await.unwrap_err();
    assert!(err.to_string().contains("missing NETBOX_URL"));
    std::env::set_current_dir(cwd).unwrap();
}

#[tokio::test]
async fn run_apply_interactive_delete_requires_allow_delete() {
    let _guard = cwd_lock().lock().await;
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    let plan_path = dir.path().join("plan.json");
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
        schema_preview: None,
    };
    write_plan(&plan_path, &plan).unwrap();
    let cwd = std::env::current_dir().unwrap();
    std::env::set_current_dir(dir.path()).unwrap();

    // django (write-only): a read-only backend now fails the capability gate before this delete-gate
    let cli = Cli {
        command: Command::Apply {
            plan: plan_path,
            output: None,
            backend: Some("django".to_string()),
            backend_config: None,
            allow_delete: false,
            interactive: true,
        },
    };
    let err = run(cli, AppConfig::load().unwrap()).await.unwrap_err();
    assert!(err
        .to_string()
        .contains("plan contains delete operations; re-run with --allow-delete"));
    std::env::set_current_dir(cwd).unwrap();
}

#[tokio::test]
async fn run_apply_read_only_backend_fails_before_prompting() {
    // the plan path is missing on purpose: the read-only error must fire before
    // read_plan (hence before the post-read_plan prompt loop) on both paths.
    // absence-of-prompt is checked out-of-process in tests/apply_capability.rs.
    let _guard = cwd_lock().lock().await;
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    let missing_plan = dir.path().join("does-not-exist.json");
    let cwd = std::env::current_dir().unwrap();
    std::env::set_current_dir(dir.path()).unwrap();

    for interactive in [true, false] {
        let cli = Cli {
            command: Command::Apply {
                plan: missing_plan.clone(),
                output: None,
                backend: Some("peeringdb".to_string()),
                backend_config: None,
                allow_delete: false,
                interactive,
            },
        };
        let err = run(cli, AppConfig::load().unwrap()).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("backend is read-only; it cannot apply changes"),
            "interactive={interactive}: expected the read-only capability error \
             before the plan is read, got: {err}"
        );
    }
    std::env::set_current_dir(cwd).unwrap();
}

// a one-create plan against a generic rest backend, plus the config pointing at
// `base_url`. shared by the apply-report tests, which differ only in what the
// mocked POST answers.
fn generic_apply_fixture(dir: &Path, base_url: &str) -> (PathBuf, PathBuf) {
    let schema: alembic_core::Schema = serde_yaml::from_str(
        r#"
types:
  dcim.site:
    key:
      name:
        type: string
    fields:
      name:
        type: string
"#,
    )
    .unwrap();
    let mut attrs = BTreeMap::new();
    attrs.insert("name".to_string(), serde_json::json!("fra1"));
    let plan = Plan {
        schema,
        ops: vec![Op::Create {
            uid: uuid::Uuid::from_u128(1),
            type_name: alembic_core::TypeName::new("dcim.site"),
            desired: alembic_core::Object {
                uid: uuid::Uuid::from_u128(1),
                type_name: alembic_core::TypeName::new("dcim.site"),
                key: key_str("name=fra1"),
                attrs: attrs.into(),
                source: None,
            },
        }],
        summary: None,
        schema_preview: None,
    };
    let plan_path = dir.join("plan.json");
    write_plan(&plan_path, &plan).unwrap();

    let config_path = dir.join("backend.yaml");
    std::fs::write(
        &config_path,
        format!(
            "backend: generic\nconfig:\n  base_url: {base_url}\n  types:\n    dcim.site:\n      path: /sites/\n"
        ),
    )
    .unwrap();
    (plan_path, config_path)
}

#[tokio::test(flavor = "multi_thread")]
async fn run_apply_writes_the_report_to_output() {
    use httpmock::Method::POST;

    let _guard = cwd_lock().lock().await;
    let server = httpmock::MockServer::start();
    server.mock(|when, then| {
        when.method(POST).path("/sites/");
        then.status(201).json_body(serde_json::json!({"id": 7}));
    });

    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    let (plan_path, config_path) = generic_apply_fixture(dir.path(), &server.base_url());
    let report_path = dir.path().join("nested/report.json");

    let cwd = std::env::current_dir().unwrap();
    std::env::set_current_dir(dir.path()).unwrap();
    let cli = Cli {
        command: Command::Apply {
            plan: plan_path,
            output: Some(report_path.clone()),
            backend: None,
            backend_config: Some(config_path),
            allow_delete: false,
            interactive: false,
        },
    };
    let result = run(cli, AppConfig::load().unwrap()).await;
    std::env::set_current_dir(cwd).unwrap();
    result.unwrap();

    let report: ApplyReport =
        serde_json::from_str(&std::fs::read_to_string(&report_path).unwrap()).unwrap();
    assert_eq!(report.applied.len(), 1);
    assert_eq!(report.applied[0].uid, uuid::Uuid::from_u128(1));
    assert_eq!(report.applied[0].type_name.as_str(), "dcim.site");
    assert_eq!(
        report.applied[0].backend_id,
        Some(alembic_engine::BackendId::Int(7)),
        "the report must carry the backend id the create returned"
    );
}

// a site plus a device referencing it, against a generic rest backend. the plan
// shape resume exists for: the run dies between the two, and the device can only
// be written once the site's backend id is known.
fn generic_resume_fixture(dir: &Path, base_url: &str) -> (PathBuf, PathBuf) {
    let schema: alembic_core::Schema = serde_yaml::from_str(
        r#"
types:
  dcim.site:
    key:
      name:
        type: string
    fields:
      name:
        type: string
  dcim.device:
    key:
      name:
        type: string
    fields:
      name:
        type: string
      site:
        type: ref
        target: dcim.site
"#,
    )
    .unwrap();
    let site_uid = uuid::Uuid::from_u128(1);
    let device_uid = uuid::Uuid::from_u128(2);
    let mut site_attrs = BTreeMap::new();
    site_attrs.insert("name".to_string(), serde_json::json!("fra1"));
    let mut device_attrs = BTreeMap::new();
    device_attrs.insert("name".to_string(), serde_json::json!("edge1"));
    device_attrs.insert("site".to_string(), serde_json::json!(site_uid.to_string()));
    let plan = Plan {
        schema,
        ops: vec![
            Op::Create {
                uid: site_uid,
                type_name: alembic_core::TypeName::new("dcim.site"),
                desired: alembic_core::Object {
                    uid: site_uid,
                    type_name: alembic_core::TypeName::new("dcim.site"),
                    key: key_str("name=fra1"),
                    attrs: site_attrs.into(),
                    source: None,
                },
            },
            Op::Create {
                uid: device_uid,
                type_name: alembic_core::TypeName::new("dcim.device"),
                desired: alembic_core::Object {
                    uid: device_uid,
                    type_name: alembic_core::TypeName::new("dcim.device"),
                    key: key_str("name=edge1"),
                    attrs: device_attrs.into(),
                    source: None,
                },
            },
        ],
        summary: None,
        schema_preview: None,
    };
    let plan_path = dir.join("plan.json");
    write_plan(&plan_path, &plan).unwrap();

    let config_path = dir.join("backend.yaml");
    std::fs::write(
        &config_path,
        format!(
            "backend: generic\nconfig:\n  base_url: {base_url}\n  types:\n    dcim.site:\n      path: /sites/\n    dcim.device:\n      path: /devices/\n"
        ),
    )
    .unwrap();
    (plan_path, config_path)
}

#[tokio::test(flavor = "multi_thread")]
async fn run_apply_resumes_with_the_ids_the_interrupted_run_created() {
    use httpmock::Method::POST;

    let _guard = cwd_lock().lock().await;
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    let report_path = dir.path().join("report.json");
    let cwd = std::env::current_dir().unwrap();

    // run 1 creates the site and dies on the device. no state is saved: apply only
    // saves on the success path.
    let first = httpmock::MockServer::start();
    first.mock(|when, then| {
        when.method(POST).path("/sites/");
        then.status(201).json_body(serde_json::json!({"id": 7}));
    });
    first.mock(|when, then| {
        when.method(POST).path("/devices/");
        then.status(500);
    });
    let (plan_path, config_path) = generic_resume_fixture(dir.path(), &first.base_url());
    std::env::set_current_dir(dir.path()).unwrap();
    let result = run(
        Cli {
            command: Command::Apply {
                plan: plan_path.clone(),
                output: Some(report_path.clone()),
                backend: None,
                backend_config: Some(config_path),
                allow_delete: false,
                interactive: false,
            },
        },
        AppConfig::load().unwrap(),
    )
    .await;
    std::env::set_current_dir(&cwd).unwrap();
    result.expect_err("the device create fails");
    assert!(!state_path.exists(), "a failed apply saves no state");

    // run 2 is a cold start against a fresh backend: the site must not be created
    // again, and the device must go out with the id the first run got for it.
    let second = httpmock::MockServer::start();
    let sites = second.mock(|when, then| {
        when.method(POST).path("/sites/");
        then.status(201).json_body(serde_json::json!({"id": 99}));
    });
    let devices = second.mock(|when, then| {
        when.method(POST)
            .path("/devices/")
            .json_body_includes(r#"{"site": 7}"#);
        then.status(201).json_body(serde_json::json!({"id": 9}));
    });
    let (plan_path, config_path) = generic_resume_fixture(dir.path(), &second.base_url());
    std::env::set_current_dir(dir.path()).unwrap();
    let result = run(
        Cli {
            command: Command::Apply {
                plan: plan_path,
                output: Some(report_path.clone()),
                backend: None,
                backend_config: Some(config_path),
                allow_delete: false,
                interactive: false,
            },
        },
        AppConfig::load().unwrap(),
    )
    .await;
    std::env::set_current_dir(cwd).unwrap();
    result.unwrap();

    sites.assert_calls(0);
    devices.assert_calls(1);

    let report: ApplyReport =
        serde_json::from_str(&std::fs::read_to_string(&report_path).unwrap()).unwrap();
    assert_eq!(
        report.applied.iter().map(|a| a.uid).collect::<Vec<_>>(),
        vec![uuid::Uuid::from_u128(2)],
        "`applied` stays this run's ops"
    );
    assert_eq!(
        report
            .resumed
            .iter()
            .map(|a| (a.uid, a.backend_id.clone()))
            .collect::<Vec<_>>(),
        vec![(
            uuid::Uuid::from_u128(1),
            Some(alembic_engine::BackendId::Int(7))
        )]
    );
    assert_eq!(report.previously_applied_count, Some(1));

    // and the recovered mapping lands in state, so a later rename plans an update
    // rather than a duplicate create.
    let state = StateStore::load(&state_path).unwrap();
    assert_eq!(
        state.backend_id(
            alembic_core::TypeName::new("dcim.site"),
            uuid::Uuid::from_u128(1)
        ),
        Some(alembic_engine::BackendId::Int(7))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn run_apply_rejects_a_bad_output_path_before_touching_the_backend() {
    use httpmock::Method::POST;

    let _guard = cwd_lock().lock().await;
    let server = httpmock::MockServer::start();
    let mock = server.mock(|when, then| {
        when.method(POST).path("/sites/");
        then.status(201).json_body(serde_json::json!({"id": 7}));
    });

    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    let (plan_path, config_path) = generic_apply_fixture(dir.path(), &server.base_url());
    // the report path's parent is a file, so it can never be created
    let blocker = dir.path().join("blocker");
    std::fs::write(&blocker, "").unwrap();

    let cwd = std::env::current_dir().unwrap();
    std::env::set_current_dir(dir.path()).unwrap();
    let cli = Cli {
        command: Command::Apply {
            plan: plan_path,
            output: Some(blocker.join("report.json")),
            backend: None,
            backend_config: Some(config_path),
            allow_delete: false,
            interactive: false,
        },
    };
    let result = run(cli, AppConfig::load().unwrap()).await;
    std::env::set_current_dir(cwd).unwrap();

    result.expect_err("a bad -o must fail the run");
    assert_eq!(
        mock.calls(),
        0,
        "the output path must be rejected before anything is written to the backend"
    );
    assert!(!state_path.exists(), "no state must be saved either");
}

#[tokio::test(flavor = "multi_thread")]
async fn run_apply_writes_no_report_when_the_apply_fails() {
    use httpmock::Method::POST;

    let _guard = cwd_lock().lock().await;
    let server = httpmock::MockServer::start();
    server.mock(|when, then| {
        when.method(POST).path("/sites/");
        then.status(500);
    });

    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    let (plan_path, config_path) = generic_apply_fixture(dir.path(), &server.base_url());
    let report_path = dir.path().join("report.json");

    let cwd = std::env::current_dir().unwrap();
    std::env::set_current_dir(dir.path()).unwrap();
    let cli = Cli {
        command: Command::Apply {
            plan: plan_path,
            output: Some(report_path.clone()),
            backend: None,
            backend_config: Some(config_path),
            allow_delete: false,
            interactive: false,
        },
    };
    let result = run(cli, AppConfig::load().unwrap()).await;
    std::env::set_current_dir(cwd).unwrap();

    result.expect_err("a failing apply must not report success");
    assert!(
        !report_path.exists(),
        "a partial apply must not leave a report claiming it completed"
    );
}

// registers the nautobot bootstrap list mocks shared by the plan/report tests:
// content-types (one `dcim.device`), empty custom-fields, empty tags, and a
// `dcim/devices/` list returning `device_results`. httpmock mocks persist for
// the server's lifetime (only `MockServer` implements `Drop`), so the mock
// handles are dropped and just the server is returned.
fn nautobot_plan_server(device_results: serde_json::Value) -> httpmock::MockServer {
    use httpmock::Method::GET;
    use serde_json::json;

    let server = httpmock::MockServer::start();
    server.mock(|when, then| {
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
    server.mock(|when, then| {
        when.method(GET).path("/api/extras/custom-fields/");
        then.status(200).json_body(json!({
            "count": 0,
            "next": null,
            "previous": null,
            "results": []
        }));
    });
    server.mock(|when, then| {
        when.method(GET).path("/api/extras/tags/");
        then.status(200).json_body(json!({
            "count": 0,
            "next": null,
            "previous": null,
            "results": []
        }));
    });
    let count = device_results.as_array().map_or(0, |r| r.len());
    server.mock(|when, then| {
        when.method(GET)
            .path("/api/dcim/devices/")
            .query_param("limit", "200")
            .query_param("offset", "0");
        then.status(200).json_body(json!({
            "count": count,
            "next": null,
            "previous": null,
            "results": device_results
        }));
    });
    server
}

#[tokio::test(flavor = "multi_thread")]
async fn run_plan_nautobot_backend() {
    use serde_json::json;

    let _guard = cwd_lock().lock().await;
    let server = nautobot_plan_server(json!([]));
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    let inventory = dir.path().join("inventory.yaml");
    let out = dir.path().join("plan.json");
    let config = dir.path().join("adapter.yaml");
    std::fs::write(
        &inventory,
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
    std::fs::write(
        &config,
        format!(
            "backend: nautobot\nurl: {}\ntoken: token\n",
            server.base_url()
        ),
    )
    .unwrap();

    let cwd = std::env::current_dir().unwrap();
    std::env::set_current_dir(dir.path()).unwrap();

    let cli = Cli {
        command: Command::Plan {
            file: inventory,
            output: Some(out.clone()),
            backend: None,
            backend_config: Some(config),
            provision: false,
            dry_run: false,
            report: false,
            allow_delete: false,
        },
    };
    run(cli, AppConfig::load().unwrap()).await.unwrap();

    let raw = std::fs::read_to_string(&out).unwrap();
    assert!(raw.contains("\"op\": \"create\""));
    assert!(raw.contains("\"type_name\": \"dcim.device\""));

    std::env::set_current_dir(cwd).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn run_plan_report_is_read_only() {
    use serde_json::json;

    let _guard = cwd_lock().lock().await;
    let server = nautobot_plan_server(json!([]));
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    let inventory = dir.path().join("inventory.yaml");
    let out = dir.path().join("plan.json");
    let config = dir.path().join("adapter.yaml");
    std::fs::write(
        &inventory,
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
    std::fs::write(
        &config,
        format!(
            "backend: nautobot\nurl: {}\ntoken: token\n",
            server.base_url()
        ),
    )
    .unwrap();

    let cwd = std::env::current_dir().unwrap();
    std::env::set_current_dir(dir.path()).unwrap();

    let cli = Cli {
        command: Command::Plan {
            file: inventory,
            output: Some(out.clone()),
            backend: None,
            backend_config: Some(config),
            provision: false,
            dry_run: false,
            report: true,
            allow_delete: false,
        },
    };
    run(cli, AppConfig::load().unwrap()).await.unwrap();

    // --report is read-only: no plan file is written and no state is persisted.
    assert!(!out.exists(), "plan file must not be written for --report");
    assert!(
        !dir.path().join(".alembic/state.json").exists(),
        "state must not be saved for --report"
    );

    std::env::set_current_dir(cwd).unwrap();
}

#[test]
fn should_detect_deletes_forces_on_for_report() {
    // report mode never applies the plan, so it forces delete-detection on to
    // surface backend-only objects as `extra`.
    assert!(should_detect_deletes(false, true));
    assert!(should_detect_deletes(true, true));
    // non-report paths are governed solely by --allow-delete.
    assert!(should_detect_deletes(true, false));
    assert!(!should_detect_deletes(false, false));
}

#[test]
fn report_and_dry_run_conflict() {
    use clap::Parser;
    // --report and --dry-run both exit without applying; passing both is rejected
    // at parse time rather than silently dropping --dry-run.
    let result = Cli::try_parse_from([
        "alembic",
        "plan",
        "-f",
        "inventory.yaml",
        "-o",
        "plan.json",
        "--report",
        "--dry-run",
    ]);
    // `Cli` is not `Debug`, so unwrap the error via `Option` rather than `expect_err`.
    let err = result.err().expect("--report and --dry-run must conflict");
    assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
}

#[test]
fn provision_conflicts_with_dry_run_but_not_report() {
    use clap::Parser;
    // --provision runs ensure_schema (a real backend schema write). --dry-run
    // promises a non-writing preview, so combining them is a footgun and is
    // rejected at parse time. --report --provision, by contrast, is the
    // documented "provision schema, then preview drift" workflow (docs/cli.md)
    // and must still parse.
    let conflict = Cli::try_parse_from([
        "alembic",
        "plan",
        "-f",
        "inventory.yaml",
        "-o",
        "plan.json",
        "--provision",
        "--dry-run",
    ]);
    // `Cli` is not `Debug`, so unwrap the error via `Option` rather than `expect_err`.
    let err = conflict
        .err()
        .expect("--provision and --dry-run must conflict");
    assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);

    // the documented provision-then-preview combo must keep parsing (it fails
    // later on the backend requirement, not here).
    let report = Cli::try_parse_from([
        "alembic",
        "plan",
        "-f",
        "inventory.yaml",
        "-o",
        "plan.json",
        "--provision",
        "--report",
    ]);
    assert!(
        report.is_ok(),
        "--report --provision must still parse: {:?}",
        report.err()
    );
}

#[test]
fn plan_report_output_optional() {
    use clap::Parser;
    // --report exits without writing a plan file, so -o/--output is not required.
    let result = Cli::try_parse_from(["alembic", "plan", "-f", "inventory.yaml", "--report"]);
    assert!(result.is_ok(), "plan --report without -o must parse");
}

#[test]
fn plan_dry_run_output_optional() {
    use clap::Parser;
    // --dry-run prints raw json to stdout without writing a plan file, so -o/--output
    // is not required.
    let result = Cli::try_parse_from(["alembic", "plan", "-f", "inventory.yaml", "--dry-run"]);
    assert!(result.is_ok(), "plan --dry-run without -o must parse");
}

#[test]
fn plan_write_mode_requires_output() {
    use clap::Parser;
    // the default write path still requires -o/--output.
    let result = Cli::try_parse_from(["alembic", "plan", "-f", "inventory.yaml"]);
    let err = result.err().expect("plan write-mode without -o must fail");
    assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
}

#[test]
fn apply_output_is_optional_and_takes_the_short_flag() {
    use clap::Parser;
    // -o is optional (an apply without it writes nothing) and spells the same
    // thing it does on plan/map/import.
    assert!(
        Cli::try_parse_from(["alembic", "apply", "-p", "plan.json"]).is_ok(),
        "apply without -o must keep parsing"
    );
    let parsed = Cli::try_parse_from(["alembic", "apply", "-p", "plan.json", "-o", "report.json"])
        .expect("apply -o must parse");
    let Command::Apply { output, .. } = parsed.command else {
        panic!("expected the apply subcommand");
    };
    assert_eq!(output, Some(PathBuf::from("report.json")));
}

#[test]
fn plan_report_with_output_still_parses() {
    use clap::Parser;
    // backward-compat: -o alongside --report is still accepted, not rejected as a
    // conflict, so existing scripts that always pass -o keep working.
    let result = Cli::try_parse_from([
        "alembic",
        "plan",
        "-f",
        "inventory.yaml",
        "-o",
        "plan.json",
        "--report",
    ]);
    assert!(result.is_ok(), "plan -o ... --report must still parse");
}

#[tokio::test(flavor = "multi_thread")]
async fn run_plan_report_surfaces_extra() {
    // an object present on the backend but not declared in intent must surface
    // under --report even without --allow-delete.
    use serde_json::json;

    let _guard = cwd_lock().lock().await;
    let server = nautobot_plan_server(json!([{ "id": "uuid-leaf01", "name": "leaf01" }]));
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    let inventory = dir.path().join("inventory.yaml");
    let config = dir.path().join("adapter.yaml");
    // intent declares the schema but no objects; the backend holds an unmanaged
    // device (leaf01), so the only drift is one `extra`.
    std::fs::write(
        &inventory,
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
objects: []
"#,
    )
    .unwrap();
    std::fs::write(
        &config,
        format!(
            "backend: nautobot\nurl: {}\ntoken: token\n",
            server.base_url()
        ),
    )
    .unwrap();

    let cwd = std::env::current_dir().unwrap();
    std::env::set_current_dir(dir.path()).unwrap();

    let inventory = load_inventory(&inventory).unwrap();
    let mut state = load_state().await.unwrap();
    let backend = create_backend(&[], None, Some(config)).unwrap();

    // the buggy threading (allow_delete alone) never emits deletes, so the
    // `extra` category is silently empty even though leaf01 is unmanaged.
    let buggy = build_plan(
        backend.observer().unwrap(),
        &inventory,
        &mut state,
        should_detect_deletes(false, false),
    )
    .await
    .unwrap();
    assert!(
        DriftReport::from_plan(&buggy).extra.is_empty(),
        "without report-forced delete-detection the extra is invisible"
    );

    // report mode forces delete-detection, so the unmanaged backend object
    // surfaces as an `extra` even though --allow-delete was not passed.
    let plan = build_plan(
        backend.observer().unwrap(),
        &inventory,
        &mut state,
        should_detect_deletes(false, true),
    )
    .await
    .unwrap();
    let drift = DriftReport::from_plan(&plan);
    assert_eq!(
        drift.extra.len(),
        1,
        "report mode must surface unmanaged backend objects as extra"
    );
    assert_eq!(
        drift.extra[0].type_name,
        alembic_core::TypeName::new("dcim.device")
    );
    assert_eq!(drift.extra[0].key, key_str("name=leaf01"));
    assert!(drift.missing.is_empty());
    assert!(drift.changed.is_empty());

    std::env::set_current_dir(cwd).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn minimal_external_adapter() {
    // this test depends on the example "minimal_external_adapter" in this crate.
    // note that `cargo test` will build all examples, so we can expect the binary to exist.

    let example_binary = find_example_binary("minimal_external_adapter");

    let config = AdapterConfig::External(ExternalConfig {
        command: Some(example_binary.to_str().unwrap().to_string()),
        args: Vec::new(),
        working_dir: None,
        env: BTreeMap::new(),
        timeout_seconds: Some(5),
        setup: serde_yaml::Value::default(),
    });

    let backend = config.build().unwrap();

    let response = backend
        .emitter()
        .unwrap()
        .write(
            &Schema::default(),
            &[],
            &StateStore::new(Option::None, StateData::default()),
        )
        .await;

    if let Ok(ok_response) = response {
        assert!(ok_response.applied.is_empty())
    } else {
        panic!("error response from plugin: {}", response.unwrap_err())
    }
}

// drives the real run() for `plan --provision` against an external adapter whose
// preview_schema errors (ensure_schema stays defaulted/Ok). the provision guard
// must propagate that Err and abort before ensure_schema, so a preview hiccup
// cannot slip past the --allow-delete gate and provision blind. this fails
// against the old `if let Ok(Some(..))` swallow (run completes Ok) and passes
// once the guard uses `?` to fail closed, mirroring the engine apply path.
#[tokio::test(flavor = "multi_thread")]
async fn run_plan_provision_fails_closed_on_preview_error() {
    let _guard = cwd_lock().lock().await;
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;

    let example_binary = find_example_binary("preview_error_adapter");
    let inventory = write_minimal_inventory(dir.path());
    let out_path = dir.path().join("plan.json");
    let config_path = dir.path().join("backend.yaml");
    std::fs::write(
        &config_path,
        format!(
            "backend: external\ncommand: \"{}\"\ntimeout_seconds: 5\n",
            example_binary.to_str().unwrap()
        ),
    )
    .unwrap();

    let cwd = std::env::current_dir().unwrap();
    std::env::set_current_dir(dir.path()).unwrap();
    let cli = Cli {
        command: Command::Plan {
            file: inventory,
            output: Some(out_path),
            backend: Some("external".to_string()),
            backend_config: Some(config_path),
            provision: true,
            dry_run: false,
            report: false,
            allow_delete: false,
        },
    };
    let result = run(cli, AppConfig::load().unwrap()).await;
    std::env::set_current_dir(cwd).unwrap();

    let err = result.expect_err("a preview error must abort provisioning, not fall through");
    assert!(
        err.to_string().contains("preview failed for test"),
        "expected the propagated preview error, got: {err:#}"
    );
}

fn find_example_binary(name: &str) -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let target_dir = manifest_dir
        .ancestors()
        .find(|p| p.join("target").exists())
        .unwrap()
        .join("target");

    let mut example_binary = target_dir;

    if std::env::var("CI").is_ok() {
        example_binary.push("ci");
    }

    example_binary.push("debug");
    example_binary.push("examples");
    example_binary.push(name);

    example_binary
}
