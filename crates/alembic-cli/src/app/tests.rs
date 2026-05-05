use super::test_support::*;
use super::*;
use crate::app::plugins::run_plugin;
use alembic_engine::plugin::PluginResponse;
use alembic_engine::Op;
use std::collections::BTreeMap;
use tempfile::tempdir;
use AppConfig;

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
    let _guard = env_lock().lock().unwrap();
    let old_backend = std::env::var("ALEMBIC_STATE_BACKEND").ok();
    let old_path = std::env::var("ALEMBIC_STATE_PATH").ok();
    std::env::remove_var("ALEMBIC_STATE_BACKEND");
    std::env::remove_var("ALEMBIC_STATE_PATH");

    let root = Path::new("/tmp/example");
    let config = resolve_state_backend_config(root).unwrap();
    assert_eq!(
        config,
        StateBackendConfig::Local {
            path: root.join(".alembic/state.json")
        }
    );

    if let Some(value) = old_backend {
        std::env::set_var("ALEMBIC_STATE_BACKEND", value);
    } else {
        std::env::remove_var("ALEMBIC_STATE_BACKEND");
    }
    if let Some(value) = old_path {
        std::env::set_var("ALEMBIC_STATE_PATH", value);
    } else {
        std::env::remove_var("ALEMBIC_STATE_PATH");
    }
}

#[test]
fn resolve_state_backend_uses_custom_local_path() {
    let _guard = env_lock().lock().unwrap();
    let old_backend = std::env::var("ALEMBIC_STATE_BACKEND").ok();
    let old_path = std::env::var("ALEMBIC_STATE_PATH").ok();
    std::env::set_var("ALEMBIC_STATE_BACKEND", "local");
    std::env::set_var("ALEMBIC_STATE_PATH", "/tmp/custom-state.json");

    let config = resolve_state_backend_config(Path::new("/tmp/ignored")).unwrap();
    assert_eq!(
        config,
        StateBackendConfig::Local {
            path: PathBuf::from("/tmp/custom-state.json")
        }
    );

    if let Some(value) = old_backend {
        std::env::set_var("ALEMBIC_STATE_BACKEND", value);
    } else {
        std::env::remove_var("ALEMBIC_STATE_BACKEND");
    }
    if let Some(value) = old_path {
        std::env::set_var("ALEMBIC_STATE_PATH", value);
    } else {
        std::env::remove_var("ALEMBIC_STATE_PATH");
    }
}

#[test]
fn resolve_state_backend_postgres_requires_url() {
    let _guard = env_lock().lock().unwrap();
    let old_backend = std::env::var("ALEMBIC_STATE_BACKEND").ok();
    let old_url = std::env::var("ALEMBIC_STATE_POSTGRES_URL").ok();
    let old_tls = std::env::var("ALEMBIC_STATE_POSTGRES_TLS").ok();
    std::env::set_var("ALEMBIC_STATE_BACKEND", "postgres");
    std::env::remove_var("ALEMBIC_STATE_POSTGRES_URL");

    let err = resolve_state_backend_config(Path::new("/tmp/ignored")).unwrap_err();
    assert!(err.to_string().contains("ALEMBIC_STATE_POSTGRES_URL"));

    if let Some(value) = old_backend {
        std::env::set_var("ALEMBIC_STATE_BACKEND", value);
    } else {
        std::env::remove_var("ALEMBIC_STATE_BACKEND");
    }
    if let Some(value) = old_url {
        std::env::set_var("ALEMBIC_STATE_POSTGRES_URL", value);
    } else {
        std::env::remove_var("ALEMBIC_STATE_POSTGRES_URL");
    }
    if let Some(value) = old_tls {
        std::env::set_var("ALEMBIC_STATE_POSTGRES_TLS", value);
    } else {
        std::env::remove_var("ALEMBIC_STATE_POSTGRES_TLS");
    }
}

#[test]
fn resolve_state_backend_postgres_with_default_key() {
    let _guard = env_lock().lock().unwrap();
    let old_backend = std::env::var("ALEMBIC_STATE_BACKEND").ok();
    let old_url = std::env::var("ALEMBIC_STATE_POSTGRES_URL").ok();
    let old_key = std::env::var("ALEMBIC_STATE_KEY").ok();
    let old_tls = std::env::var("ALEMBIC_STATE_POSTGRES_TLS").ok();
    std::env::set_var("ALEMBIC_STATE_BACKEND", "postgres");
    std::env::set_var(
        "ALEMBIC_STATE_POSTGRES_URL",
        "postgres://user:pass@localhost:5432/alembic",
    );
    std::env::remove_var("ALEMBIC_STATE_KEY");
    std::env::remove_var("ALEMBIC_STATE_POSTGRES_TLS");

    let config = resolve_state_backend_config(Path::new("/tmp/ignored")).unwrap();
    assert_eq!(
        config,
        StateBackendConfig::Postgres {
            url: "postgres://user:pass@localhost:5432/alembic".to_string(),
            key: "default".to_string(),
            tls_mode: PostgresTlsMode::Disable,
        }
    );

    if let Some(value) = old_backend {
        std::env::set_var("ALEMBIC_STATE_BACKEND", value);
    } else {
        std::env::remove_var("ALEMBIC_STATE_BACKEND");
    }
    if let Some(value) = old_url {
        std::env::set_var("ALEMBIC_STATE_POSTGRES_URL", value);
    } else {
        std::env::remove_var("ALEMBIC_STATE_POSTGRES_URL");
    }
    if let Some(value) = old_key {
        std::env::set_var("ALEMBIC_STATE_KEY", value);
    } else {
        std::env::remove_var("ALEMBIC_STATE_KEY");
    }
    if let Some(value) = old_tls {
        std::env::set_var("ALEMBIC_STATE_POSTGRES_TLS", value);
    } else {
        std::env::remove_var("ALEMBIC_STATE_POSTGRES_TLS");
    }
}

#[test]
fn resolve_state_backend_postgres_with_tls_require() {
    let _guard = env_lock().lock().unwrap();
    let old_backend = std::env::var("ALEMBIC_STATE_BACKEND").ok();
    let old_url = std::env::var("ALEMBIC_STATE_POSTGRES_URL").ok();
    let old_key = std::env::var("ALEMBIC_STATE_KEY").ok();
    let old_tls = std::env::var("ALEMBIC_STATE_POSTGRES_TLS").ok();
    std::env::set_var("ALEMBIC_STATE_BACKEND", "postgres");
    std::env::set_var(
        "ALEMBIC_STATE_POSTGRES_URL",
        "postgres://user:pass@localhost:5432/alembic",
    );
    std::env::set_var("ALEMBIC_STATE_KEY", "workspace-a");
    std::env::set_var("ALEMBIC_STATE_POSTGRES_TLS", "require");

    let config = resolve_state_backend_config(Path::new("/tmp/ignored")).unwrap();
    assert_eq!(
        config,
        StateBackendConfig::Postgres {
            url: "postgres://user:pass@localhost:5432/alembic".to_string(),
            key: "workspace-a".to_string(),
            tls_mode: PostgresTlsMode::Require,
        }
    );

    if let Some(value) = old_backend {
        std::env::set_var("ALEMBIC_STATE_BACKEND", value);
    } else {
        std::env::remove_var("ALEMBIC_STATE_BACKEND");
    }
    if let Some(value) = old_url {
        std::env::set_var("ALEMBIC_STATE_POSTGRES_URL", value);
    } else {
        std::env::remove_var("ALEMBIC_STATE_POSTGRES_URL");
    }
    if let Some(value) = old_key {
        std::env::set_var("ALEMBIC_STATE_KEY", value);
    } else {
        std::env::remove_var("ALEMBIC_STATE_KEY");
    }
    if let Some(value) = old_tls {
        std::env::set_var("ALEMBIC_STATE_POSTGRES_TLS", value);
    } else {
        std::env::remove_var("ALEMBIC_STATE_POSTGRES_TLS");
    }
}

#[test]
fn resolve_state_backend_postgres_with_invalid_tls_mode_errors() {
    let _guard = env_lock().lock().unwrap();
    let old_backend = std::env::var("ALEMBIC_STATE_BACKEND").ok();
    let old_url = std::env::var("ALEMBIC_STATE_POSTGRES_URL").ok();
    let old_tls = std::env::var("ALEMBIC_STATE_POSTGRES_TLS").ok();
    std::env::set_var("ALEMBIC_STATE_BACKEND", "postgres");
    std::env::set_var(
        "ALEMBIC_STATE_POSTGRES_URL",
        "postgres://user:pass@localhost:5432/alembic",
    );
    std::env::set_var("ALEMBIC_STATE_POSTGRES_TLS", "weird");

    let err = resolve_state_backend_config(Path::new("/tmp/ignored")).unwrap_err();
    assert!(err.to_string().contains("ALEMBIC_STATE_POSTGRES_TLS"));

    if let Some(value) = old_backend {
        std::env::set_var("ALEMBIC_STATE_BACKEND", value);
    } else {
        std::env::remove_var("ALEMBIC_STATE_BACKEND");
    }
    if let Some(value) = old_url {
        std::env::set_var("ALEMBIC_STATE_POSTGRES_URL", value);
    } else {
        std::env::remove_var("ALEMBIC_STATE_POSTGRES_URL");
    }
    if let Some(value) = old_tls {
        std::env::set_var("ALEMBIC_STATE_POSTGRES_TLS", value);
    } else {
        std::env::remove_var("ALEMBIC_STATE_POSTGRES_TLS");
    }
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
    assert!(calls
        .iter()
        .any(|call| { call.args == vec!["manage.py".to_string(), "makemigrations".to_string()] }));
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
    let source = inventory.objects[0].source.as_ref().unwrap();
    assert_eq!(source.file, raw);
    assert_eq!(source.line, None);
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
        },
    };
    run(cli, AppConfig::default()).await.unwrap();
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
    run(cli, AppConfig::default()).await.unwrap();
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
            output: out,
            backend: Some("netbox".to_string()),
            backend_config: None,
            provision: false,
            dry_run: false,
            allow_delete: false,
        },
    };
    let err = run(cli, AppConfig::default()).await.unwrap_err();
    assert!(err.to_string().contains("missing NETBOX_URL"));
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
            backend: Some("netbox".to_string()),
            backend_config: None,
            allow_delete: false,
            interactive: false,
        },
    };
    let err = run(cli, AppConfig::default()).await.unwrap_err();
    assert!(err.to_string().contains("missing NETBOX_URL"));
    std::env::set_current_dir(cwd).unwrap();
}

#[tokio::test]
async fn run_apply_interactive_delete_requires_allow_delete() {
    let _guard = cwd_lock().lock().await;
    let dir = tempdir().unwrap();
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
    };
    write_plan(&plan_path, &plan).unwrap();
    let cwd = std::env::current_dir().unwrap();
    std::env::set_current_dir(dir.path()).unwrap();

    let cli = Cli {
        command: Command::Apply {
            plan: plan_path,
            backend: Some("peeringdb".to_string()),
            backend_config: None,
            allow_delete: false,
            interactive: true,
        },
    };
    let err = run(cli, AppConfig::default()).await.unwrap_err();
    assert!(err
        .to_string()
        .contains("plan contains delete operations; re-run with --allow-delete"));
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
    let config = dir.path().join("adapter.yaml");
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
    std::fs::write(
        &config,
        format!(
            "backend: nautobot\nurl: {}\ntoken: token\n",
            server.base_url()
        ),
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
            output: out.clone(),
            backend: None,
            backend_config: Some(config),
            provision: false,
            dry_run: false,
            allow_delete: false,
        },
    };
    run(cli, AppConfig::default()).await.unwrap();

    let raw = std::fs::read_to_string(&out).unwrap();
    assert!(raw.contains("\"op\": \"create\""));
    assert!(raw.contains("\"type_name\": \"dcim.device\""));

    std::env::set_current_dir(cwd).unwrap();
}

#[test]
fn minimal_plugin() {
    let response = build_and_run_plugin("minimal_plugin", &AppConfig::default());

    if let Ok(ok_response) = response {
        assert!(ok_response.ok)
    } else {
        panic!("didn't get a response from plugin")
    }
}

#[test]
fn outdated_plugin() {
    let response = build_and_run_plugin("outdated_plugin", &AppConfig::default());

    if let Ok(ok_response) = response {
        assert!(!ok_response.ok)
    } else {
        panic!("didn't get a response from plugin")
    }
}

fn build_and_run_plugin(name: &str, config: &AppConfig) -> Result<PluginResponse> {
    escargot::CargoBuild::new().example(name);
    run_plugin(name, config)
}
