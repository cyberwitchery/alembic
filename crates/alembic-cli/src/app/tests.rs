use super::test_support::*;
use super::*;
use alembic_adapter_django::emit::{run_emit, DjangoConfig};
use alembic_adapter_registry::{AdapterConfig, ExternalConfig};
use alembic_core::{Inventory, Schema};
use alembic_engine::{Op, StateData, StateLock, StateStore};
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

/// the backend identity CLI state tests scope paths by.
fn test_identity() -> alembic_engine::BackendIdentity {
    alembic_engine::BackendIdentity::new("netbox", "https://netbox.example.com")
}
#[test]
fn state_path_is_scoped_per_backend_under_dot_alembic() {
    let root = Path::new("/tmp/example");
    let identity = test_identity();
    let path = state_path(root, &identity);
    assert!(path.starts_with("/tmp/example/.alembic/state"));
    assert!(path
        .file_name()
        .unwrap()
        .to_string_lossy()
        .starts_with("netbox-"));
    // a different instance gets a different file.
    let other = alembic_engine::BackendIdentity::new("netbox", "https://other.example.com");
    assert_ne!(path, state_path(root, &other));
}

#[test]
fn resolve_state_backend_defaults_to_local() {
    let _env = EnvVarGuard::acquire(&[
        ("ALEMBIC_STATE_BACKEND", None),
        ("ALEMBIC_STATE_PATH", None),
    ]);

    let root = Path::new("/tmp/example");
    let config = resolve_state_backend_config(root, &test_identity()).unwrap();
    assert_eq!(
        config,
        StateBackendConfig::Local {
            path: state_path(root, &test_identity())
        }
    );
}

#[test]
fn resolve_state_backend_uses_custom_local_path() {
    let _env = EnvVarGuard::acquire(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some("/tmp/custom-state.json")),
    ]);

    let config = resolve_state_backend_config(Path::new("/tmp/ignored"), &test_identity()).unwrap();
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

    let err =
        resolve_state_backend_config(Path::new("/tmp/ignored"), &test_identity()).unwrap_err();
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

    let config = resolve_state_backend_config(Path::new("/tmp/ignored"), &test_identity()).unwrap();
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

    let config = resolve_state_backend_config(Path::new("/tmp/ignored"), &test_identity()).unwrap();
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

    let err =
        resolve_state_backend_config(Path::new("/tmp/ignored"), &test_identity()).unwrap_err();
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
fn write_validation_report_creates_missing_parent_dirs() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("nested/out/validation.json");
    write_validation_report(&path, &alembic_core::LocatedReport::default()).unwrap();
    assert!(path.exists());
}

#[test]
fn validation_report_json_is_tagged_and_round_trips() {
    // the variant and its named fields are the point: a consumer switches on
    // `kind` and reads `detail`, rather than parsing the rendered message.
    let report = alembic_core::LocatedReport {
        errors: vec![alembic_core::LocatedError::with_source(
            alembic_core::ValidationError::ExtraAttrField {
                type_name: "dcim.site".to_string(),
                field: "bogus".to_string(),
            },
            Some(alembic_core::SourceLocation::file_line(
                "inventory.yaml",
                13,
            )),
        )],
    };

    let raw = serde_json::to_string_pretty(&report).unwrap();
    let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(value["errors"][0]["error"]["kind"], "extra_attr_field");
    assert_eq!(value["errors"][0]["error"]["detail"]["field"], "bogus");
    assert_eq!(value["errors"][0]["source"]["line"], 13);

    let read_back: alembic_core::LocatedReport = serde_json::from_str(&raw).unwrap();
    assert_eq!(read_back, report);
}

#[test]
fn write_drift_report_creates_missing_parent_dirs() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("nested/out/drift.json");
    write_drift_report(&path, &DriftReport::default()).unwrap();
    assert!(path.exists());
}

#[test]
fn drift_report_json_names_every_category() {
    // an empty category is written as an empty list, not omitted, so a consumer
    // reads "no drift here" rather than having to infer it from a missing key.
    let dir = tempdir().unwrap();
    let path = dir.path().join("drift.json");
    write_drift_report(&path, &DriftReport::default()).unwrap();

    let raw = std::fs::read_to_string(&path).unwrap();
    let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
    for category in ["changed", "missing", "extra"] {
        assert_eq!(
            value[category],
            serde_json::json!([]),
            "category {category} missing from an empty report: {raw}"
        );
    }
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
fn read_plan_rejects_a_misspelled_key() {
    // the plan file is the one document the host takes from someone else. a
    // misspelled `schema_preview` read as a plan carrying none, and apply's early
    // delete gate is only reached by the plans that carry one.
    let dir = tempdir().unwrap();
    let path = dir.path().join("plan.json");
    std::fs::write(
        &path,
        r#"{"schema":{"types":{}},"ops":[],"schema_preveiw":{"deleted_object_types":["dcim.site"]}}"#,
    )
    .unwrap();
    let err = read_plan(&path).unwrap_err();
    assert!(format!("{err:#}").contains("schema_preveiw"), "{err:#}");
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
fn preflight_output_path_rejects_a_directory() {
    // `-o outdir` where outdir exists: creating the parent says nothing, and the
    // real write fails with EISDIR after a full observation.
    let dir = tempdir().unwrap();
    let target = dir.path().join("outdir");
    std::fs::create_dir(&target).unwrap();
    let err = io::preflight_output_path(&target).expect_err("a directory is not a writable output");
    assert!(
        format!("{err:#}").contains("is a directory"),
        "the error must name the reason: {err:#}"
    );
}

#[cfg(unix)]
#[test]
fn preflight_output_path_rejects_an_unwritable_parent() {
    use std::os::unix::fs::PermissionsExt;
    // the parent exists and needs no creating, so ensure_parent_dir passes it and
    // the write fails with EACCES.
    let dir = tempdir().unwrap();
    let readonly = dir.path().join("ro");
    std::fs::create_dir(&readonly).unwrap();
    std::fs::set_permissions(&readonly, std::fs::Permissions::from_mode(0o555)).unwrap();

    // root ignores the mode bits, so the path really is writable there and a
    // rejection would be the wrong answer; assert whichever the os actually gives
    let denied = std::fs::write(readonly.join("direct"), b"").is_err();
    let result = io::preflight_output_path(&readonly.join("drift.json"));

    // restore before asserting, so a failure still lets tempdir clean up
    std::fs::set_permissions(&readonly, std::fs::Permissions::from_mode(0o755)).unwrap();
    if denied {
        let err = result.expect_err("a read-only parent is not a writable output");
        assert!(
            format!("{err:#}").contains("write output"),
            "the error must name the output write: {err:#}"
        );
    } else {
        result.expect("a parent this user can write to is a valid output");
    }
}

#[test]
fn preflight_output_path_leaves_no_probe_and_no_directory() {
    // side-effect-free: it creates the missing parents to probe them, then takes
    // every one back, so a run that dies later leaves nothing behind.
    let dir = tempdir().unwrap();
    let target = dir.path().join("brand").join("new").join("plan.json");
    io::preflight_output_path(&target).expect("a fresh nested path is writable");
    assert!(
        !dir.path().join("brand").exists(),
        "the probe must remove every directory it created"
    );
    assert_eq!(
        std::fs::read_dir(dir.path()).unwrap().count(),
        0,
        "and leave no probe file"
    );
}

#[cfg(unix)]
#[test]
fn preflight_output_path_rejects_an_unwritable_existing_file() {
    use std::os::unix::fs::PermissionsExt;
    // the parent accepts writes, so the sibling probe passes it and the real
    // write fails with EACCES after a full observation.
    let dir = tempdir().unwrap();

    // root ignores the mode bits; ask the os on a separate file rather than
    // skipping, and assert whichever answer it gives
    let sentinel = dir.path().join("sentinel");
    std::fs::write(&sentinel, b"x").unwrap();
    std::fs::set_permissions(&sentinel, std::fs::Permissions::from_mode(0o444)).unwrap();
    let denied = std::fs::write(&sentinel, b"y").is_err();

    let target = dir.path().join("plan.json");
    std::fs::write(&target, "previous").unwrap();
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o444)).unwrap();
    let result = io::preflight_output_path(&target);

    // restore before asserting, so a failure still lets tempdir clean up
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644)).unwrap();
    if denied {
        let err = result.expect_err("an unwritable target is not a writable output");
        assert!(
            format!("{err:#}").contains("write output"),
            "the error must name the output write: {err:#}"
        );
    } else {
        result.expect("a target this user can write to is a valid output");
    }
    // and the probe reads the permission without touching the contents
    assert_eq!(std::fs::read_to_string(&target).unwrap(), "previous");
}

#[cfg(unix)]
#[test]
fn preflight_output_path_does_not_hang_on_a_fifo() {
    // opening a fifo for write blocks until a reader attaches, so only a regular
    // target is probed directly. without that guard this never returns.
    let dir = tempdir().unwrap();
    let target = dir.path().join("fifo");
    let made = std::process::Command::new("mkfifo")
        .arg(&target)
        .status()
        .expect("mkfifo must be runnable");
    assert!(made.success(), "mkfifo failed");

    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(io::preflight_output_path(&target).is_ok());
    });
    rx.recv_timeout(std::time::Duration::from_secs(10))
        .expect("the preflight must not block on a fifo");
}

#[cfg(unix)]
#[test]
fn preflight_output_path_accepts_a_character_device() {
    // the other side of that guard: /dev/null settles its own open at once, and
    // its parent takes no new file, so the sibling probe answers the wrong
    // question and refuses a target the write accepts.
    let target = Path::new("/dev/null");
    io::preflight_output_path(target).expect("/dev/null is a writable output");
}

#[test]
fn preflight_output_path_accepts_an_existing_file_and_keeps_it() {
    // overwriting an existing output is normal; the target probe must open it
    // without truncating.
    let dir = tempdir().unwrap();
    let target = dir.path().join("plan.json");
    std::fs::write(&target, "previous").unwrap();
    io::preflight_output_path(&target).expect("an existing writable file is a fine output");
    assert_eq!(std::fs::read_to_string(&target).unwrap(), "previous");
    // and the write path still recreates what the probe removed
    let plan = Plan {
        schema: Schema::default(),
        ops: vec![],
        summary: None,
        schema_preview: None,
    };
    write_plan(&target, &plan).unwrap();
    assert_eq!(read_plan(&target).unwrap().ops.len(), 0);
}

#[test]
fn preflight_output_path_accepts_a_bare_filename() {
    // a bare filename has an empty parent, which create_dir_all cannot take.
    let _cwd = CwdGuard::acquire();
    let dir = tempdir().unwrap();
    std::env::set_current_dir(dir.path()).unwrap();
    let result = io::preflight_output_path(Path::new("plan.json"));
    let leftovers = std::fs::read_dir(dir.path()).unwrap().count();
    result.expect("a bare filename in a writable cwd is a valid output");
    assert_eq!(leftovers, 0, "the probe must clean up after itself");
}

#[test]
fn output_path_is_the_one_place_every_write_site_is_named() {
    // the chokepoint: a command that writes a file must report its path here, or
    // it goes unchecked.
    let out = PathBuf::from("out.json");
    assert_eq!(
        output_path(&Command::Plan {
            file: PathBuf::from("i.yaml"),
            output: Some(out.clone()),
            backend: None,
            backend_config: None,
            provision: false,
            dry_run: false,
            report: false,
            allow_delete: false,
            no_adopt: false,
        }),
        Some(out.as_path())
    );
    assert_eq!(
        output_path(&Command::Apply {
            plan: PathBuf::from("p.json"),
            output: Some(out.clone()),
            backend: None,
            backend_config: None,
            allow_delete: false,
            interactive: false,
        }),
        Some(out.as_path())
    );
    assert_eq!(
        output_path(&Command::Import {
            output: out.clone(),
            file: PathBuf::from("i.yaml"),
            backend: None,
            backend_config: None,
            stateless: false,
        }),
        Some(out.as_path())
    );
    assert_eq!(
        output_path(&Command::Map {
            action: None,
            file: None,
            spec: None,
            output: Some(out.clone()),
        }),
        Some(out.as_path())
    );
    assert_eq!(
        output_path(&Command::Validate {
            file: PathBuf::from("i.yaml"),
            output: Some(out.clone()),
        }),
        Some(out.as_path())
    );
    // the commands that write no file
    assert_eq!(
        output_path(&Command::Validate {
            file: PathBuf::from("i.yaml"),
            output: None,
        }),
        None
    );
    assert_eq!(
        output_path(&Command::Map {
            action: Some(MapAction::Transform {
                spec: PathBuf::from("s.yaml"),
                name: "t".into(),
                value: "1".into(),
                args: vec![],
            }),
            file: None,
            spec: None,
            output: None,
        }),
        None
    );
}

#[test]
fn cli_command_definition_is_valid() {
    // clap's own assertions over the derived command: catches duplicate flags,
    // malformed help, broken conflicts, etc. at test time rather than at runtime.
    use clap::CommandFactory;
    Cli::command().debug_assert();
}

#[test]
fn located_report_prefers_source_locations() {
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
    let located = report.located(&[object]);

    assert_eq!(located.errors.len(), 1);
    let rendered = located.errors[0].to_string();
    assert!(rendered.contains("inventory.yaml:42"));
    assert!(rendered.contains("duplicate uid"));
    assert_eq!(
        located.errors[0].source.as_ref().unwrap().line,
        Some(42),
        "the line must survive into the document, not only into the message"
    );
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
        command: Command::Validate {
            file: inventory,
            output: None,
        },
    };
    run(cli, AppConfig::load().unwrap()).await.unwrap();
}

/// an inventory declaring `dcim.site`, with `attrs` extended by `extra`.
fn validate_fixture(dir: &Path, extra: &str) -> PathBuf {
    let inventory = dir.join("inventory.yaml");
    std::fs::write(
        &inventory,
        format!(
            r#"schema:
  types:
    dcim.site:
      key:
        site:
          type: slug
      fields:
        name:
          type: string
objects:
  - uid: "00000000-0000-0000-0000-000000000001"
    type: dcim.site
    key:
      site: "fra1"
    attrs:
      name: "FRA1"
{extra}"#
        ),
    )
    .unwrap();
    inventory
}

#[tokio::test]
async fn run_validate_writes_an_empty_report_when_the_inventory_validates() {
    // the success path writes too: a ci gate wants an artifact either way, and an
    // absent file would be indistinguishable from a crash.
    let dir = tempdir().unwrap();
    let out = dir.path().join("nested/validation.json");
    let cli = Cli {
        command: Command::Validate {
            file: validate_fixture(dir.path(), ""),
            output: Some(out.clone()),
        },
    };
    run(cli, AppConfig::load().unwrap()).await.unwrap();

    let raw = std::fs::read_to_string(&out).unwrap();
    let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(
        value["errors"],
        serde_json::json!([]),
        "a passing run writes an empty list, not a missing key: {raw}"
    );
}

#[tokio::test]
async fn run_validate_writes_the_located_errors_and_still_fails() {
    let dir = tempdir().unwrap();
    let out = dir.path().join("validation.json");
    let cli = Cli {
        command: Command::Validate {
            file: validate_fixture(dir.path(), "      bogus: \"nope\"\n"),
            output: Some(out.clone()),
        },
    };
    let error = run(cli, AppConfig::load().unwrap()).await.unwrap_err();
    assert!(
        error.to_string().contains("validation failed"),
        "-o must not turn a failing validation into a success: {error}"
    );

    let report: alembic_core::LocatedReport =
        serde_json::from_str(&std::fs::read_to_string(&out).unwrap()).unwrap();
    assert_eq!(report.errors.len(), 1);
    assert_eq!(
        report.errors[0].error,
        alembic_core::ValidationError::ExtraAttrField {
            type_name: "dcim.site".to_string(),
            field: "bogus".to_string(),
        }
    );
    // the loader canonicalizes, so match the file name rather than the temp path
    let source = report.errors[0].source.as_ref().expect("source location");
    assert!(source.file.ends_with("inventory.yaml"), "{source:?}");
    assert_eq!(source.line, Some(11));
}

#[tokio::test]
async fn run_map_ir() {
    let _cwd = CwdGuard::acquire_async().await;
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
    let _cwd = CwdGuard::acquire_async().await;
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
            no_adopt: false,
        },
    };
    let err = run(cli, AppConfig::load().unwrap()).await.unwrap_err();
    assert!(err.to_string().contains("missing NETBOX_URL"));
}

#[tokio::test]
async fn run_apply_missing_credentials_errors() {
    let _cwd = CwdGuard::acquire_async().await;
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    let plan_path = dir.path().join("plan.json");
    std::fs::write(&plan_path, r#"{ "ops": [] }"#).unwrap();
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
}

#[tokio::test]
async fn run_apply_interactive_delete_requires_allow_delete() {
    let _cwd = CwdGuard::acquire_async().await;
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
    std::env::set_current_dir(dir.path()).unwrap();

    // django is write-only, so it passes the capability gate and reaches the
    // delete gate; a read-only backend fails earlier
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
}

#[tokio::test]
async fn run_apply_read_only_backend_fails_before_prompting() {
    // the plan path is missing on purpose: the read-only error must fire before
    // read_plan (hence before the post-read_plan prompt loop) on both paths.
    // absence-of-prompt is checked out-of-process in tests/apply_capability.rs.
    let _cwd = CwdGuard::acquire_async().await;
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    let missing_plan = dir.path().join("does-not-exist.json");
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

    let _cwd = CwdGuard::acquire_async().await;
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
    result.unwrap();

    let raw = std::fs::read_to_string(&report_path).unwrap();
    let report: ApplyReport = serde_json::from_str(&raw).unwrap();
    assert_eq!(report.applied.len(), 1);
    assert_eq!(report.applied[0].uid, uuid::Uuid::from_u128(1));
    assert_eq!(report.applied[0].type_name.as_str(), "dcim.site");
    assert_eq!(
        report.applied[0].backend_id,
        Some(alembic_engine::BackendId::Int(7)),
        "the report must carry the backend id the create returned"
    );
    // absent, not empty, when the run resumed from nothing
    assert!(
        !raw.contains("resumed"),
        "a non-resumed run's report json must be unchanged: {raw}"
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
            // the two runs talk to two mock servers standing in for one backend, so
            // the config pins the instance: identity does not move with the port.
            "backend: generic\ninstance: resume-test\nconfig:\n  base_url: {base_url}\n  types:\n    dcim.site:\n      path: /sites/\n    dcim.device:\n      path: /devices/\n"
        ),
    )
    .unwrap();
    (plan_path, config_path)
}

#[tokio::test(flavor = "multi_thread")]
async fn run_apply_resumes_with_the_ids_the_interrupted_run_created() {
    use httpmock::Method::POST;

    let _cwd = CwdGuard::acquire_async().await;
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    let report_path = dir.path().join("report.json");

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

    let _cwd = CwdGuard::acquire_async().await;
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

    let _cwd = CwdGuard::acquire_async().await;
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

    let _cwd = CwdGuard::acquire_async().await;
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
            no_adopt: false,
        },
    };
    run(cli, AppConfig::load().unwrap()).await.unwrap();

    let raw = std::fs::read_to_string(&out).unwrap();
    assert!(raw.contains("\"op\": \"create\""));
    assert!(raw.contains("\"type_name\": \"dcim.device\""));
}

#[tokio::test(flavor = "multi_thread")]
async fn run_plan_report_is_read_only() {
    use serde_json::json;

    let _cwd = CwdGuard::acquire_async().await;
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
            no_adopt: false,
        },
    };
    run(cli, AppConfig::load().unwrap()).await.unwrap();

    // --report is read-only: -o carries the drift report, never a plan, and no
    // state is persisted.
    let raw = std::fs::read_to_string(&out).unwrap();
    assert!(
        !raw.contains("\"ops\""),
        "--report must not write a plan file: {raw}"
    );
    assert!(
        !dir.path().join(".alembic/state.json").exists(),
        "state must not be saved for --report"
    );
}

#[test]
fn plan_takes_a_shared_lock_only_when_it_saves_nothing() {
    assert_eq!(state_lock_for_plan(true, false, false), StateLock::Shared);
    assert_eq!(state_lock_for_plan(false, true, false), StateLock::Shared);
    // --provision writes backend schema, so it is not a read-only run
    assert_eq!(state_lock_for_plan(true, false, true), StateLock::Exclusive);
    // the write path saves state
    assert_eq!(
        state_lock_for_plan(false, false, false),
        StateLock::Exclusive
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn two_concurrent_report_runs_both_succeed() {
    use serde_json::json;

    let _cwd = CwdGuard::acquire_async().await;
    let server = nautobot_plan_server(json!([]));
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    let inventory = dir.path().join("inventory.yaml");
    let config = dir.path().join("adapter.yaml");
    write_device_inventory(&inventory);
    std::fs::write(
        &config,
        format!(
            "backend: nautobot\nurl: {}\ntoken: token\n",
            server.base_url()
        ),
    )
    .unwrap();

    std::env::set_current_dir(dir.path()).unwrap();

    // a reader of the test's own, so the two runs overlap by construction rather
    // than by scheduling luck.
    let reader = StateStore::load_with(&state_path, StateLock::Shared).unwrap();
    let (first, second) = tokio::join!(
        run(
            report_cli(inventory.clone(), config.clone()),
            AppConfig::load().unwrap()
        ),
        run(report_cli(inventory, config), AppConfig::load().unwrap()),
    );
    drop(reader);

    first.expect("first drift report");
    second.expect("a second drift report must not be refused by the first");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_saving_plan_run_is_refused_while_a_report_runs() {
    use serde_json::json;

    let _cwd = CwdGuard::acquire_async().await;
    let server = nautobot_plan_server(json!([]));
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    let inventory = dir.path().join("inventory.yaml");
    let config = dir.path().join("adapter.yaml");
    write_device_inventory(&inventory);
    std::fs::write(
        &config,
        format!(
            "backend: nautobot\nurl: {}\ntoken: token\n",
            server.base_url()
        ),
    )
    .unwrap();

    std::env::set_current_dir(dir.path()).unwrap();

    // the reader stands in for a running drift report.
    let reader = StateStore::load_with(&state_path, StateLock::Shared).unwrap();
    let saving = Cli {
        command: Command::Plan {
            file: inventory.clone(),
            output: Some(dir.path().join("plan.json")),
            backend: None,
            backend_config: Some(config.clone()),
            provision: false,
            dry_run: false,
            report: false,
            allow_delete: false,
            no_adopt: false,
        },
    };
    let refused = run(saving, AppConfig::load().unwrap()).await;
    // --provision writes schema, so it stays exclusive even under --report
    let mut provisioning = report_cli(inventory, config);
    if let Command::Plan { provision, .. } = &mut provisioning.command {
        *provision = true;
    }
    let refused_provision = run(provisioning, AppConfig::load().unwrap()).await;
    drop(reader);

    for err in [
        refused.expect_err("a saving run must not start under a reader"),
        refused_provision.expect_err("--provision must not start under a reader"),
    ] {
        assert!(
            err.to_string().contains("state lock"),
            "expected a state-lock error, got: {err}"
        );
    }
}

fn write_device_inventory(path: &Path) {
    std::fs::write(
        path,
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
}

fn report_cli(file: PathBuf, config: PathBuf) -> Cli {
    Cli {
        command: Command::Plan {
            file,
            output: None,
            backend: None,
            backend_config: Some(config),
            provision: false,
            dry_run: false,
            report: true,
            allow_delete: false,
            no_adopt: false,
        },
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn run_plan_report_writes_the_drift_report_to_output() {
    use serde_json::json;

    let _cwd = CwdGuard::acquire_async().await;
    // the backend holds leaf01, intent declares leaf02: one extra, one missing.
    let server = nautobot_plan_server(json!([{ "id": "uuid-leaf01", "name": "leaf01" }]));
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    let inventory = dir.path().join("inventory.yaml");
    let out = dir.path().join("nested/drift.json");
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
  - uid: "00000000-0000-0000-0000-000000000002"
    type: dcim.device
    key:
      name: "leaf02"
    attrs:
      name: "leaf02"
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
            no_adopt: false,
        },
    };
    let result = run(cli, AppConfig::load().unwrap()).await;
    result.unwrap();

    let drift: DriftReport = serde_json::from_str(&std::fs::read_to_string(&out).unwrap()).unwrap();
    assert_eq!(drift.missing.len(), 1);
    assert_eq!(drift.missing[0].key, key_str("name=leaf02"));
    // report mode forces delete-detection, so the extra rides in the file too,
    // without --allow-delete
    assert_eq!(drift.extra.len(), 1);
    assert_eq!(drift.extra[0].key, key_str("name=leaf01"));
    assert!(drift.changed.is_empty());
    assert!(
        !state_path.exists(),
        "writing the report must not turn --report into a state write"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn run_plan_report_carries_the_schema_preview_into_the_drift_report() {
    use serde_json::json;

    let _cwd = CwdGuard::acquire_async().await;
    let server = nautobot_plan_server(json!([{ "id": "uuid-leaf01", "name": "leaf01" }]));
    // the native-field probe preview issues before deciding a declared field is
    // custom; without it the preview errors and never reaches the report.
    server.mock(|when, then| {
        when.method(httpmock::Method::GET)
            .path("/api/dcim/devices/")
            .query_param("limit", "1");
        then.status(200).json_body(json!({
            "count": 0,
            "next": null,
            "previous": null,
            "results": []
        }));
    });
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    let inventory = dir.path().join("inventory.yaml");
    let out = dir.path().join("drift.json");
    let config = dir.path().join("adapter.yaml");
    // asset_tag is not native to dcim.device and the backend has no custom
    // fields, so the preview reports one field apply would create.
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
        asset_tag:
          type: string
objects:
  - uid: "00000000-0000-0000-0000-000000000002"
    type: dcim.device
    key:
      name: "leaf02"
    attrs:
      name: "leaf02"
      asset_tag: "AT-2"
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
            no_adopt: false,
        },
    };
    let result = run(cli, AppConfig::load().unwrap()).await;
    result.unwrap();

    let raw = std::fs::read_to_string(&out).unwrap();
    let drift: DriftReport = serde_json::from_str(&raw).unwrap();
    let preview = drift.schema_preview.unwrap_or_else(|| {
        panic!("the preview the run already computed must ride in the file: {raw}")
    });
    assert_eq!(preview.created_fields, vec!["dcim.device.asset_tag"]);
    // the drift categories are unaffected by the preview riding along.
    assert_eq!(drift.missing.len(), 1);
    assert_eq!(drift.extra.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn run_plan_report_carries_an_empty_schema_preview_for_a_backend_that_provisions_nothing() {
    // an adapter overriding neither provisioning method reports nothing to
    // provision, the same document the generic backend writes.
    let _cwd = CwdGuard::acquire_async().await;
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;

    let adapter = example_binary("minimal_external_adapter");
    let inventory = write_minimal_inventory(dir.path());
    let out = dir.path().join("drift.json");
    let config = dir.path().join("backend.yaml");
    std::fs::write(
        &config,
        format!(
            "backend: external\ncommand: \"{}\"\ntimeout_seconds: 5\n",
            adapter.to_str().unwrap()
        ),
    )
    .unwrap();

    std::env::set_current_dir(dir.path()).unwrap();
    let cli = Cli {
        command: Command::Plan {
            file: inventory,
            output: Some(out.clone()),
            backend: Some("external".to_string()),
            backend_config: Some(config),
            provision: false,
            dry_run: false,
            report: true,
            allow_delete: false,
            no_adopt: false,
        },
    };
    let result = run(cli, AppConfig::load().unwrap()).await;
    result.unwrap();

    let raw = std::fs::read_to_string(&out).unwrap();
    assert_eq!(
        raw,
        "{\n  \"changed\": [],\n  \"missing\": [],\n  \"extra\": [],\n  \"schema_preview\": {}\n}",
        "nothing to provision is a report, not an omission"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn run_plan_report_rejects_a_bad_output_path_before_observing() {
    let _cwd = CwdGuard::acquire_async().await;
    let server = httpmock::MockServer::start();
    let mock = server.mock(|when, then| {
        when.any_request();
        then.status(500);
    });

    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    // a declared type, so observing would issue requests if it were reached
    let inventory = write_site_inventory(dir.path());
    let config = dir.path().join("adapter.yaml");
    std::fs::write(
        &config,
        format!(
            "backend: nautobot\nurl: {}\ntoken: token\n",
            server.base_url()
        ),
    )
    .unwrap();
    // the report path's parent is a file, so it can never be created
    let blocker = dir.path().join("blocker");
    std::fs::write(&blocker, "").unwrap();

    std::env::set_current_dir(dir.path()).unwrap();
    let cli = Cli {
        command: Command::Plan {
            file: inventory,
            output: Some(blocker.join("drift.json")),
            backend: None,
            backend_config: Some(config),
            provision: false,
            dry_run: false,
            report: true,
            allow_delete: false,
            no_adopt: false,
        },
    };
    let result = run(cli, AppConfig::load().unwrap()).await;

    let err = result.expect_err("a bad -o must fail the run");
    assert!(
        format!("{err:#}").contains("create output directory"),
        "the run must fail on the output path: {err:#}"
    );
    assert_eq!(
        mock.calls(),
        0,
        "the output path must be rejected before the backend is observed"
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn run_plan_rejects_an_unwritable_existing_output_file_before_observing() {
    use std::os::unix::fs::PermissionsExt;
    let _cwd = CwdGuard::acquire_async().await;
    let server = httpmock::MockServer::start();
    let mock = server.mock(|when, then| {
        when.any_request();
        then.status(500);
    });

    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    let inventory = write_site_inventory(dir.path());
    let config = dir.path().join("adapter.yaml");
    std::fs::write(
        &config,
        format!(
            "backend: nautobot\nurl: {}\ntoken: token\n",
            server.base_url()
        ),
    )
    .unwrap();
    // an existing target that denies writes: the parent accepts them, so only a
    // probe of the target itself catches it before the observation is paid for
    let target = dir.path().join("plan.json");
    std::fs::write(&target, "previous").unwrap();
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o444)).unwrap();
    // root ignores the mode bits, so ask the os rather than skipping
    let sentinel = dir.path().join("sentinel");
    std::fs::write(&sentinel, b"x").unwrap();
    std::fs::set_permissions(&sentinel, std::fs::Permissions::from_mode(0o444)).unwrap();
    let denied = std::fs::write(&sentinel, b"y").is_err();

    std::env::set_current_dir(dir.path()).unwrap();
    let cli = Cli {
        command: Command::Plan {
            file: inventory,
            output: Some(target.clone()),
            backend: None,
            backend_config: Some(config),
            provision: false,
            dry_run: false,
            report: false,
            allow_delete: false,
            no_adopt: false,
        },
    };
    let result = run(cli, AppConfig::load().unwrap()).await;
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644)).unwrap();

    if denied {
        let err = result.expect_err("a bad -o must fail the run");
        assert!(
            format!("{err:#}").contains("write output"),
            "the run must fail on the output path: {err:#}"
        );
        assert_eq!(
            mock.calls(),
            0,
            "the output path must be rejected before the backend is observed"
        );
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "previous");
    } else {
        // running as root: the path really is writable, so the run gets as far
        // as the backend and dies there instead
        let err = result.expect_err("the mock backend answers 500");
        assert!(
            !format!("{err:#}").contains("write output"),
            "a writable target must not be rejected: {err:#}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn run_plan_rejects_an_output_path_that_is_a_directory_before_observing() {
    let _cwd = CwdGuard::acquire_async().await;
    let server = httpmock::MockServer::start();
    let mock = server.mock(|when, then| {
        when.any_request();
        then.status(500);
    });

    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    let inventory = write_site_inventory(dir.path());
    let config = dir.path().join("adapter.yaml");
    std::fs::write(
        &config,
        format!(
            "backend: nautobot\nurl: {}\ntoken: token\n",
            server.base_url()
        ),
    )
    .unwrap();
    // an existing directory: the parent is creatable, so only a real write probe
    // catches it, and without one the run pays for a full observation first
    let target = dir.path().join("outdir");
    std::fs::create_dir(&target).unwrap();

    std::env::set_current_dir(dir.path()).unwrap();
    let cli = Cli {
        command: Command::Plan {
            file: inventory,
            output: Some(target),
            backend: None,
            backend_config: Some(config),
            provision: false,
            dry_run: false,
            report: false,
            allow_delete: false,
            no_adopt: false,
        },
    };
    let result = run(cli, AppConfig::load().unwrap()).await;

    let err = result.expect_err("a bad -o must fail the run");
    assert!(
        format!("{err:#}").contains("is a directory"),
        "the run must fail on the output path: {err:#}"
    );
    assert_eq!(
        mock.calls(),
        0,
        "the output path must be rejected before the backend is observed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn run_plan_leaves_no_output_directory_when_observing_fails() {
    let _cwd = CwdGuard::acquire_async().await;
    let server = httpmock::MockServer::start();
    let _mock = server.mock(|when, then| {
        when.any_request();
        then.status(500);
    });

    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    let inventory = write_site_inventory(dir.path());
    let config = dir.path().join("adapter.yaml");
    std::fs::write(
        &config,
        format!(
            "backend: nautobot\nurl: {}\ntoken: token\n",
            server.base_url()
        ),
    )
    .unwrap();
    let out_dir = dir.path().join("brand_new_dir");

    std::env::set_current_dir(dir.path()).unwrap();
    let cli = Cli {
        command: Command::Plan {
            file: inventory,
            output: Some(out_dir.join("plan.json")),
            backend: None,
            backend_config: Some(config),
            provision: false,
            dry_run: false,
            report: false,
            allow_delete: false,
            no_adopt: false,
        },
    };
    let result = run(cli, AppConfig::load().unwrap()).await;

    result.expect_err("observing a 500 backend must fail the run");
    assert!(
        !out_dir.exists(),
        "a plan that never got to write must leave no output directory behind"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn run_plan_report_refuses_a_write_only_backend() {
    let _cwd = CwdGuard::acquire_async().await;
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    // one declared object: the run this refuses used to report it `missing`,
    // having observed nothing.
    let inventory = write_site_inventory(dir.path());
    // a parent that does not exist yet, so the output preflight has real work to
    // do first: the refusal below is the guard's, not the preflight's.
    let out = dir.path().join("nested/drift.json");

    std::env::set_current_dir(dir.path()).unwrap();
    let cli = Cli {
        command: Command::Plan {
            file: inventory,
            output: Some(out.clone()),
            backend: Some("django".to_string()),
            backend_config: None,
            provision: false,
            dry_run: false,
            report: true,
            allow_delete: false,
            no_adopt: false,
        },
    };
    let result = run(cli, AppConfig::load().unwrap()).await;

    let err = result.expect_err("a write-only backend has no drift to report");
    let err = format!("{err:#}");
    assert!(err.contains("write-only"), "{err}");
    assert!(err.contains("cannot observe state"), "{err}");
    assert!(err.contains("without --report"), "{err}");
    // and it leaves nothing behind: no document, and the parent the preflight
    // created to probe with is gone again.
    assert!(!out.exists(), "a refused report must write no document");
    assert!(!out.parent().unwrap().exists());
    assert!(!state_path.exists());
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn run_plan_report_reports_an_unwritable_output_ahead_of_the_refusal() {
    use std::os::unix::fs::PermissionsExt;
    // both are wrong at once. the output preflight (#325) runs for every command
    // before a backend is built, so it answers first, and the refusal below never
    // gets to speak. docs/cli.md says so; this is what keeps that true.
    let _cwd = CwdGuard::acquire_async().await;
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    let inventory = write_site_inventory(dir.path());
    // an existing target that denies writes, under a parent that allows them
    let out = dir.path().join("drift.json");
    std::fs::write(&out, "previous").unwrap();
    std::fs::set_permissions(&out, std::fs::Permissions::from_mode(0o444)).unwrap();
    // root ignores the mode bits, so ask the os rather than skipping
    let sentinel = dir.path().join("sentinel");
    std::fs::write(&sentinel, b"x").unwrap();
    std::fs::set_permissions(&sentinel, std::fs::Permissions::from_mode(0o444)).unwrap();
    if std::fs::write(&sentinel, b"y").is_ok() {
        return;
    }

    std::env::set_current_dir(dir.path()).unwrap();
    let cli = Cli {
        command: Command::Plan {
            file: inventory,
            output: Some(out.clone()),
            backend: Some("django".to_string()),
            backend_config: None,
            provision: false,
            dry_run: false,
            report: true,
            allow_delete: false,
            no_adopt: false,
        },
    };
    let result = run(cli, AppConfig::load().unwrap()).await;

    let err = format!("{:#}", result.expect_err("an unwritable -o must fail"));
    assert!(err.contains("write output:"), "{err}");
    assert!(
        !err.contains("write-only"),
        "the path check answers first, not the refusal: {err}"
    );
    assert_eq!(std::fs::read_to_string(&out).unwrap(), "previous");
}

#[tokio::test(flavor = "multi_thread")]
async fn run_plan_report_refuses_an_external_adapter_declaring_emitter() {
    let _cwd = CwdGuard::acquire_async().await;
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    // the example stubs a successful empty `read`, so only the declared role
    // stands between the report and an observation of nothing.
    let adapter = example_binary("emitter_role_adapter");
    let inventory = write_site_inventory(dir.path());
    let config = dir.path().join("backend.yaml");
    std::fs::write(
        &config,
        format!(
            "backend: external\ncommand: \"{}\"\ntimeout_seconds: 5\n",
            adapter.to_str().unwrap()
        ),
    )
    .unwrap();

    std::env::set_current_dir(dir.path()).unwrap();
    let cli = Cli {
        command: Command::Plan {
            file: inventory,
            output: None,
            backend: Some("external".to_string()),
            backend_config: Some(config),
            provision: false,
            dry_run: false,
            report: true,
            allow_delete: false,
            no_adopt: false,
        },
    };
    let result = run(cli, AppConfig::load().unwrap()).await;

    let err = result.expect_err("a declared emitter has no drift to report");
    assert!(format!("{err:#}").contains("write-only"), "{err:#}");
}

// an external backend pointing at the observer example, the inventory to run it
// against, and the path the example appends each received method to.
fn observer_role_fixture(dir: &Path) -> (PathBuf, PathBuf, PathBuf) {
    let adapter = example_binary("observer_role_adapter");
    let log = dir.join("methods.log");
    let config = dir.join("backend.yaml");
    std::fs::write(
        &config,
        format!(
            "backend: external\ncommand: \"{}\"\ntimeout_seconds: 5\nenv:\n  OBSERVER_ROLE_ADAPTER_LOG: \"{}\"\n",
            adapter.display(),
            log.display()
        ),
    )
    .unwrap();
    (write_site_inventory(dir), config, log)
}

// every method the example recorded receiving, having asserted that the log
// happened at all and that none of the methods was a provisioning one.
fn methods_without_provisioning(log: &Path) -> String {
    let methods = std::fs::read_to_string(log).expect("the example wrote its method log");
    assert!(
        methods.contains("capabilities"),
        "the log records what the host sent, got: {methods}"
    );
    assert!(
        !methods.contains("ensure_schema") && !methods.contains("preview_schema"),
        "the host must never ask a declared observer for schema, got: {methods}"
    );
    methods
}

#[tokio::test(flavor = "multi_thread")]
async fn run_refuses_to_write_an_external_adapter_declaring_observer() {
    let _cwd = CwdGuard::acquire_async().await;
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    // the example stubs a successful `write`, so only the declared role stands
    // between the plan and a write.
    let (inventory, config, log) = observer_role_fixture(dir.path());
    // the plan path is missing on purpose: the refusal fires before read_plan,
    // as run_apply_read_only_backend_fails_before_prompting pins for a built-in
    let plan_path = dir.path().join("does-not-exist.json");

    std::env::set_current_dir(dir.path()).unwrap();
    let provision = run(
        Cli {
            command: Command::Plan {
                file: inventory,
                output: Some(dir.path().join("provisioned.json")),
                backend: Some("external".to_string()),
                backend_config: Some(config.clone()),
                provision: true,
                dry_run: false,
                report: false,
                allow_delete: false,
                no_adopt: false,
            },
        },
        AppConfig::load().unwrap(),
    )
    .await;
    let applied = run(
        Cli {
            command: Command::Apply {
                plan: plan_path,
                output: None,
                backend: Some("external".to_string()),
                backend_config: Some(config),
                allow_delete: false,
                interactive: false,
            },
        },
        AppConfig::load().unwrap(),
    )
    .await;

    for (command, result) in [("plan --provision", provision), ("apply", applied)] {
        let err = format!(
            "{:#}",
            result.expect_err("a declared observer cannot be written to")
        );
        assert!(
            err.contains("backend is read-only; it cannot apply changes"),
            "{command}: {err}"
        );
    }
    // the refusal comes before the request, not from an error the adapter raised
    let methods = methods_without_provisioning(&log);
    assert!(!methods.contains("write"), "nothing was written: {methods}");
}

#[tokio::test(flavor = "multi_thread")]
async fn run_plan_and_import_an_external_adapter_declaring_observer() {
    let _cwd = CwdGuard::acquire_async().await;
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    let (inventory, config, log) = observer_role_fixture(dir.path());

    std::env::set_current_dir(dir.path()).unwrap();
    let planned = run(
        Cli {
            command: Command::Plan {
                file: inventory.clone(),
                output: Some(dir.path().join("plan.json")),
                backend: Some("external".to_string()),
                backend_config: Some(config.clone()),
                provision: false,
                dry_run: false,
                report: false,
                allow_delete: false,
                no_adopt: false,
            },
        },
        AppConfig::load().unwrap(),
    )
    .await;
    let imported = run(
        Cli {
            command: Command::Import {
                output: dir.path().join("observed.json"),
                file: inventory,
                backend: Some("external".to_string()),
                backend_config: Some(config),
                stateless: false,
            },
        },
        AppConfig::load().unwrap(),
    )
    .await;

    planned.expect("a declared observer plans");
    imported.expect("a declared observer imports");
    let methods = methods_without_provisioning(&log);
    assert!(methods.contains("read"), "both commands read: {methods}");
}

fn write_ref_chain_inventory(dir: &Path) -> PathBuf {
    let inventory = dir.join("chain.yaml");
    std::fs::write(
        &inventory,
        r#"
schema:
  types:
    dcim.site:
      key:
        slug:
          type: slug
      fields:
        slug:
          type: slug
        name:
          type: string
    dcim.device:
      key:
        site:
          type: ref
          target: dcim.site
        name:
          type: string
      fields:
        site:
          type: ref
          target: dcim.site
        name:
          type: string
    dcim.interface:
      key:
        device:
          type: ref
          target: dcim.device
        name:
          type: string
      fields:
        device:
          type: ref
          target: dcim.device
        name:
          type: string
# canonical uids, as `alembic import` writes them: a ref-typed key field names
# the uid its target derives.
objects:
  - uid: "8c998348-947f-568d-bbb0-efbed3c3f903"
    type: dcim.site
    key:
      slug: "fra1"
    attrs:
      slug: "fra1"
      name: "FRA1"
  - uid: "46a4f856-9778-577f-bb3a-d9c63a59fe56"
    type: dcim.device
    key:
      site: "8c998348-947f-568d-bbb0-efbed3c3f903"
      name: "leaf01"
    attrs:
      site: "8c998348-947f-568d-bbb0-efbed3c3f903"
      name: "leaf01"
  - uid: "e677c075-7bc8-54b5-ac34-ee71611bc7a1"
    type: dcim.interface
    key:
      device: "46a4f856-9778-577f-bb3a-d9c63a59fe56"
      name: "eth0"
    attrs:
      device: "46a4f856-9778-577f-bb3a-d9c63a59fe56"
      name: "eth0"
"#,
    )
    .unwrap();
    inventory
}

#[tokio::test(flavor = "multi_thread")]
async fn run_plan_adopts_a_backend_whose_keys_hold_refs() {
    let _cwd = CwdGuard::acquire_async().await;
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;

    let adapter = example_binary("ref_chain_adapter");
    let log = dir.path().join("reads.log");
    let config = dir.path().join("backend.yaml");
    std::fs::write(
        &config,
        format!(
            "backend: external\ncommand: \"{}\"\ntimeout_seconds: 5\nenv:\n  REF_CHAIN_ADAPTER_LOG: \"{}\"\n",
            adapter.display(),
            log.display()
        ),
    )
    .unwrap();
    let inventory = write_ref_chain_inventory(dir.path());
    let out = dir.path().join("plan.json");

    std::env::set_current_dir(dir.path()).unwrap();
    let planned = run(
        Cli {
            command: Command::Plan {
                file: inventory,
                output: Some(out.clone()),
                backend: Some("external".to_string()),
                backend_config: Some(config),
                provision: false,
                dry_run: false,
                report: false,
                allow_delete: false,
                no_adopt: false,
            },
        },
        AppConfig::load().unwrap(),
    )
    .await;
    planned.expect("the chain plans");

    let plan: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&out).unwrap()).unwrap();
    assert_eq!(
        plan["ops"].as_array().map(Vec::len),
        Some(0),
        "the backend already holds the chain: {}",
        plan["ops"]
    );
    let reads = std::fs::read_to_string(&log).unwrap().lines().count();
    assert_eq!(reads, 1, "the adapter resolves the chain in its own read");
}

#[tokio::test(flavor = "multi_thread")]
async fn run_plan_refuses_an_adapter_that_reports_refs_as_backend_ids() {
    let _cwd = CwdGuard::acquire_async().await;
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;

    let config = dir.path().join("backend.yaml");
    std::fs::write(
        &config,
        format!(
            "backend: external\ncommand: \"{}\"\ntimeout_seconds: 5\n",
            example_binary("raw_ref_adapter").display()
        ),
    )
    .unwrap();
    let inventory = write_ref_chain_inventory(dir.path());
    let out = dir.path().join("plan.json");

    std::env::set_current_dir(dir.path()).unwrap();
    let planned = run(
        Cli {
            command: Command::Plan {
                file: inventory,
                output: Some(out.clone()),
                backend: Some("external".to_string()),
                backend_config: Some(config),
                provision: false,
                dry_run: false,
                report: false,
                allow_delete: false,
                no_adopt: false,
            },
        },
        AppConfig::load().unwrap(),
    )
    .await;

    let error = match planned {
        // the backend holds all three objects, so a plan here is a plan of
        // creates that duplicate them.
        Ok(()) => panic!(
            "the plan was taken: {}",
            std::fs::read_to_string(&out).unwrap_or_default()
        ),
        Err(err) => format!("{err:#}"),
    };
    assert!(
        error.contains("dcim.device.key.site -> dcim.site 1"),
        "{error}"
    );
    assert!(
        error.contains("dcim.interface.key.device -> dcim.device 2"),
        "{error}"
    );
    assert!(error.contains("docs/external-adapters.md"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn run_plan_without_report_still_plans_a_write_only_backend() {
    let _cwd = CwdGuard::acquire_async().await;
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;
    let inventory = write_site_inventory(dir.path());
    let out = dir.path().join("plan.json");

    std::env::set_current_dir(dir.path()).unwrap();
    let cli = Cli {
        command: Command::Plan {
            file: inventory,
            output: Some(out.clone()),
            backend: Some("django".to_string()),
            backend_config: None,
            provision: false,
            dry_run: false,
            report: false,
            allow_delete: false,
            no_adopt: false,
        },
    };
    let result = run(cli, AppConfig::load().unwrap()).await;

    // the all-creates plan is what apply will emit, so it stays: only the
    // report, which claims to have observed, is refused.
    result.expect("plan without --report is unaffected");
    let raw = std::fs::read_to_string(&out).unwrap();
    assert!(raw.contains("\"create\""), "{raw}");
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
fn output_and_dry_run_conflict() {
    use clap::Parser;
    // --dry-run prints the plan and writes no file, so a passed -o was accepted
    // and dropped in silence: exit 0, no file, not even the extension warning.
    let result = Cli::try_parse_from([
        "alembic",
        "plan",
        "-f",
        "inventory.yaml",
        "-o",
        "plan.json",
        "--dry-run",
    ]);
    // `Cli` is not `Debug`, so unwrap the error via `Option` rather than `expect_err`.
    let err = result.err().expect("-o and --dry-run must conflict");
    assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);

    // --dry-run alone still parses: -o is not required under it.
    assert!(
        Cli::try_parse_from(["alembic", "plan", "-f", "inventory.yaml", "--dry-run"]).is_ok(),
        "--dry-run without -o must still parse"
    );
    // and -o stays valid under --report, which does write the file.
    assert!(
        Cli::try_parse_from([
            "alembic",
            "plan",
            "-f",
            "inventory.yaml",
            "-o",
            "drift.json",
            "--report",
        ])
        .is_ok(),
        "--report -o must still parse"
    );
}

#[test]
fn map_transform_rejects_the_inventory_flow_args() {
    use clap::Parser;
    // the same accepted-and-dropped defect one command over: `map transform`
    // carries its own --spec and prints to stdout, so -f/--spec/-o at the map
    // level went nowhere. rejected by the parser, like -o with --dry-run, so
    // both spellings of "this arg has nowhere to go" exit the same way.
    for stray in [
        ["-f", "in.yaml"],
        ["--spec", "outer.yaml"],
        ["-o", "out.json"],
    ] {
        let result = Cli::try_parse_from([
            "alembic",
            "map",
            stray[0],
            stray[1],
            "transform",
            "--spec",
            "spec.yaml",
            "lower",
            "\"NXOS\"",
        ]);
        // `Cli` is not `Debug`, so unwrap the error via `Option` rather than `expect_err`.
        let err = result
            .err()
            .unwrap_or_else(|| panic!("map {} must conflict with transform", stray[0]));
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
        assert!(
            err.to_string().contains(stray[0].trim_start_matches('-')),
            "the error must name the stray arg: {err}"
        );
    }

    // transform without them still parses, and so does the inventory flow that
    // owns those args when no subcommand is given.
    assert!(
        Cli::try_parse_from([
            "alembic",
            "map",
            "transform",
            "--spec",
            "spec.yaml",
            "lower",
            "\"NXOS\"",
        ])
        .is_ok(),
        "transform with no stray args must parse"
    );
    assert!(
        Cli::try_parse_from([
            "alembic",
            "map",
            "-f",
            "in.yaml",
            "--spec",
            "spec.yaml",
            "-o",
            "out.json",
        ])
        .is_ok(),
        "the map inventory flow must still parse"
    );
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
    // --report prints its summary either way, so -o/--output is not required.
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
    // -o alongside --report is where the drift report's json goes, so the two
    // must never be rejected as a conflict.
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

    let _cwd = CwdGuard::acquire_async().await;
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

    std::env::set_current_dir(dir.path()).unwrap();

    let inventory = load_inventory(&inventory).unwrap();
    let (backend, identity) = create_backend(&[], None, Some(config)).unwrap();
    let mut state = load_state(StateLock::Exclusive, &identity).await.unwrap();

    // without report-forced delete-detection, allow_delete=false emits no
    // deletes and the `extra` category stays empty.
    let (buggy, _) = build_plan(
        backend.observer().unwrap(),
        &inventory,
        &mut state,
        should_detect_deletes(false, false),
        true,
    )
    .await
    .unwrap();
    assert!(
        DriftReport::from_plan(&buggy).extra.is_empty(),
        "without report-forced delete-detection the extra is invisible"
    );

    // report mode forces delete-detection, so the unmanaged backend object
    // surfaces as an `extra` even though --allow-delete was not passed.
    let (plan, _) = build_plan(
        backend.observer().unwrap(),
        &inventory,
        &mut state,
        should_detect_deletes(false, true),
        true,
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
}

#[tokio::test(flavor = "multi_thread")]
async fn minimal_external_adapter() {
    // this test depends on the example "minimal_external_adapter" in this crate.
    // note that `cargo test` will build all examples, so we can expect the binary to exist.

    let adapter = example_binary("minimal_external_adapter");

    let config = AdapterConfig::External(ExternalConfig {
        command: Some(adapter.to_str().unwrap().to_string()),
        args: Vec::new(),
        working_dir: None,
        env: BTreeMap::new(),
        timeout_seconds: Some(5),
        setup: serde_yaml::Value::default(),
        instance: None,
    });

    let (backend, _) = config.build().unwrap();

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
// must propagate a preview Err and abort before ensure_schema.
#[tokio::test(flavor = "multi_thread")]
async fn run_plan_provision_fails_closed_on_preview_error() {
    let _cwd = CwdGuard::acquire_async().await;
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;

    let adapter = example_binary("preview_error_adapter");
    let inventory = write_minimal_inventory(dir.path());
    let out_path = dir.path().join("plan.json");
    let config_path = dir.path().join("backend.yaml");
    std::fs::write(
        &config_path,
        format!(
            "backend: external\ncommand: \"{}\"\ntimeout_seconds: 5\n",
            adapter.to_str().unwrap()
        ),
    )
    .unwrap();

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
            no_adopt: false,
        },
    };
    let result = run(cli, AppConfig::load().unwrap()).await;

    let err = result.expect_err("a preview error must abort provisioning, not fall through");
    assert!(
        err.to_string().contains("preview failed for test"),
        "expected the propagated preview error, got: {err:#}"
    );
}

// drives the real run() for `plan --provision` against an external adapter that
// declares the emitter role and provisions schema. the declared role governs read
// vs write, so provisioning -- itself a write -- must reach the subprocess rather
// than be refused up front.
#[tokio::test(flavor = "multi_thread")]
async fn run_plan_provision_over_a_write_only_backend() {
    let _cwd = CwdGuard::acquire_async().await;
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;

    let adapter = example_binary("provisioning_emitter_adapter");
    let inventory = write_site_inventory(dir.path());
    let out_path = dir.path().join("plan.json");
    let config_path = dir.path().join("backend.yaml");
    std::fs::write(
        &config_path,
        format!(
            "backend: external\ncommand: \"{}\"\ntimeout_seconds: 5\n",
            adapter.to_str().unwrap()
        ),
    )
    .unwrap();

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
            no_adopt: false,
        },
    };
    let result = run(cli, AppConfig::load().unwrap()).await;

    result.expect("a declared emitter provisions rather than being refused");
}

// the same backend without --provision: the read-only preview is what a plain
// plan carries, so a write-only backend that can preview must fill it in.
#[tokio::test(flavor = "multi_thread")]
async fn run_plan_previews_schema_over_a_write_only_backend() {
    let _cwd = CwdGuard::acquire_async().await;
    let dir = tempdir().unwrap();
    let state_path = dir.path().join(".alembic").join("state.json");
    let _env = EnvVarGuard::acquire_async(&[
        ("ALEMBIC_STATE_BACKEND", Some("local")),
        ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
    ])
    .await;

    let adapter = example_binary("provisioning_emitter_adapter");
    let inventory = write_site_inventory(dir.path());
    let out_path = dir.path().join("plan.json");
    let config_path = dir.path().join("backend.yaml");
    std::fs::write(
        &config_path,
        format!(
            "backend: external\ncommand: \"{}\"\ntimeout_seconds: 5\n",
            adapter.to_str().unwrap()
        ),
    )
    .unwrap();

    std::env::set_current_dir(dir.path()).unwrap();
    let cli = Cli {
        command: Command::Plan {
            file: inventory,
            output: Some(out_path.clone()),
            backend: Some("external".to_string()),
            backend_config: Some(config_path),
            provision: false,
            dry_run: false,
            report: false,
            allow_delete: false,
            no_adopt: false,
        },
    };
    let result = run(cli, AppConfig::load().unwrap()).await;
    result.expect("plan over a write-only backend still plans");

    let plan: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&out_path).unwrap()).unwrap();
    assert!(
        !plan["schema_preview"]["created_object_types"]
            .as_array()
            .expect("the write-only backend's preview rides in the plan")
            .is_empty(),
        "{plan}"
    );
}

// a hand-written external adapter, the integration path docs/external-adapters.md
// sells: no sdk, so its result is exactly what it prints. it records the create in
// a store file, reads back what it recorded, and answers a converged write (no
// ops) with `converged`.
#[cfg(unix)]
fn hand_written_adapter_fixture(dir: &Path, converged: &str) -> (PathBuf, PathBuf) {
    use std::os::unix::fs::PermissionsExt;
    let script = dir.join("adapter.sh");
    std::fs::write(
        &script,
        r#"#!/usr/bin/env bash
req="$(cat)"
case "$req" in
  *'"method":"capabilities"'*)
    printf '{"ok":true,"result":{"role":"adapter"}}' ;;
  *'"method":"read"'*)
    if [ -f "$STORE" ]; then
      printf '{"ok":true,"result":[{"type_name":"dcim.site","key":{"site":"fra1"},"attrs":{"name":"FRA1","slug":"fra1"},"backend_id":"site-1"}]}'
    else
      printf '{"ok":true,"result":[]}'
    fi ;;
  *'"ops":[]'*)
    printf '%s' "$CONVERGED_WRITE" ;;
  *'"method":"write"'*)
    : >"$STORE"
    printf '{"ok":true,"result":{"applied":[{"uid":"00000000-0000-0000-0000-000000000001","type_name":"dcim.site","backend_id":"site-1"}]}}' ;;
  *)
    printf '{"ok":true,"result":{}}' ;;
esac
"#,
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

    let config = dir.join("backend.yaml");
    std::fs::write(
        &config,
        format!(
            "backend: external\ncommand: \"{}\"\ntimeout_seconds: 5\nenv:\n  STORE: \"{}\"\n  CONVERGED_WRITE: '{}'\n",
            script.display(),
            dir.join("store").display(),
            converged
        ),
    )
    .unwrap();
    (write_site_inventory(dir), config)
}

// apply calls write on every run, so a converged re-run hands the adapter an empty
// op list and it answers with an empty result. leaving `applied` out of that result
// must plan and apply exactly like spelling it out.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn run_applies_a_converged_hand_written_adapter_that_omits_applied() {
    let _cwd = CwdGuard::acquire_async().await;
    for (case, converged) in [
        ("omitted", r#"{"ok":true,"result":{}}"#),
        ("spelled out", r#"{"ok":true,"result":{"applied":[]}}"#),
    ] {
        let dir = tempdir().unwrap();
        let state_path = dir.path().join(".alembic").join("state.json");
        let _env = EnvVarGuard::acquire_async(&[
            ("ALEMBIC_STATE_BACKEND", Some("local")),
            ("ALEMBIC_STATE_PATH", Some(state_path.to_str().unwrap())),
        ])
        .await;
        let (inventory, config) = hand_written_adapter_fixture(dir.path(), converged);

        std::env::set_current_dir(dir.path()).unwrap();
        // two full cycles: the first creates the site, the second finds it there
        let mut rounds = Vec::new();
        for round in 0..2 {
            let plan_path = dir.path().join(format!("plan-{round}.json"));
            let planned = run(
                Cli {
                    command: Command::Plan {
                        file: inventory.clone(),
                        output: Some(plan_path.clone()),
                        backend: Some("external".to_string()),
                        backend_config: Some(config.clone()),
                        provision: false,
                        dry_run: false,
                        report: false,
                        allow_delete: false,
                        no_adopt: false,
                    },
                },
                AppConfig::load().unwrap(),
            )
            .await;
            let applied = run(
                Cli {
                    command: Command::Apply {
                        plan: plan_path.clone(),
                        output: None,
                        backend: Some("external".to_string()),
                        backend_config: Some(config.clone()),
                        allow_delete: false,
                        interactive: false,
                    },
                },
                AppConfig::load().unwrap(),
            )
            .await;
            rounds.push((planned, applied, plan_path));
        }

        let mut ops = Vec::new();
        for (round, (planned, applied, plan_path)) in rounds.into_iter().enumerate() {
            planned.unwrap_or_else(|err| panic!("{case}: plan {round} failed: {err:#}"));
            applied.unwrap_or_else(|err| panic!("{case}: apply {round} failed: {err:#}"));
            ops.push(read_plan(&plan_path).unwrap().ops.len());
        }
        assert_eq!(ops, [1, 0], "{case}: the second run is the converged one");
    }
}

/// a plugin directory that is genuinely absent is the default case (`./plugins`
/// usually is), so it stays an empty list rather than an error.
#[test]
fn search_for_plugins_treats_an_absent_dir_as_no_plugins() {
    let dir = tempdir().unwrap();
    let config = AppConfig {
        plugins_dir: dir.path().join("nope"),
    };
    assert!(search_for_plugins(&config).unwrap().is_empty());
}

/// a parent that is a regular file makes `read_dir` fail with `NotADirectory`
/// rather than answering absent. reporting no plugins on that answer runs the
/// command with every declared plugin missing, and says so only at `debug`.
#[test]
fn search_for_plugins_reports_a_dir_it_could_not_read() {
    let dir = tempdir().unwrap();
    let blocker = dir.path().join("blocker");
    fs::write(&blocker, "regular file").unwrap();
    let config = AppConfig {
        plugins_dir: blocker.join("plugins"),
    };

    let err = search_for_plugins(&config).expect_err("an unreadable dir is not an empty one");

    assert!(
        format!("{err:#}").contains("plugins"),
        "the error must name the path: {err:#}"
    );
}
