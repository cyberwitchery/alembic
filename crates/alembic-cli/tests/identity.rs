//! the identity law, end to end: adoption is visible, state answers to one
//! backend instance, import assigns identity state-first, and a retype renders
//! as one event. the backend is a scripted external adapter so every scenario
//! controls exactly what the observation holds.

mod support;

use std::fs;
use std::path::Path;
use std::process::Command;
use support::bin_path;
use tempfile::tempdir;

const SITE_UID: &str = "11111111-1111-1111-1111-111111111111";

/// an external adapter answering `read` with the objects baked into it and
/// erroring on writes; enough backend for plan and import.
fn write_observer(dir: &Path, objects_json: &str) -> std::path::PathBuf {
    let script = dir.join("observer.sh");
    fs::write(
        &script,
        format!(
            r#"#!/bin/bash
input=$(cat)
if [[ "$input" == *'"method":"capabilities"'* ]]; then
  echo '{{"ok":true,"result":{{"role":"adapter"}}}}'
elif [[ "$input" == *'"method":"read"'* ]]; then
  echo '{{"ok":true,"result":{objects_json}}}'
elif [[ "$input" == *'"method":"preview_schema"'* ]] || [[ "$input" == *'"method":"ensure_schema"'* ]]; then
  echo '{{"ok":true,"result":{{}}}}'
else
  echo '{{"ok":false,"error":"unsupported"}}'
fi
"#
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
    }
    script
}

fn write_backend_config(dir: &Path, script: &Path, instance: &str) -> std::path::PathBuf {
    let config = dir.join(format!("backend-{instance}.yaml"));
    fs::write(
        &config,
        format!(
            "backend: external\ncommand: {}\ninstance: {instance}\n",
            script.display()
        ),
    )
    .unwrap();
    config
}

fn write_site_inventory(dir: &Path, type_name: &str, slug: &str) -> std::path::PathBuf {
    let path = dir.join(format!("inventory-{type_name}-{slug}.yaml"));
    fs::write(
        &path,
        format!(
            r#"schema:
  types:
    dcim.site:
      key: {{slug: {{type: slug}}}}
      fields:
        name: {{type: string}}
        slug: {{type: slug}}
    location.site:
      key: {{slug: {{type: slug}}}}
      fields:
        name: {{type: string}}
        slug: {{type: slug}}
objects:
  - uid: "{SITE_UID}"
    type: {type_name}
    key: {{slug: {slug}}}
    attrs: {{name: Frankfurt, slug: {slug}}}
"#
        ),
    )
    .unwrap();
    path
}

const OBSERVED_FRA1: &str = r#"[{"type_name":"dcim.site","key":{"slug":"fra1"},"attrs":{"name":"Frankfurt","slug":"fra1"},"backend_id":7}]"#;

fn plan(dir: &Path, inventory: &Path, config: &Path, extra: &[&str]) -> std::process::Output {
    let mut command = Command::new(bin_path());
    command
        .current_dir(dir)
        .arg("plan")
        .arg("-f")
        .arg(inventory)
        .arg("-o")
        .arg(dir.join("plan.json"))
        .arg("--backend-config")
        .arg(config);
    for arg in extra {
        command.arg(arg);
    }
    command.output().unwrap()
}

fn stdout(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// a converged brownfield plan still says what it bound: adoption writes
/// identity memory, so it is never silent, and the scoped state file holds
/// exactly the binding the run reported.
#[test]
fn adoption_is_reported_and_persisted_to_scoped_state() {
    let dir = tempdir().unwrap();
    let script = write_observer(dir.path(), OBSERVED_FRA1);
    let config = write_backend_config(dir.path(), &script, "site-a");
    let inventory = write_site_inventory(dir.path(), "dcim.site", "fra1");

    let output = plan(dir.path(), &inventory, &config, &[]);
    assert!(output.status.success(), "{}", stderr(&output));
    let out = stdout(&output);
    assert!(
        out.contains("adopted 1 existing object(s) by key:"),
        "{out}"
    );
    assert!(
        out.contains("dcim.site {\"slug\":\"fra1\"} -> backend id 7"),
        "{out}"
    );
    assert!(
        out.contains("plan: 0 to create, 0 to update, 0 to delete"),
        "{out}"
    );

    // the state landed under the backend-scoped path, stamped and mapped.
    let state_dir = dir.path().join(".alembic").join("state");
    let entries: Vec<_> = fs::read_dir(&state_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect();
    assert_eq!(entries.len(), 1, "one backend, one state file");
    let raw = fs::read_to_string(&entries[0]).unwrap();
    let state: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(state["backend"]["adapter"], "external");
    assert_eq!(state["backend"]["instance"], "site-a");
    assert_eq!(state["mappings"]["dcim.site"][SITE_UID], 7);
}

/// --no-adopt refuses to identify declared objects with backend ones: nothing
/// is reported because nothing is bound, and the unbound object plans as a
/// create.
#[test]
fn no_adopt_binds_nothing_and_plans_a_create() {
    let dir = tempdir().unwrap();
    let script = write_observer(dir.path(), OBSERVED_FRA1);
    let config = write_backend_config(dir.path(), &script, "site-a");
    let inventory = write_site_inventory(dir.path(), "dcim.site", "fra1");

    let output = plan(dir.path(), &inventory, &config, &["--no-adopt"]);
    assert!(output.status.success(), "{}", stderr(&output));
    let out = stdout(&output);
    assert!(!out.contains("adopted"), "{out}");
    assert!(
        out.contains("plan: 1 to create, 0 to update, 0 to delete"),
        "{out}"
    );

    let state_dir = dir.path().join(".alembic").join("state");
    let entries: Vec<_> = fs::read_dir(&state_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect();
    let raw = fs::read_to_string(&entries[0]).unwrap();
    let state: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert!(
        state["mappings"]
            .as_object()
            .map(|m| m.values().all(|t| t.as_object().unwrap().is_empty()))
            .unwrap_or(true),
        "no identity binding may persist under --no-adopt: {state}"
    );
}

/// state is identity memory for exactly one backend instance: pointed at
/// another backend, the run is refused naming both identities.
#[test]
fn a_state_file_refuses_another_backend_instance() {
    let dir = tempdir().unwrap();
    let script = write_observer(dir.path(), OBSERVED_FRA1);
    let inventory = write_site_inventory(dir.path(), "dcim.site", "fra1");
    let state_path = dir.path().join("state.json");

    let config_a = write_backend_config(dir.path(), &script, "site-a");
    let mut command = Command::new(bin_path());
    command
        .current_dir(dir.path())
        .env("ALEMBIC_STATE_PATH", &state_path)
        .arg("plan")
        .arg("-f")
        .arg(&inventory)
        .arg("-o")
        .arg(dir.path().join("plan.json"))
        .arg("--backend-config")
        .arg(&config_a);
    let output = command.output().unwrap();
    assert!(output.status.success(), "{}", stderr(&output));

    let config_b = write_backend_config(dir.path(), &script, "site-b");
    let mut command = Command::new(bin_path());
    command
        .current_dir(dir.path())
        .env("ALEMBIC_STATE_PATH", &state_path)
        .arg("plan")
        .arg("-f")
        .arg(&inventory)
        .arg("-o")
        .arg(dir.path().join("plan.json"))
        .arg("--backend-config")
        .arg(&config_b);
    let output = command.output().unwrap();
    assert!(!output.status.success());
    let err = stderr(&output);
    assert!(err.contains("external (site-a)"), "{err}");
    assert!(err.contains("external (site-b)"), "{err}");
}

/// an unstamped state file carrying mappings predates backend-scoped state; it
/// is refused, never claimed for whichever backend happens to load it first.
#[test]
fn an_unstamped_state_file_with_mappings_is_refused() {
    let dir = tempdir().unwrap();
    let script = write_observer(dir.path(), OBSERVED_FRA1);
    let config = write_backend_config(dir.path(), &script, "site-a");
    let inventory = write_site_inventory(dir.path(), "dcim.site", "fra1");
    let state_path = dir.path().join("state.json");
    fs::write(
        &state_path,
        format!(r#"{{"mappings": {{"dcim.site": {{"{SITE_UID}": 7}}}}}}"#),
    )
    .unwrap();

    let mut command = Command::new(bin_path());
    command
        .current_dir(dir.path())
        .env("ALEMBIC_STATE_PATH", &state_path)
        .arg("plan")
        .arg("-f")
        .arg(&inventory)
        .arg("-o")
        .arg(dir.path().join("plan.json"))
        .arg("--backend-config")
        .arg(&config);
    let output = command.output().unwrap();
    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("no backend stamp"),
        "{}",
        stderr(&output)
    );
}

/// write a stamped state file binding the site uid to backend id 7 for the
/// given instance, at the backend-scoped default path.
fn seed_scoped_state(dir: &Path, instance: &str) {
    // the scoped file name embeds the identity hash the cli computes; easiest
    // to let the cli create it via one adopting plan, which the tests above
    // pin. here we just re-run that flow.
    let script = write_observer(dir, OBSERVED_FRA1);
    let config = write_backend_config(dir, &script, instance);
    let inventory = write_site_inventory(dir, "dcim.site", "fra1");
    let output = plan(dir, &inventory, &config, &[]);
    assert!(output.status.success(), "{}", stderr(&output));
}

/// retype: the same uid declared under a new type is one logical object
/// re-materialized, and the plan says so instead of listing an unrelated
/// create and delete.
#[test]
fn a_same_uid_type_change_renders_as_a_retype() {
    let dir = tempdir().unwrap();
    seed_scoped_state(dir.path(), "site-a");
    let script = write_observer(dir.path(), OBSERVED_FRA1);
    let config = write_backend_config(dir.path(), &script, "site-a");
    let retyped = write_site_inventory(dir.path(), "location.site", "fra1");

    let output = plan(dir.path(), &retyped, &config, &["--allow-delete"]);
    assert!(output.status.success(), "{}", stderr(&output));
    let out = stdout(&output);
    assert!(out.contains("retype"), "{out}");
    assert!(out.contains("dcim.site -> location.site"), "{out}");
    assert!(
        out.contains("plan: 1 to create, 0 to update, 1 to delete"),
        "{out}"
    );
}

/// import assigns identity state-first: the object state binds keeps its uid
/// across a backend-side rename, while --stateless mints from the observed
/// (type, key) and forgets.
#[test]
fn import_keeps_state_known_identity_across_a_backend_rename() {
    let dir = tempdir().unwrap();
    seed_scoped_state(dir.path(), "site-a");
    // the backend renamed fra1 to fra9 behind alembic's back.
    let renamed = r#"[{"type_name":"dcim.site","key":{"slug":"fra9"},"attrs":{"name":"Frankfurt","slug":"fra9"},"backend_id":7}]"#;
    let script = write_observer(dir.path(), renamed);
    let config = write_backend_config(dir.path(), &script, "site-a");
    let inventory = write_site_inventory(dir.path(), "dcim.site", "fra1");

    let observed = dir.path().join("observed.json");
    let mut command = Command::new(bin_path());
    command
        .current_dir(dir.path())
        .arg("import")
        .arg("-f")
        .arg(&inventory)
        .arg("-o")
        .arg(&observed)
        .arg("--backend-config")
        .arg(&config);
    let output = command.output().unwrap();
    assert!(output.status.success(), "{}", stderr(&output));
    let imported: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&observed).unwrap()).unwrap();
    assert_eq!(imported["objects"][0]["uid"], SITE_UID);
    assert_eq!(imported["objects"][0]["key"]["slug"], "fra9");

    // --stateless drops the memory: value identity, minted from (type, key).
    let mut command = Command::new(bin_path());
    command
        .current_dir(dir.path())
        .arg("import")
        .arg("-f")
        .arg(&inventory)
        .arg("-o")
        .arg(&observed)
        .arg("--backend-config")
        .arg(&config)
        .arg("--stateless");
    let output = command.output().unwrap();
    assert!(output.status.success(), "{}", stderr(&output));
    let imported: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&observed).unwrap()).unwrap();
    assert_ne!(imported["objects"][0]["uid"], SITE_UID);
}
