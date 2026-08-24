//! the observation law, end to end: key ambiguity among observed objects is
//! data, and a run fails only when it must dereference an ambiguous key --
//! adoption, key matching, or import -- naming every candidate.

mod support;

use std::fs;
use std::path::Path;
use std::process::Command;
use support::bin_path;
use tempfile::tempdir;

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

/// two unmanaged ghosts sharing a key, plus the managed fra1.
const OBSERVED_WITH_GHOSTS: &str = r#"[
 {"type_name":"dcim.site","key":{"slug":"fra1"},"attrs":{"name":"Frankfurt","slug":"fra1"},"backend_id":7},
 {"type_name":"dcim.site","key":{"slug":"dup"},"attrs":{"name":"Ghost A","slug":"dup"},"backend_id":8},
 {"type_name":"dcim.site","key":{"slug":"dup"},"attrs":{"name":"Ghost B","slug":"dup"},"backend_id":9}
]"#;

fn write_site_inventory(dir: &Path, slug: &str) -> std::path::PathBuf {
    let path = dir.join(format!("inventory-{slug}.yaml"));
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
objects:
  - uid: "11111111-1111-1111-1111-111111111111"
    type: dcim.site
    key: {{slug: {slug}}}
    attrs: {{name: Frankfurt, slug: {slug}}}
"#
        ),
    )
    .unwrap();
    path
}

fn setup(dir: &Path, observed: &str) -> std::path::PathBuf {
    let script = write_observer(dir, observed);
    let config = dir.join("backend.yaml");
    fs::write(
        &config,
        format!(
            "backend: external\ncommand: {}\ninstance: obs\n",
            script.display()
        ),
    )
    .unwrap();
    config
}

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

fn stderr(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// unmanaged backend objects sharing a key are the neighbors' data, not this
/// plan's problem: the run proceeds and converges its own object.
#[test]
fn unmanaged_duplicate_keys_do_not_deny_an_unrelated_plan() {
    let dir = tempdir().unwrap();
    let config = setup(dir.path(), OBSERVED_WITH_GHOSTS);
    let inventory = write_site_inventory(dir.path(), "fra1");
    let output = plan(dir.path(), &inventory, &config, &[]);
    assert!(output.status.success(), "{}", stderr(&output));
    let out = String::from_utf8_lossy(&output.stdout);
    assert!(
        out.contains("plan: 0 to create, 0 to update, 0 to delete"),
        "{out}"
    );
}

/// declaring the contested key is the failure: adoption never picks among
/// same-key backend objects, and the error names every candidate.
#[test]
fn adopting_an_ambiguous_key_fails_naming_every_candidate() {
    let dir = tempdir().unwrap();
    let config = setup(dir.path(), OBSERVED_WITH_GHOSTS);
    let inventory = write_site_inventory(dir.path(), "dup");
    let output = plan(dir.path(), &inventory, &config, &[]);
    assert!(!output.status.success());
    let err = stderr(&output);
    assert!(err.contains("cannot adopt dcim.site"), "{err}");
    assert!(err.contains("2 dcim.site objects share the key"), "{err}");
    assert!(err.contains("8, 9"), "{err}");
}

/// deletion addresses objects by backend id, so under --allow-delete both
/// twins plan as deletes and --report lists both as extra.
#[test]
fn unmanaged_twins_are_deletable_and_reportable() {
    let dir = tempdir().unwrap();
    let config = setup(dir.path(), OBSERVED_WITH_GHOSTS);
    let inventory = write_site_inventory(dir.path(), "fra1");

    let output = plan(dir.path(), &inventory, &config, &["--allow-delete"]);
    assert!(output.status.success(), "{}", stderr(&output));
    let out = String::from_utf8_lossy(&output.stdout);
    assert!(
        out.contains("plan: 0 to create, 0 to update, 2 to delete"),
        "{out}"
    );

    let mut command = Command::new(bin_path());
    command
        .current_dir(dir.path())
        .arg("plan")
        .arg("-f")
        .arg(&inventory)
        .arg("--report")
        .arg("--backend-config")
        .arg(&config);
    let output = command.output().unwrap();
    assert!(output.status.success(), "{}", stderr(&output));
    let out = String::from_utf8_lossy(&output.stdout);
    let extras = out.matches("dcim.site {\"slug\":\"dup\"}").count();
    assert_eq!(extras, 2, "both twins surface as extra: {out}");
}

/// import converts the whole observation into an inventory, which cannot hold
/// two objects under one key: every ambiguous key is refused by name.
#[test]
fn import_refuses_ambiguous_keys_naming_all_of_them() {
    let dir = tempdir().unwrap();
    let config = setup(dir.path(), OBSERVED_WITH_GHOSTS);
    let inventory = write_site_inventory(dir.path(), "fra1");
    let mut command = Command::new(bin_path());
    command
        .current_dir(dir.path())
        .arg("import")
        .arg("-f")
        .arg(&inventory)
        .arg("-o")
        .arg(dir.path().join("observed.json"))
        .arg("--backend-config")
        .arg(&config)
        .arg("--stateless");
    let output = command.output().unwrap();
    assert!(!output.status.success());
    let err = stderr(&output);
    assert!(err.contains("name more than one backend object"), "{err}");
    assert!(err.contains("backend ids 8, 9"), "{err}");
}
