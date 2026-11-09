//! every yaml file in the top-level `examples/` goes through the command it is
//! written for: an inventory through `validate`, a map spec through `map` over
//! its input. the backend and adapter configs are built in the registry's own
//! tests. the directory is read, so a file added later is gated without touching
//! this one.

mod support;

use std::fs;
use std::path::Path;
use std::process::Command;

use serde_json::Value;
use support::{bin_path, examples_dir, run_command};
use tempfile::tempdir;

/// the inventory each map spec in `examples/` is written against.
const MAP_INPUTS: &[(&str, &str)] = &[("map.yaml", "map-input.yaml")];

fn parse_yaml(path: &Path) -> Value {
    alembic_engine::yaml::from_str(&fs::read_to_string(path).expect("read example"))
        .expect("example parses as yaml")
}

#[test]
fn every_committed_example_runs() {
    let dir = examples_dir();
    let out = tempdir().expect("temp dir");
    let (mut inventories, mut specs) = (0, 0);

    for entry in fs::read_dir(&dir).expect("examples dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().is_none_or(|ext| ext != "yaml") {
            continue;
        }
        let name = path.file_name().unwrap().to_str().unwrap();
        let doc = parse_yaml(&path);

        let mut cmd = Command::new(bin_path());
        if doc.get("backend").is_some() || doc.get("base_url").is_some() {
            // a backend or generic adapter config: alembic-adapter-registry builds it.
            continue;
        } else if doc.get("rules").is_some() {
            let input = MAP_INPUTS
                .iter()
                .find(|(spec, _)| *spec == name)
                .map(|(_, input)| dir.join(input))
                .unwrap_or_else(|| panic!("map spec {name} has no input in MAP_INPUTS"));
            cmd.args(["map", "-f", input.to_str().unwrap(), "--spec"])
                .arg(&path)
                .arg("-o")
                .arg(out.path().join(format!("{name}.json")));
            specs += 1;
        } else if doc.get("objects").is_some() {
            cmd.args(["validate", "-f"]).arg(&path);
            inventories += 1;
        } else {
            panic!("{name} is not an inventory, a map spec, or a backend config");
        }
        run_command(cmd, name);
    }

    assert!(inventories > 0, "no inventory found in {}", dir.display());
    assert!(specs > 0, "no map spec found in {}", dir.display());
}
