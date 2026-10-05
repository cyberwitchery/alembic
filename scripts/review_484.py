from pathlib import Path
import sys

root = Path(sys.argv[1])


def replace_once(rel: str, old: str, new: str) -> None:
    path = root / rel
    text = path.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{rel}: expected one match, found {count}: {old!r}")
    path.write_text(text.replace(old, new, 1))


replace_once(
    "crates/alembic-cli/src/app/mod.rs",
    "  - apply --plan reads a plan by the same rule: YAML from a .yaml or .yml path,\n    JSON otherwise.\")]",
    "  - apply --plan reads a plan by the same rule: YAML from a .yaml or .yml path,\n    JSON otherwise; --plan - reads a JSON plan from stdin.\")]",
)
replace_once(
    "crates/alembic-cli/src/app/mod.rs",
    "        /// plan file produced by `alembic plan` (yaml or json by extension).\n        #[arg(short = 'p', long)]",
    "        /// plan file produced by `alembic plan` (yaml or json by extension);\n        /// pass `-` to read a json plan from stdin.\n        #[arg(short = 'p', long)]",
)
replace_once(
    "crates/alembic-cli/src/app/mod.rs",
    "        /// prompt for confirmation per operation, applying only approved ops.\n        #[arg(short = 'i', long, default_value_t = false)]",
    "        /// prompt for confirmation per operation, applying only approved ops;\n        /// cannot be combined with `--plan -`, which also consumes stdin.\n        #[arg(short = 'i', long, default_value_t = false)]",
)
replace_once(
    "crates/alembic-cli/src/app/mod.rs",
    "            interactive,\n        } => {\n            let plugins = search_for_plugins(&config)?;",
    "            interactive,\n        } => {\n            if interactive && plan == Path::new(\"-\") {\n                return Err(anyhow!(\n                    \"--interactive cannot be combined with --plan - because stdin is already used for the plan\"\n                ));\n            }\n            let plugins = search_for_plugins(&config)?;",
)

replace_once(
    "crates/alembic-cli/src/app/io.rs",
    "        // no extension and a non-yaml extension both stay json; stdin uses the\n        // same json representation emitted by `plan --dry-run`.\n        assert_eq!(output_kind(Path::new(\"plan\")), OutputKind::Json);\n        assert_eq!(output_kind(Path::new(\"plan.txt\")), OutputKind::Json);\n        assert_eq!(output_kind(Path::new(\"-\")), OutputKind::Json);",
    "        // no extension and a non-yaml extension both stay json.\n        assert_eq!(output_kind(Path::new(\"plan\")), OutputKind::Json);\n        assert_eq!(output_kind(Path::new(\"plan.txt\")), OutputKind::Json);",
)

replace_once(
    "docs/cli.md",
    "alembic apply -p plan.json -o apply-report.json \\\n  --backend-config examples/backend-infrahub.yaml \\\n  --allow-delete\n```",
    "alembic apply -p plan.json -o apply-report.json \\\n  --backend-config examples/backend-infrahub.yaml \\\n  --allow-delete\n\nalembic plan -f examples/inventory.yaml --dry-run --allow-delete \\\n  --backend-config examples/backend-netbox.yaml | \\\n  alembic apply -p - --allow-delete \\\n    --backend-config examples/backend-netbox.yaml\n```",
)
replace_once(
    "docs/cli.md",
    "- applies a plan file",
    "- applies a plan file; `-p -`/`--plan -` reads a json plan from stdin, so the json emitted by `plan --dry-run` can be piped directly into apply (yaml plans still require a file path)",
)
replace_once(
    "docs/cli.md",
    "- `--interactive` prompts per operation and applies only approved ops\n  through the same engine path used by non-interactive apply.",
    "- `--interactive` prompts per operation and applies only approved ops\n  through the same engine path used by non-interactive apply. it cannot be\n  combined with `-p -`/`--plan -`, because both the plan and the prompts would\n  need stdin; that combination is rejected before the backend is constructed.",
)

replace_once(
    "CHANGELOG.md",
    "## Unreleased\n\n",
    "## Unreleased\n\n- cli: `apply --plan -` reads a json plan from stdin, so `plan --dry-run` can pipe directly into apply; `--interactive` rejects stdin plans because its prompts also require stdin (#442)\n",
)

(root / "crates/alembic-cli/tests/apply_stdin.rs").write_text(r'''//! drives the real binary to cover `apply --plan -`: the producer is the exact
//! json emitted by `plan --dry-run`, not a fixture that bypasses the pipe.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use tempfile::tempdir;

mod support;

use support::{bin_path, example_binary};

fn backend_config(dir: &Path) -> PathBuf {
    let config = dir.join("backend.yaml");
    std::fs::write(
        &config,
        format!(
            "backend: external\ncommand: \"{}\"\ntimeout_seconds: 5\n",
            example_binary("minimal_external_adapter").display()
        ),
    )
    .expect("write backend config");
    config
}

#[test]
fn plan_dry_run_feeds_apply_from_stdin() {
    let dir = tempdir().expect("create temp dir");
    let inventory = dir.path().join("inventory.yaml");
    std::fs::write(&inventory, "schema:\n  types: {}\nobjects: []\n")
        .expect("write inventory");
    let config = backend_config(dir.path());
    let state = dir.path().join("state.json");

    let plan = Command::new(bin_path())
        .env("ALEMBIC_STATE_PATH", &state)
        .arg("plan")
        .arg("-f")
        .arg(&inventory)
        .arg("--backend-config")
        .arg(&config)
        .arg("--dry-run")
        .output()
        .expect("run plan --dry-run");
    assert!(
        plan.status.success(),
        "plan failed; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&plan.stdout),
        String::from_utf8_lossy(&plan.stderr)
    );

    let mut apply = Command::new(bin_path());
    apply
        .env("ALEMBIC_STATE_PATH", &state)
        .arg("apply")
        .arg("--backend-config")
        .arg(&config)
        .arg("--plan")
        .arg("-")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = apply.spawn().expect("run apply --plan -");
    child
        .stdin
        .take()
        .expect("apply stdin")
        .write_all(&plan.stdout)
        .expect("pipe dry-run plan into apply");
    let output = child.wait_with_output().expect("wait for apply");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stdin apply failed; stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("applied 0 operations"),
        "expected apply report; stdout:\n{stdout}"
    );
}

#[test]
fn interactive_refuses_stdin_plan_before_backend_setup() {
    let output = Command::new(bin_path())
        .args(["apply", "--plan", "-", "--interactive"])
        .stdin(Stdio::null())
        .output()
        .expect("run interactive stdin apply");
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success(), "combination must be rejected");
    assert!(
        stderr.contains("--interactive cannot be combined with --plan -"),
        "expected explicit stdin conflict; stderr:\n{stderr}"
    );
    assert!(
        !stderr.contains("read plan: stdin"),
        "the conflict should fail before attempting to parse stdin; stderr:\n{stderr}"
    );
}
''')
