//! cli for the external adapter conformance runner.

use alembic_adapter_test::{load_cases, run_builtin_with, run_cases, Builtins, Failure, Outcome};
use clap::Parser;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

#[derive(Parser)]
#[command(about = "check an external adapter executable for protocol conformance")]
struct Cli {
    /// file or directory of {name,request,expect} cases to run after the built-in checks.
    #[arg(long)]
    cases: Option<PathBuf>,
    /// per-check timeout, in seconds.
    #[arg(long, default_value_t = 10)]
    timeout: u64,
    /// skip `protocol/ensure-schema-empty`, the one built-in that writes. it
    /// sends a real `ensure_schema` with an empty schema, which an adapter that
    /// converges reads as "delete everything you own"; the runner has no
    /// `--allow-delete` gate of its own. leave it on unless the backend behind
    /// the adapter is not disposable.
    #[arg(long)]
    no_provisioning_check: bool,
    /// the adapter command and its arguments, after `--`.
    #[arg(last = true, required = true)]
    adapter: Vec<String>,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let timeout = Duration::from_secs(cli.timeout);

    let builtins = Builtins {
        provisioning: !cli.no_provisioning_check,
    };
    let mut outcomes = run_builtin_with(&cli.adapter, timeout, builtins);
    if let Some(path) = cli.cases {
        match load_cases(&path) {
            // a suite that ran no cases must not report a pass.
            Ok(cases) if cases.is_empty() => {
                eprintln!(
                    "error: no cases at {}: looked for `.json` files in that directory itself, not in subdirectories",
                    path.display()
                );
                return ExitCode::from(2);
            }
            Ok(cases) => outcomes.extend(run_cases(&cli.adapter, timeout, &cases)),
            Err(e) => {
                // the cause carries what is wrong with the fixture (the stray or
                // malformed key); the context alone only names the file.
                eprintln!("error: {e:#}");
                return ExitCode::from(2);
            }
        }
    }
    report(&outcomes)
}

/// print one line per outcome, a summary, and pick the exit code.
fn report(outcomes: &[Outcome]) -> ExitCode {
    let width = outcomes.iter().map(|o| o.name.len()).max().unwrap_or(0);
    let mut failed = 0;
    let mut skipped = 0;
    for outcome in outcomes {
        let name = &outcome.name;
        match (&outcome.skipped, &outcome.failure) {
            (Some(reason), _) => {
                skipped += 1;
                println!("{name:<width$}   skipped ({reason})");
            }
            (None, None) => println!("{name:<width$}   ok"),
            (None, Some(failure)) => {
                failed += 1;
                println!("{name:<width$}   FAILED");
                print_failure(failure);
            }
        }
    }

    let passed = outcomes.len() - failed - skipped;
    let mut summary = format!("{passed} passed");
    if skipped > 0 {
        summary.push_str(&format!(", {skipped} skipped"));
    }
    println!();
    if failed == 0 {
        println!("{summary}");
        ExitCode::SUCCESS
    } else {
        println!("{summary}, {failed} failed");
        ExitCode::from(1)
    }
}

/// print the indented diagnostics for a failed check.
fn print_failure(failure: &Failure) {
    println!("    error:  {}", failure.message);
    println!("    status: {}", failure.status);
    print_stream("stdout", &failure.stdout);
    print_stream("stderr", &failure.stderr);
}

fn print_stream(label: &str, content: &str) {
    let content = content.trim_end_matches('\n');
    if content.is_empty() {
        println!("    {label}: <empty>");
    } else {
        println!("    {label}:");
        for line in content.lines() {
            println!("      {line}");
        }
    }
}
