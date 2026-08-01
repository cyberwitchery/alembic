//! cli for the external adapter conformance runner.

use alembic_adapter_test::{load_cases, run_builtin, run_cases, Failure, Outcome};
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
    /// the adapter command and its arguments, after `--`.
    #[arg(last = true, required = true)]
    adapter: Vec<String>,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let timeout = Duration::from_secs(cli.timeout);

    let mut outcomes = run_builtin(&cli.adapter, timeout);
    if let Some(path) = cli.cases {
        match load_cases(&path) {
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
    for outcome in outcomes {
        let name = &outcome.name;
        match &outcome.failure {
            None => println!("{name:<width$}   ok"),
            Some(failure) => {
                failed += 1;
                println!("{name:<width$}   FAILED");
                print_failure(failure);
            }
        }
    }

    let passed = outcomes.len() - failed;
    println!();
    if failed == 0 {
        println!("{passed} passed");
        ExitCode::SUCCESS
    } else {
        println!("{passed} passed, {failed} failed");
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
