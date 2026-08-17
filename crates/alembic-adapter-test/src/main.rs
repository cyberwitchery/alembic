//! cli for the external adapter conformance runner.

use alembic_adapter_test::{
    load_cases, run_builtin_with, run_cases, Builtins, Case, Failure, Outcome,
};
use anyhow::Context;
use clap::Parser;
use std::path::{Path, PathBuf};
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
    /// also run `protocol/write-empty` and `protocol/ensure-schema-empty`: they write for real.
    #[arg(long)]
    write_checks: bool,
    /// deprecated: off is the default now.
    #[arg(long, conflicts_with = "write_checks")]
    no_provisioning_check: bool,
    /// the adapter command and its arguments, after `--`.
    #[arg(last = true, required = true)]
    adapter: Vec<String>,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let timeout = Duration::from_secs(cli.timeout);

    if cli.no_provisioning_check {
        eprintln!("warning: --no-provisioning-check is the default now; drop it, or pass --write-checks to run the writing checks");
    }
    let builtins = Builtins {
        writes: cli.write_checks,
    };
    let mut outcomes = run_builtin_with(&cli.adapter, timeout, builtins);
    if let Some(path) = cli.cases {
        let cases = match load_cases(&path) {
            // a suite that ran no cases must not report a pass.
            Ok(cases) if cases.is_empty() => {
                eprintln!(
                    "error: no cases at {}: looked for `.json` files in that directory itself, not in subdirectories",
                    path.display()
                );
                return ExitCode::from(2);
            }
            Ok(cases) => cases,
            Err(e) => {
                // the cause carries what is wrong with the fixture (the stray or
                // malformed key); the context alone only names the file.
                eprintln!("error: {e:#}");
                return ExitCode::from(2);
            }
        };
        // a suite that ran some of them must not either.
        match unloaded_case_dirs(&path) {
            Ok(dirs) if !dirs.is_empty() => {
                let list: Vec<String> = dirs.iter().map(|d| d.display().to_string()).collect();
                eprintln!(
                    "error: cases in {} are not loaded: `--cases` reads {} itself, not its subdirectories",
                    list.join(", "),
                    path.display()
                );
                return ExitCode::from(2);
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("error: {e:#}");
                return ExitCode::from(2);
            }
        }
        outcomes.extend(run_cases(&cli.adapter, timeout, &cases));
    }
    report(&outcomes)
}

/// subdirectories of a `--cases` directory holding case files, sorted. a `--cases`
/// file has none.
fn unloaded_case_dirs(path: &Path) -> anyhow::Result<Vec<PathBuf>> {
    if !path.is_dir() {
        return Ok(Vec::new());
    }
    let mut dirs = Vec::new();
    for entry in std::fs::read_dir(path).with_context(|| format!("reading {}", path.display()))? {
        let entry = entry?;
        let child = entry.path();
        if entry.file_type()?.is_dir() && holds_case_files(&child)? {
            dirs.push(child);
        }
    }
    dirs.sort();
    Ok(dirs)
}

/// whether a tree holds a file that loads as a case: one grouped two levels down
/// is dropped as silently as one directly inside.
///
/// the boundary is a case rather than a `.json`, so a directory kept for notes or
/// editor settings does not stop the run. the cost is a malformed case grouped
/// below, which no longer reports here; the zero-case guard still catches the
/// layout that puts every case in a subdirectory.
///
/// a symlinked directory reads as a file rather than a directory here, so the
/// walk cannot be sent out of the tree or around a cycle.
fn holds_case_files(dir: &Path) -> anyhow::Result<bool> {
    for entry in std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            if holds_case_files(&path)? {
                return Ok(true);
            }
        } else if path.extension().and_then(|e| e.to_str()) == Some("json")
            && loads_as_a_case(&path)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// whether `path` parses as a case, the judgement `load_cases` applies one
/// directory up. unreadable is not a case: this decides whether to report, and
/// the run that reports it never reads the file.
fn loads_as_a_case(path: &Path) -> bool {
    std::fs::read_to_string(path).is_ok_and(|text| serde_json::from_str::<Case>(&text).is_ok())
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
