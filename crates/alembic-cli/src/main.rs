//! cli entrypoint for alembic.

mod app;
mod telemetry;

use app::config::AppConfig;
use app::exit;
use app::Cli;
use clap::Parser;
use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    telemetry::init_tracing();
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(err) => return exit::parse_failure(&err),
    };
    let result = match AppConfig::load() {
        Ok(config) => app::run(cli, config).await,
        Err(err) => Err(anyhow::anyhow!("{}", err)),
    };
    result.unwrap_or_else(|err| {
        // what returning the error from main printed, with a code that says which
        // kind of failure it was
        eprintln!("Error: {err:?}");
        ExitCode::from(exit::code_for(&err))
    })
}
