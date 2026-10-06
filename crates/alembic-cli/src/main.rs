//! cli entrypoint for alembic.

mod app;
mod telemetry;

use app::config::AppConfig;
use app::Cli;
use clap::Parser;
use std::process::ExitCode;

#[tokio::main]
async fn main() -> anyhow::Result<ExitCode> {
    telemetry::init_tracing();
    let cli = Cli::parse();
    let config = AppConfig::load().map_err(|err| anyhow::anyhow!("{}", err))?;

    app::run(cli, config).await
}
