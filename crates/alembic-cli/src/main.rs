//! cli entrypoint for alembic.

mod app;
mod telemetry;

use app::Cli;
use app::config::AppConfig;
use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init_tracing();
    let cli = Cli::parse();
    let config = AppConfig::load()
        .map_err(|err| anyhow::anyhow!("{}", err))?;

    app::run(cli, config).await
}
