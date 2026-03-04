//! cli entrypoint for alembic.

mod app;
mod telemetry;

use app::Cli;
use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init_tracing();
    let cli = Cli::parse();
    app::run(cli).await
}
