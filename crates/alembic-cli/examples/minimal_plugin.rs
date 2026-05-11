use alembic_engine::alembic_plugin_main;
use alembic_engine::plugin::{PluginError, PluginRequest, PluginResponse};

fn handle(_request: PluginRequest) -> PluginResponse {
    PluginResponse::ok(vec!["I'm a minimal plugin.".to_string()])
}

alembic_plugin_main!(handle, ">=0.2.0");
