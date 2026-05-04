use alembic_engine::alembic_plugin_main;
use alembic_engine::plugin::{PluginRequest, PluginResponse};

fn handle(_request: PluginRequest) -> PluginResponse {
    PluginResponse::ok(vec!["I'm an outdated plugin.".to_string()])
}

alembic_plugin_main!(handle, "0.0.1");
