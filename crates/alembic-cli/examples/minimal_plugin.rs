use alembic_engine::alembic_plugin_main;
use alembic_engine::plugin::{PluginError, PluginRequest, PluginResponse};

fn handle(request: PluginRequest) -> PluginResponse {
    PluginResponse::ok(vec![format!(
        "I'm a minimal plugin. You said: '{}'.",
        request.json
    )])
}

alembic_plugin_main!(handle, ">=0.2.0");
