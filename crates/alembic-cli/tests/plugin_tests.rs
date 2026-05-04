mod support;

#[path = "../src/app/plugins.rs"]
mod plugins;

use alembic_engine::plugin::PluginResponse;
use anyhow::Result;
use plugins::run_plugin;

#[test]
fn minimal_plugin() {
    let response = build_and_run_plugin("minimal_plugin");

    if let Ok(ok_response) = response {
        assert!(ok_response.ok)
    } else {
        panic!("didn't get a response from plugin")
    }
}

#[test]
fn outdated_plugin() {
    let response = build_and_run_plugin("outdated_plugin");

    if let Ok(ok_response) = response {
        assert!(!ok_response.ok)
    } else {
        panic!("didn't get a response from plugin")
    }
}

pub fn build_and_run_plugin(name: &str) -> Result<PluginResponse> {
    escargot::CargoBuild::new().example(name);
    run_plugin(name)
}
