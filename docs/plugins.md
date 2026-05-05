# plugins

plugins are separate executable binaries that conform to the
inter-process communication (ipc) protocol defined by `PluginRequest`
and `PluginResponse`.

To make plugin creation as easy as possible, use the
`alembic_plugin_main!`  macro in `alembic_engine`. The macro will
define a main function with an appropriate send/receive loop. Supply
it with a handler of type `PluginRequest -> PluginResponse` and the
required cli version (in semantic versioning format).

Here's an example of a minimal plugin:

```rust
use alembic_engine::alembic_plugin_main;
use alembic_engine::plugin::{PluginRequest, PluginResponse};

fn handle(request: PluginRequest) -> PluginResponse {
    PluginResponse::ok(vec![format!(
        "I'm a minimal plugin. You said: '{}'.",
        request.json
    )])
}

alembic_plugin_main!(handle, ">=0.2.0");
```
