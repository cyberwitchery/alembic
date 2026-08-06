//! an emitter that dispatches on `method` itself and never reads `version`, the
//! bug the suite exists to catch. it refuses `read` for role reasons, which is
//! what used to answer the version probe on its behalf.

use alembic_engine::ApplyReport;
use serde_json::{json, Value};
use std::io::Read;

fn main() {
    let mut input = String::new();
    let _ = std::io::stdin().read_to_string(&mut input);
    let request: Value = match serde_json::from_str(&input) {
        Ok(request) => request,
        Err(e) => {
            emit(&json!({ "ok": false, "error": format!("invalid request: {e}") }));
            return;
        }
    };
    // `version` is deliberately never consulted.
    let response = match request.get("method").and_then(Value::as_str) {
        Some("capabilities") => json!({ "ok": true, "result": { "role": "emitter" } }),
        Some("write") => json!({ "ok": true, "result": ApplyReport::default() }),
        Some("preview_schema") => json!({ "ok": true, "result": null }),
        Some("read") => {
            json!({ "ok": false, "error": "this adapter is write-only; it cannot observe state" })
        }
        method => {
            json!({ "ok": false, "error": format!("unknown method {}", method.unwrap_or("")) })
        }
    };
    emit(&response);
}

fn emit(response: &Value) {
    println!("{response}");
}
