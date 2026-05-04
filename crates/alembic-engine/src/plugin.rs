//! plugin: allow extension of alembic using external binaries

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{self, BufRead, Write};

#[macro_export]
macro_rules! alembic_plugin_main {
    ($handler:path) => {
        fn main() -> std::io::Result<()> {
            $crate::plugin::plugin_loop($handler)
        }
    };
}

/// ipc request sent to a plugin.
#[derive(Debug, Deserialize, Serialize)]
pub struct PluginRequest {
    pub json: Value,
}

/// ipc response returned by a plugin.
#[derive(Debug, Serialize, Deserialize)]
pub struct PluginResponse {
    /// whether the plugin request succeeded.
    pub ok: bool,
    /// response text.
    pub lines: Vec<String>,
    /// optional error message.
    pub error: Option<String>,
}

impl PluginResponse {
    /// successful response with rendered lines.
    pub fn ok(lines: Vec<String>) -> Self {
        Self {
            ok: true,
            lines,
            error: None,
        }
    }

    /// error response with message.
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            ok: false,
            lines: Vec::new(),
            error: Some(message.into()),
        }
    }
}

/// runs a newline-delimited json plugin loop.
///
/// the handler is invoked once per request. responses are serialized back to stdout.
pub fn plugin_loop<F>(mut handler: F) -> io::Result<()>
where
    F: FnMut(PluginRequest) -> PluginResponse,
{
    let stdin = io::stdin();
    let mut stdout = io::BufWriter::new(io::stdout());
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(line) => line,
            Err(_) => break,
        };
        if line.trim().is_empty() {
            continue;
        }
        let req: Result<PluginRequest, _> = serde_json::from_str(&line);
        let resp = match req {
            Ok(req) => handler(req),
            Err(err) => PluginResponse::error(format!("invalid request: {err}")),
        };
        let json = serde_json::to_string(&resp).unwrap_or_else(|_| {
            "{\"ok\":false,\"lines\":[],\"error\":\"encode failed\"}".to_string()
        });
        writeln!(stdout, "{json}")?;
        stdout.flush()?;
    }
    Ok(())
}
