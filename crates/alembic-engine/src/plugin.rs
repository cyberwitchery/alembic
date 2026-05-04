//! plugin: allow extension of alembic using external binaries

use semver::{BuildMetadata, Prerelease, Version, VersionReq};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{self, BufRead, Write};

#[macro_export]
macro_rules! alembic_plugin_main {
    ($handler:path, $required_version:literal) => {
        fn main() -> anyhow::Result<()> {
            $crate::plugin::plugin_loop($handler, $required_version)
        }
    };
}

/// ipc request sent to a plugin.
#[derive(Debug, Deserialize, Serialize)]
pub struct PluginRequest {
    pub json: Value,
    /// the version of the alembic cli (in semantic versioning format).
    pub version: String,
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
pub fn plugin_loop<F>(mut handler: F, required_version: &str) -> anyhow::Result<()>
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
        let request: Result<PluginRequest, _> = serde_json::from_str(&line);
        let resp = match request {
            Ok(req) => {
                if let Err(version_error) = check_alembic_cli_version(required_version, &req) {
                    version_error
                } else {
                    handler(req)
                }
            }
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

fn check_alembic_cli_version(
    required_version: &str,
    request: &PluginRequest,
) -> Result<(), PluginResponse> {
    let version_req = VersionReq::parse(required_version).expect("FIXME");
    let version_actual = Version::parse(&request.version).expect("FIXME");

    if version_req.matches(&version_actual) {
        Ok(())
    } else {
        Err(PluginResponse::error(format!(
            "invalid alembic cli version {}, plugin requires {}",
            request.version, required_version
        )))
    }
}
