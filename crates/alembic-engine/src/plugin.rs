//! plugin: allow extension of alembic using external binaries

use semver::{Version, VersionReq};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::io::{BufRead, BufReader, Read, Write};

#[macro_export]
macro_rules! alembic_plugin_main {
    ($handler:path, $required_version:literal) => {
        fn main() -> std::result::Result<(), PluginError> {
            let stdin = std::io::stdin();
            let mut stdout = std::io::BufWriter::new(std::io::stdout());
            $crate::plugin::plugin_loop($handler, $required_version, (stdin, stdout))
        }
    };
}

/// ipc request sent to a plugin.
#[derive(Debug, Deserialize, Serialize)]
pub struct PluginRequest {
    pub command: PluginCommand,
    /// the version of the alembic cli (in semantic versioning format).
    pub version: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum PluginCommand {
    /// initial check to see that the plugin works
    Handshake,
    /// corresponds to the cli command 'apply'
    Apply,
}

impl PluginRequest {
    pub fn handshake(version: String) -> Self {
        Self {
            command: PluginCommand::Handshake,
            version,
        }
    }
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

#[derive(Debug)]
pub enum PluginError {
    IoError(std::io::Error),
}

impl std::error::Error for PluginError {}

impl From<std::io::Error> for PluginError {
    fn from(e: std::io::Error) -> Self {
        PluginError::IoError(e)
    }
}

impl Display for PluginError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PluginError::IoError(e) => {
                write!(f, "Plugin IO error: {}", e)
            }
        }
    }
}

/// runs a newline-delimited json plugin loop.
///
/// the handler is invoked once per request. responses are serialized back to `writer`.
pub fn plugin_loop<F>(
    mut handler: F,
    required_version: &str,
    (reader, mut writer): (impl Read, impl Write),
) -> Result<(), PluginError>
where
    F: FnMut(PluginRequest) -> PluginResponse,
{
    for line in BufReader::new(reader).lines() {
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
        writeln!(writer, "{json}")?;
        writer.flush()?;
    }
    Ok(())
}

fn check_alembic_cli_version(
    required_version: &str,
    request: &PluginRequest,
) -> Result<(), PluginResponse> {
    let Ok(version_req) = VersionReq::parse(required_version) else {
        return Err(PluginResponse::error(format!(
            "failed to parse version requirement: '{}'",
            required_version
        )));
    };
    let Ok(version_actual) = Version::parse(&request.version) else {
        return Err(PluginResponse::error(format!(
            "failed to parse alembic cli version: '{}'",
            request.version
        )));
    };

    if version_req.matches(&version_actual) {
        Ok(())
    } else {
        Err(PluginResponse::error(format!(
            "unsupported alembic cli version {}, plugin requires {}",
            request.version, required_version
        )))
    }
}

#[test]
fn cli_ok_version_check() {
    assert!(check_alembic_cli_version(">0.5", &PluginRequest::handshake("0.6.0".into())).is_ok());
}

#[test]
fn cli_outdated_version_check() {
    assert!(check_alembic_cli_version(">0.5", &PluginRequest::handshake("0.4.0".into())).is_err());
}

#[test]
fn plugin_loop_responds() {
    fn handler(request: PluginRequest) -> PluginResponse {
        match request.command {
            PluginCommand::Handshake => PluginResponse::ok(vec!["hand shaken".to_string()]),
            PluginCommand::Apply => panic!("wrong command"),
        }
    }

    let (in_reader, mut in_writer) = std::io::pipe().unwrap();
    let (out_reader, out_writer) = std::io::pipe().unwrap();

    let t = std::thread::spawn(move || {
        assert!(plugin_loop(handler, ">=0.1.0", (in_reader, out_writer)).is_ok());
    });

    let request = PluginRequest {
        command: PluginCommand::Handshake,
        version: "0.1.0".to_string(),
    };
    writeln!(in_writer, "{}", serde_json::to_string(&request).unwrap()).unwrap();
    drop(in_writer);

    let mut response = String::new();
    BufReader::new(out_reader).read_line(&mut response).unwrap();

    let response: PluginResponse = serde_json::from_str(&response).unwrap();
    assert!(response.ok);
    assert_eq!(response.lines, vec!["hand shaken".to_string()]);

    t.join().unwrap();
}
