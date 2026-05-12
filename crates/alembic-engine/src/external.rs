//! helpers for implementing external adapters.

use crate::{ApplyReport, BackendId, Op, ProvisionReport, StateData};
use alembic_core::{JsonMap, Key, Schema, TypeName};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};

/// current external adapter protocol version.
pub const EXTERNAL_PROTOCOL_VERSION: u8 = 1;

/// request envelope sent to external adapters.
#[derive(Debug, Serialize, Deserialize)]
pub struct ExternalEnvelope {
    /// protocol version.
    pub version: u8,
    /// custom plugin configuration.
    pub setup: serde_yaml::Value,
    /// request payload.
    #[serde(flatten)]
    pub request: ExternalRequest,
}

/// external adapter request variants.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum ExternalRequest {
    /// read inventory for the requested types.
    Read {
        schema: Schema,
        types: Vec<TypeName>,
        state: StateData,
    },
    /// apply a set of operations.
    Write {
        schema: Schema,
        ops: Vec<Op>,
        state: StateData,
    },
    /// ensure the backend schema exists.
    EnsureSchema { schema: Schema },
}

/// observed object representation for external adapters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalObject {
    /// object type.
    pub type_name: TypeName,
    /// natural key for matching.
    pub key: Key,
    /// observed attributes.
    pub attrs: JsonMap,
    /// backend id when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend_id: Option<BackendId>,
}

/// response wrapper for external adapters.
#[derive(Debug, Serialize, Deserialize)]
pub struct ExternalResponse<T> {
    /// whether the request succeeded.
    pub ok: bool,
    /// payload on success.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<T>,
    /// error message on failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl<T> ExternalResponse<T> {
    /// build a success response.
    pub fn ok(result: T) -> Self {
        Self {
            ok: true,
            result: Some(result),
            error: None,
        }
    }

    /// build an error response.
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            ok: false,
            result: None,
            error: Some(message.into()),
        }
    }

    /// convert a result into a response.
    pub fn from_result(result: Result<T>) -> Self {
        match result {
            Ok(value) => Self::ok(value),
            Err(err) => Self::error(err.to_string()),
        }
    }
}

/// external adapter helper trait.
pub trait ExternalAdapter {
    /// initial configuration of the adapter
    fn setup(&mut self, configuration: &serde_yaml::Value) -> Result<()>;

    /// read objects from the backend.
    fn read(
        &mut self,
        schema: &Schema,
        types: &[TypeName],
        state: &StateData,
    ) -> Result<Vec<ExternalObject>>;

    /// apply operations to the backend.
    fn write(&mut self, schema: &Schema, ops: &[Op], state: &StateData) -> Result<ApplyReport>;

    /// provision backend schema elements.
    fn ensure_schema(&mut self, schema: &Schema) -> Result<ProvisionReport> {
        let _ = schema;
        Ok(ProvisionReport::default())
    }
}

/// run an external adapter using stdin/stdout for a single request.
pub fn run_external_adapter<A: ExternalAdapter>(mut adapter: A) -> io::Result<()> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;

    let envelope: ExternalEnvelope = match serde_json::from_str(&input) {
        Ok(envelope) => envelope,
        Err(err) => return write_error(format!("invalid request: {err}")),
    };

    adapter.setup(&envelope.setup).map_err(io::Error::other)?;

    if envelope.version != EXTERNAL_PROTOCOL_VERSION {
        return write_error(format!(
            "unsupported protocol version {} (expected {})",
            envelope.version, EXTERNAL_PROTOCOL_VERSION
        ));
    }

    let mut stdout = io::BufWriter::new(io::stdout());
    match envelope.request {
        ExternalRequest::Read {
            schema,
            types,
            state,
        } => {
            let response = ExternalResponse::from_result(adapter.read(&schema, &types, &state));
            write_response(&mut stdout, response)
        }
        ExternalRequest::Write { schema, ops, state } => {
            let response = ExternalResponse::from_result(adapter.write(&schema, &ops, &state));
            write_response(&mut stdout, response)
        }
        ExternalRequest::EnsureSchema { schema } => {
            let response = ExternalResponse::from_result(adapter.ensure_schema(&schema));
            write_response(&mut stdout, response)
        }
    }
}

fn write_error(message: String) -> io::Result<()> {
    let mut stdout = io::BufWriter::new(io::stdout());
    let response = ExternalResponse::<serde_json::Value>::error(message);
    write_response(&mut stdout, response)
}

fn write_response<T: Serialize>(
    out: &mut impl Write,
    response: ExternalResponse<T>,
) -> io::Result<()> {
    serde_json::to_writer(&mut *out, &response).map_err(io::Error::other)?;
    out.write_all(b"\n")?;
    out.flush()
}

/// convenience macro to define an external adapter main.
#[macro_export]
macro_rules! alembic_external_main {
    ($adapter:expr) => {
        fn main() -> std::io::Result<()> {
            $crate::external::run_external_adapter($adapter)
        }
    };
}

#[cfg(test)]
mod tests {
    use super::ExternalResponse;
    use serde_json::json;

    #[test]
    fn external_response_ok_serializes() {
        let response = ExternalResponse::ok(vec!["one".to_string()]);
        let value = serde_json::to_value(&response).unwrap();
        assert_eq!(value, json!({"ok": true, "result": ["one"]}));
    }

    #[test]
    fn external_response_error_serializes() {
        let response: ExternalResponse<Vec<String>> = ExternalResponse::error("boom");
        let value = serde_json::to_value(&response).unwrap();
        assert_eq!(value, json!({"ok": false, "error": "boom"}));
    }
}
