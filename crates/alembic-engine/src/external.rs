//! helpers for implementing external adapters.

use crate::{ApplyReport, BackendId, Op, ProvisionReport, StateData};
use alembic_core::{JsonMap, Key, Schema, TypeName};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::io::{self, BufReader, Read, Write};

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
pub fn run_external_adapter<A: ExternalAdapter>(
    mut adapter: A,
    (reader, mut writer): (impl Read, impl Write),
) -> io::Result<()> {
    let mut input = String::new();
    BufReader::new(reader).read_to_string(&mut input)?;

    let envelope: ExternalEnvelope = match serde_json::from_str(&input) {
        Ok(envelope) => envelope,
        Err(err) => return write_error(&mut writer, format!("invalid request: {err}")),
    };

    if envelope.version != EXTERNAL_PROTOCOL_VERSION {
        return write_error(
            &mut writer,
            format!(
                "unsupported protocol version {} (expected {})",
                envelope.version, EXTERNAL_PROTOCOL_VERSION
            ),
        );
    }

    adapter.setup(&envelope.setup).map_err(io::Error::other)?;

    match envelope.request {
        ExternalRequest::Read {
            schema,
            types,
            state,
        } => {
            let response = ExternalResponse::from_result(adapter.read(&schema, &types, &state));
            write_response(&mut writer, response)
        }
        ExternalRequest::Write { schema, ops, state } => {
            let response = ExternalResponse::from_result(adapter.write(&schema, &ops, &state));
            write_response(&mut writer, response)
        }
        ExternalRequest::EnsureSchema { schema } => {
            let response = ExternalResponse::from_result(adapter.ensure_schema(&schema));
            write_response(&mut writer, response)
        }
    }
}

fn write_error(out: &mut impl Write, message: String) -> io::Result<()> {
    let response = ExternalResponse::<serde_json::Value>::error(message);
    write_response(out, response)
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
            let stdin = std::io::stdin();
            let mut stdout = std::io::BufWriter::new(std::io::stdout());
            $crate::external::run_external_adapter($adapter, (stdin, stdout))
        }
    };
}

#[cfg(test)]
mod tests {
    use super::ExternalResponse;
    use crate::{
        run_external_adapter, ApplyReport, ExternalAdapter, ExternalEnvelope, ExternalObject,
        ExternalRequest, Op, ProvisionReport, StateData, EXTERNAL_PROTOCOL_VERSION,
    };
    use alembic_core::{Schema, TypeName, TypeSchema};
    use serde_json::json;
    use serde_yaml::Value;
    use std::io::BufReader;
    use std::io::{BufRead, Write};

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

    #[derive(Debug, Default)]
    struct TestExternalAdapter {
        pub x: i64,
    }

    impl ExternalAdapter for TestExternalAdapter {
        fn setup(&mut self, configuration: &Value) -> anyhow::Result<()> {
            if let Some(x) = configuration.get("x").and_then(serde_yaml::Value::as_i64) {
                self.x = x;
            }
            Ok(())
        }

        fn read(
            &mut self,
            _schema: &Schema,
            _types: &[TypeName],
            _state: &StateData,
        ) -> anyhow::Result<Vec<ExternalObject>> {
            let mut result = vec![];
            for _ in 0..self.x {
                result.push(ExternalObject {
                    type_name: TypeName::new(""),
                    key: Default::default(),
                    attrs: Default::default(),
                    backend_id: None,
                })
            }
            Ok(result)
        }

        fn write(
            &mut self,
            _schema: &Schema,
            _ops: &[Op],
            _state: &StateData,
        ) -> anyhow::Result<ApplyReport> {
            Err(anyhow::anyhow!("unsupported operation"))
        }

        fn ensure_schema(&mut self, schema: &Schema) -> anyhow::Result<ProvisionReport> {
            let mut created_fields = vec![];
            for ty_name in schema.types.keys() {
                created_fields.push(ty_name.clone());
            }
            Ok(ProvisionReport {
                created_fields,
                ..Default::default()
            })
        }
    }

    #[test]
    fn external_adapter_communication_over_stdio() {
        let adapter = TestExternalAdapter::default();

        let (in_reader, mut in_writer) = std::io::pipe().unwrap();
        let (out_reader, out_writer) = std::io::pipe().unwrap();

        let t = std::thread::spawn(move || {
            assert!(run_external_adapter(adapter, (in_reader, out_writer)).is_ok());
        });

        let dummy_type_schema = TypeSchema {
            key: [].into(),
            fields: [].into(),
        };

        let request = ExternalRequest::EnsureSchema {
            schema: Schema {
                types: [
                    ("a".to_string(), dummy_type_schema.clone()),
                    ("b".to_string(), dummy_type_schema.clone()),
                ]
                .into(),
            },
        };
        let envelope = ExternalEnvelope {
            version: EXTERNAL_PROTOCOL_VERSION,
            setup: Default::default(),
            request,
        };

        writeln!(in_writer, "{}", serde_json::to_string(&envelope).unwrap()).unwrap();
        drop(in_writer);

        let mut response = String::new();
        BufReader::new(out_reader).read_line(&mut response).unwrap();

        let response: ExternalResponse<ProvisionReport> = serde_json::from_str(&response).unwrap();
        assert!(response.ok);
        assert_eq!(
            response.result.unwrap().created_fields,
            vec!["a".to_string(), "b".to_string()]
        );

        t.join().unwrap();
    }

    #[test]
    fn external_adapter_communication_error() {
        let adapter = TestExternalAdapter::default();

        let (in_reader, mut in_writer) = std::io::pipe().unwrap();
        let (out_reader, out_writer) = std::io::pipe().unwrap();

        let t = std::thread::spawn(move || {
            assert!(run_external_adapter(adapter, (in_reader, out_writer)).is_ok());
        });

        // The 'Write' request is booby trapped on TestExternalAdapter
        let request = ExternalRequest::Write {
            schema: Default::default(),
            ops: vec![],
            state: Default::default(),
        };
        let envelope = ExternalEnvelope {
            version: EXTERNAL_PROTOCOL_VERSION,
            setup: Default::default(),
            request,
        };

        writeln!(in_writer, "{}", serde_json::to_string(&envelope).unwrap()).unwrap();
        drop(in_writer);

        let mut response = String::new();
        BufReader::new(out_reader).read_line(&mut response).unwrap();

        let response: ExternalResponse<ProvisionReport> = serde_json::from_str(&response).unwrap();
        assert!(response.error.is_some());
        assert!(!response.ok);

        t.join().unwrap();
    }

    #[test]
    fn external_adapter_outdated() {
        let adapter = TestExternalAdapter::default();

        let (in_reader, mut in_writer) = std::io::pipe().unwrap();
        let (out_reader, out_writer) = std::io::pipe().unwrap();

        let t = std::thread::spawn(move || {
            assert!(run_external_adapter(adapter, (in_reader, out_writer)).is_ok());
        });

        // The 'Write' request is booby trapped on TestExternalAdapter
        let request = ExternalRequest::EnsureSchema {
            schema: Default::default(),
        };
        let envelope = ExternalEnvelope {
            version: EXTERNAL_PROTOCOL_VERSION + 1,
            setup: Default::default(),
            request,
        };

        writeln!(in_writer, "{}", serde_json::to_string(&envelope).unwrap()).unwrap();
        drop(in_writer);

        let mut response = String::new();
        BufReader::new(out_reader).read_line(&mut response).unwrap();

        let response: ExternalResponse<ProvisionReport> = serde_json::from_str(&response).unwrap();
        if let Some(error) = response.error {
            assert_eq!(
                error,
                format!(
                    "unsupported protocol version {} (expected {})",
                    EXTERNAL_PROTOCOL_VERSION + 1,
                    EXTERNAL_PROTOCOL_VERSION
                )
            );
        }
        assert!(!response.ok);

        t.join().unwrap();
    }

    #[test]
    fn external_adapter_configuration() {
        let adapter = TestExternalAdapter::default();

        let (in_reader, mut in_writer) = std::io::pipe().unwrap();
        let (out_reader, out_writer) = std::io::pipe().unwrap();

        let t = std::thread::spawn(move || {
            assert!(run_external_adapter(adapter, (in_reader, out_writer)).is_ok());
        });

        // The 'Write' request is booby trapped on TestExternalAdapter
        let request = ExternalRequest::Read {
            schema: Default::default(),
            types: vec![],
            state: Default::default(),
        };
        const MAGIC_NUMBER: usize = 13;

        let envelope = ExternalEnvelope {
            version: EXTERNAL_PROTOCOL_VERSION,
            setup: serde_yaml::from_str(&format!("x: {MAGIC_NUMBER}")).unwrap(),
            request,
        };

        writeln!(in_writer, "{}", serde_json::to_string(&envelope).unwrap()).unwrap();
        drop(in_writer);

        let mut response = String::new();
        BufReader::new(out_reader).read_line(&mut response).unwrap();

        let response: ExternalResponse<Vec<ExternalObject>> =
            serde_json::from_str(&response).unwrap();
        assert!(response.ok);
        assert_eq!(response.result.unwrap().len(), MAGIC_NUMBER,);

        t.join().unwrap();
    }
}
