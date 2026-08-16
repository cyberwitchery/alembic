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
    /// preview what ensuring the backend schema would provision, writing nothing.
    PreviewSchema { schema: Schema },
    /// report which role the adapter implements.
    Capabilities,
}

/// borrowed host-side serializer; keep field-compatible with [`ExternalEnvelope`].
#[derive(Debug, Serialize)]
pub struct ExternalEnvelopeRef<'a> {
    /// protocol version.
    pub version: u8,
    /// custom plugin configuration.
    pub setup: serde_yaml::Value,
    /// request payload.
    #[serde(flatten)]
    pub request: ExternalRequestRef<'a>,
}

/// borrowed host-side serializer; keep field-compatible with [`ExternalRequest`].
#[derive(Debug, Serialize)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum ExternalRequestRef<'a> {
    /// read inventory for the requested types.
    Read {
        schema: &'a Schema,
        types: &'a [TypeName],
        state: StateData,
    },
    /// apply a set of operations.
    Write {
        schema: &'a Schema,
        ops: &'a [Op],
        state: StateData,
    },
    /// ensure the backend schema exists.
    EnsureSchema { schema: &'a Schema },
    /// preview what ensuring the backend schema would provision, writing nothing.
    PreviewSchema { schema: &'a Schema },
    /// report which role the adapter implements.
    Capabilities,
}

impl ExternalRequestRef<'_> {
    /// the `method` this request carries on the wire.
    pub fn method(&self) -> &'static str {
        match self {
            Self::Read { .. } => "read",
            Self::Write { .. } => "write",
            Self::EnsureSchema { .. } => "ensure_schema",
            Self::PreviewSchema { .. } => "preview_schema",
            Self::Capabilities => "capabilities",
        }
    }
}

/// the role an external adapter reports through the capabilities method.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExternalRole {
    /// read-only: the adapter observes state but cannot apply changes.
    Observer,
    /// write-only: the adapter applies changes but cannot observe state.
    Emitter,
    /// read+write; the default for an adapter that does not answer capabilities.
    #[default]
    Adapter,
}

/// result payload of the capabilities method.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalCapabilities {
    /// which side of the adapter contract the adapter implements.
    #[serde(default)]
    pub role: ExternalRole,
}

/// observed object representation for external adapters.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalObject {
    /// object type.
    pub type_name: TypeName,
    /// natural key for matching.
    pub key: Key,
    /// observed attributes.
    #[serde(default)]
    pub attrs: JsonMap,
    /// backend id when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend_id: Option<BackendId>,
}

/// response wrapper for external adapters.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
            Err(err) => Self::error(format!("{err:#}")),
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

    /// preview schema provisioning, writing nothing. the default pairs with
    /// [`ExternalAdapter::ensure_schema`]'s: nothing to provision. `None` means
    /// "cannot preview", and refuses to provision at all.
    fn preview_schema(&mut self, schema: &Schema) -> Result<Option<ProvisionReport>> {
        let _ = schema;
        Ok(Some(ProvisionReport::default()))
    }

    /// report which role this adapter implements. the default, the full read+write
    /// [`ExternalRole::Adapter`], keeps existing adapters unchanged; an emit-only
    /// adapter overrides this to report [`ExternalRole::Emitter`] so the host
    /// errors on observe instead of reading nothing; a read-only one reports
    /// [`ExternalRole::Observer`] so the host errors on apply instead of writing.
    fn capabilities(&mut self) -> ExternalCapabilities {
        ExternalCapabilities::default()
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

    if let Err(e) = adapter.setup(&envelope.setup) {
        return write_error(&mut writer, format!("invalid setup: {e}"));
    }

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
        ExternalRequest::PreviewSchema { schema } => {
            let response = ExternalResponse::from_result(adapter.preview_schema(&schema));
            write_response(&mut writer, response)
        }
        ExternalRequest::Capabilities => {
            write_response(&mut writer, ExternalResponse::ok(adapter.capabilities()))
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
        run_external_adapter, AppliedOp, ApplyReport, ExternalAdapter, ExternalCapabilities,
        ExternalEnvelope, ExternalEnvelopeRef, ExternalObject, ExternalRequest, ExternalRequestRef,
        ExternalRole, Op, ProvisionReport, StateData, EXTERNAL_PROTOCOL_VERSION,
    };
    use alembic_core::{Key, Object, Schema, TypeName, TypeSchema, Uid};
    use serde_json::json;
    use serde_yaml::Value;
    use std::io::BufReader;
    use std::io::{BufRead, Write};

    #[test]
    fn a_misspelled_attrs_key_is_rejected() {
        // `attrs` defaults, so a typo would otherwise observe the object with no
        // attributes and replan every one of them as a change, forever.
        let err = serde_json::from_str::<ExternalObject>(
            r#"{"type_name":"device","key":{"name":"fra1"},"atrs":{"name":"FRA1"}}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("atrs"), "{err}");
    }

    #[test]
    fn an_observed_object_may_still_omit_attrs() {
        let object: ExternalObject =
            serde_json::from_str(r#"{"type_name":"device","key":{"name":"fra1"}}"#).unwrap();
        assert!(object.attrs.is_empty());
    }

    #[test]
    fn a_misspelled_role_key_is_rejected() {
        let err = serde_json::from_str::<ExternalCapabilities>(r#"{"rol":"adapter"}"#).unwrap_err();
        assert!(err.to_string().contains("rol"), "{err}");
    }

    #[test]
    fn a_misspelled_applied_key_is_rejected() {
        let err = serde_json::from_str::<ApplyReport>(r#"{"aplied":[]}"#).unwrap_err();
        assert!(err.to_string().contains("aplied"), "{err}");
    }

    #[test]
    fn an_apply_report_may_still_omit_applied() {
        let report: ApplyReport = serde_json::from_str("{}").unwrap();
        assert!(report.applied.is_empty());
    }

    #[test]
    fn a_misspelled_backend_id_key_is_rejected() {
        // a typo'd id reads as the adapter returning none, and the run drops a
        // mapping it was told to keep.
        let err = serde_json::from_str::<AppliedOp>(
            r#"{"uid":"11111111-1111-1111-1111-111111111111","type_name":"device","bakcend_id":"7"}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("bakcend_id"), "{err}");
    }

    #[test]
    fn an_applied_op_may_still_omit_backend_id() {
        let applied: AppliedOp = serde_json::from_str(
            r#"{"uid":"11111111-1111-1111-1111-111111111111","type_name":"device"}"#,
        )
        .unwrap();
        assert!(applied.backend_id.is_none());
    }

    #[test]
    fn a_misspelled_provision_category_is_rejected() {
        // every category defaults, so a typo'd delete reads as an empty report and
        // provisions past the --allow-delete gate.
        let err =
            serde_json::from_str::<ProvisionReport>(r#"{"deleted_obejct_types":["dcim.site"]}"#)
                .unwrap_err();
        assert!(err.to_string().contains("deleted_obejct_types"), "{err}");
    }

    #[test]
    fn a_provision_report_may_still_omit_every_category() {
        let report: ProvisionReport = serde_json::from_str("{}").unwrap();
        assert_eq!(report, ProvisionReport::default());
    }

    #[test]
    fn a_misspelled_result_key_is_rejected() {
        // a typo'd `result` reads as an absent one, which preview_schema takes for
        // "cannot preview" and skips the gate on.
        let err = serde_json::from_str::<ExternalResponse<ProvisionReport>>(
            r#"{"ok":true,"reslt":{"deleted_object_types":["dcim.site"]}}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("reslt"), "{err}");
    }

    #[test]
    fn a_response_may_still_omit_result() {
        let response: ExternalResponse<ProvisionReport> =
            serde_json::from_str(r#"{"ok":true}"#).unwrap();
        assert!(response.result.is_none());
    }

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

    #[test]
    fn external_response_from_result_renders_error_chain() {
        let err = anyhow::anyhow!("connection refused")
            .context("connecting to backend")
            .context("reading inventory");
        let response: ExternalResponse<()> = ExternalResponse::from_result(Err(err));
        let error = response.error.unwrap();
        assert!(error.contains("reading inventory"));
        assert!(error.contains("connecting to backend"));
        assert!(error.contains("connection refused"));
    }

    #[test]
    fn ref_and_owned_request_types_serialize_identically() {
        let schema = Schema {
            types: [(
                "dcim.device".to_string(),
                TypeSchema {
                    key: [].into(),
                    fields: [].into(),
                },
            )]
            .into(),
        };
        let types = vec![TypeName::new("dcim.device")];
        let ops = vec![Op::Create {
            uid: Uid::from_u128(1),
            type_name: TypeName::new("dcim.device"),
            desired: Object {
                uid: Uid::from_u128(1),
                type_name: TypeName::new("dcim.device"),
                key: Key::default(),
                attrs: Default::default(),
                source: None,
            },
        }];
        let state = StateData::default();

        let owned_read = serde_json::to_value(ExternalRequest::Read {
            schema: schema.clone(),
            types: types.clone(),
            state: state.clone(),
        })
        .unwrap();
        let ref_read = serde_json::to_value(ExternalRequestRef::Read {
            schema: &schema,
            types: &types,
            state: state.clone(),
        })
        .unwrap();
        assert_eq!(owned_read, ref_read);

        let owned_write = serde_json::to_value(ExternalRequest::Write {
            schema: schema.clone(),
            ops: ops.clone(),
            state: state.clone(),
        })
        .unwrap();
        let ref_write = serde_json::to_value(ExternalRequestRef::Write {
            schema: &schema,
            ops: &ops,
            state: state.clone(),
        })
        .unwrap();
        assert_eq!(owned_write, ref_write);

        let owned_ensure = serde_json::to_value(ExternalRequest::EnsureSchema {
            schema: schema.clone(),
        })
        .unwrap();
        let ref_ensure =
            serde_json::to_value(ExternalRequestRef::EnsureSchema { schema: &schema }).unwrap();
        assert_eq!(owned_ensure, ref_ensure);

        let owned_preview = serde_json::to_value(ExternalRequest::PreviewSchema {
            schema: schema.clone(),
        })
        .unwrap();
        let ref_preview =
            serde_json::to_value(ExternalRequestRef::PreviewSchema { schema: &schema }).unwrap();
        assert_eq!(owned_preview, ref_preview);

        let owned_capabilities = serde_json::to_value(ExternalRequest::Capabilities).unwrap();
        let ref_capabilities = serde_json::to_value(ExternalRequestRef::Capabilities).unwrap();
        assert_eq!(owned_capabilities, ref_capabilities);
        assert_eq!(owned_capabilities, json!({"method": "capabilities"}));

        let owned_envelope = serde_json::to_value(ExternalEnvelope {
            version: EXTERNAL_PROTOCOL_VERSION,
            setup: Default::default(),
            request: ExternalRequest::Read {
                schema: schema.clone(),
                types: types.clone(),
                state: state.clone(),
            },
        })
        .unwrap();
        let ref_envelope = serde_json::to_value(ExternalEnvelopeRef {
            version: EXTERNAL_PROTOCOL_VERSION,
            setup: Default::default(),
            request: ExternalRequestRef::Read {
                schema: &schema,
                types: &types,
                state,
            },
        })
        .unwrap();
        assert_eq!(owned_envelope, ref_envelope);
    }

    #[test]
    fn request_method_names_the_serde_tag() {
        // the error context an unreadable result is reported under comes from
        // method(), so it must keep saying what the request said.
        let schema = Schema::default();
        let types = Vec::new();
        let ops = Vec::new();
        let state = StateData::default();
        let requests = [
            ExternalRequestRef::Read {
                schema: &schema,
                types: &types,
                state: state.clone(),
            },
            ExternalRequestRef::Write {
                schema: &schema,
                ops: &ops,
                state: state.clone(),
            },
            ExternalRequestRef::EnsureSchema { schema: &schema },
            ExternalRequestRef::PreviewSchema { schema: &schema },
            ExternalRequestRef::Capabilities,
        ];
        for request in requests {
            let value = serde_json::to_value(&request).unwrap();
            assert_eq!(value["method"], request.method(), "{value}");
        }
    }

    #[test]
    fn external_object_defaults_omitted_attrs() {
        // a key-only object has nothing to put in attrs, and a non-Rust adapter
        // answers that by leaving the key out.
        let object: ExternalObject =
            serde_json::from_value(json!({"type_name": "dcim.site", "key": {"site": "fra1"}}))
                .unwrap();
        assert!(object.attrs.is_empty());
        assert!(object.backend_id.is_none());
    }

    #[derive(Debug, Default)]
    struct TestExternalAdapter {
        pub x: i64,
    }

    impl ExternalAdapter for TestExternalAdapter {
        fn setup(&mut self, configuration: &Value) -> anyhow::Result<()> {
            if configuration
                .get("fail_setup")
                .and_then(serde_yaml::Value::as_bool)
                == Some(true)
            {
                anyhow::bail!("rejected by test adapter");
            }
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

        fn preview_schema(&mut self, schema: &Schema) -> anyhow::Result<Option<ProvisionReport>> {
            // read-only mirror of ensure_schema: report the same fields, provision none.
            Ok(Some(ProvisionReport {
                created_fields: schema.types.keys().cloned().collect(),
                ..Default::default()
            }))
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
    fn external_adapter_preview_schema_roundtrip() {
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
        let request = ExternalRequest::PreviewSchema {
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

        let response: ExternalResponse<Option<ProvisionReport>> =
            serde_json::from_str(&response).unwrap();
        assert!(response.ok);
        // preview returned Some(report) with the same fields ensure_schema would create.
        assert_eq!(
            response.result.flatten().unwrap().created_fields,
            vec!["a".to_string(), "b".to_string()]
        );

        t.join().unwrap();
    }

    #[test]
    fn preview_schema_none_roundtrips_as_null_result() {
        // the honesty-critical case: an adapter that cannot preview returns Ok(None),
        // which must survive the wire as an explicit null result (not a missing one)
        // so the host reads it back as None, never as an empty "no schema changes".
        let response: ExternalResponse<Option<ProvisionReport>> =
            ExternalResponse::from_result(Ok(None));
        let wire = serde_json::to_value(&response).unwrap();
        assert_eq!(wire, json!({"ok": true, "result": null}));
        let back: ExternalResponse<Option<ProvisionReport>> = serde_json::from_value(wire).unwrap();
        assert!(back.ok);
        assert!(back.result.flatten().is_none());
    }

    #[test]
    fn external_adapter_communication_error() {
        let adapter = TestExternalAdapter::default();

        let (in_reader, mut in_writer) = std::io::pipe().unwrap();
        let (out_reader, out_writer) = std::io::pipe().unwrap();

        let t = std::thread::spawn(move || {
            assert!(run_external_adapter(adapter, (in_reader, out_writer)).is_ok());
        });

        // the 'Write' request is booby trapped on TestExternalAdapter
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
    fn external_adapter_rejects_invalid_request() {
        let adapter = TestExternalAdapter::default();

        let (in_reader, mut in_writer) = std::io::pipe().unwrap();
        let (out_reader, out_writer) = std::io::pipe().unwrap();

        let t = std::thread::spawn(move || {
            assert!(run_external_adapter(adapter, (in_reader, out_writer)).is_ok());
        });

        writeln!(in_writer, "this is not json").unwrap();
        drop(in_writer);

        let mut response = String::new();
        BufReader::new(out_reader).read_line(&mut response).unwrap();

        let response: ExternalResponse<serde_json::Value> =
            serde_json::from_str(&response).unwrap();
        assert!(!response.ok);
        assert!(response.error.unwrap().contains("invalid request"));

        t.join().unwrap();
    }

    #[test]
    fn external_adapter_rejects_invalid_setup() {
        let adapter = TestExternalAdapter::default();

        let (in_reader, mut in_writer) = std::io::pipe().unwrap();
        let (out_reader, out_writer) = std::io::pipe().unwrap();

        let t = std::thread::spawn(move || {
            assert!(run_external_adapter(adapter, (in_reader, out_writer)).is_ok());
        });

        let envelope = ExternalEnvelope {
            version: EXTERNAL_PROTOCOL_VERSION,
            setup: serde_yaml::from_str("fail_setup: true").unwrap(),
            request: ExternalRequest::Read {
                schema: Default::default(),
                types: vec![],
                state: Default::default(),
            },
        };

        writeln!(in_writer, "{}", serde_json::to_string(&envelope).unwrap()).unwrap();
        drop(in_writer);

        let mut response = String::new();
        BufReader::new(out_reader).read_line(&mut response).unwrap();

        let response: ExternalResponse<Vec<ExternalObject>> =
            serde_json::from_str(&response).unwrap();
        assert!(!response.ok);
        assert!(response.error.unwrap().contains("invalid setup"));

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

    #[test]
    fn capabilities_role_wire_shape() {
        // the wire contract for the capabilities result: a lowercase role string,
        // defaulting to the full read+write role when omitted.
        let value = serde_json::to_value(ExternalCapabilities {
            role: ExternalRole::Emitter,
        })
        .unwrap();
        assert_eq!(value, json!({"role": "emitter"}));

        let empty: ExternalCapabilities = serde_json::from_value(json!({})).unwrap();
        assert_eq!(empty.role, ExternalRole::Adapter);

        // an unknown role is a deserialization error; the host maps it to the
        // default role rather than failing construction.
        assert!(
            serde_json::from_value::<ExternalCapabilities>(json!({"role": "frobnicator"})).is_err()
        );
    }

    fn capabilities_over_stdio<A: ExternalAdapter + Send + 'static>(
        adapter: A,
    ) -> ExternalResponse<ExternalCapabilities> {
        let (in_reader, mut in_writer) = std::io::pipe().unwrap();
        let (out_reader, out_writer) = std::io::pipe().unwrap();

        let t = std::thread::spawn(move || {
            assert!(run_external_adapter(adapter, (in_reader, out_writer)).is_ok());
        });

        let envelope = ExternalEnvelope {
            version: EXTERNAL_PROTOCOL_VERSION,
            setup: Default::default(),
            request: ExternalRequest::Capabilities,
        };
        writeln!(in_writer, "{}", serde_json::to_string(&envelope).unwrap()).unwrap();
        drop(in_writer);

        let mut response = String::new();
        BufReader::new(out_reader).read_line(&mut response).unwrap();
        t.join().unwrap();
        serde_json::from_str(&response).unwrap()
    }

    #[test]
    fn capabilities_defaults_to_the_adapter_role() {
        // TestExternalAdapter does not override capabilities, so the sdk answers
        // for it with the full read+write role.
        let response = capabilities_over_stdio(TestExternalAdapter::default());
        assert!(response.ok);
        assert_eq!(response.result.unwrap().role, ExternalRole::Adapter);
    }

    #[test]
    fn capabilities_override_reports_the_emitter_role() {
        // the one-line override an emit-only adapter ships.
        #[derive(Default)]
        struct EmitOnly;
        impl ExternalAdapter for EmitOnly {
            fn setup(&mut self, _configuration: &Value) -> anyhow::Result<()> {
                Ok(())
            }
            fn read(
                &mut self,
                _schema: &Schema,
                _types: &[TypeName],
                _state: &StateData,
            ) -> anyhow::Result<Vec<ExternalObject>> {
                Err(anyhow::anyhow!("read is not supported"))
            }
            fn write(
                &mut self,
                _schema: &Schema,
                _ops: &[Op],
                _state: &StateData,
            ) -> anyhow::Result<ApplyReport> {
                Ok(ApplyReport::default())
            }
            fn capabilities(&mut self) -> ExternalCapabilities {
                ExternalCapabilities {
                    role: ExternalRole::Emitter,
                }
            }
        }

        let response = capabilities_over_stdio(EmitOnly);
        assert!(response.ok);
        assert_eq!(response.result.unwrap().role, ExternalRole::Emitter);
    }
}
