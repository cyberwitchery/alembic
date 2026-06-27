//! conformance harness for the external adapter protocol.
//!
//! adapters run as separate processes, so conformance is about what a process
//! writes: `check_protocol` runs an adapter command for each fixture in
//! `fixtures/external_protocol/` and checks the boundary — one json line on
//! stdout, a valid response envelope, a payload that fits the method. those are
//! the mistakes a fresh implementation makes that an in-process type check never
//! sees: a log line on stdout, extra output, a crash.

use alembic_core::{Schema, TypeName};
use alembic_engine::{
    run_external_adapter, AppliedOp, ApplyReport, BackendId, ExternalAdapter, ExternalObject,
    ExternalResponse, Op, ProvisionReport, StateData,
};
use serde_json::{json, Value};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

struct Fixture {
    name: String,
    method: String,
    request: Value,
    response: Value,
}

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/external_protocol")
}

fn load_fixtures() -> Vec<Fixture> {
    let dir = fixtures_dir();
    let mut fixtures = Vec::new();
    for entry in fs::read_dir(&dir).expect("read fixtures dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let name = path.file_stem().unwrap().to_string_lossy().into_owned();
        let value: Value = serde_json::from_str(&fs::read_to_string(&path).expect("read fixture"))
            .unwrap_or_else(|e| panic!("{name}: fixture is not valid json: {e}"));
        let method = value["request"]["method"]
            .as_str()
            .unwrap_or_else(|| panic!("{name}: fixture request has no method"))
            .to_string();
        fixtures.push(Fixture {
            name,
            method,
            request: value["request"].clone(),
            response: value["response"].clone(),
        });
    }
    assert!(!fixtures.is_empty(), "no fixtures in {}", dir.display());
    fixtures
}

fn read_fixture() -> Fixture {
    load_fixtures()
        .into_iter()
        .find(|f| f.name == "read")
        .expect("read fixture")
}

/// what an adapter process produced for one request.
struct AdapterOutput {
    status_ok: bool,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

/// verify one exchange against the protocol, returning the parsed response or the
/// first violation. language-neutral: it only inspects the bytes the adapter wrote.
fn check_protocol(method: &str, out: &AdapterOutput) -> Result<Value, String> {
    if !out.status_ok {
        return Err(format!(
            "adapter exited unsuccessfully: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    let text = std::str::from_utf8(&out.stdout).map_err(|e| format!("stdout is not utf-8: {e}"))?;
    let line = text.strip_suffix('\n').unwrap_or(text);
    if line.is_empty() {
        return Err("adapter wrote nothing to stdout".into());
    }
    if line.contains('\n') {
        return Err("adapter wrote more than one line to stdout".into());
    }
    let value: Value =
        serde_json::from_str(line).map_err(|e| format!("stdout is not one json line: {e}"))?;
    let response: ExternalResponse<Value> = serde_json::from_value(value.clone())
        .map_err(|e| format!("not a response envelope: {e}"))?;
    match (response.ok, response.result, response.error) {
        (true, Some(result), None) => check_payload(method, result)?,
        (false, None, Some(_)) => {}
        (ok, result, error) => {
            return Err(format!(
                "inconsistent envelope: ok={ok}, has_result={}, has_error={}",
                result.is_some(),
                error.is_some()
            ))
        }
    }
    Ok(value)
}

fn check_payload(method: &str, result: Value) -> Result<(), String> {
    match method {
        "read" => serde_json::from_value::<Vec<ExternalObject>>(result)
            .map(drop)
            .map_err(|e| format!("bad read result: {e}")),
        "write" => serde_json::from_value::<ApplyReport>(result)
            .map(drop)
            .map_err(|e| format!("bad write result: {e}")),
        "ensure_schema" => serde_json::from_value::<ProvisionReport>(result)
            .map(drop)
            .map_err(|e| format!("bad ensure_schema result: {e}")),
        other => Err(format!("unknown method {other}")),
    }
}

/// run an adapter command for one request and capture what it produced.
fn run_adapter(mut command: Command, request: &Value) -> AdapterOutput {
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn adapter");
    let input = serde_json::to_vec(request).expect("serialize request");
    let mut stdin = child.stdin.take().expect("adapter stdin");
    // an adapter that crashes early may close stdin before we finish writing; that
    // surfaces as the status/stdout failure we are checking for, so don't panic.
    let _ = stdin.write_all(&input);
    drop(stdin);
    let out = child.wait_with_output().expect("wait for adapter");
    AdapterOutput {
        status_ok: out.status.success(),
        stdout: out.stdout,
        stderr: out.stderr,
    }
}

/// a stand-in adapter: `sh -c <script>`, language-neutral on purpose.
fn sh(script: &str) -> Command {
    let mut command = Command::new("sh");
    command.arg("-c").arg(script);
    command
}

/// `sh` with `$RESP` bound to `response` serialized as one line.
fn sh_resp(script: &str, response: &Value) -> Command {
    let mut command = sh(script);
    command.env(
        "RESP",
        serde_json::to_string(response).expect("serialize response"),
    );
    command
}

/// the canonical rust adapter the fixtures describe: reads one site, applies
/// create ops, provisions a site type, and rejects anything else.
struct ReferenceAdapter;

impl ExternalAdapter for ReferenceAdapter {
    fn setup(&mut self, _configuration: &serde_yaml::Value) -> anyhow::Result<()> {
        Ok(())
    }

    fn read(
        &mut self,
        _schema: &Schema,
        _types: &[TypeName],
        _state: &StateData,
    ) -> anyhow::Result<Vec<ExternalObject>> {
        let site = serde_json::from_value(json!({
            "type_name": "dcim.site",
            "key": { "site": "fra1" },
            "attrs": { "name": "FRA1", "slug": "fra1" },
            "backend_id": 1
        }))?;
        Ok(vec![site])
    }

    fn write(
        &mut self,
        _schema: &Schema,
        ops: &[Op],
        _state: &StateData,
    ) -> anyhow::Result<ApplyReport> {
        let mut applied = Vec::new();
        for op in ops {
            match op {
                Op::Create { uid, type_name, .. } => applied.push(AppliedOp {
                    uid: *uid,
                    type_name: type_name.clone(),
                    backend_id: Some(BackendId::Int(1)),
                }),
                Op::Update { .. } => anyhow::bail!("update is not supported"),
                Op::Delete { .. } => anyhow::bail!("delete is not supported"),
            }
        }
        Ok(ApplyReport {
            applied,
            ..Default::default()
        })
    }

    fn ensure_schema(&mut self, _schema: &Schema) -> anyhow::Result<ProvisionReport> {
        Ok(ProvisionReport {
            created_object_types: vec!["dcim.site".to_string()],
            created_object_fields: vec!["dcim.site.name".to_string()],
            ..Default::default()
        })
    }
}

/// drive the reference adapter in-process, shaped like a subprocess result.
fn run_reference(request: &Value) -> AdapterOutput {
    let input = serde_json::to_vec(request).expect("serialize request");
    let mut stdout = Vec::new();
    run_external_adapter(ReferenceAdapter, (input.as_slice(), &mut stdout)).expect("run reference");
    AdapterOutput {
        status_ok: true,
        stdout,
        stderr: Vec::new(),
    }
}

#[test]
fn reference_adapter_conforms_to_fixtures() {
    for fixture in load_fixtures() {
        let out = run_reference(&fixture.request);
        let response = check_protocol(&fixture.method, &out)
            .unwrap_or_else(|e| panic!("{}: {e}", fixture.name));
        assert_eq!(
            response, fixture.response,
            "{}: response did not match the fixture",
            fixture.name
        );
    }
}

#[test]
fn harness_accepts_a_conforming_subprocess() {
    for fixture in load_fixtures() {
        let out = run_adapter(
            sh_resp(
                "cat >/dev/null; printf '%s\\n' \"$RESP\"",
                &fixture.response,
            ),
            &fixture.request,
        );
        check_protocol(&fixture.method, &out).unwrap_or_else(|e| panic!("{}: {e}", fixture.name));
    }
}

#[test]
fn harness_rejects_a_log_line_on_stdout() {
    let fixture = read_fixture();
    let out = run_adapter(
        sh_resp(
            "printf 'connecting...\\n'; cat >/dev/null; printf '%s\\n' \"$RESP\"",
            &fixture.response,
        ),
        &fixture.request,
    );
    let err = check_protocol(&fixture.method, &out).unwrap_err();
    assert!(
        err.contains("more than one line"),
        "unexpected error: {err}"
    );
}

#[test]
fn harness_rejects_non_json_stdout() {
    let fixture = read_fixture();
    let out = run_adapter(sh("cat >/dev/null; printf 'not json\\n'"), &fixture.request);
    assert!(check_protocol(&fixture.method, &out).is_err());
}

#[test]
fn harness_rejects_empty_stdout() {
    let fixture = read_fixture();
    let out = run_adapter(sh("cat >/dev/null"), &fixture.request);
    assert!(check_protocol(&fixture.method, &out).is_err());
}

#[test]
fn harness_rejects_a_crash() {
    let fixture = read_fixture();
    let out = run_adapter(sh("exit 1"), &fixture.request);
    assert!(check_protocol(&fixture.method, &out).is_err());
}

#[test]
fn harness_rejects_a_payload_that_does_not_fit_the_method() {
    let fixture = read_fixture();
    let out = run_adapter(
        sh("cat >/dev/null; printf '%s\\n' '{\"ok\":true,\"result\":\"nope\"}'"),
        &fixture.request,
    );
    assert!(check_protocol(&fixture.method, &out).is_err());
}
