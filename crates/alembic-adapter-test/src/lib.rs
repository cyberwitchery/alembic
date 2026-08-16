//! conformance checks for external adapter executables.

use alembic_core::TypeName;
use alembic_engine::{
    AppliedOp, ApplyReport, BackendId, ExternalCapabilities, ExternalObject, ExternalResponse,
    ExternalRole, ProvisionReport, EXTERNAL_PROTOCOL_VERSION,
};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::path::Path;
use std::process::{Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

/// the result of one conformance check; `failure` is `None` when it passed.
#[derive(Debug, Clone)]
pub struct Outcome {
    pub name: String,
    pub failure: Option<Failure>,
    /// why the check did not run. a skipped check certifies nothing, so it is
    /// neither a pass nor a failure and the report counts it apart from both.
    pub skipped: Option<String>,
}

impl Outcome {
    /// whether the check ran and passed.
    pub fn passed(&self) -> bool {
        self.failure.is_none() && self.skipped.is_none()
    }

    /// whether the check was not sent rather than run.
    pub fn skipped(&self) -> bool {
        self.skipped.is_some()
    }
}

/// which built-in checks to run. the writing ones are off by default: a
/// conformance run is not asked to touch the operator's backend.
#[derive(Debug, Clone, Copy, Default)]
pub struct Builtins {
    /// `protocol/write-empty` and `protocol/ensure-schema-empty` both write, at
    /// the adapter's own default target.
    pub writes: bool,
}

/// why a check failed, with the diagnostics needed to debug the adapter.
#[derive(Debug, Clone)]
pub struct Failure {
    pub message: String,
    pub status: String,
    pub stdout: String,
    pub stderr: String,
}

/// an adapter-specific test case: a full request and what to expect back.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Case {
    pub name: String,
    pub request: Value,
    pub expect: Expect,
}

/// the expectation for a case; `result`/`error` are optional. a stray key is
/// rejected: a typo there silently removes the assertion the case exists for.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Expect {
    pub ok: bool,
    pub result: Option<Value>,
    pub error: Option<String>,
}

/// run the backend-independent protocol checks that do not write. see [`Builtins`].
pub fn run_builtin(adapter: &[String], timeout: Duration) -> Vec<Outcome> {
    run_builtin_with(adapter, timeout, Builtins::default())
}

/// the built-ins, with the writing ones selectable. see [`Builtins`].
pub fn run_builtin_with(adapter: &[String], timeout: Duration, builtins: Builtins) -> Vec<Outcome> {
    // requests use the version the engine sends, so the suite tracks the protocol it tests.
    let version = EXTERNAL_PROTOCOL_VERSION;
    // the declared role decides which liveness checks run below, so probe it first.
    let (role, capabilities) = probe_capabilities(adapter, timeout);
    let read_empty = (
        "protocol/read-empty",
        "read",
        json!({
            "version": version,
            "setup": {},
            "method": "read",
            "schema": { "types": {} },
            "types": [],
            "state": {}
        }),
    );
    let write_empty = (
        "protocol/write-empty",
        "write",
        json!({
            "version": version,
            "setup": {},
            "method": "write",
            "schema": { "types": {} },
            "ops": [],
            "state": {}
        }),
    );
    let preview_empty = (
        "protocol/preview-schema-empty",
        "preview_schema",
        json!({
            "version": version,
            "setup": {},
            "method": "preview_schema",
            "schema": { "types": {} }
        }),
    );
    // the version probe rides a method the role implements that writes nothing:
    // an emitter refuses a read for role reasons, answering without reading `version`.
    let (_, probe_method, probe_request) = match role {
        ExternalRole::Emitter => &preview_empty,
        ExternalRole::Observer | ExternalRole::Adapter => &read_empty,
    };
    let mut mismatched = probe_request.clone();
    mismatched["version"] = json!(version + 1);
    let version_mismatch = check(
        adapter,
        timeout,
        "protocol/version-mismatch",
        &request_bytes(&mismatched),
        probe_method,
        Expectation::MustError,
    );
    // the liveness checks are the methods the host sends this role: a read for an
    // observer, a write for an emitter, both for a full read+write adapter.
    let liveness = match role {
        ExternalRole::Emitter => vec![write_empty],
        ExternalRole::Observer => vec![read_empty],
        ExternalRole::Adapter => vec![read_empty, write_empty],
    };
    let mut outcomes = vec![
        check(
            adapter,
            timeout,
            "protocol/invalid-json",
            b"not json",
            "read",
            Expectation::MustError,
        ),
        version_mismatch,
        check(
            adapter,
            timeout,
            "protocol/unknown-method",
            &request_bytes(&json!({ "version": version, "setup": {}, "method": "frobnicate" })),
            "read",
            Expectation::MustError,
        ),
        capabilities,
    ];
    // a check is gated because it writes, not because of the role it was picked
    // for: the empty write rides the same gate as the provisioning one, the empty
    // read writes nothing and never does.
    outcomes.extend(liveness.iter().map(|(name, method, request)| {
        if builtins.writes || *method != "write" {
            check(
                adapter,
                timeout,
                name,
                &request_bytes(request),
                method,
                Expectation::MustSucceed,
            )
        } else {
            skipped(name, WRITES_ARE_OPT_IN)
        }
    }));
    // previewing is provisioning, so the host reaches both methods through an
    // emitter and both checks follow the declared role, as the liveness probe does.
    if matches!(role, ExternalRole::Emitter | ExternalRole::Adapter) {
        let (name, method, request) = &preview_empty;
        outcomes.push(check(
            adapter,
            timeout,
            name,
            &request_bytes(request),
            method,
            // a conformant adapter answers preview_schema, either with a report or
            // a null result ("cannot preview"); both count as success.
            Expectation::MustSucceed,
        ));
        outcomes.push(if builtins.writes {
            check(
                adapter,
                timeout,
                "protocol/ensure-schema-empty",
                &request_bytes(&json!({
                    "version": version,
                    "setup": {},
                    "method": "ensure_schema",
                    "schema": { "types": {} }
                })),
                "ensure_schema",
                Expectation::MustSucceed,
            )
        } else {
            skipped("protocol/ensure-schema-empty", WRITES_ARE_OPT_IN)
        });
    }
    outcomes
}

const WRITES_ARE_OPT_IN: &str = "writes are opt-in, pass --write-checks";

/// a check the runner did not send, reported rather than omitted: a reader counting
/// passes must not read a suite that never sent the request as one that certified it.
fn skipped(name: &str, reason: &str) -> Outcome {
    Outcome {
        name: name.to_string(),
        failure: None,
        skipped: Some(reason.to_string()),
    }
}

/// probe the adapter's declared role with a capabilities request. answering with
/// a structured error (an adapter predating the method) is conformant and means
/// the default read+write role, exactly as the registry defaults at construction;
/// only a broken exchange fails the check.
fn probe_capabilities(adapter: &[String], timeout: Duration) -> (ExternalRole, Outcome) {
    let request = request_bytes(&json!({
        "version": EXTERNAL_PROTOCOL_VERSION,
        "setup": {},
        "method": "capabilities"
    }));
    let run = run_once(adapter, timeout, &request);
    let judged = parse_response(&run).and_then(|response| {
        match (response.ok, response.result, response.error) {
            // key-checked like the rest: a misspelled `role` would default the
            // adapter to read+write and report the fault as the `read` it then fails.
            (true, Some(result), None) => check_payload("capabilities", &result).and_then(|()| {
                serde_json::from_value::<ExternalCapabilities>(result)
                    .map(|capabilities| capabilities.role)
                    .map_err(|e| format!("bad capabilities result: {e}"))
            }),
            (false, None, Some(error)) => {
                if error.is_empty() {
                    Err("inconsistent envelope: error message is empty".to_string())
                } else {
                    Ok(ExternalRole::default())
                }
            }
            (ok, result, error) => Err(format!(
                "inconsistent envelope: ok={}, has_result={}, has_error={}",
                ok,
                result.is_some(),
                error.is_some()
            )),
        }
    });
    let (role, failure) = match judged {
        Ok(role) => (role, None),
        Err(message) => (
            ExternalRole::default(),
            Some(Failure {
                message,
                status: status_label(&run),
                stdout: String::from_utf8_lossy(&run.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&run.stderr).into_owned(),
            }),
        ),
    };
    (
        role,
        Outcome {
            name: "protocol/capabilities".to_string(),
            failure,
            skipped: None,
        },
    )
}

/// run adapter-specific cases against the adapter command.
pub fn run_cases(adapter: &[String], timeout: Duration, cases: &[Case]) -> Vec<Outcome> {
    cases
        .iter()
        .map(|case| {
            let method = case
                .request
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or("");
            check(
                adapter,
                timeout,
                &format!("case/{}", case.name),
                &request_bytes(&case.request),
                method,
                Expectation::Case(&case.expect),
            )
        })
        .collect()
}

/// load cases from a `.json` file or a directory of `.json` files (sorted).
pub fn load_cases(path: &Path) -> anyhow::Result<Vec<Case>> {
    let metadata =
        std::fs::metadata(path).with_context(|| format!("reading cases at {}", path.display()))?;
    let mut files = Vec::new();
    if metadata.is_dir() {
        for entry in
            std::fs::read_dir(path).with_context(|| format!("reading {}", path.display()))?
        {
            let entry = entry?;
            let file = entry.path();
            if file.extension().and_then(|e| e.to_str()) == Some("json") {
                files.push(file);
            }
        }
        files.sort();
    } else {
        files.push(path.to_path_buf());
    }
    let mut cases = Vec::new();
    for file in files {
        let text = std::fs::read_to_string(&file)
            .with_context(|| format!("reading {}", file.display()))?;
        let case: Case =
            serde_json::from_str(&text).with_context(|| format!("parsing {}", file.display()))?;
        cases.push(case);
    }
    Ok(cases)
}

/// how to judge a response.
enum Expectation<'a> {
    /// the adapter must reject the request with a structured error.
    MustError,
    /// a valid request the adapter must answer with ok=true and a right-shaped payload.
    MustSucceed,
    /// an adapter-specific expectation.
    Case(&'a Expect),
}

/// run one request and turn any violation into an `Outcome`.
fn check(
    adapter: &[String],
    timeout: Duration,
    name: &str,
    stdin: &[u8],
    method: &str,
    expectation: Expectation,
) -> Outcome {
    let run = run_once(adapter, timeout, stdin);
    let failure = validate(&run, method, &expectation)
        .err()
        .map(|message| Failure {
            message,
            status: status_label(&run),
            stdout: String::from_utf8_lossy(&run.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&run.stderr).into_owned(),
        });
    Outcome {
        name: name.to_string(),
        failure,
        skipped: None,
    }
}

/// check the process ran cleanly and parse its stdout into a response envelope.
fn parse_response(run: &RunResult) -> Result<ExternalResponse<Value>, String> {
    if run.timed_out {
        return Err("did not terminate within the timeout".to_string());
    }
    match run.status {
        Some(status) if status.success() => {}
        Some(status) => return Err(format!("adapter exited unsuccessfully ({status})")),
        None => return Err("adapter did not start".to_string()),
    }

    // exactly one json document: whitespace and multi-line json are fine, a trailing
    // log line is not.
    let mut de = serde_json::Deserializer::from_slice(&run.stdout);
    let value =
        Value::deserialize(&mut de).map_err(|e| format!("stdout is not one json document: {e}"))?;
    de.end()
        .map_err(|_| "stdout has trailing output after the json document".to_string())?;

    // [`check_payload`]'s rule at the outer level: `result` misspelled deserializes
    // to `None`, which for preview_schema reads as "cannot preview" and skips the
    // --allow-delete gate the payload check exists to defend.
    reject_unknown_keys(&as_template(response_envelope())?, &value, "")
        .map_err(|e| format!("bad response envelope: {e}"))?;

    serde_json::from_value(value).map_err(|e| format!("not a response envelope: {e}"))
}

/// validate what the adapter wrote against the protocol and the expectation.
fn validate(run: &RunResult, method: &str, expectation: &Expectation) -> Result<(), String> {
    let response = parse_response(run)?;

    let consistent = match (response.ok, &response.result, &response.error) {
        (true, Some(_), None) => true,
        // preview_schema may answer with a null/absent result to mean "cannot preview
        // schema"; the host's call_optional maps that to Ok(None), so a correct adapter
        // passes against the registry and must pass conformance too. this stays
        // preview_schema-only: read/write/ensure_schema go through call(), which
        // hard-errors on a null result, so a null there is a real failure to still catch.
        (true, None, None) if method == "preview_schema" => true,
        (false, None, Some(error)) => {
            if error.is_empty() {
                return Err("inconsistent envelope: error message is empty".to_string());
            }
            true
        }
        _ => false,
    };
    if !consistent {
        return Err(format!(
            "inconsistent envelope: ok={}, has_result={}, has_error={}",
            response.ok,
            response.result.is_some(),
            response.error.is_some()
        ));
    }

    match expectation {
        Expectation::MustError => {
            if response.ok {
                return Err("expected a structured error, got ok=true".to_string());
            }
        }
        Expectation::MustSucceed => {
            if let Some(error) = &response.error {
                return Err(format!(
                    "a valid request must succeed, but the adapter returned an error: {error}"
                ));
            }
            if let Some(result) = &response.result {
                check_payload(method, result)?;
            }
        }
        Expectation::Case(expect) => {
            if response.ok != expect.ok {
                return Err(format!("expected ok={}, got ok={}", expect.ok, response.ok));
            }
            match (&response.result, &response.error) {
                (Some(result), _) => {
                    check_payload(method, result)?;
                    if let Some(want) = &expect.result {
                        if result != want {
                            return Err(format!(
                                "result did not match: expected {want}, got {result}"
                            ));
                        }
                    }
                }
                (None, Some(error)) => {
                    if let Some(want) = &expect.error {
                        if error != want {
                            return Err(format!(
                                "error did not match: expected {want:?}, got {error:?}"
                            ));
                        }
                    }
                }
                (None, None) => {
                    if let Some(want) = &expect.result {
                        return Err(format!("result did not match: expected {want}, got null"));
                    }
                }
            }
        }
    }
    Ok(())
}

/// deserialize a success payload into the type its method requires, then reject a
/// key that type does not own: nearly every field defaults, so a misspelled one
/// deserializes to the default and reads as conformant. [`Expect`]'s rule, other channel.
fn check_payload(method: &str, result: &Value) -> Result<(), String> {
    let template = match method {
        "read" => {
            serde_json::from_value::<Vec<ExternalObject>>(result.clone())
                .map_err(|e| format!("bad read result: {e}"))?;
            as_template(vec![observed_object()])
        }
        "write" => {
            serde_json::from_value::<ApplyReport>(result.clone())
                .map_err(|e| format!("bad write result: {e}"))?;
            as_template(apply_report())
        }
        "ensure_schema" => {
            serde_json::from_value::<ProvisionReport>(result.clone())
                .map_err(|e| format!("bad ensure_schema result: {e}"))?;
            as_template(provision_report())
        }
        "preview_schema" => {
            serde_json::from_value::<Option<ProvisionReport>>(result.clone())
                .map_err(|e| format!("bad preview_schema result: {e}"))?;
            as_template(provision_report())
        }
        "capabilities" => {
            serde_json::from_value::<ExternalCapabilities>(result.clone())
                .map_err(|e| format!("bad capabilities result: {e}"))?;
            as_template(ExternalCapabilities {
                role: ExternalRole::default(),
            })
        }
        other => return Err(format!("unknown method {other}")),
    }?;
    reject_unknown_keys(&template, result, "").map_err(|e| format!("bad {method} result: {e}"))
}

/// serialize a payload into the template [`reject_unknown_keys`] walks. strict
/// here only: the host takes an extra key as forward compatibility.
fn as_template<T: Serialize>(populated: T) -> Result<Value, String> {
    serde_json::to_value(populated).map_err(|e| format!("building the template: {e}"))
}

/// the templates. every literal is exhaustive and every field carries a value, so
/// a field added to one of these types stops the build here rather than defaulting
/// past `skip_serializing_if` into a key the runner would then reject.
///
/// `result` is null, so [`check_payload`] stays the one owner of what is inside it.
fn response_envelope() -> ExternalResponse<Value> {
    ExternalResponse {
        ok: true,
        result: Some(Value::Null),
        error: Some(String::new()),
    }
}

fn provision_report() -> ProvisionReport {
    let one = || vec![String::new()];
    ProvisionReport {
        created_fields: one(),
        updated_fields: one(),
        created_tags: one(),
        created_object_types: one(),
        created_object_fields: one(),
        updated_object_fields: one(),
        deprecated_object_types: one(),
        deprecated_object_fields: one(),
        deleted_object_types: one(),
        deleted_object_fields: one(),
    }
}

fn apply_report() -> ApplyReport {
    ApplyReport {
        applied: vec![applied_op()],
        resumed: vec![applied_op()],
        previously_applied_count: Some(0),
        provision: provision_report(),
    }
}

fn applied_op() -> AppliedOp {
    AppliedOp {
        uid: Default::default(),
        type_name: TypeName::new(""),
        backend_id: Some(BackendId::Int(0)),
    }
}

/// `key` and `attrs` stay empty on purpose: they are the adapter's own maps, and
/// an empty template object is open.
fn observed_object() -> ExternalObject {
    ExternalObject {
        type_name: TypeName::new(""),
        key: Default::default(),
        attrs: Default::default(),
        backend_id: Some(BackendId::Int(0)),
    }
}

/// reject a key the payload's type does not own, walking template and payload
/// together. an empty template object is open: it stands for a free-form map.
fn reject_unknown_keys(template: &Value, payload: &Value, path: &str) -> Result<(), String> {
    match (template, payload) {
        (Value::Object(known), Value::Object(got)) if !known.is_empty() => {
            for (key, value) in got {
                let path = at(path, key);
                let nested = known
                    .get(key)
                    .ok_or_else(|| format!("unknown key {path}"))?;
                reject_unknown_keys(nested, value, &path)?;
            }
            Ok(())
        }
        // one template element stands for every payload element.
        (Value::Array(known), Value::Array(got)) => match known.first() {
            Some(element) => got.iter().enumerate().try_for_each(|(index, value)| {
                reject_unknown_keys(element, value, &format!("{path}[{index}]"))
            }),
            None => Ok(()),
        },
        _ => Ok(()),
    }
}

/// where a key sits in the payload, for the failure message.
fn at(path: &str, key: &str) -> String {
    if path.is_empty() {
        key.to_string()
    } else {
        format!("{path}.{key}")
    }
}

/// what the adapter process produced for one request.
struct RunResult {
    timed_out: bool,
    status: Option<ExitStatus>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

/// a human-readable process status for failure diagnostics.
fn status_label(run: &RunResult) -> String {
    if run.timed_out {
        "timed out".to_string()
    } else {
        match run.status {
            Some(status) => status.to_string(),
            None => "did not start".to_string(),
        }
    }
}

/// serialize a request value to bytes; `Value`'s display never fails.
fn request_bytes(value: &Value) -> Vec<u8> {
    value.to_string().into_bytes()
}

/// spawn the adapter, feed it one request, and capture what it wrote.
fn run_once(adapter: &[String], timeout: Duration, stdin: &[u8]) -> RunResult {
    let mut child = match Command::new(&adapter[0])
        .args(&adapter[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => {
            return RunResult {
                timed_out: false,
                status: None,
                stdout: Vec::new(),
                stderr: format!("could not start adapter: {e}").into_bytes(),
            }
        }
    };

    let stdout = child.stdout.take().map(drain);
    let stderr = child.stderr.take().map(drain);
    if let Some(pipe) = child.stdin.take() {
        feed(pipe, stdin.to_vec());
    }

    let deadline = Instant::now() + timeout;
    let mut timed_out = false;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    timed_out = true;
                    break None;
                }
                thread::sleep(Duration::from_millis(20));
            }
            Err(_) => break None,
        }
    };

    // the drains run concurrently, so wait for both against one shared deadline
    // rather than spending a full grace on each in series.
    let drain_deadline = Instant::now() + DRAIN_GRACE;
    RunResult {
        timed_out,
        status,
        stdout: stdout
            .map(|rx| collect(rx, drain_deadline))
            .unwrap_or_default(),
        stderr: stderr
            .map(|rx| collect(rx, drain_deadline))
            .unwrap_or_default(),
    }
}

/// write the request on its own thread so a non-reading adapter can't block the runner.
fn feed<W: Write + Send + 'static>(mut pipe: W, bytes: Vec<u8>) {
    thread::spawn(move || {
        // an early-crashing adapter may close stdin before we finish writing; that
        // surfaces as the status/stdout failure being tested, so ignore the error.
        let _ = pipe.write_all(&bytes);
    });
}

/// how long to keep reading a pipe after the adapter has exited or been killed.
/// a pipe closes the instant its last writer exits, so a well-behaved adapter
/// drains far inside this; it only bites when a forked grandchild outlived the
/// adapter still holding the pipe open (see `collect`).
const DRAIN_GRACE: Duration = Duration::from_secs(1);

/// drain a pipe to end on its own thread so a chatty adapter can't deadlock,
/// handing the bytes back over a channel so the caller can stop waiting on it.
fn drain<R: Read + Send + 'static>(mut pipe: R) -> mpsc::Receiver<Vec<u8>> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = pipe.read_to_end(&mut buf);
        let _ = tx.send(buf);
    });
    rx
}

/// collect what a drain thread read, but give up at `deadline`. killing the
/// adapter only signals the process we spawned; a grandchild it forked can outlive
/// it still holding the pipe open, which would block `read_to_end` (and us) until
/// that grandchild exits on its own, defeating the timeout. capping the wait keeps
/// the runner's wall-clock bounded by the timeout; anything unread reads as empty.
fn collect(rx: mpsc::Receiver<Vec<u8>>, deadline: Instant) -> Vec<u8> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    rx.recv_timeout(remaining).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{load_cases, Case, Expect};
    use std::path::Path;

    fn examples() -> &'static Path {
        Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/examples/cases"))
    }

    #[test]
    fn a_case_rejects_an_unknown_key() {
        let err = serde_json::from_str::<Case>(
            r#"{"name": "c", "requst": {}, "request": {}, "expect": {"ok": true}}"#,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("requst"), "{err}");
    }

    #[test]
    fn an_expectation_rejects_an_unknown_key() {
        // a typo'd `error`/`result` used to drop the assertion and report ok,
        // so the case certified the adapter having compared nothing.
        for typo in ["errror", "reslt"] {
            let err = serde_json::from_str::<Expect>(&format!(r#"{{"ok": false, "{typo}": "x"}}"#))
                .unwrap_err()
                .to_string();
            assert!(err.contains(typo), "{err}");
        }
    }

    #[test]
    fn an_expectation_still_accepts_ok_result_and_error() {
        let expect: Expect =
            serde_json::from_str(r#"{"ok": false, "result": null, "error": "boom"}"#).unwrap();
        assert!(!expect.ok);
        assert_eq!(expect.error.as_deref(), Some("boom"));
    }

    #[test]
    fn the_committed_fixtures_still_load() {
        let cases = load_cases(examples()).unwrap();
        assert_eq!(cases.len(), 6, "every committed fixture must still parse");
    }
}
