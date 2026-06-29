//! conformance checks for external adapter executables.

use alembic_engine::{
    ApplyReport, ExternalObject, ExternalResponse, ProvisionReport, EXTERNAL_PROTOCOL_VERSION,
};
use anyhow::Context;
use serde::Deserialize;
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
}

impl Outcome {
    /// whether the check passed.
    pub fn passed(&self) -> bool {
        self.failure.is_none()
    }
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
pub struct Case {
    pub name: String,
    pub request: Value,
    pub expect: Expect,
}

/// the expectation for a case; `result`/`error` are optional.
#[derive(Debug, Clone, Deserialize)]
pub struct Expect {
    pub ok: bool,
    pub result: Option<Value>,
    pub error: Option<String>,
}

/// run the backend-independent protocol checks against the adapter command.
pub fn run_builtin(adapter: &[String], timeout: Duration) -> Vec<Outcome> {
    // requests use the version the engine sends, so the suite tracks the protocol it tests.
    let version = EXTERNAL_PROTOCOL_VERSION;
    let read_request = json!({
        "version": version,
        "setup": {},
        "method": "read",
        "schema": { "types": {} },
        "types": [],
        "state": {}
    });
    vec![
        check(
            adapter,
            timeout,
            "protocol/invalid-json",
            b"not json",
            "read",
            Expectation::MustError,
        ),
        check(
            adapter,
            timeout,
            "protocol/version-mismatch",
            &request_bytes(&json!({
                "version": version + 1,
                "setup": {},
                "method": "read",
                "schema": { "types": {} },
                "types": [],
                "state": {}
            })),
            "read",
            Expectation::MustError,
        ),
        check(
            adapter,
            timeout,
            "protocol/unknown-method",
            &request_bytes(&json!({ "version": version, "setup": {}, "method": "frobnicate" })),
            "read",
            Expectation::MustError,
        ),
        check(
            adapter,
            timeout,
            "protocol/read-empty",
            &request_bytes(&read_request),
            "read",
            Expectation::MustSucceed,
        ),
    ]
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
    }
}

/// validate what the adapter wrote against the protocol and the expectation.
fn validate(run: &RunResult, method: &str, expectation: &Expectation) -> Result<(), String> {
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

    let response: ExternalResponse<Value> =
        serde_json::from_value(value).map_err(|e| format!("not a response envelope: {e}"))?;

    let consistent = match (response.ok, &response.result, &response.error) {
        (true, Some(_), None) => true,
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
                (None, None) => {}
            }
        }
    }
    Ok(())
}

/// deserialize a success payload into the type its method requires.
fn check_payload(method: &str, result: &Value) -> Result<(), String> {
    match method {
        "read" => serde_json::from_value::<Vec<ExternalObject>>(result.clone())
            .map(drop)
            .map_err(|e| format!("bad read result: {e}")),
        "write" => serde_json::from_value::<ApplyReport>(result.clone())
            .map(drop)
            .map_err(|e| format!("bad write result: {e}")),
        "ensure_schema" => serde_json::from_value::<ProvisionReport>(result.clone())
            .map(drop)
            .map_err(|e| format!("bad ensure_schema result: {e}")),
        other => Err(format!("unknown method {other}")),
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

    if let Some(mut pipe) = child.stdin.take() {
        // an early-crashing adapter may close stdin before we finish writing; that
        // surfaces as the status/stdout failure being tested, so ignore the error.
        let _ = pipe.write_all(stdin);
    }
    let stdout = child.stdout.take().map(drain);
    let stderr = child.stderr.take().map(drain);

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
