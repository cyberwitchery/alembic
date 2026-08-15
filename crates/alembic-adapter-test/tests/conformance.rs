use alembic_adapter_test::{
    load_cases, run_builtin, run_builtin_with, run_cases, Builtins, Case, Expect, Outcome,
};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};
use tempfile::tempdir;

const TIMEOUT: Duration = Duration::from_secs(10);

fn sh(script: &str) -> Vec<String> {
    vec!["sh".into(), "-c".into(), script.into()]
}

/// the built-ins with the writing checks asked for, now that they are opt-in.
fn run_builtin_writing(adapter: &[String]) -> Vec<Outcome> {
    run_builtin_with(adapter, TIMEOUT, Builtins { writes: true })
}

fn find<'a>(outcomes: &'a [Outcome], name: &str) -> &'a Outcome {
    outcomes
        .iter()
        .find(|o| o.name == name)
        .unwrap_or_else(|| panic!("no outcome named {name}"))
}

fn message(outcome: &Outcome) -> &str {
    &outcome.failure.as_ref().expect("a failure").message
}

fn python3_available() -> bool {
    Command::new("python3").arg("--version").output().is_ok()
}

fn python_adapter() -> Vec<String> {
    vec![
        "python3".into(),
        concat!(env!("CARGO_MANIFEST_DIR"), "/examples/adapter.py").into(),
    ]
}

#[test]
fn rejects_a_crash() {
    let outcomes = run_builtin(&sh("exit 1"), TIMEOUT);
    assert!(outcomes.iter().any(|o| !o.passed()));
    assert!(message(find(&outcomes, "protocol/read-empty")).contains("exited unsuccessfully"));
}

#[test]
fn rejects_a_timeout() {
    let outcomes = run_builtin(&sh("sleep 30"), Duration::from_millis(300));
    let failed = outcomes.iter().find(|o| !o.passed()).expect("a failure");
    assert!(message(failed).contains("terminate"), "{}", message(failed));
}

#[test]
fn forked_adapter_does_not_outlast_the_timeout() {
    // `sleep 30 & wait`: the shell backgrounds a sleep that inherits stdout, then
    // waits on it. killing the shell at the timeout orphans the sleep, which keeps
    // the pipe open. the runner must not block reading it until the orphan exits
    // (which would take ~30s a check); the bounded drain caps that. each check
    // (plus the capabilities probe) is ~timeout + a grace, so eight runs stay
    // under this bound.
    let start = Instant::now();
    let outcomes = run_builtin_with(
        &sh("sleep 30 & wait"),
        Duration::from_millis(300),
        Builtins { writes: true },
    );
    assert!(outcomes.iter().all(|o| !o.passed()));
    assert!(
        start.elapsed() < Duration::from_secs(15),
        "runner blocked on an orphaned grandchild for {:?}",
        start.elapsed()
    );
}

#[test]
fn oversized_request_does_not_outlast_the_timeout() {
    // an adapter that never reads its stdin must trip the timeout, not block the
    // runner in write_all: the request must exceed the pipe buffer to bind.
    let case = Case {
        name: "oversized".into(),
        request: json!({
            "version": 1,
            "setup": {},
            "method": "read",
            "schema": {"types": {}},
            "types": [],
            "state": {},
            "pad": "x".repeat(256 * 1024),
        }),
        expect: Expect {
            ok: true,
            result: None,
            error: None,
        },
    };
    let start = Instant::now();
    let outcomes = run_cases(
        &sh("sleep 30"),
        Duration::from_millis(300),
        std::slice::from_ref(&case),
    );
    assert!(outcomes.iter().all(|o| !o.passed()));
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "runner blocked writing the request for {:?}",
        start.elapsed()
    );
}

#[test]
fn rejects_non_json() {
    let outcomes = run_builtin(&sh("cat >/dev/null; printf 'not json\\n'"), TIMEOUT);
    let msg = message(find(&outcomes, "protocol/read-empty"));
    assert!(msg.contains("not one json document"), "{msg}");
}

#[test]
fn rejects_stdout_noise() {
    let outcomes = run_builtin(
        &sh(r#"cat >/dev/null; printf '{"ok":true,"result":[]}\nconnected\n'"#),
        TIMEOUT,
    );
    let msg = message(find(&outcomes, "protocol/read-empty"));
    assert!(msg.contains("trailing output"), "{msg}");
}

#[test]
fn rejects_inconsistent_envelope() {
    let outcomes = run_builtin(
        &sh(r#"cat >/dev/null; printf '{"ok":true,"error":"both"}\n'"#),
        TIMEOUT,
    );
    let msg = message(find(&outcomes, "protocol/read-empty"));
    assert!(msg.contains("inconsistent envelope"), "{msg}");
}

#[test]
fn preview_schema_may_return_null() {
    // a null result is the canonical "cannot preview schema" signal; the host's
    // call_optional maps it to Ok(None), so conformance must accept it too.
    let case = Case {
        name: "preview-null".into(),
        request: json!({
            "version": 1, "setup": {}, "method": "preview_schema",
            "schema": { "types": {} }
        }),
        expect: Expect {
            ok: true,
            result: None,
            error: None,
        },
    };
    let outcomes = run_cases(
        &sh(r#"cat >/dev/null; printf '{"ok":true,"result":null}\n'"#),
        TIMEOUT,
        std::slice::from_ref(&case),
    );
    assert!(
        outcomes[0].passed(),
        "preview_schema null result rejected: {:?}",
        outcomes[0].failure
    );
}

#[test]
fn read_null_result_still_rejected() {
    // the null-result allowance is preview_schema-only: read dispatches through the
    // host's call(), which hard-errors on a null result, so conformance must still
    // reject a null read. guards the arm against over-broadening.
    let case = Case {
        name: "read-null".into(),
        request: json!({
            "version": 1, "setup": {}, "method": "read",
            "schema": { "types": {} }, "types": [], "state": {}
        }),
        expect: Expect {
            ok: true,
            result: None,
            error: None,
        },
    };
    let outcomes = run_cases(
        &sh(r#"cat >/dev/null; printf '{"ok":true,"result":null}\n'"#),
        TIMEOUT,
        std::slice::from_ref(&case),
    );
    assert!(
        !outcomes[0].passed(),
        "a null read result must still be rejected"
    );
    assert!(
        message(&outcomes[0]).contains("inconsistent envelope"),
        "{}",
        message(&outcomes[0])
    );
}

#[test]
fn rejects_bad_payload() {
    let outcomes = run_builtin(
        &sh(r#"cat >/dev/null; printf '{"ok":true,"result":"nope"}\n'"#),
        TIMEOUT,
    );
    let msg = message(find(&outcomes, "protocol/read-empty"));
    assert!(msg.contains("bad read result"), "{msg}");
}

#[test]
fn rejects_an_always_erroring_adapter() {
    // an adapter that ignores its input and always returns a structured error
    // satisfies every must-error check, so the empty read has to demand success
    // for the runner to reject it.
    let outcomes = run_builtin(
        &sh(r#"cat >/dev/null; printf '{"ok":false,"error":"no"}\n'"#),
        TIMEOUT,
    );
    let read = find(&outcomes, "protocol/read-empty");
    assert!(
        !read.passed(),
        "the empty read must reject an always-erroring adapter"
    );
    assert!(message(read).contains("must succeed"), "{}", message(read));
    // the must-error checks still pass, so read-empty is the discriminating check.
    assert!(find(&outcomes, "protocol/invalid-json").passed());
    assert!(find(&outcomes, "protocol/version-mismatch").passed());
    assert!(find(&outcomes, "protocol/unknown-method").passed());
    // answering capabilities with a structured error is conformant (an adapter
    // predating the method); it just means the default read+write role, which is
    // exactly why the erroring read must still fail above.
    assert!(find(&outcomes, "protocol/capabilities").passed());
}

#[test]
fn declared_emitter_with_erroring_read_passes() {
    // the emit-only shape from issue #117: the adapter declares the emitter
    // role and errors on read. the runner skips the empty read for a declared
    // emitter and probes liveness with an empty write instead.
    // the version gate comes first, as the sdk's does: without it the emitter
    // would fail the version probe, which now rides the same write.
    let script = r#"req=$(cat)
    case "$req" in
      *'"version":1'*) ;;
      *) printf '{"ok":false,"error":"unsupported protocol version"}'; exit 0 ;;
    esac
    case "$req" in
      *'"method":"capabilities"'*) printf '{"ok":true,"result":{"role":"emitter"}}' ;;
      *'"method":"write"'*) printf '{"ok":true,"result":{"applied":[]}}' ;;
      *'"method":"ensure_schema"'*) printf '{"ok":true,"result":{}}' ;;
      *'"method":"preview_schema"'*) printf '{"ok":true,"result":null}' ;;
      *) printf '{"ok":false,"error":"read is not supported"}' ;;
    esac"#;
    let outcomes = run_builtin_writing(&sh(script));
    for outcome in &outcomes {
        assert!(
            outcome.passed(),
            "{} failed: {:?}",
            outcome.name,
            outcome.failure
        );
    }
    // the liveness check ran against a method the adapter claims to implement.
    assert!(outcomes.iter().any(|o| o.name == "protocol/write-empty"));
    assert!(!outcomes.iter().any(|o| o.name == "protocol/read-empty"));
}

#[test]
fn a_full_adapter_with_a_broken_write_is_caught() {
    // the default read+write role implements both, so it is sent both liveness
    // checks; an adapter answering read and garbling write fails the write.
    let script = r#"req=$(cat)
    case "$req" in
      *'"version":1'*) ;;
      *) printf '{"ok":false,"error":"unsupported protocol version"}'; exit 0 ;;
    esac
    case "$req" in
      *'"method":"capabilities"'*) printf '{"ok":true,"result":{"role":"adapter"}}' ;;
      *'"method":"write"'*) printf 'TOTAL GARBAGE, NOT EVEN JSON' ;;
      *'"method":"read"'*) printf '{"ok":true,"result":[]}' ;;
      *'"method":"ensure_schema"'*) printf '{"ok":true,"result":{}}' ;;
      *'"method":"preview_schema"'*) printf '{"ok":true,"result":null}' ;;
      *) printf '{"ok":false,"error":"unknown method"}' ;;
    esac"#;
    let outcomes = run_builtin_writing(&sh(script));
    let write = find(&outcomes, "protocol/write-empty");
    assert!(!write.passed(), "a full adapter's write must be checked");
    assert!(
        message(write).contains("not one json document"),
        "{}",
        message(write)
    );
    // the read it was already sent still runs and passes, so the failure is the
    // write rather than the suite going red on this adapter.
    assert!(find(&outcomes, "protocol/read-empty").passed());
    let failed: Vec<&str> = outcomes
        .iter()
        .filter(|o| !o.passed())
        .map(|o| o.name.as_str())
        .collect();
    assert_eq!(failed, ["protocol/write-empty"]);
}

#[test]
fn an_emitter_that_leaves_ensure_schema_to_unknown_method_fails() {
    // apply propagates ensure_schema for every emitter, so an adapter answering
    // every other method and dropping this one is certified into a failing run.
    let script = r#"req=$(cat)
    case "$req" in
      *'"version":1'*) ;;
      *) printf '{"ok":false,"error":"unsupported protocol version"}'; exit 0 ;;
    esac
    case "$req" in
      *'"method":"capabilities"'*) printf '{"ok":true,"result":{"role":"emitter"}}' ;;
      *'"method":"write"'*) printf '{"ok":true,"result":{"applied":[]}}' ;;
      *'"method":"preview_schema"'*) printf '{"ok":true,"result":null}' ;;
      *) printf '{"ok":false,"error":"unknown method"}' ;;
    esac"#;
    let outcomes = run_builtin_writing(&sh(script));
    let ensure = find(&outcomes, "protocol/ensure-schema-empty");
    assert!(
        message(ensure).contains("unknown method"),
        "{}",
        message(ensure)
    );
    // and only that check: the rest of the suite still certifies it.
    let failed: Vec<&str> = outcomes
        .iter()
        .filter(|o| !o.passed())
        .map(|o| o.name.as_str())
        .collect();
    assert_eq!(failed, ["protocol/ensure-schema-empty"]);
}

#[test]
fn ensure_schema_check_follows_the_declared_role() {
    // an observer is never asked to provision, so refusing the method must not
    // cost it the certification; the emitter arm keeps that absence meaningful.
    let script = r#"req=$(cat)
    case "$req" in
      *'"version":1'*) ;;
      *) printf '{"ok":false,"error":"unsupported protocol version"}'; exit 0 ;;
    esac
    case "$req" in
      *'"method":"capabilities"'*) printf '{"ok":true,"result":{"role":"observer"}}' ;;
      *'"method":"read"'*) printf '{"ok":true,"result":[]}' ;;
      *) printf '{"ok":false,"error":"write is not supported"}' ;;
    esac"#;
    let outcomes = run_builtin_writing(&sh(script));
    assert!(!outcomes
        .iter()
        .any(|o| o.name == "protocol/ensure-schema-empty"));
    // and it passes what it is sent: asserting only the absence let two other
    // checks fail here unnoticed.
    for outcome in &outcomes {
        assert!(
            outcome.passed(),
            "{} failed: {:?}",
            outcome.name,
            outcome.failure
        );
    }

    let script = script.replace("observer", "emitter");
    let outcomes = run_builtin_writing(&sh(&script));
    assert!(outcomes
        .iter()
        .any(|o| o.name == "protocol/ensure-schema-empty"));
}

#[test]
fn preview_schema_check_follows_the_declared_role() {
    // previewing is provisioning, so a read-only adapter refusing the method is
    // conformant: the host reaches preview_schema through the emitter only.
    let script = r#"req=$(cat)
    case "$req" in
      *'"version":1'*) ;;
      *) printf '{"ok":false,"error":"unsupported protocol version"}'; exit 0 ;;
    esac
    case "$req" in
      *'"method":"capabilities"'*) printf '{"ok":true,"result":{"role":"observer"}}' ;;
      *'"method":"read"'*) printf '{"ok":true,"result":[]}' ;;
      *) printf '{"ok":false,"error":"this adapter is read-only"}' ;;
    esac"#;
    let outcomes = run_builtin(&sh(script), TIMEOUT);
    assert!(!outcomes
        .iter()
        .any(|o| o.name == "protocol/preview-schema-empty"));
    for outcome in &outcomes {
        assert!(
            outcome.passed(),
            "{} failed: {:?}",
            outcome.name,
            outcome.failure
        );
    }

    // the roles the host does preview are still sent it, and still fail on a
    // refusal: without this the gate is indistinguishable from a dropped check.
    for role in ["emitter", "adapter"] {
        let outcomes = run_builtin(&sh(&script.replace("observer", role)), TIMEOUT);
        let preview = find(&outcomes, "protocol/preview-schema-empty");
        assert!(!preview.passed(), "{role} was not sent the preview");
        assert!(
            message(preview).contains("this adapter is read-only"),
            "{}",
            message(preview)
        );
    }
}

#[test]
fn garbage_capabilities_fails_and_defaults_to_adapter() {
    // a well-formed envelope carrying a nonsense role fails the capabilities
    // check (unlike the conformant unknown-method answer), and the runner still
    // falls back to the default read+write role for the remaining checks.
    let script = r#"req=$(cat); case "$req" in
      *'"method":"capabilities"'*) printf '{"ok":true,"result":{"role":"frobnicator"}}' ;;
      *'"method":"read"'*) printf '{"ok":true,"result":[]}' ;;
      *) printf '{"ok":false,"error":"unsupported"}' ;;
    esac"#;
    let outcomes = run_builtin_writing(&sh(script));
    let capabilities = find(&outcomes, "protocol/capabilities");
    assert!(!capabilities.passed());
    assert!(
        message(capabilities).contains("bad capabilities result"),
        "{}",
        message(capabilities)
    );
    // default role: it is sent both liveness checks. the read passes here and the
    // write fails, since this adapter answers everything else with an error.
    assert!(find(&outcomes, "protocol/read-empty").passed());
    assert!(!find(&outcomes, "protocol/write-empty").passed());
}

#[test]
fn accepts_multiline_json() {
    let outcomes = run_builtin(
        &sh("cat >/dev/null; printf '{\\n  \"ok\": true,\\n  \"result\": []\\n}\\n'"),
        TIMEOUT,
    );
    let read = find(&outcomes, "protocol/read-empty");
    assert!(read.passed(), "multiline json rejected: {:?}", read.failure);
}

#[test]
fn version_mismatch_follows_the_declared_role() {
    // a declared emitter refuses read for role reasons, so a probe sent as a read
    // is answered without the adapter ever reading `version`. this adapter is
    // version-blind and must still be caught.
    let script = r#"req=$(cat); case "$req" in
      *'"method":"capabilities"'*) printf '{"ok":true,"result":{"role":"emitter"}}' ;;
      *'"method":"write"'*) printf '{"ok":true,"result":{"applied":[]}}' ;;
      *'"method":"preview_schema"'*) printf '{"ok":true,"result":null}' ;;
      *) printf '{"ok":false,"error":"read is not supported"}' ;;
    esac"#;
    let outcomes = run_builtin_writing(&sh(script));
    let mismatch = find(&outcomes, "protocol/version-mismatch");
    assert!(
        !mismatch.passed(),
        "the version probe must ride a method the emitter implements"
    );
    assert!(
        message(mismatch).contains("expected a structured error"),
        "{}",
        message(mismatch)
    );
    // an observer keeps the read: it implements read and refuses write, so the
    // probe would be vacuous the other way round.
    let script = r#"req=$(cat); case "$req" in
      *'"method":"capabilities"'*) printf '{"ok":true,"result":{"role":"observer"}}' ;;
      *'"method":"read"'*) printf '{"ok":true,"result":[]}' ;;
      *) printf '{"ok":false,"error":"write is not supported"}' ;;
    esac"#;
    let outcomes = run_builtin_writing(&sh(script));
    assert!(!find(&outcomes, "protocol/version-mismatch").passed());
}

#[test]
fn version_mismatch_uses_the_next_protocol_version() {
    use alembic_engine::EXTERNAL_PROTOCOL_VERSION;
    // this adapter answers ok only to EXTERNAL_PROTOCOL_VERSION + 1, so the must-error
    // check fails iff the runner sent exactly that valid-but-unsupported version (and
    // not a hardcoded, u8-overflowing 999, which this adapter would have rejected).
    let next = format!(r#""version":{}"#, EXTERNAL_PROTOCOL_VERSION + 1);
    let script = format!(
        r#"req=$(cat); case "$req" in *'{next}'*) printf '{{"ok":true,"result":[]}}' ;; *) printf '{{"ok":false,"error":"unsupported"}}' ;; esac"#
    );
    let outcomes = run_builtin(&sh(&script), TIMEOUT);
    assert!(
        !find(&outcomes, "protocol/version-mismatch").passed(),
        "version-mismatch did not send EXTERNAL_PROTOCOL_VERSION + 1"
    );
}

#[test]
fn python_example_passes_builtin() {
    if !python3_available() {
        eprintln!("skipping python_example_passes_builtin: python3 not found");
        return;
    }
    let outcomes = run_builtin_writing(&python_adapter());
    for outcome in &outcomes {
        assert!(
            outcome.passed(),
            "{} failed: {:?}",
            outcome.name,
            outcome.failure
        );
    }
}

#[test]
fn python_example_passes_cases() {
    if !python3_available() {
        eprintln!("skipping python_example_passes_cases: python3 not found");
        return;
    }
    let dir = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/examples/cases"));
    let cases = load_cases(&dir).expect("load example cases");
    let outcomes = run_cases(&python_adapter(), TIMEOUT, &cases);
    assert_eq!(outcomes.len(), 6);
    for outcome in &outcomes {
        assert!(
            outcome.passed(),
            "{} failed: {:?}",
            outcome.name,
            outcome.failure
        );
    }
}

#[test]
fn case_result_mismatch_reported() {
    if !python3_available() {
        eprintln!("skipping case_result_mismatch_reported: python3 not found");
        return;
    }
    let case = Case {
        name: "wrong expectation".into(),
        request: json!({
            "version": 1, "setup": {}, "method": "read",
            "schema": { "types": {} }, "types": [], "state": { "mappings": {} }
        }),
        expect: Expect {
            ok: true,
            result: Some(json!([{ "type_name": "dcim.site", "key": {}, "attrs": {} }])),
            error: None,
        },
    };
    let outcomes = run_cases(&python_adapter(), TIMEOUT, std::slice::from_ref(&case));
    assert!(
        message(&outcomes[0]).contains("result did not match"),
        "{}",
        message(&outcomes[0])
    );
}

#[test]
fn case_result_pinned_against_null_reported() {
    // a case that pins a report must not pass against "cannot preview".
    let case = Case {
        name: "pinned vs null".into(),
        request: json!({
            "version": 1, "setup": {}, "method": "preview_schema",
            "schema": { "types": {} }
        }),
        expect: Expect {
            ok: true,
            result: Some(json!({
                "created_fields": [],
                "created_tags": [],
                "created_object_types": ["dcim.site"],
                "created_object_fields": []
            })),
            error: None,
        },
    };
    let outcomes = run_cases(
        &sh(r#"cat >/dev/null; printf '{"ok":true,"result":null}\n'"#),
        TIMEOUT,
        std::slice::from_ref(&case),
    );
    assert!(
        message(&outcomes[0]).contains("result did not match"),
        "{}",
        message(&outcomes[0])
    );
}

// the fixtures under fixtures/external_protocol/ are documented as the
// cross-language protocol contract, so they must stay shape-compatible with the
// real envelope and payload types. nothing else loads them, so guard them here.
#[test]
fn fixtures_match_the_protocol_types() {
    use alembic_engine::{
        ApplyReport, ExternalCapabilities, ExternalObject, ExternalResponse, ProvisionReport,
    };
    use serde_json::Value;

    let dir = PathBuf::from(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/external_protocol"
    ));
    let mut checked = 0;
    for entry in std::fs::read_dir(&dir).expect("read fixtures dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let name = path.file_stem().unwrap().to_string_lossy().into_owned();
        let fixture: Value =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("read fixture"))
                .unwrap_or_else(|e| panic!("{name}: invalid json: {e}"));
        let method = fixture["request"]["method"]
            .as_str()
            .unwrap_or_else(|| panic!("{name}: request has no method"));
        let response: ExternalResponse<Value> = serde_json::from_value(fixture["response"].clone())
            .unwrap_or_else(|e| panic!("{name}: response is not an envelope: {e}"));
        match (response.ok, response.result, response.error) {
            (true, Some(result), None) => match method {
                "read" => drop(
                    serde_json::from_value::<Vec<ExternalObject>>(result)
                        .unwrap_or_else(|e| panic!("{name}: bad read result: {e}")),
                ),
                "write" => drop(
                    serde_json::from_value::<ApplyReport>(result)
                        .unwrap_or_else(|e| panic!("{name}: bad write result: {e}")),
                ),
                "ensure_schema" => drop(
                    serde_json::from_value::<ProvisionReport>(result)
                        .unwrap_or_else(|e| panic!("{name}: bad ensure_schema result: {e}")),
                ),
                "preview_schema" => drop(
                    serde_json::from_value::<ProvisionReport>(result)
                        .unwrap_or_else(|e| panic!("{name}: bad preview_schema result: {e}")),
                ),
                "capabilities" => drop(
                    serde_json::from_value::<ExternalCapabilities>(result)
                        .unwrap_or_else(|e| panic!("{name}: bad capabilities result: {e}")),
                ),
                other => panic!("{name}: unknown method {other}"),
            },
            // a null preview result means "cannot preview schema", which the runner
            // accepts (src/lib.rs) and call_optional maps to Ok(None).
            (true, None, None) if method == "preview_schema" => {}
            (false, None, Some(error)) => assert!(!error.is_empty(), "{name}: empty error"),
            (ok, result, error) => panic!(
                "{name}: inconsistent envelope: ok={ok}, has_result={}, has_error={}",
                result.is_some(),
                error.is_some()
            ),
        }
        checked += 1;
    }
    assert_eq!(checked, 8, "expected 8 fixtures, checked {checked}");
}

/// opt-in: `run_builtin`, what a caller passing no `Builtins` gets, sends neither
/// writing check and reports both as skipped rather than dropping them.
#[test]
fn run_builtin_leaves_the_writing_checks_off() {
    assert!(!Builtins::default().writes);

    let outcomes = run_builtin(&sh(&emitter_writing("/dev/null")), TIMEOUT);
    for name in ["protocol/write-empty", "protocol/ensure-schema-empty"] {
        let outcome = find(&outcomes, name);
        assert!(outcome.skipped(), "{name} must be reported, not dropped");
        assert!(!outcome.passed(), "a skipped check certifies nothing");
        assert!(outcome.failure.is_none(), "and it is not a failure either");
    }
}

/// asking for them runs both, and neither reads as skipped.
#[test]
fn the_writing_checks_run_when_asked_for() {
    let outcomes = run_builtin_writing(&sh(&emitter_writing("/dev/null")));
    for name in ["protocol/write-empty", "protocol/ensure-schema-empty"] {
        let outcome = find(&outcomes, name);
        assert!(outcome.passed(), "{name}: {:?}", outcome.failure);
        assert!(!outcome.skipped(), "--write-checks must not skip {name}");
    }
}

/// an emitter whose `write` renders a file, the shape every shipped emitter has.
/// a real one takes the path from its own default; here it is handed in.
fn emitter_writing(path: &str) -> String {
    format!(
        r#"req=$(cat)
    case "$req" in
      *'"version":1'*) ;;
      *) printf '{{"ok":false,"error":"unsupported protocol version"}}'; exit 0 ;;
    esac
    case "$req" in
      *'"method":"capabilities"'*) printf '{{"ok":true,"result":{{"role":"emitter"}}}}' ;;
      *'"method":"write"'*) printf 'rendered' > {path}
        printf '{{"ok":true,"result":{{"applied":[]}}}}' ;;
      *'"method":"ensure_schema"'*) printf '{{"ok":true,"result":{{}}}}' ;;
      *'"method":"preview_schema"'*) printf '{{"ok":true,"result":null}}' ;;
      *) printf '{{"ok":false,"error":"this adapter is write-only"}}' ;;
    esac"#
    )
}

/// the same emitter, appending every method it is sent to `log`, so a check can
/// assert on what arrived rather than on what the report claims.
fn emitter_logging(log: &str) -> String {
    format!(
        r#"req=$(cat)
    case "$req" in
      *'"version":1'*) ;;
      *) printf '{{"ok":false,"error":"unsupported protocol version"}}'; exit 0 ;;
    esac
    case "$req" in
      *'"method":"capabilities"'*) echo capabilities >> {log}
        printf '{{"ok":true,"result":{{"role":"emitter"}}}}' ;;
      *'"method":"write"'*) echo write >> {log}
        printf '{{"ok":true,"result":{{"applied":[]}}}}' ;;
      *'"method":"ensure_schema"'*) echo ensure_schema >> {log}
        printf '{{"ok":true,"result":{{}}}}' ;;
      *'"method":"preview_schema"'*) echo preview_schema >> {log}
        printf '{{"ok":true,"result":null}}' ;;
      *) printf '{{"ok":false,"error":"this adapter is write-only"}}' ;;
    esac"#
    )
}

fn received(log: &Path) -> Vec<String> {
    std::fs::read_to_string(log)
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

/// what the adapter was sent, not what the report says about it: the methods that
/// do arrive are the control on the two that must not.
#[test]
fn no_writing_method_reaches_the_adapter_by_default() {
    let dir = tempdir().unwrap();
    let log = dir.path().join("methods");
    let adapter = sh(&emitter_logging(&log.display().to_string()));

    run_builtin(&adapter, TIMEOUT);
    assert_eq!(received(&log), ["capabilities", "preview_schema"]);

    std::fs::remove_file(&log).unwrap();
    run_builtin_writing(&adapter);
    assert_eq!(
        received(&log),
        ["capabilities", "write", "preview_schema", "ensure_schema"]
    );
}

/// an emitter's liveness check is a real `write`: gated off it must not touch
/// the artifact, on it must.
#[test]
fn the_gate_decides_whether_the_liveness_check_reaches_the_artifact() {
    let dir = tempdir().unwrap();
    let artifact = dir.path().join("topology.dot");
    let before = "digraph topology {\n  \"core-1\" -> \"leaf-1\";\n}\n";
    std::fs::write(&artifact, before).unwrap();
    let adapter = sh(&emitter_writing(&artifact.display().to_string()));

    let outcomes = run_builtin_with(&adapter, TIMEOUT, Builtins { writes: false });
    assert_eq!(
        std::fs::read_to_string(&artifact).unwrap(),
        before,
        "a turned-off check must not write"
    );
    assert!(find(&outcomes, "protocol/write-empty").skipped());

    let outcomes = run_builtin_with(&adapter, TIMEOUT, Builtins { writes: true });
    assert_eq!(
        std::fs::read_to_string(&artifact).unwrap(),
        "rendered",
        "with the gate on the emitter is still probed with a real write"
    );
    assert!(find(&outcomes, "protocol/write-empty").passed());
}

/// the version probe rides an emitter's write, so off it is reported as skipped
/// rather than sent, and the adapter it exists to catch goes uncaught.
#[test]
fn the_version_probe_is_skipped_for_an_emitter_with_writes_off() {
    let dir = tempdir().unwrap();
    let artifact = dir.path().join("topology.dot");
    let before = "digraph topology {\n}\n";
    std::fs::write(&artifact, before).unwrap();
    let script = format!(
        r#"req=$(cat)
    case "$req" in
      *'"method":"capabilities"'*) printf '{{"ok":true,"result":{{"role":"emitter"}}}}' ;;
      *'"method":"write"'*) printf 'rendered' > {path}
        printf '{{"ok":true,"result":{{"applied":[]}}}}' ;;
      *'"method":"preview_schema"'*) printf '{{"ok":true,"result":null}}' ;;
      *) printf '{{"ok":false,"error":"this adapter is write-only"}}' ;;
    esac"#,
        path = artifact.display()
    );

    let outcomes = run_builtin(&sh(&script), TIMEOUT);
    let probe = find(&outcomes, "protocol/version-mismatch");
    assert!(probe.skipped(), "the probe must be reported, not dropped");
    assert!(!probe.passed(), "a skipped probe certifies nothing");
    assert!(
        probe.skipped.as_deref().unwrap().contains("emitter"),
        "{:?}",
        probe.skipped
    );
    assert_eq!(
        std::fs::read_to_string(&artifact).unwrap(),
        before,
        "and it did not ride a write"
    );

    let outcomes = run_builtin_writing(&sh(&script));
    let probe = find(&outcomes, "protocol/version-mismatch");
    assert!(!probe.skipped(), "asked for, the probe is sent");
    assert!(
        probe.failure.is_some(),
        "which is what catches this adapter"
    );
    assert_eq!(std::fs::read_to_string(&artifact).unwrap(), "rendered");
}

/// the gate does not weaken what it leaves on: a failing `write` still fails.
#[test]
fn an_emitter_whose_write_errors_still_fails_with_the_gate_on() {
    let script = r#"req=$(cat)
    case "$req" in
      *'"version":1'*) ;;
      *) printf '{"ok":false,"error":"unsupported protocol version"}'; exit 0 ;;
    esac
    case "$req" in
      *'"method":"capabilities"'*) printf '{"ok":true,"result":{"role":"emitter"}}' ;;
      *'"method":"ensure_schema"'*) printf '{"ok":true,"result":{}}' ;;
      *'"method":"preview_schema"'*) printf '{"ok":true,"result":null}' ;;
      *) printf '{"ok":false,"error":"the output path is not writable"}' ;;
    esac"#;
    let outcomes = run_builtin_writing(&sh(script));
    let liveness = find(&outcomes, "protocol/write-empty");
    assert!(
        message(liveness).contains("not writable"),
        "{}",
        message(liveness)
    );
}

/// an observer's liveness check is an empty read, so the gate leaves it alone;
/// an observer is never sent the writing checks at all.
#[test]
fn the_gate_leaves_a_non_writing_liveness_check_alone() {
    let script = r#"req=$(cat)
    case "$req" in
      *'"version":1'*) ;;
      *) printf '{"ok":false,"error":"unsupported protocol version"}'; exit 0 ;;
    esac
    case "$req" in
      *'"method":"capabilities"'*) printf '{"ok":true,"result":{"role":"observer"}}' ;;
      *'"method":"read"'*) printf '{"ok":true,"result":[]}' ;;
      *) printf '{"ok":false,"error":"write is not supported"}' ;;
    esac"#;
    let outcomes = run_builtin_with(&sh(script), TIMEOUT, Builtins { writes: false });
    for outcome in &outcomes {
        assert!(
            outcome.passed(),
            "{} did not pass: {:?}",
            outcome.name,
            outcome.failure
        );
    }
    assert!(find(&outcomes, "protocol/read-empty").passed());
    for name in ["protocol/write-empty", "protocol/ensure-schema-empty"] {
        assert!(!outcomes.iter().any(|o| o.name == name), "{name}");
    }
}

/// a full read+write adapter gets both liveness checks, so the gate takes its
/// write and leaves its read: the gate follows the method, not the role.
#[test]
fn the_gate_turns_off_a_full_adapters_write_and_leaves_its_read() {
    let script = r#"req=$(cat)
    case "$req" in
      *'"version":1'*) ;;
      *) printf '{"ok":false,"error":"unsupported protocol version"}'; exit 0 ;;
    esac
    case "$req" in
      *'"method":"capabilities"'*) printf '{"ok":true,"result":{"role":"adapter"}}' ;;
      *'"method":"write"'*) printf '{"ok":true,"result":{"applied":[]}}' ;;
      *'"method":"read"'*) printf '{"ok":true,"result":[]}' ;;
      *'"method":"ensure_schema"'*) printf '{"ok":true,"result":{}}' ;;
      *'"method":"preview_schema"'*) printf '{"ok":true,"result":null}' ;;
      *) printf '{"ok":false,"error":"unknown method"}' ;;
    esac"#;
    let outcomes = run_builtin_with(&sh(script), TIMEOUT, Builtins { writes: false });
    let write = find(&outcomes, "protocol/write-empty");
    assert!(write.skipped(), "the write must be reported, not dropped");
    assert!(!write.passed(), "a skipped check certifies nothing");
    assert!(find(&outcomes, "protocol/read-empty").passed());

    let outcomes = run_builtin_with(&sh(script), TIMEOUT, Builtins { writes: true });
    assert!(find(&outcomes, "protocol/write-empty").passed());
}

/// an emitter answering both provisioning methods with `key` as its delete list,
/// carried under `envelope` rather than `result`.
fn emitter_reporting_deletes(envelope: &str, key: &str) -> String {
    format!(
        r#"req=$(cat)
    case "$req" in
      *'"version":1'*) ;;
      *) printf '{{"ok":false,"error":"unsupported protocol version"}}'; exit 0 ;;
    esac
    case "$req" in
      *'"method":"capabilities"'*) printf '{{"ok":true,"result":{{"role":"emitter"}}}}' ;;
      *'"method":"write"'*) printf '{{"ok":true,"result":{{"applied":[]}}}}' ;;
      *'"method":"preview_schema"'*|*'"method":"ensure_schema"'*)
        printf '{{"ok":true,"{envelope}":{{"{key}":["dcim.site"]}}}}' ;;
      *) printf '{{"ok":false,"error":"unknown method"}}' ;;
    esac"#
    )
}

#[test]
fn a_misspelled_schema_delete_fails() {
    // this report is the --allow-delete gate, so a typo here reports nothing to
    // delete and provisions past it in silence.
    let outcomes = run_builtin_writing(&sh(&emitter_reporting_deletes(
        "result",
        "deletd_object_types",
    )));
    for name in [
        "protocol/preview-schema-empty",
        "protocol/ensure-schema-empty",
    ] {
        let outcome = find(&outcomes, name);
        assert!(!outcome.passed(), "{name} certified a misspelled key");
        assert!(
            message(outcome).contains("deletd_object_types"),
            "{}",
            message(outcome)
        );
    }
}

#[test]
fn a_misspelled_result_key_fails() {
    // one level up, same gate: preview_schema is allowed to answer `(true, None,
    // None)`, so a typo'd `result` reads as "cannot preview" and the run skips the
    // --allow-delete refusal rather than hitting it.
    let outcomes = run_builtin(
        &sh(&emitter_reporting_deletes("resutl", "deleted_object_types")),
        TIMEOUT,
    );
    let outcome = find(&outcomes, "protocol/preview-schema-empty");
    assert!(!outcome.passed(), "a misspelled result key was certified");
    assert!(message(outcome).contains("resutl"), "{}", message(outcome));
}

#[test]
fn a_correctly_spelled_schema_delete_still_passes() {
    // the control for both checks above: the same adapter, one word apart. without
    // it the suite cannot tell them from checks that reject both spellings.
    let outcomes = run_builtin_writing(&sh(&emitter_reporting_deletes(
        "result",
        "deleted_object_types",
    )));
    for outcome in &outcomes {
        assert!(
            outcome.passed(),
            "{} failed: {:?}",
            outcome.name,
            outcome.failure
        );
    }
}

#[test]
fn an_unknown_key_one_level_down_fails() {
    // examples/cases/write-create.json asserts only `ok`, so a case's response is
    // never compared and only shape-checked; the check has to reach inside
    // `applied`. a typo'd backend_id deserializes to None, which apply reads as
    // remove rather than unchanged, dropping a mapping alembic already held.
    let case = Case {
        name: "nested".into(),
        request: json!({
            "version": 1, "setup": {}, "method": "write",
            "schema": { "types": {} }, "ops": [], "state": {}
        }),
        expect: Expect {
            ok: true,
            result: None,
            error: None,
        },
    };
    let script = r#"cat >/dev/null; printf '{"ok":true,"result":{"applied":[{"uid":"11111111-1111-1111-1111-111111111111","type_name":"dcim.site","backend_ids":1}]}}\n'"#;
    let outcomes = run_cases(&sh(script), TIMEOUT, std::slice::from_ref(&case));
    assert!(!outcomes[0].passed(), "a nested unknown key was certified");
    assert!(
        message(&outcomes[0]).contains("applied[0].backend_ids"),
        "{}",
        message(&outcomes[0])
    );
}

#[test]
fn an_unknown_key_beside_the_result_fails() {
    // the report nested one level too high: `result` is spelled right, so the
    // payload check walks a payload the key is not in and every method certified it.
    let case = Case {
        name: "envelope-extra".into(),
        request: json!({
            "version": 1, "setup": {}, "method": "write",
            "schema": { "types": {} }, "ops": [], "state": {}
        }),
        expect: Expect {
            ok: true,
            result: None,
            error: None,
        },
    };
    let script = r#"cat >/dev/null; printf '{"ok":true,"result":{"applied":[]},"deleted_object_types":["dcim.site"]}\n'"#;
    let outcomes = run_cases(&sh(script), TIMEOUT, std::slice::from_ref(&case));
    assert!(!outcomes[0].passed(), "an envelope extra was certified");
    assert!(
        message(&outcomes[0]).contains("deleted_object_types"),
        "{}",
        message(&outcomes[0])
    );
}

#[test]
fn an_explicitly_empty_provision_list_passes() {
    // `skip_serializing_if` means the type would not serialize these back, but
    // examples/adapter.py sends them: the check is on the key, not a round-trip.
    let case = Case {
        name: "empty-lists".into(),
        request: json!({
            "version": 1, "setup": {}, "method": "ensure_schema",
            "schema": { "types": {} }
        }),
        expect: Expect {
            ok: true,
            result: None,
            error: None,
        },
    };
    let script = r#"cat >/dev/null; printf '{"ok":true,"result":{"created_fields":[],"created_tags":[]}}\n'"#;
    let outcomes = run_cases(&sh(script), TIMEOUT, std::slice::from_ref(&case));
    assert!(
        outcomes[0].passed(),
        "an explicitly empty list was rejected: {:?}",
        outcomes[0].failure
    );
}

#[test]
fn a_read_keeps_its_own_key_and_attr_names() {
    // `key` and `attrs` are the adapter's maps, not protocol fields, so the check
    // must not reach into them.
    let case = Case {
        name: "free-form".into(),
        request: json!({
            "version": 1, "setup": {}, "method": "read",
            "schema": { "types": {} }, "types": [], "state": {}
        }),
        expect: Expect {
            ok: true,
            result: None,
            error: None,
        },
    };
    let script = r#"cat >/dev/null; printf '{"ok":true,"result":[{"type_name":"dcim.site","key":{"site":"fra1"},"attrs":{"name":"FRA1","slug":"fra1"}}]}\n'"#;
    let outcomes = run_cases(&sh(script), TIMEOUT, std::slice::from_ref(&case));
    assert!(
        outcomes[0].passed(),
        "a free-form map was walked: {:?}",
        outcomes[0].failure
    );
}

#[test]
fn a_misspelled_role_names_the_key() {
    // the role picks the liveness check, so a typo defaults the adapter to
    // read+write and the run fails on a read it never claimed to implement.
    let script = r#"cat >/dev/null; printf '{"ok":true,"result":{"rol":"emitter"}}\n'"#;
    let outcomes = run_builtin(&sh(script), TIMEOUT);
    let capabilities = find(&outcomes, "protocol/capabilities");
    assert!(!capabilities.passed(), "a misspelled role was certified");
    assert!(
        message(capabilities).contains("rol"),
        "{}",
        message(capabilities)
    );
}
