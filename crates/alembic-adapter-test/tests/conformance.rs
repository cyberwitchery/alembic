use alembic_adapter_test::{load_cases, run_builtin, run_cases, Case, Expect, Outcome};
use serde_json::json;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

const TIMEOUT: Duration = Duration::from_secs(10);

fn sh(script: &str) -> Vec<String> {
    vec!["sh".into(), "-c".into(), script.into()]
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
    // (plus the capabilities probe) is ~timeout + a grace, so seven runs stay
    // under this bound, while the pre-fix behavior was ~120s.
    let start = Instant::now();
    let outcomes = run_builtin(&sh("sleep 30 & wait"), Duration::from_millis(300));
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
    // call_optional maps it to Ok(None), so conformance must accept it too. red before
    // the guarded (true, None, None) arm, green after.
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
    // the emit-only shape from issue #117: the adapter declares the emitter role
    // and errors on read. pre-capabilities it had to stub read() -> [] to pass;
    // now the runner skips the empty read and probes liveness with an empty
    // write instead.
    let script = r#"req=$(cat); case "$req" in
      *'"method":"capabilities"'*) printf '{"ok":true,"result":{"role":"emitter"}}' ;;
      *'"method":"write"'*) printf '{"ok":true,"result":{"applied":[]}}' ;;
      *'"method":"preview_schema"'*) printf '{"ok":true,"result":null}' ;;
      *) printf '{"ok":false,"error":"read is not supported"}' ;;
    esac"#;
    let outcomes = run_builtin(&sh(script), TIMEOUT);
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
fn garbage_capabilities_fails_and_defaults_to_adapter() {
    // a well-formed envelope carrying a nonsense role fails the capabilities
    // check (unlike the conformant unknown-method answer), and the runner still
    // falls back to the default read+write role for the remaining checks.
    let script = r#"req=$(cat); case "$req" in
      *'"method":"capabilities"'*) printf '{"ok":true,"result":{"role":"frobnicator"}}' ;;
      *'"method":"read"'*) printf '{"ok":true,"result":[]}' ;;
      *) printf '{"ok":false,"error":"unsupported"}' ;;
    esac"#;
    let outcomes = run_builtin(&sh(script), TIMEOUT);
    let capabilities = find(&outcomes, "protocol/capabilities");
    assert!(!capabilities.passed());
    assert!(
        message(capabilities).contains("bad capabilities result"),
        "{}",
        message(capabilities)
    );
    // default role: the empty read still runs (and passes here).
    assert!(find(&outcomes, "protocol/read-empty").passed());
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
    let outcomes = run_builtin(&python_adapter(), TIMEOUT);
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
