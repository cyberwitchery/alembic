use alembic_adapter_test::{load_cases, run_builtin, run_cases, Case, Expect, Outcome};
use serde_json::json;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

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
fn rejects_bad_payload() {
    let outcomes = run_builtin(
        &sh(r#"cat >/dev/null; printf '{"ok":true,"result":"nope"}\n'"#),
        TIMEOUT,
    );
    let msg = message(find(&outcomes, "protocol/read-empty"));
    assert!(msg.contains("bad read result"), "{msg}");
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
    assert_eq!(outcomes.len(), 5);
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
