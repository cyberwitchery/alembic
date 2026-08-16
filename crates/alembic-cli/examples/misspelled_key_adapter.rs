use std::io::Read;

// writes its responses by hand: the sdk serializes a typed report, so a
// misspelled key cannot come out of it. the two env vars pick the spellings, so
// a test can run the same adapter as its own control.
fn main() {
    let mut request = String::new();
    std::io::stdin()
        .read_to_string(&mut request)
        .expect("read request");
    let method = serde_json::from_str::<serde_json::Value>(&request)
        .ok()
        .and_then(|envelope| envelope["method"].as_str().map(str::to_string))
        .unwrap_or_default();
    let deleted =
        std::env::var("ADAPTER_DELETED_KEY").unwrap_or_else(|_| "deleted_object_types".to_string());
    let result = std::env::var("ADAPTER_RESULT_KEY").unwrap_or_else(|_| "result".to_string());

    let response = match method.as_str() {
        "capabilities" => r#"{"ok":true,"result":{"role":"adapter"}}"#.to_string(),
        "read" => r#"{"ok":true,"result":[]}"#.to_string(),
        "preview_schema" => format!(r#"{{"ok":true,"{result}":{{"{deleted}":["dcim.site"]}}}}"#),
        // the envelope key stays correct here: a run that skips the gate on a
        // typo'd preview still reaches a provisioning it can read.
        "ensure_schema" => format!(r#"{{"ok":true,"result":{{"{deleted}":["dcim.site"]}}}}"#),
        "write" => r#"{"ok":true,"result":{"applied":[]}}"#.to_string(),
        _ => r#"{"ok":false,"error":"unknown method"}"#.to_string(),
    };
    println!("{response}");
}
