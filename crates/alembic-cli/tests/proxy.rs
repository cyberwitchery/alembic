//! docs/configuration.md, proxies: the http adapters send their requests through
//! the proxy `HTTP_PROXY` names. drives the binary so the proxy is set in the
//! child's environment, never in this process's.

mod support;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::{Command, Output};
use std::sync::mpsc;
use std::thread;
use support::bin_path;
use tempfile::tempdir;

const INVENTORY: &str = "schema:\n  types:\n    dcim.site:\n      key: [slug]\n      fields:\n        name: {type: string}\n        slug: {type: string}\nobjects:\n  - uid: 00000000-0000-0000-0000-000000000001\n    type: dcim.site\n    key: {slug: fra1}\n    attrs: {name: fra1, slug: fra1}\n";

/// runs `plan` against `backend` at `http://<host>`, a name that does not
/// resolve, with `HTTP_PROXY` naming a local listener that answers `401`.
/// returns the request line of the first request the listener got, if any.
fn plan_through_proxy(mut command: Command, backend: &str, host: &str) -> (Option<String>, Output) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind proxy");
    let proxy = format!("http://{}", listener.local_addr().expect("proxy address"));
    let (requests, received) = mpsc::channel();
    thread::spawn(move || {
        for mut stream in listener.incoming().flatten() {
            let mut reader = BufReader::new(&stream);
            let mut request_line = String::new();
            let mut length = 0;
            let mut line = String::new();
            while reader.read_line(&mut line).is_ok_and(|n| n > 0) && line != "\r\n" {
                if request_line.is_empty() {
                    request_line = line.trim_end().to_string();
                } else if let Some((name, value)) = line.split_once(':') {
                    if name.eq_ignore_ascii_case("content-length") {
                        length = value.trim().parse().unwrap_or(0);
                    }
                }
                line.clear();
            }
            if requests.send(request_line).is_err() {
                break;
            }
            // the run may have hung up already; its request line is recorded.
            let _ = reader.take(length).read_to_end(&mut Vec::new());
            let _ = stream.write_all(
                b"HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
            );
        }
    });

    let dir = tempdir().expect("tempdir");
    std::fs::write(dir.path().join("inventory.yaml"), INVENTORY).expect("write inventory");
    let prefix = backend.to_uppercase();
    let output = command
        .current_dir(dir.path())
        .args([
            "plan",
            "--backend",
            backend,
            "-f",
            "inventory.yaml",
            "--dry-run",
        ])
        .env(format!("{prefix}_URL"), format!("http://{host}"))
        .env(format!("{prefix}_TOKEN"), "token")
        .env("HTTP_PROXY", proxy)
        .env_remove("NO_PROXY")
        .env_remove("no_proxy")
        .output()
        .expect("run alembic");
    (received.try_recv().ok(), output)
}

/// a proxied request names its target in absolute form: `GET http://host/path`.
fn assert_proxied(backend: &str, (request, output): (Option<String>, Output), host: &str) {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let request = request
        .unwrap_or_else(|| panic!("{backend} sent no request to the proxy; stderr: {stderr}"));
    let target = request.split(' ').nth(1).unwrap_or_default();
    assert!(
        target.starts_with(&format!("http://{host}/")),
        "{backend} sent `{request}` to the proxy; stderr: {stderr}"
    );
}

#[test]
fn netbox_requests_go_through_the_proxy() {
    let host = "netbox.invalid";
    let run = plan_through_proxy(Command::new(bin_path()), "netbox", host);
    assert_proxied("netbox", run, host);
}

#[test]
fn infrahub_requests_go_through_the_proxy() {
    let host = "infrahub.invalid";
    let run = plan_through_proxy(Command::new(bin_path()), "infrahub", host);
    assert_proxied("infrahub", run, host);
}

/// with the system configuration daemon out of reach, as in a sandbox, macos
/// gives no proxy settings; the run goes on with the environment's proxy.
#[cfg(target_os = "macos")]
#[test]
fn an_unreachable_macos_proxy_store_does_not_panic() {
    let host = "netbox.invalid";
    let mut sandboxed = Command::new("/usr/bin/sandbox-exec");
    sandboxed.args([
        "-p",
        "(version 1)(allow default)(deny mach-lookup (global-name \"com.apple.SystemConfiguration.configd\"))",
    ]);
    sandboxed.arg(bin_path());
    let run = plan_through_proxy(sandboxed, "netbox", host);
    assert_ne!(
        run.1.status.code(),
        Some(101),
        "stderr: {}",
        String::from_utf8_lossy(&run.1.stderr)
    );
    assert_proxied("netbox", run, host);
}
