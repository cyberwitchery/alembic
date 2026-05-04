//! handling of plugin subprocesses.

use alembic_engine::plugin::PluginRequest;
use alembic_engine::plugin::PluginResponse;
use anyhow::{anyhow, Context, Result};
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc;
use std::sync::mpsc::RecvTimeoutError;
use std::thread;
use std::time::Duration;

pub(crate) struct PluginProcess {
    child: Child,
    stdin: BufWriter<ChildStdin>,
    /// Receives lines read by a background reader thread.
    rx: mpsc::Receiver<io::Result<String>>,
}

impl Drop for PluginProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl PluginProcess {
    pub(crate) fn spawn(full_exe_path: &str) -> Result<Self> {
        let mut cmd = Command::new(full_exe_path);
        let args: &[String] = &[];
        cmd.args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut child = cmd
            .spawn()
            .with_context(|| format!("spawn {full_exe_path}"))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow::anyhow!("missing plugin stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("missing plugin stdout"))?;

        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) => {
                        let _ = tx.send(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "plugin stdout closed unexpectedly (process likely crashed)",
                        )));
                        break;
                    }
                    Ok(_) => {
                        if tx.send(Ok(line)).is_err() {
                            break; // receiver dropped
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e));
                        break;
                    }
                }
            }
        });

        Ok(PluginProcess {
            child,
            stdin: BufWriter::new(stdin),
            rx,
        })
    }

    pub(crate) fn send_request(
        &mut self,
        request: &PluginRequest,
        timeout: Duration,
    ) -> Result<PluginResponse, anyhow::Error> {
        let Ok(mut payload) = serde_json::to_string(request) else {
            return Err(anyhow!("failed to serialize plugin request"));
        };
        payload.push('\n');

        self.stdin.write_all(payload.as_bytes())?;
        self.stdin.flush()?;

        match self.rx.recv_timeout(timeout) {
            Ok(Ok(line)) => {
                let response = serde_json::from_str(&line)?;
                Ok(response)
            }
            Ok(Err(io_err)) => Err(anyhow!("plugin stdout error: {io_err}")),
            Err(RecvTimeoutError::Timeout) => Err(anyhow!("plugin timed out")),
            Err(RecvTimeoutError::Disconnected) => Err(anyhow!("plugin disconnected")),
        }
    }
}

fn spawn_first_acceptable_candidate(
    plugin_name: &str,
    search_paths: &[String],
) -> Result<PluginProcess> {
    let prefixes = ["", "alembic-", "alembic-adapter-"];

    for candidate_path in search_paths {
        for prefix in prefixes {
            let full_exe_path = format!("{}{}{}", candidate_path, prefix, plugin_name);
            match PluginProcess::spawn(&full_exe_path) {
                Ok(process) => return Ok(process),
                Err(_err) => continue,
            }
        }
    }

    Err(anyhow!(
        "couldn't find a plugin with the name '{}' on any of the {} search paths",
        plugin_name,
        search_paths.len(),
    ))
}

pub fn run_plugin(name: &str) -> Result<PluginResponse> {
    let search_paths = vec![
        "../../target/debug/examples/".to_string(), // For tests
        "../alembic-ops/target/debug/".to_string(), // For local usage
    ];
    let mut proc = spawn_first_acceptable_candidate(name, &search_paths)?;
    let version = env!("CARGO_PKG_VERSION").to_string();
    let timeout = Duration::from_secs(3);
    proc.send_request(&PluginRequest::empty(version), timeout)
}
