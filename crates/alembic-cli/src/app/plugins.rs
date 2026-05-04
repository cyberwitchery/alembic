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
    pub(crate) fn spawn(plugin_name: &str) -> Result<Self> {
        let full_exe_path = format!(
            "../alembic-ops/target/debug/alembic-adapter-{}",
            plugin_name
        );
        let mut cmd = Command::new(&full_exe_path);
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
    ) -> Result<PluginResponse, anyhow::Error> {
        let mut payload = serde_json::to_string(request).expect("FIXME");
        payload.push('\n');

        self.stdin.write_all(payload.as_bytes())?;
        self.stdin.flush()?;

        match self.rx.recv_timeout(Duration::from_secs(2)) {
            Ok(Ok(line)) => Ok(PluginResponse::ok(vec![line])),
            Ok(Err(io_err)) => Err(anyhow!("plugin stdout error: {io_err}")),
            Err(RecvTimeoutError::Timeout) => Err(anyhow!("plugin timed out")),
            Err(RecvTimeoutError::Disconnected) => Err(anyhow!("plugin disconnected")),
        }
    }
}
