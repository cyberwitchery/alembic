//! exit codes, so a caller can tell failures apart without matching message
//! text. the numbers follow sysexits(3); `docs/cli.md` lists them.

use alembic_adapter_sdk::apply_retry::RetryApplyError;
use alembic_engine::InputError;
use std::process::ExitCode;

/// `plan --report` found drift.
pub(crate) const DRIFT: u8 = 2;
/// arguments the run cannot start with, or a file it cannot read.
pub(crate) const USAGE: u8 = 64;
/// a file that is not a valid document of its kind, or an inventory that does
/// not validate.
pub(crate) const INVALID_INPUT: u8 = 65;
/// a backend that could not be reached or refused the credentials.
pub(crate) const UNAVAILABLE: u8 = 69;
/// an apply that stopped partway with its journal kept: re-running the same
/// plan resumes.
pub(crate) const RESUMABLE: u8 = 75;

/// the code for a failed run. an interrupted apply wins over its cause: whatever
/// stopped it, resuming is what the caller should offer. `downcast_ref` rather
/// than a walk of `chain()`, which yields context layers as opaque wrappers.
pub(crate) fn code_for(err: &anyhow::Error) -> u8 {
    if let Some(RetryApplyError::Interrupted { .. }) = err.downcast_ref::<RetryApplyError>() {
        return RESUMABLE;
    }
    if alembic_adapter_registry::backend_unavailable(err) {
        return UNAVAILABLE;
    }
    match err.downcast_ref::<InputError>() {
        Some(InputError::Usage(_)) => USAGE,
        Some(InputError::Invalid(_)) => INVALID_INPUT,
        None => 1,
    }
}

/// what clap's own exit would do, with a usage error on `USAGE` instead of 2,
/// which `plan --report` already means as drift.
pub(crate) fn parse_failure(err: &clap::Error) -> ExitCode {
    // help and version arrive as errors too; they print to stdout and succeed.
    // a print that fails has nowhere left to report to, so the code stands alone
    let _ = err.print();
    if err.use_stderr() {
        ExitCode::from(USAGE)
    } else {
        ExitCode::SUCCESS
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;

    #[test]
    fn code_for_reads_the_marker_anywhere_in_the_chain() {
        let usage = anyhow::Error::from(InputError::Usage("read plan: p.json".into()));
        let invalid = anyhow!("unknown field `ops`")
            .context(InputError::Invalid("parse yaml: inv.yaml".into()))
            .context("while planning");
        let interrupted = anyhow::Error::from(RetryApplyError::Interrupted {
            applied: 1,
            total: 3,
            journal: "j".into(),
            source: anyhow::Error::from(InputError::Invalid("x".into())).into(),
        })
        .context("netbox apply");
        for (err, code) in [
            (usage, USAGE),
            (invalid, INVALID_INPUT),
            (interrupted, RESUMABLE),
            (anyhow!("something else"), 1),
        ] {
            assert_eq!(code_for(&err), code, "{err:#}");
        }
    }

    #[test]
    fn marking_an_error_keeps_its_message() {
        let err = anyhow::Error::from(InputError::Invalid("validation failed".into()));
        assert_eq!(format!("{err:#}"), "validation failed");
    }
}
