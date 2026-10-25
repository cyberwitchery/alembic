//! failures caused by what a run was given rather than by a backend, typed so
//! the cli can exit with a code a caller branches on.

use thiserror::Error;

/// context on an error the caller's arguments or files caused; displays as the
/// message it carries, so marking an error does not change what it says.
#[derive(Debug, Error)]
pub enum InputError {
    /// the run cannot start as asked: a file that cannot be read, or arguments
    /// that do not fit together in a way the parser cannot see.
    #[error("{0}")]
    Usage(String),
    /// a file was read but is not a valid document of its kind.
    #[error("{0}")]
    Invalid(String),
}
