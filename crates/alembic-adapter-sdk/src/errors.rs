use alembic_core::Uid;
use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AdapterApplyError {
    #[error("missing referenced uid {uid}")]
    MissingRef { uid: Uid },
    #[error("backend object not found: {entity}")]
    NotFound { entity: String },
    #[error("schema mismatch: {message}")]
    SchemaMismatch { message: String },
    #[error("transport error: {message}")]
    Transport { message: String },
}

/// whether an http failure says the backend could not be reached, refused the
/// credentials or is overloaded, rather than rejecting what was sent: worth a
/// retry or an alert, not a change to the input.
pub fn http_unavailable(connect_or_timeout: bool, status: Option<u16>) -> bool {
    connect_or_timeout || matches!(status, Some(401 | 403 | 429 | 502 | 503 | 504))
}
