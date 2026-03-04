use alembic_core::Uid;
use thiserror::Error;

#[derive(Debug, Error)]
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
