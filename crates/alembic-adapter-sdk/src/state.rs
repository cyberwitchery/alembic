use crate::types::BackendId;
use alembic_core::{TypeName, Uid};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// the uid -> backend id mappings. this is also the shape external adapters
/// receive in read/write requests, so the backend stamp lives in [`StateFile`],
/// never here.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StateData {
    #[serde(default)]
    pub mappings: BTreeMap<TypeName, BTreeMap<Uid, BackendId>>,
}
