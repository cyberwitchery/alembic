//! nautobot adapter implementation.

mod client;
mod mapping;
mod ops;
mod registry;
mod state;

use anyhow::Result;
use std::sync::Arc;

use client::NautobotClient;

/// nautobot adapter that maps ir objects to nautobot api calls.
pub struct NautobotAdapter {
    client: Arc<NautobotClient>,
}

impl NautobotAdapter {
    /// create a new adapter with url, token, and state store.
    pub fn new(url: &str, token: &str) -> Result<Self> {
        let client = Arc::new(NautobotClient::new(url, token)?);
        Ok(Self { client })
    }
}
