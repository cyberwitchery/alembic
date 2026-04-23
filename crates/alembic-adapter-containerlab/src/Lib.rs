//! containerlab adapter implementation

#![allow(unused_imports)]
#![allow(dead_code)]

use alembic_core::{JsonMap, Key, Schema, TypeName, Uid};
use alembic_engine::{
    apply_non_delete_with_retries, Adapter, AdapterApplyError, AppliedOp, ApplyReport, BackendId,
    ObservedObject, ObservedState, Op, RetryApplyDriver, StateStore,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::default;
use std::sync::Arc;
use url::Url;

/// containerlab adapter that maps ir objects to topologies
pub struct ContainerlabAdapter {}

impl ContainerlabAdapter {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl Adapter for ContainerlabAdapter {
    async fn read(
        &self,
        _schema: &Schema,
        _types: &[TypeName],
        _state_store: &StateStore,
    ) -> anyhow::Result<ObservedState> {
        let mut state = ObservedState::default();

        state.insert(ObservedObject {
            type_name: TypeName::new("string"),
            key: Default::default(),
            attrs: Default::default(),
            backend_id: Some(BackendId::Int(0 as u64)),
        });

        Ok(state)
    }

    async fn write(
        &self,
        _schema: &Schema,
        _ops: &[Op],
        _state: &StateStore,
    ) -> anyhow::Result<crate::ApplyReport> {
        todo!()
    }
}

/// A containerlab topology describes which devices should be
/// deployed and how they should be configured.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct Topology {
    nodes: HashMap<String, Node>,
    /// Environment flags that will be used throughout the topology,
    /// can be overridden by specific nodes.
    defaults: HashMap<String, String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct Node {
    kind: NodeKind,
    ty: NodeType,
    image: Option<url::Url>,
    image_pull_policy: Option<ImagePullPolicy>,
    startup_config: std::path::PathBuf,
    binds: Vec<String>,
    ports: Vec<String>,
    user: String,
    env: HashMap<String, String>,
    cmd: String,
    restart_policy: RestartPolicy,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct NodeKind(String);

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct NodeType(String);

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
enum ImagePullPolicy {
    #[default]
    IfNotPresent,
    Never,
    Always,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
enum RestartPolicy {
    #[default]
    No,
    OnFailure,
    Always,
    UnlessStopped,
}
