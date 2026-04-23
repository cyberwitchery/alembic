//! containerlab adapter implementation

#![allow(unused_imports)]
#![allow(dead_code)]

use alembic_core::{Inventory, JsonMap, Key, Schema, TypeName, Uid};
use alembic_engine::{
    apply_non_delete_with_retries, Adapter, AdapterApplyError, AppliedOp, ApplyReport, BackendId,
    ObservedObject, ObservedState, Op, RetryApplyDriver, StateStore,
};
use anyhow::Context;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::collections::HashSet;
use std::default;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use url::Url;

/// containerlab adapter that maps ir objects to topologies
pub struct ContainerlabAdapter {}

impl ContainerlabAdapter {
    pub fn new() -> Self {
        Self {}
    }

    pub fn to_topology(inventory: &Inventory) -> Topology {
        let mut topology = Topology::default();

        for object in inventory.objects.iter() {
            topology.add_node(object.uid.to_string(), Node::default());
            let linked = inventory.objects.iter().filter(|o| are_linked(object, o));
            for o in linked {
                topology.add_link(Link::new(
                    o.attrs.get_str("name").unwrap_or_default().to_string(),
                    object.attrs.get_str("name").unwrap_or_default().to_string(),
                ));
            }
        }

        topology
    }

    pub fn write_topology(path: &Path, topology: &Topology) -> Result<()> {
        let raw = serde_yaml::to_string(topology)?;
        fs::write(path, raw).with_context(|| format!("write topology: {}", path.display()))
    }
}

fn are_linked(_a: &alembic_core::Object, _b: &alembic_core::Object) -> bool {
    true
}

#[async_trait]
impl Adapter for ContainerlabAdapter {
    async fn read(
        &self,
        _schema: &Schema,
        _types: &[TypeName],
        _state_store: &StateStore,
    ) -> anyhow::Result<ObservedState> {
        let state = ObservedState::default();
        Ok(state)
    }

    async fn write(
        &self,
        _schema: &Schema,
        _ops: &[Op],
        _state: &StateStore,
    ) -> anyhow::Result<crate::ApplyReport> {
        panic!("can't write yet")
    }
}

/// A containerlab topology describes which devices should be
/// deployed and how they should be configured.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Topology {
    nodes: HashMap<String, Node>,
    /// Environment flags that will be used throughout the topology,
    /// can be overridden by specific nodes.
    defaults: HashMap<String, String>,
    groups: HashMap<String, Group>,
    links: HashSet<Link>,
}

impl Topology {
    pub fn add_node(&mut self, name: String, node: Node) {
        self.nodes.insert(name, node);
    }

    pub fn add_link(&mut self, link: Link) {
        self.links.insert(link);
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Node {
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

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct Group {}

#[derive(Debug, Clone, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Link {
    endpoints: [String; 2],
    #[serde(skip_serializing_if = "Option::is_none")]
    mtu: Option<u32>,
}

impl Link {
    fn new(endpoint_a: String, endpoint_b: String) -> Self {
        Self {
            endpoints: [endpoint_a, endpoint_b],
            mtu: None,
        }
    }
}
