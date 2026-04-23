use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use url::Url;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct Topology {
    nodes: HashMap<String, Node>,
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
