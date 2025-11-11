use std::collections::HashMap;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct NodeId(pub String);

impl NodeId {
    pub fn new(id: String) -> Self {
        Self(id)
    }
    
    pub fn generate() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum NodeRole {
    Control,
    Ingest,
    Search,
    Storage,
    Query,
    Kafka,
    Hybrid(Vec<NodeRole>),
}

impl fmt::Display for NodeRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeRole::Control => write!(f, "control"),
            NodeRole::Ingest => write!(f, "ingest"),
            NodeRole::Search => write!(f, "search"),
            NodeRole::Storage => write!(f, "storage"),
            NodeRole::Query => write!(f, "query"),
            NodeRole::Kafka => write!(f, "kafka"),
            NodeRole::Hybrid(roles) => {
                let role_names: Vec<String> = roles.iter().map(|r| r.to_string()).collect();
                write!(f, "hybrid[{}]", role_names.join(","))
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetadata {
    pub id: NodeId,
    pub role: NodeRole,
    pub state: NodeState,
    pub last_heartbeat: DateTime<Utc>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeState {
    Starting,
    Running,
    Degraded,
    Stopping,
    Stopped,
    Failed,
}



#[derive(Debug, Clone)]
pub struct ClusterSnapshot {
    pub nodes: HashMap<NodeId, NodeMetadata>,
    pub timestamp: DateTime<Utc>,
}

impl ClusterSnapshot {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            timestamp: Utc::now(),
        }
    }
}