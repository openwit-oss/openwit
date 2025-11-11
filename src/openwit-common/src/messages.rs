use serde::{Serialize, Deserialize};


#[derive(Debug, Clone)]
pub enum ClusterMessage {
    RequestClaimPartition(String),
    RedistributeIngest {
        from_node: String,
        to_node: String,
    },
    Heartbeat(NodeStatus),
    RequestFlush(String),
}

// Add supporting types here too if needed
#[derive(Debug, Clone,Serialize,Deserialize)]
pub struct NodeStatus {
    pub node_id: String,
    pub cpu_load: f32,
    pub memtable_usage: f32,
    pub ingest_rate: u64,
}

