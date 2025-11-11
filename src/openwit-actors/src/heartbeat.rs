use std::time::Duration;

use tokio::time::interval;
use openwit_common::messages::NodeStatus;
use openwit_metrics::{
    SYSTEM_CPU_GAUGE,
    SYSTEM_MEM_GAUGE,
};
use openwit_network::{ ClusterHandle };

pub async fn spawn_heartbeat_monitor(cluster_handle: ClusterHandle, node_id: String) {
    let mut ticker = interval(Duration::from_secs(5));

    loop {
        ticker.tick().await;

        // 1. Gather metrics (use actual logic later)
        let cpu_load = SYSTEM_CPU_GAUGE.get() as f32;
        let memtable_usage = SYSTEM_MEM_GAUGE.get() as f32; // TODO: query from memtable
        let ingest_rate = 10000; // TODO: pull from ingest pipeline

        let status = NodeStatus {
            node_id: node_id.clone(),
            cpu_load,
            memtable_usage,
            ingest_rate,
        };

        // 2. Serialize and broadcast to gossip
        let json_payload = serde_json::to_string(&status).unwrap();

        cluster_handle.chitchat_handle().with_chitchat(|chitchat| {
            // here, chitchat is a &mut Chitchat
            // mutate *your* node’s state—only that’s allowed
            chitchat.self_node_state().set("heartbeat".to_string(), json_payload.clone());
        }).await;
    }
}
