use std::time::Duration;
use tokio::{sync::mpsc::Receiver, time::interval};
use tracing::{debug, info, warn};

use crate::messages::ScaleCommand;
use openwit_network::{ClusterHandle};

fn assign_partition_to_node(topic: &str, partition: i32, nodes: &[String]) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    topic.hash(&mut hasher);
    partition.hash(&mut hasher);
    let hash = hasher.finish();

    let index = (hash as usize) % nodes.len();
    nodes[index].clone()
}

pub struct ScaleActor {
    pub self_id: String,
    pub cluster: ClusterHandle,
}

impl ScaleActor {
    pub fn new(self_id: String, cluster: ClusterHandle) -> Self {
        Self { self_id, cluster }
    }

    pub async fn run(&self, mut scale_rx: Receiver<ScaleCommand>) {
        let cluster = self.cluster.clone();
        let self_id = self.self_id.clone();

        info!("ScaleActor is running. Waiting for scale commands and gossip updates.");

        // Periodic partition assignment
        {
            let cluster = cluster.clone();
            let self_id = self_id.clone();
            tokio::spawn(async move {
                let mut tick = interval(Duration::from_secs(15));

                let topics = vec!["traces".to_string(), "logs".to_string()];
                let partitions_per_topic = 3;

                loop {
                    tick.tick().await;

                    let node_ids = cluster.view().get_roles().await.keys().cloned().collect::<Vec<_>>();

                    for topic in &topics {
                        for partition in 0..partitions_per_topic {
                            let assigned_node = assign_partition_to_node(topic, partition, &node_ids);
                            if assigned_node == self_id {
                                let key = format!("assign:{}:{}", topic, partition);
                                cluster.set_self_kv(&key, &self_id).await;
                                debug!("Assigned partition ({}, {}) to self ({})", topic, partition, self_id);
                            }
                        }
                    }
                }
            });
        }

        // Handle scale commands
        while let Some(cmd) = scale_rx.recv().await {
            debug!("Scale command received: {:?}", cmd);

            let snapshot = cluster.view().node_states.read().await;
            let low_watermark = 50u8;
            let mut candidate: Option<(String, u8)> = None;

            for node_state in snapshot.node_states.iter() {
                let peer_id = node_state.chitchat_id().node_id.clone();
                if peer_id == self_id {
                    continue;
                }

                let cpu = node_state
                    .key_values_including_deleted()
                    .find(|(k, _)| *k == "cpu")
                    .map(|(_, v)| v.value.clone())
                    .and_then(|s| s.parse::<u8>().ok());

                if let Some(cpu_val) = cpu {
                    if cpu_val < low_watermark {
                        match &candidate {
                            None => candidate = Some((peer_id.clone(), cpu_val)),
                            Some((_, best)) if cpu_val < *best => candidate = Some((peer_id.clone(), cpu_val)),
                            _ => {}
                        }
                    }
                }
            }

            if let Some((target_peer_id, _)) = candidate {
                if let Some(target_node_state) = snapshot.node_states
                    .iter()
                    .find(|n| n.chitchat_id().node_id == target_peer_id) {

                    let target_ip = target_node_state.chitchat_id().gossip_advertise_addr.ip();
                    let target_port = 9001;
                    let url = format!("http://{}:{}/forward-alert", target_ip, target_port);

                    let payload = serde_json::json!({
                        "reason": cmd.reason,
                        "resource": cmd.resource,
                        "usage_percent": cmd.usage_percent,
                    });

                    let client = reqwest::Client::new();
                    match client.post(&url).json(&payload).send().await {
                        Ok(resp) if resp.status().is_success() => {
                            info!("Successfully forwarded to {}", target_peer_id);
                        }
                        Ok(resp) => {
                            warn!("Failed to forward to {} - Status: {}", target_peer_id, resp.status());
                        }
                        Err(e) => {
                            warn!("Error forwarding to {}: {:?}", target_peer_id, e);
                        }
                    }
                }
            }
        }
    }
}
