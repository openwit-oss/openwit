use openwit_network::ClusterRuntime;
use tokio::sync::mpsc;
use tracing::{ info, warn };
use std::net::SocketAddr;
use openwit_common::messages::{ ClusterMessage, NodeStatus };
use chitchat::ClusterStateSnapshot;

pub enum ClusterCommand {
    GetRoleMap(tokio::sync::oneshot::Sender<std::collections::HashMap<String, String>>),
    GetNodesByRole(String, tokio::sync::oneshot::Sender<Vec<String>>),
    ForwardClusterMessage(ClusterMessage),
}

pub struct ClusterActor {
    runtime: ClusterRuntime,
    receiver: mpsc::Receiver<ClusterCommand>,
    ingest_tx: mpsc::Sender<ClusterMessage>,
}

#[allow(dead_code)]
fn heartbeat_status() -> ClusterMessage {
    ClusterMessage::Heartbeat(NodeStatus {
        node_id: "node-1".to_string(),
        cpu_load: 0.62,
        memtable_usage: 0.45,
        ingest_rate: 11000,
    })
}

impl ClusterActor {
    pub async fn start(
        self_node_name: &str,
        listen_addr: SocketAddr,
        seeds: Vec<SocketAddr>,
        role: &str,
        receiver: mpsc::Receiver<ClusterCommand>,
        ingest_tx: mpsc::Sender<ClusterMessage>,
    ) -> anyhow::Result<()> {
        let runtime = ClusterRuntime::start(self_node_name, listen_addr, seeds, role).await?;
        let actor = Self {
            runtime,
            receiver,
            ingest_tx,
        };
        actor.run().await;
        Ok(())
    }

    async fn run(mut self) {
        let handle = self.runtime.handle();
        let ingest_tx = self.ingest_tx.clone(); // 👈 clone it for use in the spawn block

        tokio::spawn({
            let handle = handle.clone();
            async move {
                use tokio::time::{ sleep, Duration };

                loop {
                    sleep(Duration::from_secs(5)).await;

                    let snapshot_guard = handle.view().node_states.read().await;
                    let heartbeats = extract_heartbeats(&snapshot_guard);

                    if let Some((overloaded, underloaded)) = detect_imbalance(&heartbeats) {
                        info!("Ingest imbalance: {} → {}", overloaded, underloaded);

                        let msg = ClusterMessage::RedistributeIngest {
                            from_node: overloaded,
                            to_node: underloaded,
                        };

                        if let Err(err) = ingest_tx.send(msg).await {
                            warn!("Failed to send ingest rebalance command: {:?}", err);
                        }

                        // 👇 TODO: Send ClusterMessage::RedistributeIngest to ingest actor or router
                        // You can connect this to another mpsc or shared channel if needed
                    }
                }
            }
        });

        while let Some(msg) = self.receiver.recv().await {
            match msg {
                ClusterCommand::ForwardClusterMessage(cluster_msg) => {
                    match cluster_msg {
                        ClusterMessage::RedistributeIngest { from_node, to_node } => {
                            // Pick a partition from `from_node`
                            let snapshot_guard = handle.view().node_states.read().await;
                            for state in &snapshot_guard.node_states {
                                for (key, val) in state.key_values_including_deleted() {
                                    if key.starts_with("assign:") && val.value == from_node {
                                        let parts: Vec<&str> = key.split(':').collect();
                                        if parts.len() == 3 {
                                            let _topic = parts[1].to_string();
                                            let _partition = parts[2].parse::<i32>().unwrap_or(0);

                                            handle.chitchat_handle().with_chitchat(|c| {
                                                c.self_node_state().set(
                                                    key,
                                                    to_node.clone()
                                                );
                                            }).await;

                                            tracing::info!(
                                                "Reassigned {} from {} → {}",
                                                key,
                                                from_node,
                                                to_node
                                            );
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        _ => {
                            warn!("ForwardClusterMessage received unsupported variant");
                        }
                    }
                }
                ClusterCommand::GetRoleMap(resp) => {
                    let roles = handle.view().get_roles().await;
                    let _ = resp.send(roles);
                }
                ClusterCommand::GetNodesByRole(role, resp) => {
                    let nodes = handle.view().get_nodes_with_role(&role).await;
                    let _ = resp.send(nodes);
                }
            }
        }
    }
}

pub fn spawn_cluster_actor(
    self_node_name: String,
    listen_addr: SocketAddr,
    seeds: Vec<SocketAddr>,
    role: String,
    ingest_tx: mpsc::Sender<ClusterMessage>,
) -> mpsc::Sender<ClusterCommand> {
    let (tx, rx) = mpsc::channel(32);
    tokio::spawn(async move {
        if
            let Err(err) = ClusterActor::start(
                &self_node_name,
                listen_addr,
                seeds,
                &role,
                rx,
                ingest_tx,
            ).await
        {
            warn!("ClusterActor exited with error: {:?}", err);
        }
    });
    tx
}

fn extract_heartbeats(snapshot: &ClusterStateSnapshot) -> Vec<NodeStatus> {
    snapshot.node_states
        .iter()
        .filter_map(|node| {
            node.get("heartbeat").and_then(|val| { serde_json::from_str::<NodeStatus>(val).ok() })
        })
        .collect()
}

fn detect_imbalance(heartbeats: &[NodeStatus]) -> Option<(String, String)> {
    if heartbeats.len() < 2 {
        return None;
    }

    let mut sorted = heartbeats.to_vec();
    sorted.sort_by(|a, b| b.ingest_rate.cmp(&a.ingest_rate)); // descending

    let overloaded = &sorted[0];
    let underloaded = &sorted[sorted.len() - 1];

    let imbalance_ratio =
        (overloaded.ingest_rate as f64) / ((underloaded.ingest_rate as f64) + 1.0); // avoid /0

    if imbalance_ratio > 2.0 {
        Some((overloaded.node_id.clone(), underloaded.node_id.clone()))
    } else {
        None
    }
}
