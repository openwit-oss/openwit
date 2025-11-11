use std::net::SocketAddr;
use std::time::Duration;
use anyhow::Result;
use tokio::time::sleep;
use tracing::{info};
use tracing_subscriber;

use openwit_network::{ClusterRuntime, ClusterHandleLeaderExt, LeaderInfo};

/// Example showing how nodes become aware of their leader status
#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    
    info!("=== Leader Awareness Example ===");
    info!("");
    info!("This example shows how nodes discover their leader status through gossip");
    info!("");
    
    // Start control node first (it will elect leaders)
    let control_handle = tokio::spawn(async move {
        let control_runtime = ClusterRuntime::start(
            "control-1",
            "127.0.0.1:9000".parse().unwrap(),
            vec![],
            "control",
        ).await.unwrap();
        
        let control_handle = control_runtime.handle();
        
        // Keep running
        loop {
            sleep(Duration::from_secs(60)).await;
        }
    });
    
    sleep(Duration::from_secs(2)).await;
    
    // Start 3 ingest nodes
    let mut ingest_handles = vec![];
    
    for i in 1..=3 {
        let handle = tokio::spawn(async move {
            let node_id = format!("ingest-{}", i);
            let port = 9000 + i;
            let listen_addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
            
            info!("🚀 Starting {} on port {}", node_id, port);
            
            // Create cluster runtime
            let runtime = ClusterRuntime::start(
                &node_id,
                listen_addr,
                vec!["127.0.0.1:9000".parse().unwrap()], // Connect to control
                "ingest",
            ).await.unwrap();
            
            // Get cluster handle with leader awareness
            let (cluster_handle, leader_awareness) = runtime.handle()
                .with_leader_awareness(node_id.clone(), "ingest".to_string());
            
            // Simulate the control plane announcing this node as leader
            if i == 1 {
                sleep(Duration::from_secs(5)).await;
                info!("📢 Control plane announcing {} as leader", node_id);
                
                let announcement = LeaderInfo {
                    role_group: "ingest_nodes".to_string(),
                    leader_node_id: node_id.clone(),
                    term: 1,
                    elected_at: chrono::Utc::now().to_rfc3339(),
                };
                
                leader_awareness.handle_leader_announcement(announcement).await.unwrap();
            }
            
            // Monitor leader status
            let mut was_leader = false;
            loop {
                sleep(Duration::from_secs(3)).await;
                
                let is_leader = leader_awareness.is_leader().await;
                
                if is_leader != was_leader {
                    if is_leader {
                        info!("✨ {} became LEADER - starting leader tasks", node_id);
                        
                        // Leader-specific tasks
                        cluster_handle.set_self_kv("partition_assignments", "managing").await;
                        cluster_handle.set_self_kv("load_balancing", "active").await;
                    } else {
                        info!("📍 {} is FOLLOWER - ready for assignments", node_id);
                        
                        // Follower-specific behavior
                        cluster_handle.set_self_kv("partition_assignments", "accepting").await;
                        cluster_handle.set_self_kv("load_balancing", "passive").await;
                    }
                    was_leader = is_leader;
                }
                
                // Show role group status
                if is_leader {
                    let status = leader_awareness.get_role_group_status().await;
                    info!("📊 Role group status from leader {}:", node_id);
                    for (node, is_leader) in status {
                        info!("   - {}: {}", node, if is_leader { "LEADER" } else { "FOLLOWER" });
                    }
                }
            }
        });
        
        ingest_handles.push(handle);
        sleep(Duration::from_secs(1)).await; // Stagger node starts
    }
    
    // Let the example run
    sleep(Duration::from_secs(30)).await;
    
    info!("");
    info!("🎯 Example complete! Key takeaways:");
    info!("   1. Nodes monitor gossip to discover their leader status");
    info!("   2. Control plane announces leaders via gossip");
    info!("   3. Nodes react differently based on leader/follower role");
    info!("   4. Leader nodes can see status of all nodes in their group");
    
    Ok(())
}

/// Example of how different node types handle leader responsibilities
async fn demonstrate_role_specific_leadership() {
    info!("=== Role-Specific Leader Behaviors ===");
    
    // Ingest Leader
    info!("📥 INGEST Leader responsibilities:");
    info!("   - Assigns Kafka partitions to follower nodes");
    info!("   - Monitors ingestion load and rebalances");
    info!("   - Coordinates buffer flushes");
    
    // Search Leader  
    info!("🔍 SEARCH Leader responsibilities:");
    info!("   - Distributes queries across followers");
    info!("   - Aggregates search results");
    info!("   - Manages query cache");
    
    // Janitor Leader
    info!("🧹 JANITOR Leader responsibilities:");
    info!("   - Schedules cleanup tasks");
    info!("   - Assigns compaction jobs to followers");
    info!("   - Monitors disk usage across nodes");
    
    // Indexer Leader
    info!("📇 INDEXER Leader responsibilities:");
    info!("   - Assigns indexing jobs to followers");
    info!("   - Monitors indexing progress");
    info!("   - Handles index segment merging");
    
    // Storage Leader
    info!("💾 STORAGE Leader responsibilities:");
    info!("   - Decides data placement strategy");
    info!("   - Coordinates replication");
    info!("   - Handles storage tier transitions");
}