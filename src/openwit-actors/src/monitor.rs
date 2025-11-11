use tokio::sync::mpsc::Sender;
use tokio::time::{sleep, Duration};

use crate::messages::{ResourceReportMessage, ResourceType};
use openwit_metrics::{
    SYSTEM_CPU_GAUGE, SYSTEM_MEM_GAUGE, SYSTEM_MEMORY_TOTAL_GAUGE,
    SYSTEM_SWAP_TOTAL_GAUGE, SYSTEM_SWAP_USED_GAUGE,
};

pub async fn start_monitor_actor(report_tx: Sender<ResourceReportMessage>, threshold: u8) {
    loop {
        // CPU
        let cpu = SYSTEM_CPU_GAUGE.get() as u8;
        if cpu > threshold {
            let _ = report_tx.send(ResourceReportMessage {
                resource: ResourceType::Cpu,
                usage_percent: cpu,
                threshold_percent: threshold,
            }).await;
        }

        // Memory
        let total = SYSTEM_MEMORY_TOTAL_GAUGE.get() as f64;
        let used = SYSTEM_MEM_GAUGE.get() as f64;
        let mem_percent = if total > 0.0 { (used / total * 100.0) as u8 } else { 0 };
        if mem_percent > threshold {
            let _ = report_tx.send(ResourceReportMessage {
                resource: ResourceType::Memory,
                usage_percent: mem_percent,
                threshold_percent: threshold,
            }).await;
        }

        // Swap
        let swap_total = SYSTEM_SWAP_TOTAL_GAUGE.get() as f64;
        let swap_used = SYSTEM_SWAP_USED_GAUGE.get() as f64;
        let swap_percent = if swap_total > 0.0 { (swap_used / swap_total * 100.0) as u8 } else { 0 };
        if swap_percent > threshold {
            let _ = report_tx.send(ResourceReportMessage {
                resource: ResourceType::Swap,
                usage_percent: swap_percent,
                threshold_percent: threshold,
            }).await;
        }

        // TODO: Add thread monitoring if needed

        sleep(Duration::from_secs(5)).await;
    }
}
