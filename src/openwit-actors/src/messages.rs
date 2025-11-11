use serde::{Serialize, Deserialize};

/// Message emitted by monitor actors when resource usage crosses threshold
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceReportMessage {
    pub resource: ResourceType,
    pub usage_percent: u8,
    pub threshold_percent: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResourceType {
    Cpu,
    Memory,
    Swap,
    Threads,
}

/// Message sent by DecisionActor to trigger a scale-up
#[derive(Debug, Clone)]
pub struct ScaleCommand {
    pub reason: String,
    pub resource: ResourceType,
    pub usage_percent: u8,
}


