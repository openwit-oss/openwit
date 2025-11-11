#![allow(dead_code)]

pub mod transfer_manager;
pub mod command_executor;
pub mod node_client;
pub mod node_server;
pub mod data_transfer_actor;
pub mod arrow_flight_transfer;
pub mod arrow_flight_service;
pub mod address_cache;

pub use transfer_manager::TransferManager;
pub use command_executor::CommandExecutor;
pub use node_client::InterNodeClient;
pub use node_server::InterNodeServer;
pub use data_transfer_actor::DataTransferActor;
pub use arrow_flight_service::{ArrowFlightService, ArrowFlightClient, ArrowFlightBatch};
pub use address_cache::{NodeAddressCache, NodeAddress};

// Re-export proto types
pub mod proto {
    tonic::include_proto!("inter_node");
}

