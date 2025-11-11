// pub mod opentelemetry {
//     pub mod proto {
//         pub mod common {
//             pub mod v1 {
//                 include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.common.v1.rs"));
//             }
//         }
//         pub mod resource {
//             pub mod v1 {
//                 include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.resource.v1.rs"));
//             }
//         }
//         pub mod logs {
//             pub mod v1 {
//                 include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.logs.v1.rs"));
//             }
//         }
//         pub mod collector {
//             pub mod logs {
//                 pub mod v1 {
//                     include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.collector.logs.v1.rs"));
//                 }
//             }
//         }
//     }
// }
// The generated modules include the gRPC server and client code for LogsService,
// as well as all message types (ExportLogsServiceRequest/Response, LogRecord, etc).
// Future: Additional OTLP protos (metrics, trace) can be added and compiled similarly.

// Re-export prost_types so generated code can find it
pub use prost_types;

pub mod control {
    tonic::include_proto!("openwit.control");
}

pub mod ingestion {
    tonic::include_proto!("openwit.ingestion");
}

pub mod storage {
    tonic::include_proto!("openwit.storage");
}

// File descriptor set for reflection
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("control_descriptor");