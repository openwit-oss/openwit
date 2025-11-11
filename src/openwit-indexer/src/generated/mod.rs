// Generated protobuf code module

#[cfg(feature = "events")]
pub mod openwit {
    pub mod indexer {
        pub mod v1 {
            include!("openwit.indexer.v1.rs");
        }
    }
}

#[cfg(not(feature = "events"))]
pub mod openwit {
    pub mod indexer {
        pub mod v1 {
            // Empty module for when events feature is not enabled
        }
    }
}