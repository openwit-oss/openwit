use tonic::transport::{Channel, Server};
use std::time::Duration;
use anyhow::Result;

/// Configure gRPC server for high-performance scenarios
pub fn configure_high_performance_server() -> Server {
    Server::builder()
        // Connection settings
        .initial_stream_window_size(10 * 1024 * 1024)      // 10MB per stream
        .initial_connection_window_size(100 * 1024 * 1024) // 100MB per connection
        .max_frame_size(16 * 1024 * 1024)                  // 16MB max frame
        
        // Concurrency
        .max_concurrent_streams(Some(10000))                // High concurrency
        
        // Timeouts
        .timeout(Duration::from_secs(30))
        .tcp_keepalive(Some(Duration::from_secs(10)))
        .tcp_nodelay(true)
        
        // HTTP2 settings
        .http2_keepalive_interval(Some(Duration::from_secs(10)))
        .http2_keepalive_timeout(Some(Duration::from_secs(5)))
        .http2_adaptive_window(Some(true))
        
        // Accept more connections
        .accept_http1(false)  // HTTP2 only for performance
}

/// Configure gRPC channel for high-performance client
pub async fn configure_high_performance_channel(uri: String) -> Result<Channel> {
    let channel = Channel::from_shared(uri)?
        .initial_stream_window_size(Some(10 * 1024 * 1024))      // 10MB per stream
        .initial_connection_window_size(Some(100 * 1024 * 1024)) // 100MB per connection
        
        // Connection settings
        .tcp_keepalive(Some(Duration::from_secs(10)))
        .http2_keep_alive_interval(Duration::from_secs(10))
        .keep_alive_timeout(Duration::from_secs(5))
        .tcp_nodelay(true)
        
        // Timeouts
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(5))
        
        // Performance
        .http2_adaptive_window(true)
        .buffer_size(1024 * 1024)  // 1MB buffer
        
        .connect()
        .await?;
        
    Ok(channel)
}

/// gRPC interceptor for performance monitoring
pub mod performance_interceptor {
    use tonic::{Request, Status};
    use std::time::Instant;
    
    pub fn measure_request_time<T>(mut req: Request<T>) -> Result<Request<T>, Status> {
        let start = Instant::now();
        req.extensions_mut().insert(start);
        Ok(req)
    }
    
    pub fn log_request_time<T>(response: Result<T, Status>) -> Result<T, Status> {
        // Log performance metrics here if needed
        response
    }
}

/// Optimized settings for different scenarios
pub mod presets {
    use super::*;
    
    /// Ultra-low latency configuration (sub-millisecond)
    pub fn ultra_low_latency_server() -> Server {
        Server::builder()
            .initial_stream_window_size(1024 * 1024)        // 1MB
            .initial_connection_window_size(10 * 1024 * 1024) // 10MB
            .max_concurrent_streams(Some(1000))
            .timeout(Duration::from_millis(100))            // 100ms timeout
            .tcp_nodelay(true)
            .http2_adaptive_window(Some(false))                   // Predictable performance
    }
    
    /// High-throughput configuration (millions of messages)
    pub fn high_throughput_server() -> Server {
        Server::builder()
            .initial_stream_window_size(100 * 1024 * 1024)    // 100MB
            .initial_connection_window_size(1024 * 1024 * 1024) // 1GB
            .max_frame_size(16 * 1024 * 1024)                 // 16MB frames
            .max_concurrent_streams(Some(50000))               // Very high concurrency
            .timeout(Duration::from_secs(60))
            .tcp_nodelay(true)
            .http2_adaptive_window(Some(true))
            .http2_keepalive_interval(Some(Duration::from_secs(30)))
    }
    
    /// Balanced configuration
    pub fn balanced_server() -> Server {
        Server::builder()
            .initial_stream_window_size(10 * 1024 * 1024)
            .initial_connection_window_size(50 * 1024 * 1024)
            .max_concurrent_streams(Some(5000))
            .timeout(Duration::from_secs(30))
            .tcp_nodelay(true)
            .http2_adaptive_window(Some(true))
    }
}