use std::sync::Arc;
use anyhow::Result;
use axum::{
    Router,
    routing::{get, any},
    extract::{State, Json, Path, Query, Request},
    response::{IntoResponse, Response},
    http::{StatusCode, HeaderMap, header},
    body::Body,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn, error};
use reqwest::Client;
use tokio::time::timeout;

use crate::control_plane_client::{ProxyControlPlaneClient};
use crate::health_aware_router::HealthAwareRouter;

/// Pass-through HTTP handler that routes requests directly without buffering
pub struct PassthroughHttpHandler {
    node_id: String,
    control_client: Arc<ProxyControlPlaneClient>,
    health_router: Option<Arc<HealthAwareRouter>>,
    http_client: Client,
    /// Timeout for upstream requests
    request_timeout: std::time::Duration,
    /// Statistics
    stats: Arc<tokio::sync::RwLock<HandlerStats>>,
}

#[derive(Debug, Default)]
struct HandlerStats {
    total_requests: u64,
    successful_requests: u64,
    failed_requests: u64,
    bytes_proxied: u64,
}

#[derive(Debug, Serialize)]
pub struct ProxyResponse {
    pub success: bool,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_node: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ProxyQuery {
    /// Preferred upstream node (optional)
    pub prefer_node: Option<String>,
    /// Maximum retries
    pub max_retries: Option<u32>,
}

impl PassthroughHttpHandler {
    pub fn new(
        node_id: String,
        control_client: Arc<ProxyControlPlaneClient>,
    ) -> Self {
        let http_client = Client::builder()
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("Failed to build HTTP client");
            
        Self {
            node_id,
            control_client,
            health_router: None,
            http_client,
            request_timeout: std::time::Duration::from_secs(30),
            stats: Arc::new(tokio::sync::RwLock::new(HandlerStats::default())),
        }
    }
    
    /// Set the health-aware router for round-robin load balancing
    pub fn set_health_router(&mut self, router: Arc<HealthAwareRouter>) {
        self.health_router = Some(router);
    }
    
    /// Create Axum router for pass-through proxy
    pub fn create_router(self: Arc<Self>) -> Router {
        Router::new()
            // Specific ingestion routes
            .route("/ingest", any(Self::proxy_ingest))
            .route("/ingest/*path", any(Self::proxy_ingest_path))
            
            // Health and stats
            .route("/health", get(Self::health_check))
            .route("/stats", get(Self::get_stats))
            
            // Catch-all proxy route
            .route("/*path", any(Self::proxy_request))
            
            .with_state(self)
    }
    
    /// Proxy ingestion requests to healthy upstream services
    async fn proxy_ingest(
        State(handler): State<Arc<PassthroughHttpHandler>>,
        Query(params): Query<ProxyQuery>,
        headers: HeaderMap,
        request: Request,
    ) -> Result<impl IntoResponse, ProxyError> {
        handler.proxy_to_upstream("/ingest", params, headers, request).await
    }
    
    /// Proxy ingestion requests with path
    async fn proxy_ingest_path(
        State(handler): State<Arc<PassthroughHttpHandler>>,
        Path(path): Path<String>,
        Query(params): Query<ProxyQuery>,
        headers: HeaderMap,
        request: Request,
    ) -> Result<impl IntoResponse, ProxyError> {
        let full_path = format!("/ingest/{}", path);
        handler.proxy_to_upstream(&full_path, params, headers, request).await
    }
    
    /// Generic proxy for any path
    async fn proxy_request(
        State(handler): State<Arc<PassthroughHttpHandler>>,
        Path(path): Path<String>,
        Query(params): Query<ProxyQuery>,
        headers: HeaderMap,
        request: Request,
    ) -> Result<impl IntoResponse, ProxyError> {
        let full_path = format!("/{}", path);
        handler.proxy_to_upstream(&full_path, params, headers, request).await
    }
    
    /// Core proxy logic
    async fn proxy_to_upstream(
        &self,
        path: &str,
        params: ProxyQuery,
        headers: HeaderMap,
        request: Request,
    ) -> Result<Response, ProxyError> {
        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.total_requests += 1;
        }
        
        // Extract request parts
        let method = request.method().clone();
        let body_bytes = axum::body::to_bytes(request.into_body(), usize::MAX).await
            .map_err(|e| ProxyError::RequestBody(e.to_string()))?;
            
        let max_retries = params.max_retries.unwrap_or(2);
        let mut last_error = None;
        let mut tried_nodes = std::collections::HashSet::new();
        
        // Try up to max_retries times
        for attempt in 0..=max_retries {
            // Select target service for this attempt
            let target = if attempt == 0 && params.prefer_node.is_some() {
                // First attempt with preferred node
                let services = self.control_client.get_healthy_http_services().await
                    .map_err(|e| ProxyError::ServiceDiscovery(e.to_string()))?;
                    
                services.iter()
                    .find(|s| &s.node_id == params.prefer_node.as_ref().unwrap())
                    .cloned()
                    .or_else(|| {
                        // Preferred node not available, use router
                        None
                    })
            } else {
                None
            };
            
            // If no preferred target or not first attempt, use router
            let target = if let Some(target) = target {
                target
            } else if let Some(router) = &self.health_router {
                // Use health-aware router for proper round-robin distribution
                match router.select_http_service(None).await {
                    Ok(t) => {
                        // Skip if we already tried this node
                        if tried_nodes.contains(&t.node_id) && attempt < max_retries {
                            continue;
                        }
                        t
                    }
                    Err(e) => {
                        if attempt == 0 {
                            return Err(ProxyError::ServiceDiscovery(e.to_string()));
                        } else {
                            // No more services available for retry
                            break;
                        }
                    }
                }
            } else {
                // Fallback to control client's direct method
                let services = self.control_client.get_healthy_http_services().await
                    .map_err(|e| ProxyError::ServiceDiscovery(e.to_string()))?;
                    
                if services.is_empty() {
                    return Err(ProxyError::NoHealthyServices);
                }
                
                // Filter out already tried nodes
                let available: Vec<_> = services.iter()
                    .filter(|s| !tried_nodes.contains(&s.node_id))
                    .collect();
                    
                if available.is_empty() && attempt > 0 {
                    // All nodes tried
                    break;
                }
                
                let candidates = if available.is_empty() { &services } else { &available.into_iter().cloned().collect() };
                
                ProxyControlPlaneClient::select_best_endpoint(candidates)
                    .ok_or(ProxyError::NoHealthyServices)?
                    .clone()
            };
            
            // Mark this node as tried
            tried_nodes.insert(target.node_id.clone());
            
            debug!("Proxying request to {} -> {} (attempt {})", path, target.endpoint, attempt + 1);
            
            // Ensure endpoint has http:// prefix
            let upstream_url = if target.endpoint.starts_with("http://") || target.endpoint.starts_with("https://") {
                format!("{}{}", target.endpoint, path)
            } else {
                format!("http://{}{}", target.endpoint, path)
            };
            
            // Build upstream request
            let mut upstream_req = self.http_client
                .request(method.clone(), &upstream_url)
                .body(body_bytes.clone());
                
            // Forward relevant headers
            for (key, value) in headers.iter() {
                if !is_hop_by_hop_header(key) {
                    upstream_req = upstream_req.header(key, value);
                }
            }
            
            // Add proxy headers
            upstream_req = upstream_req
                .header("X-Forwarded-For", &self.node_id)
                .header("X-Proxy-Node", &self.node_id)
                .header("X-Upstream-Node", &target.node_id);
            
            // Send request with timeout
            match timeout(self.request_timeout, upstream_req.send()).await {
                Ok(Ok(response)) => {
                    // Update success stats
                    {
                        let mut stats = self.stats.write().await;
                        stats.successful_requests += 1;
                        stats.bytes_proxied += body_bytes.len() as u64;
                    }
                    
                    // Report success to health router if available
                    if let Some(router) = &self.health_router {
                        router.report_success(&target.node_id).await;
                    }
                    
                    // Convert response
                    let status = response.status();
                    let headers = response.headers().clone();
                    let body = response.bytes().await
                        .map_err(|e| ProxyError::ResponseBody(e.to_string()))?;
                    
                    // Build response
                    let mut builder = Response::builder().status(status);
                    
                    // Forward response headers
                    for (key, value) in headers.iter() {
                        if !is_hop_by_hop_header(key) {
                            builder = builder.header(key, value);
                        }
                    }
                    
                    // Add proxy response headers
                    builder = builder
                        .header("X-Proxy-Node", &self.node_id)
                        .header("X-Upstream-Node", &target.node_id);
                    
                    return builder
                        .body(Body::from(body))
                        .map_err(|e| ProxyError::Internal(e.to_string()));
                }
                Ok(Err(e)) => {
                    warn!("Upstream request failed (attempt {}): {}", attempt + 1, e);
                    last_error = Some(e.to_string());
                    
                    // Report failure to health router if available
                    if let Some(router) = &self.health_router {
                        router.report_failure(&target.node_id, "http").await;
                    }
                }
                Err(_) => {
                    warn!("Upstream request timed out (attempt {})", attempt + 1);
                    last_error = Some("Request timeout".to_string());
                    
                    // Report failure to health router if available
                    if let Some(router) = &self.health_router {
                        router.report_failure(&target.node_id, "http").await;
                    }
                }
            }
        }
        
        // All attempts failed
        {
            let mut stats = self.stats.write().await;
            stats.failed_requests += 1;
        }
        
        Err(ProxyError::UpstreamError(
            last_error.unwrap_or_else(|| "All retry attempts failed".to_string())
        ))
    }
    
    /// Health check endpoint
    async fn health_check(
        State(handler): State<Arc<PassthroughHttpHandler>>,
    ) -> impl IntoResponse {
        let services = handler.control_client.get_healthy_http_services().await
            .unwrap_or_default();
            
        let stats = handler.stats.read().await;
        
        Json(serde_json::json!({
            "status": if services.is_empty() { "degraded" } else { "healthy" },
            "node_id": handler.node_id,
            "proxy_type": "passthrough",
            "available_services": services.len(),
            "total_requests": stats.total_requests,
            "success_rate": if stats.total_requests > 0 {
                (stats.successful_requests as f64 / stats.total_requests as f64) * 100.0
            } else {
                0.0
            },
        }))
    }
    
    /// Statistics endpoint
    async fn get_stats(
        State(handler): State<Arc<PassthroughHttpHandler>>,
    ) -> impl IntoResponse {
        let stats = handler.stats.read().await;
        let services = handler.control_client.get_healthy_http_services().await
            .unwrap_or_default();
        
        Json(serde_json::json!({
            "node_id": handler.node_id,
            "proxy_stats": {
                "total_requests": stats.total_requests,
                "successful_requests": stats.successful_requests,
                "failed_requests": stats.failed_requests,
                "bytes_proxied": stats.bytes_proxied,
                "success_rate": if stats.total_requests > 0 {
                    (stats.successful_requests as f64 / stats.total_requests as f64) * 100.0
                } else {
                    0.0
                },
            },
            "upstream_services": services.iter().map(|s| {
                serde_json::json!({
                    "node_id": s.node_id,
                    "endpoint": s.endpoint,
                    "health_score": s.health_score,
                    "cpu_percent": s.cpu_percent,
                    "memory_percent": s.memory_percent,
                })
            }).collect::<Vec<_>>(),
        }))
    }
    
    /// Get current handler statistics
    pub async fn get_handler_stats(&self) -> (u64, u64, u64, u64) {
        let stats = self.stats.read().await;
        (
            stats.total_requests,
            stats.successful_requests,
            stats.failed_requests,
            stats.bytes_proxied
        )
    }
}

/// Check if a header is hop-by-hop and shouldn't be forwarded
fn is_hop_by_hop_header(name: &header::HeaderName) -> bool {
    matches!(
        name,
        &header::CONNECTION
        | &header::PROXY_AUTHENTICATE
        | &header::PROXY_AUTHORIZATION
        | &header::TE
        | &header::TRAILER
        | &header::TRANSFER_ENCODING
        | &header::UPGRADE
    )
}

/// Error types for proxy operations
#[derive(Debug)]
enum ProxyError {
    ServiceDiscovery(String),
    NoHealthyServices,
    RequestBody(String),
    ResponseBody(String),
    UpstreamError(String),
    Internal(String),
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ProxyError::ServiceDiscovery(msg) => {
                (StatusCode::SERVICE_UNAVAILABLE, format!("Service discovery failed: {}", msg))
            }
            ProxyError::NoHealthyServices => {
                (StatusCode::SERVICE_UNAVAILABLE, "No healthy upstream services available".to_string())
            }
            ProxyError::RequestBody(msg) => {
                (StatusCode::BAD_REQUEST, format!("Failed to read request body: {}", msg))
            }
            ProxyError::ResponseBody(msg) => {
                (StatusCode::BAD_GATEWAY, format!("Failed to read response body: {}", msg))
            }
            ProxyError::UpstreamError(msg) => {
                (StatusCode::BAD_GATEWAY, format!("Upstream error: {}", msg))
            }
            ProxyError::Internal(msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("Internal error: {}", msg))
            }
        };
        
        error!("Proxy error: {}", message);
        
        (status, Json(ProxyResponse {
            success: false,
            message,
            upstream_node: None,
        })).into_response()
    }
}