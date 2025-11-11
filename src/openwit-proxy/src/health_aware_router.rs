use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use anyhow::Result;
use tokio::sync::RwLock;
use tracing::{info, debug, warn};

use crate::control_plane_client::{ProxyControlPlaneClient, ServiceEndpoint, KafkaEndpoint};

/// Health-aware router that manages routing decisions for all service types
pub struct HealthAwareRouter {
    node_id: String,
    control_client: Arc<ProxyControlPlaneClient>,
    /// Routing strategies
    routing_strategy: RoutingStrategy,
    /// Circuit breaker states for services
    circuit_breakers: Arc<RwLock<std::collections::HashMap<String, CircuitBreaker>>>,
    /// Global statistics
    stats: Arc<RouterStats>,
}

#[derive(Debug, Clone)]
pub enum RoutingStrategy {
    /// Round-robin across healthy services
    RoundRobin,
    /// Least connections/load
    LeastLoad,
    /// Weighted based on health score
    WeightedHealth,
    /// Consistent hashing for sticky sessions
    ConsistentHash,
}

#[derive(Debug)]
struct CircuitBreaker {
    node_id: String,
    failures: u32,
    last_failure: Option<std::time::Instant>,
    state: CircuitState,
}

#[derive(Debug, Clone, Copy)]
enum CircuitState {
    Closed,    // Normal operation
    Open,      // Failing, reject requests
    HalfOpen,  // Testing if recovered
}

#[derive(Debug, Default)]
struct RouterStats {
    http_requests: AtomicU64,
    grpc_requests: AtomicU64,
    kafka_messages: AtomicU64,
    routing_failures: AtomicU64,
    circuit_breaker_trips: AtomicU64,
}

impl HealthAwareRouter {
    pub fn new(
        node_id: String,
        control_client: Arc<ProxyControlPlaneClient>,
        routing_strategy: RoutingStrategy,
    ) -> Self {
        Self {
            node_id,
            control_client,
            routing_strategy,
            circuit_breakers: Arc::new(RwLock::new(std::collections::HashMap::new())),
            stats: Arc::new(RouterStats::default()),
        }
    }
    
    /// Select the best HTTP service endpoint
    pub async fn select_http_service(&self, request_hash: Option<u64>) -> Result<ServiceEndpoint> {
        self.stats.http_requests.fetch_add(1, Ordering::Relaxed);
        
        let services = self.control_client.get_healthy_http_services().await?;
        if services.is_empty() {
            self.stats.routing_failures.fetch_add(1, Ordering::Relaxed);
            return Err(anyhow::anyhow!("No healthy HTTP services available"));
        }
        
        // Filter out services with open circuit breakers
        let available_services = self.filter_by_circuit_state(services).await;
        if available_services.is_empty() {
            self.stats.routing_failures.fetch_add(1, Ordering::Relaxed);
            return Err(anyhow::anyhow!("All HTTP services have open circuit breakers"));
        }
        
        // Apply routing strategy
        let selected = match &self.routing_strategy {
            RoutingStrategy::RoundRobin => {
                self.round_robin_select(&available_services)
            }
            RoutingStrategy::LeastLoad => {
                self.least_load_select(&available_services)
            }
            RoutingStrategy::WeightedHealth => {
                self.weighted_health_select(&available_services)
            }
            RoutingStrategy::ConsistentHash => {
                self.consistent_hash_select(&available_services, request_hash.unwrap_or(0))
            }
        };
        
        debug!("Selected HTTP service: {} (strategy: {:?})", 
            selected.node_id, self.routing_strategy);
        
        Ok(selected)
    }
    
    /// Select the best gRPC service endpoint
    pub async fn select_grpc_service(&self, method_hash: Option<u64>) -> Result<ServiceEndpoint> {
        self.stats.grpc_requests.fetch_add(1, Ordering::Relaxed);
        
        let services = self.control_client.get_healthy_grpc_services().await?;
        if services.is_empty() {
            self.stats.routing_failures.fetch_add(1, Ordering::Relaxed);
            return Err(anyhow::anyhow!("No healthy gRPC services available"));
        }
        
        let available_services = self.filter_by_circuit_state(services).await;
        if available_services.is_empty() {
            self.stats.routing_failures.fetch_add(1, Ordering::Relaxed);
            return Err(anyhow::anyhow!("All gRPC services have open circuit breakers"));
        }
        
        let selected = match &self.routing_strategy {
            RoutingStrategy::RoundRobin => self.round_robin_select(&available_services),
            RoutingStrategy::LeastLoad => self.least_load_select(&available_services),
            RoutingStrategy::WeightedHealth => self.weighted_health_select(&available_services),
            RoutingStrategy::ConsistentHash => {
                self.consistent_hash_select(&available_services, method_hash.unwrap_or(0))
            }
        };
        
        debug!("Selected gRPC service: {} (strategy: {:?})", 
            selected.node_id, self.routing_strategy);
        
        Ok(selected)
    }
    
    /// Select the best Kafka broker
    pub async fn select_kafka_broker(&self, topic_hash: Option<u64>) -> Result<KafkaEndpoint> {
        self.stats.kafka_messages.fetch_add(1, Ordering::Relaxed);
        
        let brokers = self.control_client.get_healthy_kafka_brokers().await?;
        if brokers.is_empty() {
            self.stats.routing_failures.fetch_add(1, Ordering::Relaxed);
            return Err(anyhow::anyhow!("No healthy Kafka brokers available"));
        }
        
        // For Kafka, we typically use consistent hashing based on topic
        let index = if let Some(hash) = topic_hash {
            (hash as usize) % brokers.len()
        } else {
            rand::random::<usize>() % brokers.len()
        };
        
        Ok(brokers[index].clone())
    }
    
    /// Report a service failure
    pub async fn report_failure(&self, node_id: &str, service_type: &str) {
        let mut breakers = self.circuit_breakers.write().await;
        let breaker = breakers.entry(node_id.to_string()).or_insert_with(|| {
            CircuitBreaker {
                node_id: node_id.to_string(),
                failures: 0,
                last_failure: None,
                state: CircuitState::Closed,
            }
        });
        
        breaker.failures += 1;
        breaker.last_failure = Some(std::time::Instant::now());
        
        // Trip circuit breaker after 5 consecutive failures
        if breaker.failures >= 5 && matches!(breaker.state, CircuitState::Closed) {
            breaker.state = CircuitState::Open;
            self.stats.circuit_breaker_trips.fetch_add(1, Ordering::Relaxed);
            
            warn!("Circuit breaker OPEN for {} service: {} (failures: {})", 
                service_type, node_id, breaker.failures);
            
            // Schedule half-open state after cooldown
            let node_id = node_id.to_string();
            let breakers_clone = self.circuit_breakers.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                
                let mut breakers = breakers_clone.write().await;
                if let Some(breaker) = breakers.get_mut(&node_id) {
                    if matches!(breaker.state, CircuitState::Open) {
                        breaker.state = CircuitState::HalfOpen;
                        info!("Circuit breaker HALF-OPEN for service: {}", node_id);
                    }
                }
            });
        }
    }
    
    /// Report a service success
    pub async fn report_success(&self, node_id: &str) {
        let mut breakers = self.circuit_breakers.write().await;
        if let Some(breaker) = breakers.get_mut(node_id) {
            match breaker.state {
                CircuitState::HalfOpen => {
                    // Successful request in half-open state, close the circuit
                    breaker.state = CircuitState::Closed;
                    breaker.failures = 0;
                    breaker.last_failure = None;
                    info!("Circuit breaker CLOSED for service: {}", node_id);
                }
                CircuitState::Closed => {
                    // Reset failure count on success
                    if breaker.failures > 0 {
                        breaker.failures = breaker.failures.saturating_sub(1);
                    }
                }
                _ => {}
            }
        }
    }
    
    /// Filter services by circuit breaker state
    async fn filter_by_circuit_state(&self, services: Vec<ServiceEndpoint>) -> Vec<ServiceEndpoint> {
        let breakers = self.circuit_breakers.read().await;
        
        services.into_iter()
            .filter(|service| {
                if let Some(breaker) = breakers.get(&service.node_id) {
                    !matches!(breaker.state, CircuitState::Open)
                } else {
                    true
                }
            })
            .collect()
    }
    
    /// Round-robin selection
    fn round_robin_select(&self, services: &[ServiceEndpoint]) -> ServiceEndpoint {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let index = (COUNTER.fetch_add(1, Ordering::Relaxed) as usize) % services.len();
        services[index].clone()
    }
    
    /// Least load selection
    fn least_load_select(&self, services: &[ServiceEndpoint]) -> ServiceEndpoint {
        services.iter()
            .min_by_key(|s| {
                // Combine CPU, memory, and connection count for load score
                let load_score = (s.cpu_percent * 0.3 + 
                                 s.memory_percent * 0.3 + 
                                 (s.active_connections as f64 / 1000.0) * 0.4) * 1000.0;
                load_score as u64
            })
            .cloned()
            .unwrap_or_else(|| services[0].clone())
    }
    
    /// Weighted selection based on health score
    fn weighted_health_select(&self, services: &[ServiceEndpoint]) -> ServiceEndpoint {
        use rand::prelude::*;
        
        // Calculate total weight
        let total_weight: f64 = services.iter().map(|s| s.health_score).sum();
        
        if total_weight == 0.0 {
            return services[0].clone();
        }
        
        // Random selection weighted by health score
        let mut rng = thread_rng();
        let mut random_weight = rng.gen::<f64>() * total_weight;
        
        for service in services {
            random_weight -= service.health_score;
            if random_weight <= 0.0 {
                return service.clone();
            }
        }
        
        services.last().unwrap().clone()
    }
    
    /// Consistent hash selection for sticky routing
    fn consistent_hash_select(&self, services: &[ServiceEndpoint], hash: u64) -> ServiceEndpoint {
        let index = (hash as usize) % services.len();
        services[index].clone()
    }
    
    /// Get router statistics
    pub async fn get_stats(&self) -> serde_json::Value {
        let breakers = self.circuit_breakers.read().await;
        let open_circuits: Vec<_> = breakers.values()
            .filter(|b| matches!(b.state, CircuitState::Open))
            .map(|b| &b.node_id)
            .collect();
            
        serde_json::json!({
            "node_id": self.node_id,
            "routing_strategy": format!("{:?}", self.routing_strategy),
            "requests": {
                "http": self.stats.http_requests.load(Ordering::Relaxed),
                "grpc": self.stats.grpc_requests.load(Ordering::Relaxed),
                "kafka": self.stats.kafka_messages.load(Ordering::Relaxed),
            },
            "failures": {
                "routing_failures": self.stats.routing_failures.load(Ordering::Relaxed),
                "circuit_breaker_trips": self.stats.circuit_breaker_trips.load(Ordering::Relaxed),
            },
            "circuit_breakers": {
                "total": breakers.len(),
                "open": open_circuits.len(),
                "open_services": open_circuits,
            },
        })
    }
    
    /// Start periodic circuit breaker cleanup
    pub async fn start_maintenance(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(300)); // 5 minutes
            
            loop {
                interval.tick().await;
                
                // Clean up old circuit breakers
                let mut breakers = self.circuit_breakers.write().await;
                breakers.retain(|_, breaker| {
                    // Keep if recently failed or not in closed state
                    if let Some(last_failure) = breaker.last_failure {
                        last_failure.elapsed() < std::time::Duration::from_secs(600) // 10 minutes
                    } else {
                        !matches!(breaker.state, CircuitState::Closed) || breaker.failures > 0
                    }
                });
                
                debug!("Circuit breaker cleanup: {} breakers remaining", breakers.len());
            }
        });
    }
}

/// Helper to compute hash for consistent routing
pub fn compute_hash(data: &str) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    hasher.finish()
}