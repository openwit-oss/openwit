//! Resource monitoring for OpenWit components

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use std::collections::HashMap;

/// Monitors resource usage across components
pub struct ResourceMonitor {
    components: Arc<RwLock<HashMap<String, Arc<ComponentMetrics>>>>,
    start_time: Instant,
}

#[derive(Debug)]
pub struct ComponentMetrics {
    pub name: String,
    pub thread_count: AtomicUsize,
    pub cpu_time_ns: AtomicU64,
    pub memory_bytes: AtomicUsize,
    pub operations: AtomicU64,
    pub last_update: RwLock<Instant>,
}

impl ResourceMonitor {
    pub fn new() -> Self {
        Self {
            components: Arc::new(RwLock::new(HashMap::new())),
            start_time: Instant::now(),
        }
    }
    
    pub async fn register_component(&self, name: String) -> ComponentHandle {
        let metrics = Arc::new(ComponentMetrics {
            name: name.clone(),
            thread_count: AtomicUsize::new(0),
            cpu_time_ns: AtomicU64::new(0),
            memory_bytes: AtomicUsize::new(0),
            operations: AtomicU64::new(0),
            last_update: RwLock::new(Instant::now()),
        });
        
        self.components.write().await.insert(name.clone(), metrics.clone());
        
        ComponentHandle {
            metrics,
            monitor: self.clone(),
        }
    }
    
    pub async fn get_summary(&self) -> ResourceSummary {
        let components = self.components.read().await;
        let elapsed = self.start_time.elapsed();
        
        let mut total_threads = 0;
        let mut total_memory = 0;
        let mut total_operations = 0;
        let mut component_stats = Vec::new();
        
        for (name, metrics) in components.iter() {
            let threads = metrics.thread_count.load(Ordering::Relaxed);
            let memory = metrics.memory_bytes.load(Ordering::Relaxed);
            let ops = metrics.operations.load(Ordering::Relaxed);
            let cpu_ns = metrics.cpu_time_ns.load(Ordering::Relaxed);
            
            total_threads += threads;
            total_memory += memory;
            total_operations += ops;
            
            component_stats.push(ComponentSummary {
                name: name.clone(),
                threads,
                memory_mb: memory as f64 / 1_048_576.0,
                cpu_percent: (cpu_ns as f64 / elapsed.as_nanos() as f64) * 100.0,
                ops_per_sec: ops as f64 / elapsed.as_secs_f64(),
            });
        }
        
        ResourceSummary {
            elapsed,
            total_threads,
            total_memory_mb: total_memory as f64 / 1_048_576.0,
            total_ops_per_sec: total_operations as f64 / elapsed.as_secs_f64(),
            components: component_stats,
        }
    }
}

impl Clone for ResourceMonitor {
    fn clone(&self) -> Self {
        Self {
            components: self.components.clone(),
            start_time: self.start_time,
        }
    }
}

#[allow(dead_code)]
pub struct ComponentHandle {
    metrics: Arc<ComponentMetrics>,
    monitor: ResourceMonitor,
}

impl ComponentHandle {
    pub fn add_thread(&self) {
        self.metrics.thread_count.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn remove_thread(&self) {
        self.metrics.thread_count.fetch_sub(1, Ordering::Relaxed);
    }
    
    pub fn set_memory(&self, bytes: usize) {
        self.metrics.memory_bytes.store(bytes, Ordering::Relaxed);
    }
    
    pub fn add_cpu_time(&self, nanos: u64) {
        self.metrics.cpu_time_ns.fetch_add(nanos, Ordering::Relaxed);
    }
    
    pub fn increment_operations(&self, count: u64) {
        self.metrics.operations.fetch_add(count, Ordering::Relaxed);
    }
    
    pub async fn update(&self) {
        *self.metrics.last_update.write().await = Instant::now();
    }
}

#[derive(Debug)]
pub struct ResourceSummary {
    pub elapsed: Duration,
    pub total_threads: usize,
    pub total_memory_mb: f64,
    pub total_ops_per_sec: f64,
    pub components: Vec<ComponentSummary>,
}

#[derive(Debug)]
pub struct ComponentSummary {
    pub name: String,
    pub threads: usize,
    pub memory_mb: f64,
    pub cpu_percent: f64,
    pub ops_per_sec: f64,
}

impl ResourceSummary {
    pub fn print(&self) {
        println!("\n=== Resource Usage Summary ===");
        println!("Elapsed: {:?}", self.elapsed);
        println!("Total Threads: {}", self.total_threads);
        println!("Total Memory: {:.1} MB", self.total_memory_mb);
        println!("Total Throughput: {:.0} ops/sec", self.total_ops_per_sec);
        println!("\nPer Component:");
        println!("{:<20} {:>8} {:>10} {:>10} {:>12}", 
                 "Component", "Threads", "Memory MB", "CPU %", "Ops/sec");
        println!("{:-<60}", "");
        
        for comp in &self.components {
            println!("{:<20} {:>8} {:>10.1} {:>10.1} {:>12.0}",
                     comp.name,
                     comp.threads,
                     comp.memory_mb,
                     comp.cpu_percent,
                     comp.ops_per_sec);
        }
    }
}

/// Get thread count for current process
pub fn get_thread_count() -> usize {
    #[cfg(target_os = "linux")]
    {
        use std::fs;
        
        fs::read_dir("/proc/self/task")
            .map(|entries| entries.count())
            .unwrap_or(1)
    }
    
    #[cfg(not(target_os = "linux"))]
    {
        1 // Default fallback
    }
}

/// Get CPU time for current thread
pub fn get_thread_cpu_time() -> Duration {
    #[cfg(target_os = "linux")]
    {
        use libc::{clock_gettime, timespec, CLOCK_THREAD_CPUTIME_ID};
        
        let mut ts = timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        
        unsafe {
            if clock_gettime(CLOCK_THREAD_CPUTIME_ID, &mut ts) == 0 {
                Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
            } else {
                Duration::ZERO
            }
        }
    }
    
    #[cfg(not(target_os = "linux"))]
    {
        Duration::ZERO
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_resource_monitoring() {
        let monitor = ResourceMonitor::new();
        
        // Register components
        let wal_handle = monitor.register_component("WAL".to_string()).await;
        let index_handle = monitor.register_component("Indexer".to_string()).await;
        
        // Simulate resource usage
        wal_handle.add_thread();
        wal_handle.set_memory(128 * 1024 * 1024); // 128MB
        wal_handle.increment_operations(1000);
        
        index_handle.add_thread();
        index_handle.add_thread();
        index_handle.set_memory(512 * 1024 * 1024); // 512MB
        index_handle.increment_operations(5000);
        
        // Get summary
        let summary = monitor.get_summary().await;
        summary.print();
        
        assert_eq!(summary.total_threads, 3);
        assert_eq!(summary.components.len(), 2);
    }
}