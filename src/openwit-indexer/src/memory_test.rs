//! Memory usage testing utilities

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Tracking allocator to measure memory usage
pub struct TrackingAllocator {
    inner: System,
    allocated: Arc<AtomicUsize>,
    deallocated: Arc<AtomicUsize>,
}

impl TrackingAllocator {
    pub fn new() -> (Self, MemoryTracker) {
        let allocated = Arc::new(AtomicUsize::new(0));
        let deallocated = Arc::new(AtomicUsize::new(0));
        
        let allocator = Self {
            inner: System,
            allocated: allocated.clone(),
            deallocated: deallocated.clone(),
        };
        
        let tracker = MemoryTracker {
            allocated,
            deallocated,
        };
        
        (allocator, tracker)
    }
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ret = unsafe { self.inner.alloc(layout) };
        if !ret.is_null() {
            self.allocated.fetch_add(layout.size(), Ordering::Relaxed);
        }
        ret
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { self.inner.dealloc(ptr, layout) };
        self.deallocated.fetch_add(layout.size(), Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub struct MemoryTracker {
    allocated: Arc<AtomicUsize>,
    deallocated: Arc<AtomicUsize>,
}

impl MemoryTracker {
    pub fn current_usage(&self) -> usize {
        let alloc = self.allocated.load(Ordering::Relaxed);
        let dealloc = self.deallocated.load(Ordering::Relaxed);
        alloc.saturating_sub(dealloc)
    }
    
    pub fn total_allocated(&self) -> usize {
        self.allocated.load(Ordering::Relaxed)
    }
    
    pub fn reset(&self) {
        self.allocated.store(0, Ordering::Relaxed);
        self.deallocated.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;
    
    #[test]
    fn test_inverted_index_memory() {
        let (allocator, tracker) = TrackingAllocator::new();
        
        // Would need to set as global allocator in test binary
        tracker.reset();
        let start_mem = tracker.current_usage();
        
        let mut index = InvertedIndex::new();
        
        // Index 1M documents
        for i in 0..1_000_000 {
            let mut fields = HashMap::new();
            fields.insert("level".to_string(), 
                FieldValue::String(["ERROR", "WARN", "INFO", "DEBUG"][i % 4].to_string())
            );
            fields.insert("service".to_string(),
                FieldValue::String(format!("service-{}", i % 100))
            );
            
            let doc = Document {
                id: format!("doc-{}", i),
                timestamp: Utc::now(),
                fields,
                raw_size: 256,
            };
            
            index.index_single(doc);
        }
        
        let end_mem = tracker.current_usage();
        let used_memory = end_mem - start_mem;
        
        println!("Inverted index memory for 1M docs: {:.2} MB", used_memory as f64 / 1_048_576.0);
        println!("Memory per document: {} bytes", used_memory / 1_000_000);
        
        // Verify it matches our estimates
        // Low cardinality fields should use ~2MB per 1M docs
        assert!(used_memory < 50 * 1024 * 1024); // Should be under 50MB
    }
    
    #[test]
    fn test_bloom_filter_memory() {
        let (allocator, tracker) = TrackingAllocator::new();
        
        tracker.reset();
        let start_mem = tracker.current_usage();
        
        let config = BloomConfig {
            false_positive_rate: 0.01,
            expected_items: 1_000_000,
            partitions: 16,
        };
        let mut bloom = BloomIndex::new(config);
        
        // Add 1M items
        for i in 0..1_000_000 {
            bloom.add_item(&format!("item-{}", i));
        }
        
        let end_mem = tracker.current_usage();
        let used_memory = end_mem - start_mem;
        
        println!("Bloom filter memory for 1M items (0.01 FPP): {:.2} MB", 
                 used_memory as f64 / 1_048_576.0);
        println!("Bits per item: {:.1}", (used_memory * 8) as f64 / 1_000_000.0);
        
        // Should be around 10 bits per item for 0.01 FPP
        assert!((used_memory * 8) / 1_000_000 < 15); // Less than 15 bits per item
    }
}

/// Measure stack usage for recursive operations
pub fn measure_stack_usage<F, R>(f: F) -> (R, usize)
where
    F: FnOnce() -> R,
{
    use std::cell::Cell;

    thread_local! {
        static STACK_BOTTOM: Cell<usize> = Cell::new(0);
    }
    
    // Mark stack bottom
    let bottom_marker = 0u8;
    let bottom = &bottom_marker as *const _ as usize;
    STACK_BOTTOM.with(|c| c.set(bottom));
    
    // Run function
    let result = f();
    
    // Measure stack top
    let top_marker = 0u8;
    let top = &top_marker as *const _ as usize;
    let stack_used = STACK_BOTTOM.with(|c| c.get().saturating_sub(top));
    
    (result, stack_used)
}

/// Get current process memory stats
pub fn get_process_memory() -> MemoryStats {
    #[cfg(target_os = "linux")]
    {
        use std::fs;
        
        let status = fs::read_to_string("/proc/self/status").unwrap();
        let mut stats = MemoryStats::default();
        
        for line in status.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                match parts[0] {
                    "VmRSS:" => stats.resident = parts[1].parse::<usize>().unwrap_or(0) * 1024,
                    "VmSize:" => stats.virtual_size = parts[1].parse::<usize>().unwrap_or(0) * 1024,
                    "VmPeak:" => stats.peak_virtual = parts[1].parse::<usize>().unwrap_or(0) * 1024,
                    "VmHWM:" => stats.peak_resident = parts[1].parse::<usize>().unwrap_or(0) * 1024,
                    _ => {}
                }
            }
        }
        
        stats
    }
    
    #[cfg(not(target_os = "linux"))]
    {
        MemoryStats::default()
    }
}

#[derive(Debug, Default)]
pub struct MemoryStats {
    pub resident: usize,
    pub virtual_size: usize,
    pub peak_resident: usize,
    pub peak_virtual: usize,
}

impl MemoryStats {
    pub fn print(&self) {
        println!("Memory Stats:");
        println!("  Resident: {:.1} MB", self.resident as f64 / 1_048_576.0);
        println!("  Virtual: {:.1} MB", self.virtual_size as f64 / 1_048_576.0);
        println!("  Peak Resident: {:.1} MB", self.peak_resident as f64 / 1_048_576.0);
        println!("  Peak Virtual: {:.1} MB", self.peak_virtual as f64 / 1_048_576.0);
    }
}