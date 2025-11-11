use sysinfo::System;
use tokio::time::{sleep, Duration};

use openwit_metrics::{
    SYSTEM_CPU_GAUGE,
    SYSTEM_MEM_GAUGE,
    SYSTEM_MEMORY_TOTAL_GAUGE,
    SYSTEM_SWAP_USED_GAUGE,
    SYSTEM_SWAP_TOTAL_GAUGE,
    SYSTEM_THREADS_GAUGE,
};

pub async fn start_system_metrics_collector() {
    let mut sys = System::new_all();

    loop {
        sys.refresh_all();

        let total_memory = sys.total_memory();
        let used_memory = sys.used_memory();
        let total_swap = sys.total_swap();
        let used_swap = sys.used_swap();
        let cpu_usage = sys.global_cpu_usage();
        let current_pid = sysinfo::get_current_pid().unwrap();
        let thread_count = sys
            .process(current_pid)
            .and_then(|proc|
                proc
                    .tasks()
                    .as_ref()
                    .map(|t| t.len() as i64)  // ← Prometheus IntGauge uses i64
            )
            .unwrap_or(0);

        SYSTEM_MEM_GAUGE.set(used_memory as i64);
        SYSTEM_MEMORY_TOTAL_GAUGE.set(total_memory as i64);
        SYSTEM_SWAP_USED_GAUGE.set(used_swap as i64);
        SYSTEM_SWAP_TOTAL_GAUGE.set(total_swap as i64);
        SYSTEM_CPU_GAUGE.set(cpu_usage as i64);  // If this is percent, OK as is
        SYSTEM_THREADS_GAUGE.set(thread_count);

        sleep(Duration::from_secs(5)).await;
    }
}
