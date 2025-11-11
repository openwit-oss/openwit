use metrics_exporter_prometheus::PrometheusHandle;
use std::sync::OnceLock;

#[allow(dead_code)]
static PROM_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

// pub fn init_observability() {
//     let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_|
//         EnvFilter::new("otel_cli_server=info,tonic::transport=info,axum::rejection=false")
//     );

//     tracing_subscriber
//         ::registry()
//         .with(fmt::layer())
//         .with(console_subscriber::spawn())
//         .with(env_filter)
//         .init();

//         let builder = PrometheusBuilder::new();
//         let (recorder, handle) = builder.build().expect("Prometheus build failed");
    
//         metrics::set_boxed_recorder(Box::new(recorder)).expect("Failed to set Prometheus recorder");

//     if PROM_HANDLE.set(handle).is_err() {
//         tracing::warn!("Prometheus handle already set");
//     }
    
// }

// pub fn get_prometheus_metrics() -> String {
//     PROM_HANDLE.get().expect("Prometheus recorder not initialized").render()
// }
