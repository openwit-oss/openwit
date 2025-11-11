fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile control plane proto with file descriptor set for reflection
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
    
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("control_descriptor.bin"))
        .compile_protos(
            &["protos/control/control_plane.proto"],
            &["protos"],
        )?;
    
    // Compile ingestion protos
    tonic_build::configure()
        .compile_protos(
            &["protos/ingestion/telemetry_ingest.proto"],
            &["protos"],
        )?;
    
    // Compile storage query protos
    tonic_build::configure()
        .compile_protos(
            &["protos/storage/storage_query.proto"],
            &["protos"],
        )?;
    
    // Uncomment to compile OTLP protos
    // tonic_build::configure()
    //     .compile(
    //         &["protos/opentelemetry/proto/collector/logs/v1/logs_service.proto"],
    //         &["protos"],
    //     )?;
    
    Ok(())
}