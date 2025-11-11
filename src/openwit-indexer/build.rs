fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Only run build script if events feature is enabled
    if std::env::var("CARGO_FEATURE_EVENTS").is_ok() {
        // Create the generated directory if it doesn't exist
        std::fs::create_dir_all("src/generated")?;
        
        tonic_build::configure()
            .build_server(true)
            .build_client(true)
            .out_dir("src/generated")
            .compile_protos(&["proto/index_service.proto"], &["proto/"])?;
    }
    Ok(())
}