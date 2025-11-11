use std::net::TcpListener;
use anyhow::Result;
use tracing::{debug, info};

/// Find an available port starting from the given base port
pub fn find_available_port(base_port: u16, max_attempts: u16) -> Result<u16> {
    info!("Searching for available port starting from {}", base_port);
    
    for offset in 0..max_attempts {
        let port = base_port + offset;
        if is_port_available(port) {
            info!("Found available port: {}", port);
            return Ok(port);
        } else {
            debug!("Port {} is already in use, trying next...", port);
        }
    }
    
    // If no port found in range, try random high ports
    for _ in 0..10 {
        if let Ok(port) = get_random_available_port() {
            return Ok(port);
        }
    }
    
    anyhow::bail!("Could not find an available port after {} attempts", max_attempts + 10)
}

/// Check if a specific port is available for both TCP and UDP
pub fn is_port_available(port: u16) -> bool {
    // Check TCP on both 127.0.0.1 and 0.0.0.0
    let tcp_available = match TcpListener::bind(("0.0.0.0", port)) {
        Ok(_) => true,
        Err(_) => {
            // If 0.0.0.0 fails, also try 127.0.0.1
            match TcpListener::bind(("127.0.0.1", port)) {
                Ok(_) => true,
                Err(_) => false,
            }
        }
    };
    
    if !tcp_available {
        return false;
    }
    
    // Also check UDP since gossip uses UDP
    use std::net::UdpSocket;
    match UdpSocket::bind(("0.0.0.0", port)) {
        Ok(_) => true,
        Err(_) => {
            // If 0.0.0.0 fails, also try 127.0.0.1
            match UdpSocket::bind(("127.0.0.1", port)) {
                Ok(_) => true,
                Err(_) => false,
            }
        }
    }
}

/// Get a random available port by binding to port 0
pub fn get_random_available_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    Ok(port)
}

