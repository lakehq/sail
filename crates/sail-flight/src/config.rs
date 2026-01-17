/// Configuration for the Sail Arrow Flight SQL server
///
/// This module defines the configuration options for the Flight SQL server:
/// - Server binding (host/port)
/// - Session management options (future)
/// - Execution options (future)
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

/// Main server configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FlightSqlServerConfig {
    /// Server binding configuration
    #[serde(default)]
    pub server: ServerConfig,

    /// Session management configuration (future)
    #[serde(default)]
    pub session: SessionConfig,
}

/// Server binding configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Host to bind to (default: 127.0.0.1)
    #[serde(default = "default_host")]
    pub host: String,

    /// Port to bind to (default: 32010)
    #[serde(default = "default_port")]
    pub port: u16,
}

/// Session management configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SessionConfig {
    /// Session timeout in seconds (0 = no timeout)
    #[serde(default)]
    pub timeout_secs: u64,

    /// Maximum number of concurrent sessions (0 = unlimited)
    #[serde(default)]
    pub max_sessions: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
        }
    }
}

impl FlightSqlServerConfig {
    /// Get the socket address to bind to
    pub fn bind_address(&self) -> Result<SocketAddr, std::io::Error> {
        let addr = format!("{}:{}", self.server.host, self.server.port);
        addr.parse().map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid bind address: {}", e),
            )
        })
    }
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    32010
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = FlightSqlServerConfig::default();
        assert_eq!(config.server.host, "127.0.0.1");
        assert_eq!(config.server.port, 32010);
    }

    #[test]
    fn test_bind_address() {
        let config = FlightSqlServerConfig::default();
        let addr = config.bind_address().unwrap();
        assert_eq!(addr.to_string(), "127.0.0.1:32010");
    }
}
