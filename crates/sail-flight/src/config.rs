/// Configuration for the Sail Arrow Flight SQL server
///
/// This module defines the configuration options for the Flight SQL server:
/// - Server binding (host/port)
/// - Query execution limits
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

/// Main server configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FlightSqlServerConfig {
    /// Server binding configuration
    #[serde(default)]
    pub server: ServerConfig,

    /// Query execution limits
    #[serde(default)]
    pub limits: QueryLimitsConfig,
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

/// Query execution limits configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QueryLimitsConfig {
    /// Maximum number of rows to return (0 = unlimited, default: 0)
    /// If a query returns more rows, results will be truncated with a warning
    #[serde(default)]
    pub max_rows: usize,
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

    const DEFAULT_HOST: &str = "127.0.0.1";
    const DEFAULT_PORT: u16 = 32010;
    const CUSTOM_PORT: u16 = 8080;
    const ANY_HOST: &str = "0.0.0.0";

    #[test]
    fn test_default_config() {
        let config = FlightSqlServerConfig::default();
        assert_eq!(config.server.host, DEFAULT_HOST);
        assert_eq!(config.server.port, DEFAULT_PORT);
    }

    #[test]
    fn test_bind_address() {
        let config = FlightSqlServerConfig::default();
        let addr = config.bind_address().unwrap();
        assert_eq!(addr.to_string(), format!("{DEFAULT_HOST}:{DEFAULT_PORT}"));
    }

    #[test]
    fn test_custom_server_config() {
        let config = FlightSqlServerConfig {
            server: ServerConfig {
                host: ANY_HOST.to_string(),
                port: CUSTOM_PORT,
            },
            ..Default::default()
        };
        let addr = config.bind_address().unwrap();
        assert_eq!(addr.to_string(), format!("{ANY_HOST}:{CUSTOM_PORT}"));
    }
}
