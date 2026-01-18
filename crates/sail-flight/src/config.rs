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

    /// Prepared statement cache configuration
    #[serde(default)]
    pub cache: CacheConfig,

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

/// Prepared statement cache configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Maximum cache size in bytes (default: 1 GB)
    #[serde(default = "default_cache_size")]
    pub max_size_bytes: usize,

    /// Enable cache statistics logging
    #[serde(default = "default_cache_stats")]
    pub enable_stats: bool,
}

/// Query execution limits configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryLimitsConfig {
    /// Maximum number of rows to return (0 = unlimited, default: 0)
    /// If a query returns more rows, results will be truncated with a warning
    #[serde(default)]
    pub max_rows: usize,
}

impl Default for QueryLimitsConfig {
    fn default() -> Self {
        Self { max_rows: 0 } // 0 = unlimited
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_size_bytes: default_cache_size(),
            enable_stats: default_cache_stats(),
        }
    }
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

fn default_cache_size() -> usize {
    1024 * 1024 * 1024 // 1 GB
}

fn default_cache_stats() -> bool {
    true // Enable stats by default for visibility
}

#[cfg(test)]
mod tests {
    use super::*;

    const ONE_GB: usize = 1024 * 1024 * 1024;
    const HALF_GB: usize = 512 * 1024 * 1024;
    const DEFAULT_HOST: &str = "127.0.0.1";
    const DEFAULT_PORT: u16 = 32010;
    const CUSTOM_PORT: u16 = 8080;
    const ANY_HOST: &str = "0.0.0.0";

    #[test]
    fn test_default_config() {
        let config = FlightSqlServerConfig::default();
        assert_eq!(config.server.host, DEFAULT_HOST);
        assert_eq!(config.server.port, DEFAULT_PORT);
        assert_eq!(config.cache.max_size_bytes, ONE_GB);
        assert!(config.cache.enable_stats); // Stats enabled by default for visibility
    }

    #[test]
    fn test_bind_address() {
        let config = FlightSqlServerConfig::default();
        let addr = config.bind_address().unwrap();
        assert_eq!(addr.to_string(), format!("{DEFAULT_HOST}:{DEFAULT_PORT}"));
    }

    #[test]
    fn test_custom_cache_size() {
        let config = FlightSqlServerConfig {
            cache: CacheConfig {
                max_size_bytes: HALF_GB,
                enable_stats: true,
            },
            ..Default::default()
        };
        assert_eq!(config.cache.max_size_bytes, HALF_GB);
        assert!(config.cache.enable_stats);
    }

    #[test]
    fn test_cache_stats_disabled() {
        let config = FlightSqlServerConfig {
            cache: CacheConfig {
                max_size_bytes: ONE_GB,
                enable_stats: false,
            },
            ..Default::default()
        };
        assert!(!config.cache.enable_stats);
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
