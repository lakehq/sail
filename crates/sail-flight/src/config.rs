use std::net::SocketAddr;

pub struct FlightSqlServerOptions {
    pub host: String,
    pub port: u16,
}

impl Default for FlightSqlServerOptions {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 32010,
        }
    }
}

impl FlightSqlServerOptions {
    pub fn bind_address(&self) -> Result<SocketAddr, std::io::Error> {
        let addr = format!("{}:{}", self.host, self.port);
        addr.parse().map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid bind address: {}", e),
            )
        })
    }
}
