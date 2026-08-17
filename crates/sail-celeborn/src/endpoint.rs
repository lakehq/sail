use std::collections::HashMap;
use std::fmt::Debug;

/// Resolves endpoints advertised by a remote service to addresses reachable by this client.
///
/// This is needed when a service advertises an address from an isolated network, such as a Docker
/// network, while the client runs somewhere else and must use a mapped host address and port.
pub trait EndpointResolver: Debug + Send + Sync + 'static {
    fn resolve(&self, host: &str, port: u16) -> (String, u16);
}

/// Resolves a fixed set of advertised endpoints to reachable addresses.
#[derive(Debug, Clone, Default)]
pub struct StaticEndpointResolver {
    overrides: HashMap<(String, u16), (String, u16)>,
}

impl StaticEndpointResolver {
    pub fn new(overrides: HashMap<(String, u16), (String, u16)>) -> Self {
        Self { overrides }
    }
}

impl EndpointResolver for StaticEndpointResolver {
    fn resolve(&self, host: &str, port: u16) -> (String, u16) {
        self.overrides
            .get(&(host.to_string(), port))
            .cloned()
            .unwrap_or_else(|| (host.to_string(), port))
    }
}
