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

    /// Creates a resolver from `host:port` endpoint mappings, ignoring invalid mappings.
    pub fn from_mappings(mappings: HashMap<String, String>) -> Self {
        Self {
            overrides: mappings
                .into_iter()
                .filter_map(|(internal, external)| {
                    let (internal_host, internal_port) = internal.rsplit_once(':')?;
                    let internal_port = internal_port.parse().ok()?;
                    let (external_host, external_port) = external.rsplit_once(':')?;
                    let external_port = external_port.parse().ok()?;
                    if internal_host.is_empty() || external_host.is_empty() {
                        return None;
                    }
                    Some((
                        (internal_host.to_string(), internal_port),
                        (external_host.to_string(), external_port),
                    ))
                })
                .collect(),
        }
    }
}

impl EndpointResolver for StaticEndpointResolver {
    fn resolve(&self, host: &str, port: u16) -> (String, u16) {
        let endpoint = (host.to_string(), port);
        self.overrides.get(&endpoint).cloned().unwrap_or(endpoint)
    }
}
