use std::fmt::Debug;

/// Resolves endpoints advertised by a remote service to addresses reachable by this client.
///
/// This is needed when a service advertises an address from an isolated network, such as a Docker
/// network, while the client runs somewhere else and must use a mapped host address and port.
pub trait EndpointResolver: Debug + Send + Sync + 'static {
    fn resolve(&self, host: &str, port: u16) -> (String, u16);
}
