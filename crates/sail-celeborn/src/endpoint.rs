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
                    Some((parse_endpoint(&internal)?, parse_endpoint(&external)?))
                })
                .collect(),
        }
    }
}

/// Parses a `host:port` endpoint, accepting the bracketed IPv6 form.
pub(crate) fn parse_endpoint(endpoint: &str) -> Option<(String, u16)> {
    let (host, port) = endpoint.rsplit_once(':')?;
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    if host.is_empty() {
        return None;
    }
    Some((host.to_string(), port.parse().ok()?))
}

impl EndpointResolver for StaticEndpointResolver {
    fn resolve(&self, host: &str, port: u16) -> (String, u16) {
        let endpoint = (host.to_string(), port);
        self.overrides.get(&endpoint).cloned().unwrap_or(endpoint)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{EndpointResolver, StaticEndpointResolver, parse_endpoint};

    #[test]
    fn parses_bracketed_ipv6_endpoint() {
        assert_eq!(
            parse_endpoint("[::1]:12097"),
            Some(("::1".to_string(), 12097))
        );
    }

    #[test]
    fn resolves_bracketed_ipv6_mapping() {
        let resolver = StaticEndpointResolver::from_mappings(HashMap::from([(
            "[fd00::1]:12000".to_string(),
            "[::1]:32000".to_string(),
        )]));

        assert_eq!(
            resolver.resolve("fd00::1", 12000),
            ("::1".to_string(), 32000)
        );
    }
}
