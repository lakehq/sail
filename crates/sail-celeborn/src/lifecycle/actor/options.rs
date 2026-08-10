use std::sync::Arc;

use crate::endpoint::EndpointResolver;
use crate::master::MasterClientOptions;

/// Configuration owned by a lifecycle manager actor.
#[readonly::make]
#[derive(Debug)]
pub struct LifecycleManagerOptions {
    pub application_id: String,
    pub master: MasterClientOptions,
    pub hostname: String,
    pub tenant_id: String,
    pub user_name: String,
    pub endpoint_resolver: Option<Arc<dyn EndpointResolver>>,
}

impl LifecycleManagerOptions {
    pub fn new(application_id: impl Into<String>, master: MasterClientOptions) -> Self {
        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string());
        Self {
            application_id: application_id.into(),
            master,
            hostname,
            // Match Celeborn's DefaultIdentityProvider defaults.
            tenant_id: "default".to_string(),
            user_name: "default".to_string(),
            endpoint_resolver: None,
        }
    }

    pub fn with_endpoint_resolver(mut self, endpoint_resolver: Arc<dyn EndpointResolver>) -> Self {
        self.endpoint_resolver = Some(endpoint_resolver);
        self
    }
}
