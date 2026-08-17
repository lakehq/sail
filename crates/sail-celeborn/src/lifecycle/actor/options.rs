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
    pub partition_split_threshold: i64,
    pub partition_split_mode: i32,
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
            partition_split_threshold: 1_i64 << 30,
            partition_split_mode: 0,
        }
    }

    pub fn with_endpoint_resolver(mut self, endpoint_resolver: Arc<dyn EndpointResolver>) -> Self {
        self.endpoint_resolver = Some(endpoint_resolver);
        self
    }

    pub fn with_partition_split(mut self, threshold: i64, mode: i32) -> Self {
        self.partition_split_threshold = threshold;
        self.partition_split_mode = mode;
        self
    }
}
