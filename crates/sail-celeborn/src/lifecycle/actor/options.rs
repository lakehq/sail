use std::sync::Arc;
use std::time::Duration;

use crate::common::PartitionSplitMode;
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
    pub partition_split_mode: PartitionSplitMode,
    pub heartbeat_interval: Duration,
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
            partition_split_mode: PartitionSplitMode::Soft,
            heartbeat_interval: Duration::from_secs(10),
        }
    }

    pub fn with_endpoint_resolver(mut self, endpoint_resolver: Arc<dyn EndpointResolver>) -> Self {
        self.endpoint_resolver = Some(endpoint_resolver);
        self
    }

    pub fn with_partition_split(mut self, threshold: i64, mode: PartitionSplitMode) -> Self {
        self.partition_split_threshold = threshold;
        self.partition_split_mode = mode;
        self
    }

    pub fn with_heartbeat_interval(mut self, heartbeat_interval: Duration) -> Self {
        self.heartbeat_interval = heartbeat_interval;
        self
    }
}
