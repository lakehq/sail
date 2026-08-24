use std::sync::Arc;

use crate::common::CompressionCodec;
use crate::endpoint::EndpointResolver;
use crate::lifecycle::LifecycleManager;

#[readonly::make]
pub struct ShuffleClientOptions {
    pub application_id: String,
    pub lifecycle_manager: Arc<dyn LifecycleManager>,
    pub endpoint_resolver: Option<Arc<dyn EndpointResolver>>,
    pub compression: CompressionCodec,
}

impl ShuffleClientOptions {
    pub fn new(
        application_id: String,
        lifecycle_manager: Arc<dyn LifecycleManager>,
        endpoint_resolver: Option<Arc<dyn EndpointResolver>>,
        compression: CompressionCodec,
    ) -> Self {
        Self {
            application_id,
            lifecycle_manager,
            endpoint_resolver,
            compression,
        }
    }
}
