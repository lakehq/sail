#[path = "core.rs"]
mod actor_core;
mod handler;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::endpoint::EndpointResolver;
use crate::lifecycle::LifecycleManager;
use crate::master::{PartitionLocation, WorkerSlotLocations};

/// Serializes local shuffle-client operations using an external lifecycle manager.
pub struct ShuffleClientActor {
    application_id: String,
    lifecycle_manager: Arc<dyn LifecycleManager>,
    locations: HashMap<(i32, i32), PartitionLocation>,
    worker_locations: HashMap<i32, HashMap<String, WorkerSlotLocations>>,
    batch_ids: HashMap<(i32, i32, i32), i32>,
    mapper_attempts: HashMap<i32, Vec<i32>>,
    committing_shuffles: HashSet<i32>,
    committed_shuffles: HashSet<i32>,
    endpoint_resolver: Option<Arc<dyn EndpointResolver>>,
}

#[readonly::make]
pub struct ShuffleClientOptions {
    pub application_id: String,
    pub lifecycle_manager: Arc<dyn LifecycleManager>,
    pub endpoint_resolver: Option<Arc<dyn EndpointResolver>>,
}

impl ShuffleClientOptions {
    pub fn new(
        application_id: String,
        lifecycle_manager: Arc<dyn LifecycleManager>,
        endpoint_resolver: Option<Arc<dyn EndpointResolver>>,
    ) -> Self {
        Self {
            application_id,
            lifecycle_manager,
            endpoint_resolver,
        }
    }
}
