mod instruments;

mod gen {
    include!(concat!(env!("OUT_DIR"), "/metric_registry.rs"));
    include!(concat!(env!("OUT_DIR"), "/metric_attributes.rs"));
}

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

pub use gen::{MetricAttribute, MetricRegistry};
pub use instruments::*;

/// Encapsulates a [`MetricRegistry`] together with the metrics collection interval.
/// When metrics are enabled, both the registry and the interval are always present together.
#[derive(Clone, Debug)]
pub struct MetricManager {
    pub registry: Arc<MetricRegistry>,
    pub collection_interval: Duration,
}

impl fmt::Debug for MetricRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricRegistry").finish()
    }
}
