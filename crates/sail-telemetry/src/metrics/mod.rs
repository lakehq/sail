mod instruments;

mod gen {
    include!(concat!(env!("OUT_DIR"), "/metric_registry.rs"));
    include!(concat!(env!("OUT_DIR"), "/metric_attributes.rs"));
}

use std::fmt;

pub use gen::{MetricAttribute, MetricRegistry};
pub use instruments::*;

impl fmt::Debug for MetricRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricRegistry").finish()
    }
}
