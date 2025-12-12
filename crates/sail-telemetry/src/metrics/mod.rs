mod instruments;

mod gen {
    include!(concat!(env!("OUT_DIR"), "/metric_registry.rs"));
    include!(concat!(env!("OUT_DIR"), "/metric_attributes.rs"));
}

pub use gen::{MetricAttribute, MetricRegistry};
pub use instruments::*;
