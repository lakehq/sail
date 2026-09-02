pub mod error;
pub mod events;
mod execution;
mod loggers;
pub mod metrics;
pub mod telemetry;

pub const SCOPE_NAME: &str = "sail";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResourceKind {
    Server,
    FlightServer,
    Worker,
}

impl ResourceKind {
    pub const fn service_name(self) -> &'static str {
        match self {
            Self::Server => "sail-server",
            Self::FlightServer => "sail-flight-server",
            Self::Worker => "sail-worker",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ResourceOptions {
    pub kind: ResourceKind,
}

pub use execution::physical_plan::{TracingExecOptions, trace_execution_plan};
pub use metrics::MetricManager;
