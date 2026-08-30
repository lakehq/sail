pub mod error;
mod execution;
mod loggers;
pub mod metrics;
pub mod system_event;
pub mod telemetry;

pub const SCOPE_NAME: &str = "sail";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResourceKind {
    Server,
    FlightServer,
    Worker,
}

impl ResourceKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Server => "server",
            Self::FlightServer => "flight-server",
            Self::Worker => "worker",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ResourceOptions {
    pub kind: ResourceKind,
}

pub use execution::physical_plan::{TracingExecOptions, trace_execution_plan};
pub use metrics::MetricManager;
