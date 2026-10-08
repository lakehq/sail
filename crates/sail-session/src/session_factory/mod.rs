mod job_runner;
mod server;
mod worker;

use datafusion::common::Result;
use datafusion::prelude::SessionContext;
pub use job_runner::{
    ServerSessionJobRunnerFactory, SessionJobRunner, SessionJobRunnerFactory, SessionJobRunnerInfo,
};
pub use server::{ServerSessionFactory, ServerSessionInfo, ServerSessionMutator};
pub use worker::WorkerSessionFactory;

pub trait SessionFactory<I>: Send {
    /// Create a DataFusion [`SessionContext`].
    /// This method takes `&mut self` so that the factory can maintain internal state if needed.
    /// This method takes an opaque parameter of type `I` for session-specific information.
    fn create(&mut self, info: I) -> Result<SessionContext>;
}

fn spill_compression(
    compression: sail_common::config::SpillCompression,
) -> datafusion::common::config::SpillCompression {
    use datafusion::common::config::SpillCompression;
    use sail_common::config::SpillCompression as SailSpillCompression;

    match compression {
        SailSpillCompression::Uncompressed => SpillCompression::Uncompressed,
        SailSpillCompression::Lz4Frame => SpillCompression::Lz4Frame,
        SailSpillCompression::Zstd => SpillCompression::Zstd,
    }
}
