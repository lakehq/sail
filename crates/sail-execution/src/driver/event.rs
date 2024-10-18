use crate::id::WorkerId;
use crate::job::JobDefinition;

pub enum DriverEvent {
    ServerReady {
        /// The local port that the server listens on.
        /// This may be different from the port accessible from other nodes.
        port: u16,
    },
    RegisterWorker {
        id: WorkerId,
        host: String,
        port: u16,
    },
    ExecuteJob {
        job: JobDefinition,
    },
    Shutdown,
}
