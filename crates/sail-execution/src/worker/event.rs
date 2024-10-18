use tokio::sync::oneshot;

use crate::id::TaskId;

pub enum WorkerEvent {
    ServerReady {
        /// The local port that the worker server listens on.
        /// This may be different from the port accessible from other nodes.
        port: u16,
        signal: oneshot::Sender<()>,
    },
    RunTask {
        task_id: TaskId,
        partition: usize,
        plan: Vec<u8>,
    },
    StopTask {
        task_id: TaskId,
        partition: usize,
    },
    Shutdown,
}
