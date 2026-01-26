use async_trait::async_trait;
use datafusion::common::Result;
use datafusion_common::DataFusionError;
use tokio::sync::oneshot;

use crate::system::catalog::{JobRow, SessionRow, StageRow, TaskRow, WorkerRow};
use crate::system::predicate::Predicate;

/// A trait for observing the state of a component.
/// The observer type `O` intentionally has a bound on the private trait `Observer` to
/// ensure that a component can only choose from allowed observer types.
#[async_trait]
pub trait StateObservable<O: Observer>: Send + Sync {
    async fn observe(&self, observer: O);
}

/// A private marker trait to ensure only allowed observer types are used.
trait Sealed {}

/// An observer that contains inputs to the observation and
/// an output channel to send back the observed result.
#[expect(private_bounds)]
pub trait Observer: Sealed {
    fn nothing(self);
    fn fail(self, e: DataFusionError);
}

impl Sealed for SessionManagerObserver {}
impl Sealed for JobRunnerObserver {}

/// Observer for the session manager state
/// that collects various information across all sessions.
pub enum SessionManagerObserver {
    Jobs {
        session_id: Predicate<String>,
        job_id: Predicate<u64>,
        fetch: usize,
        result: oneshot::Sender<Result<Vec<JobRow>>>,
    },
    Stages {
        session_id: Predicate<String>,
        job_id: Predicate<u64>,
        fetch: usize,
        result: oneshot::Sender<Result<Vec<StageRow>>>,
    },
    Tasks {
        session_id: Predicate<String>,
        job_id: Predicate<u64>,
        fetch: usize,
        result: oneshot::Sender<Result<Vec<TaskRow>>>,
    },
    Sessions {
        session_id: Predicate<String>,
        fetch: usize,
        result: oneshot::Sender<Result<Vec<SessionRow>>>,
    },
    Workers {
        session_id: Predicate<String>,
        worker_id: Predicate<u64>,
        fetch: usize,
        result: oneshot::Sender<Result<Vec<WorkerRow>>>,
    },
}

impl Observer for SessionManagerObserver {
    fn nothing(self) {
        match self {
            SessionManagerObserver::Jobs { result, .. } => {
                let _ = result.send(Ok(vec![]));
            }
            SessionManagerObserver::Stages { result, .. } => {
                let _ = result.send(Ok(vec![]));
            }
            SessionManagerObserver::Tasks { result, .. } => {
                let _ = result.send(Ok(vec![]));
            }
            SessionManagerObserver::Sessions { result, .. } => {
                let _ = result.send(Ok(vec![]));
            }
            SessionManagerObserver::Workers { result, .. } => {
                let _ = result.send(Ok(vec![]));
            }
        }
    }

    fn fail(self, e: DataFusionError) {
        match self {
            SessionManagerObserver::Jobs { result, .. } => {
                let _ = result.send(Err(e));
            }
            SessionManagerObserver::Stages { result, .. } => {
                let _ = result.send(Err(e));
            }
            SessionManagerObserver::Tasks { result, .. } => {
                let _ = result.send(Err(e));
            }
            SessionManagerObserver::Sessions { result, .. } => {
                let _ = result.send(Err(e));
            }
            SessionManagerObserver::Workers { result, .. } => {
                let _ = result.send(Err(e));
            }
        }
    }
}

/// Observer for the job runner state within a specific session.
pub enum JobRunnerObserver {
    Jobs {
        session_id: String,
        job_id: Predicate<u64>,
        fetch: usize,
        result: oneshot::Sender<Result<Vec<JobRow>>>,
    },
    Stages {
        session_id: String,
        job_id: Predicate<u64>,
        fetch: usize,
        result: oneshot::Sender<Result<Vec<StageRow>>>,
    },
    Tasks {
        session_id: String,
        job_id: Predicate<u64>,
        fetch: usize,
        result: oneshot::Sender<Result<Vec<TaskRow>>>,
    },
    Workers {
        session_id: String,
        worker_id: Predicate<u64>,
        fetch: usize,
        result: oneshot::Sender<Result<Vec<WorkerRow>>>,
    },
}

impl Observer for JobRunnerObserver {
    fn nothing(self) {
        match self {
            JobRunnerObserver::Jobs { result, .. } => {
                let _ = result.send(Ok(vec![]));
            }
            JobRunnerObserver::Stages { result, .. } => {
                let _ = result.send(Ok(vec![]));
            }
            JobRunnerObserver::Tasks { result, .. } => {
                let _ = result.send(Ok(vec![]));
            }
            JobRunnerObserver::Workers { result, .. } => {
                let _ = result.send(Ok(vec![]));
            }
        }
    }

    fn fail(self, e: DataFusionError) {
        match self {
            JobRunnerObserver::Jobs { result, .. } => {
                let _ = result.send(Err(e));
            }
            JobRunnerObserver::Stages { result, .. } => {
                let _ = result.send(Err(e));
            }
            JobRunnerObserver::Tasks { result, .. } => {
                let _ = result.send(Err(e));
            }
            JobRunnerObserver::Workers { result, .. } => {
                let _ = result.send(Err(e));
            }
        }
    }
}
