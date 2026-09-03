//! Public row-batch reader for system tables.

use tokio::sync::mpsc;

use crate::actor::SystemStoreMessage;
use crate::catalog::{JobRow, MetricRow, OptionRow, SessionRow, StageRow, TaskRow, WorkerRow};
use crate::engine::SystemStoreQuery;
use crate::handle::SystemStoreHandleInner;
use crate::predicate::{MapValueFilter, TimestampMicros, ValueFilter};
use crate::{SystemStoreError, SystemStoreResult};

// Keep at most one batch queued behind the batch currently being converted by the service.
const ROW_BATCH_CHANNEL_CAPACITY: usize = 1;

#[derive(Clone, Debug)]
pub struct SystemStoreReader {
    pub(crate) inner: SystemStoreHandleInner,
}

impl SystemStoreReader {
    pub fn read_jobs(
        &self,
        session_id: ValueFilter<String>,
        job_id: ValueFilter<u64>,
        fetch: usize,
        batch_size: usize,
    ) -> SystemStoreResult<mpsc::Receiver<SystemStoreResult<Option<Vec<JobRow>>>>> {
        self.start(|sender| SystemStoreQuery::Jobs {
            session_id,
            job_id,
            fetch,
            batch_size,
            sender,
        })
    }

    pub fn read_stages(
        &self,
        session_id: ValueFilter<String>,
        job_id: ValueFilter<u64>,
        stage: ValueFilter<u64>,
        fetch: usize,
        batch_size: usize,
    ) -> SystemStoreResult<mpsc::Receiver<SystemStoreResult<Option<Vec<StageRow>>>>> {
        self.start(|sender| SystemStoreQuery::Stages {
            session_id,
            job_id,
            stage,
            fetch,
            batch_size,
            sender,
        })
    }

    pub fn read_tasks(
        &self,
        session_id: ValueFilter<String>,
        job_id: ValueFilter<u64>,
        stage: ValueFilter<u64>,
        partition: ValueFilter<u64>,
        attempt: ValueFilter<u64>,
        fetch: usize,
        batch_size: usize,
    ) -> SystemStoreResult<mpsc::Receiver<SystemStoreResult<Option<Vec<TaskRow>>>>> {
        self.start(|sender| SystemStoreQuery::Tasks {
            session_id,
            job_id,
            stage,
            partition,
            attempt,
            fetch,
            batch_size,
            sender,
        })
    }

    pub fn read_options(
        &self,
        key: ValueFilter<String>,
        fetch: usize,
        batch_size: usize,
    ) -> SystemStoreResult<mpsc::Receiver<SystemStoreResult<Option<Vec<OptionRow>>>>> {
        self.start(|sender| SystemStoreQuery::Options {
            key,
            fetch,
            batch_size,
            sender,
        })
    }

    pub fn read_sessions(
        &self,
        session_id: ValueFilter<String>,
        fetch: usize,
        batch_size: usize,
    ) -> SystemStoreResult<mpsc::Receiver<SystemStoreResult<Option<Vec<SessionRow>>>>> {
        self.start(|sender| SystemStoreQuery::Sessions {
            session_id,
            fetch,
            batch_size,
            sender,
        })
    }

    pub fn read_workers(
        &self,
        session_id: ValueFilter<String>,
        worker_id: ValueFilter<u64>,
        fetch: usize,
        batch_size: usize,
    ) -> SystemStoreResult<mpsc::Receiver<SystemStoreResult<Option<Vec<WorkerRow>>>>> {
        self.start(|sender| SystemStoreQuery::Workers {
            session_id,
            worker_id,
            fetch,
            batch_size,
            sender,
        })
    }

    pub fn read_metrics(
        &self,
        timestamp: ValueFilter<TimestampMicros>,
        name: ValueFilter<String>,
        attributes: Vec<MapValueFilter<String, String>>,
        fetch: usize,
        batch_size: usize,
    ) -> SystemStoreResult<mpsc::Receiver<SystemStoreResult<Option<Vec<MetricRow>>>>> {
        self.start(|sender| SystemStoreQuery::Metrics {
            timestamp,
            name,
            attributes,
            fetch,
            batch_size,
            sender,
        })
    }

    fn start<T>(
        &self,
        query: impl FnOnce(mpsc::Sender<SystemStoreResult<Option<Vec<T>>>>) -> SystemStoreQuery,
    ) -> SystemStoreResult<mpsc::Receiver<SystemStoreResult<Option<Vec<T>>>>> {
        let (sender, receiver) = mpsc::channel(ROW_BATCH_CHANNEL_CAPACITY);
        self.inner
            .send(SystemStoreMessage::Read(query(sender)))
            .map_err(|error| {
                SystemStoreError::internal(format!("failed to send system store read: {error}"))
            })?;
        Ok(receiver)
    }
}
