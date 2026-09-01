//! Public asynchronous reader for system tables.

use sail_common_datafusion::system::catalog::{
    JobRow, MetricRow, OptionRow, SessionRow, StageRow, TaskRow, WorkerRow,
};
use sail_common_datafusion::system::predicate::{MapValueFilter, TimestampMicros, ValueFilter};
use tokio::sync::oneshot;

use crate::actor::SystemStoreMessage;
use crate::engine::SystemStoreQuery;
use crate::handle::SystemStoreHandleInner;
use crate::{SystemStoreError, SystemStoreResult};

#[derive(Clone, Debug)]
pub struct SystemStoreReader {
    pub(crate) inner: SystemStoreHandleInner,
}

impl SystemStoreReader {
    pub async fn read_jobs(
        &self,
        session_id: ValueFilter<String>,
        job_id: ValueFilter<u64>,
        fetch: usize,
    ) -> SystemStoreResult<Vec<JobRow>> {
        let (reply, receiver) = oneshot::channel();
        self.send(SystemStoreQuery::Jobs {
            session_id,
            job_id,
            fetch,
            reply,
        })?;
        receiver.await.map_err(|error| {
            SystemStoreError::internal(format!("system store jobs read cancelled: {error}"))
        })?
    }

    pub async fn read_stages(
        &self,
        session_id: ValueFilter<String>,
        job_id: ValueFilter<u64>,
        stage: ValueFilter<u64>,
        fetch: usize,
    ) -> SystemStoreResult<Vec<StageRow>> {
        let (reply, receiver) = oneshot::channel();
        self.send(SystemStoreQuery::Stages {
            session_id,
            job_id,
            stage,
            fetch,
            reply,
        })?;
        receiver.await.map_err(|error| {
            SystemStoreError::internal(format!("system store stages read cancelled: {error}"))
        })?
    }

    pub async fn read_tasks(
        &self,
        session_id: ValueFilter<String>,
        job_id: ValueFilter<u64>,
        stage: ValueFilter<u64>,
        partition: ValueFilter<u64>,
        attempt: ValueFilter<u64>,
        fetch: usize,
    ) -> SystemStoreResult<Vec<TaskRow>> {
        let (reply, receiver) = oneshot::channel();
        self.send(SystemStoreQuery::Tasks {
            session_id,
            job_id,
            stage,
            partition,
            attempt,
            fetch,
            reply,
        })?;
        receiver.await.map_err(|error| {
            SystemStoreError::internal(format!("system store tasks read cancelled: {error}"))
        })?
    }

    pub async fn read_options(
        &self,
        key: ValueFilter<String>,
        fetch: usize,
    ) -> SystemStoreResult<Vec<OptionRow>> {
        let (reply, receiver) = oneshot::channel();
        self.send(SystemStoreQuery::Options { key, fetch, reply })?;
        receiver.await.map_err(|error| {
            SystemStoreError::internal(format!("system store options read cancelled: {error}"))
        })?
    }

    pub async fn read_sessions(
        &self,
        session_id: ValueFilter<String>,
        fetch: usize,
    ) -> SystemStoreResult<Vec<SessionRow>> {
        let (reply, receiver) = oneshot::channel();
        self.send(SystemStoreQuery::Sessions {
            session_id,
            fetch,
            reply,
        })?;
        receiver.await.map_err(|error| {
            SystemStoreError::internal(format!("system store sessions read cancelled: {error}"))
        })?
    }

    pub async fn read_workers(
        &self,
        session_id: ValueFilter<String>,
        worker_id: ValueFilter<u64>,
        fetch: usize,
    ) -> SystemStoreResult<Vec<WorkerRow>> {
        let (reply, receiver) = oneshot::channel();
        self.send(SystemStoreQuery::Workers {
            session_id,
            worker_id,
            fetch,
            reply,
        })?;
        receiver.await.map_err(|error| {
            SystemStoreError::internal(format!("system store workers read cancelled: {error}"))
        })?
    }

    pub async fn read_metrics(
        &self,
        timestamp: ValueFilter<TimestampMicros>,
        name: ValueFilter<String>,
        attributes: Vec<MapValueFilter<String, String>>,
        fetch: usize,
    ) -> SystemStoreResult<Vec<MetricRow>> {
        let (reply, receiver) = oneshot::channel();
        self.send(SystemStoreQuery::Metrics {
            timestamp,
            name,
            attributes,
            fetch,
            reply,
        })?;
        receiver.await.map_err(|error| {
            SystemStoreError::internal(format!("system store metrics read cancelled: {error}"))
        })?
    }

    fn send(&self, query: SystemStoreQuery) -> SystemStoreResult<()> {
        self.inner
            .send(SystemStoreMessage::Read(query))
            .map_err(|error| {
                SystemStoreError::internal(format!("failed to send system store read: {error}"))
            })
    }
}
