use datafusion::arrow::array::RecordBatch;
use datafusion::common::{Result, internal_datafusion_err};
use sail_common::actor::ActorHandle;
use sail_common_datafusion::system::predicate::ValueFilter;
use tokio::sync::oneshot;

use crate::system_event::{SystemEventActor, SystemEventActorMessage};

/// A handle for reading materialized system-event rows from the actor.
#[derive(Clone, Debug)]
pub struct SystemEventReader {
    actor: ActorHandle<SystemEventActor>,
}

impl SystemEventReader {
    pub fn new(actor: ActorHandle<SystemEventActor>) -> Self {
        Self { actor }
    }

    pub async fn read_jobs(
        &self,
        session_id: ValueFilter<String>,
        job_id: ValueFilter<u64>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let (result, receiver) = oneshot::channel();
        self.send(SystemEventActorMessage::ReadJobs {
            session_id,
            job_id,
            fetch,
            result,
        })
        .await?;
        receiver
            .await
            .map_err(|e| internal_datafusion_err!("failed to read system event jobs: {e}"))?
    }

    pub async fn read_stages(
        &self,
        session_id: ValueFilter<String>,
        job_id: ValueFilter<u64>,
        stage: ValueFilter<u64>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let (result, receiver) = oneshot::channel();
        self.send(SystemEventActorMessage::ReadStages {
            session_id,
            job_id,
            stage,
            fetch,
            result,
        })
        .await?;
        receiver
            .await
            .map_err(|e| internal_datafusion_err!("failed to read system event stages: {e}"))?
    }

    pub async fn read_tasks(
        &self,
        session_id: ValueFilter<String>,
        job_id: ValueFilter<u64>,
        stage: ValueFilter<u64>,
        partition: ValueFilter<u64>,
        attempt: ValueFilter<u64>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let (result, receiver) = oneshot::channel();
        self.send(SystemEventActorMessage::ReadTasks {
            session_id,
            job_id,
            stage,
            partition,
            attempt,
            fetch,
            result,
        })
        .await?;
        receiver
            .await
            .map_err(|e| internal_datafusion_err!("failed to read system event tasks: {e}"))?
    }

    pub async fn read_options(
        &self,
        key: ValueFilter<String>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let (result, receiver) = oneshot::channel();
        self.send(SystemEventActorMessage::ReadOptions { key, fetch, result })
            .await?;
        receiver
            .await
            .map_err(|e| internal_datafusion_err!("failed to read system event options: {e}"))?
    }

    pub async fn read_sessions(
        &self,
        session_id: ValueFilter<String>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let (result, receiver) = oneshot::channel();
        self.send(SystemEventActorMessage::ReadSessions {
            session_id,
            fetch,
            result,
        })
        .await?;
        receiver
            .await
            .map_err(|e| internal_datafusion_err!("failed to read system event sessions: {e}"))?
    }

    pub async fn read_workers(
        &self,
        session_id: ValueFilter<String>,
        worker_id: ValueFilter<u64>,
        fetch: usize,
    ) -> Result<RecordBatch> {
        let (result, receiver) = oneshot::channel();
        self.send(SystemEventActorMessage::ReadWorkers {
            session_id,
            worker_id,
            fetch,
            result,
        })
        .await?;
        receiver
            .await
            .map_err(|e| internal_datafusion_err!("failed to read system event workers: {e}"))?
    }

    async fn send(&self, message: SystemEventActorMessage) -> Result<()> {
        self.actor
            .send(message)
            .await
            .map_err(|e| internal_datafusion_err!("failed to send system event message: {e}"))
    }
}
