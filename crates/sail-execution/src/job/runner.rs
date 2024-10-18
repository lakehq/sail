use std::sync::Arc;

use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::execute_stream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use sail_server::actor::ActorHandle;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

use crate::driver::{DriverActor, DriverEvent, DriverOptions};
use crate::error::ExecutionResult;
use crate::job::definition::JobDefinition;

#[tonic::async_trait]
pub trait JobRunner: Send + Sync + 'static {
    async fn execute(&self, job: JobDefinition) -> ExecutionResult<SendableRecordBatchStream>;
}

pub struct LocalJobRunner {}

impl LocalJobRunner {
    pub fn new() -> Self {
        Self {}
    }
}

#[tonic::async_trait]
impl JobRunner for LocalJobRunner {
    async fn execute(&self, job: JobDefinition) -> ExecutionResult<SendableRecordBatchStream> {
        // TODO: construct task context from job definition
        let ctx = TaskContext::default();
        Ok(execute_stream(job.plan, Arc::new(ctx))?)
    }
}

pub struct ClusterJobRunner {
    driver: ActorHandle<DriverActor>,
    buffer_size: usize,
}

impl ClusterJobRunner {
    pub fn start() -> ExecutionResult<Self> {
        let options = DriverOptions {
            enable_tls: false,
            driver_listen_host: "127.0.0.1".to_string(),
            driver_listen_port: 0,
            driver_external_host: "127.0.0.1".to_string(),
            driver_external_port: None,
        };
        let driver = ActorHandle::new(options);
        Ok(Self {
            driver,
            buffer_size: 16,
        })
    }
}

#[tonic::async_trait]
impl JobRunner for ClusterJobRunner {
    async fn execute(&self, job: JobDefinition) -> ExecutionResult<SendableRecordBatchStream> {
        let schema = job.plan.schema();
        let (tx, rx) = tokio::sync::mpsc::channel(self.buffer_size);
        self.driver
            .send(DriverEvent::ExecuteJob { job, channel: tx })
            .await?;
        let stream = ReceiverStream::new(rx);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
