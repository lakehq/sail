use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::SessionContext;
use sail_server::actor::{ActorHandle, ActorSystem};
use tokio::sync::oneshot;

use crate::driver::{DriverActor, DriverEvent, DriverOptions};
use crate::error::{ExecutionError, ExecutionResult};

#[tonic::async_trait]
pub trait JobRunner: Send + Sync + 'static {
    async fn execute(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> ExecutionResult<SendableRecordBatchStream>;
}

pub struct LocalJobRunner {
    context: SessionContext,
}

impl LocalJobRunner {
    pub fn new(context: SessionContext) -> Self {
        Self { context }
    }
}

#[tonic::async_trait]
impl JobRunner for LocalJobRunner {
    async fn execute(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> ExecutionResult<SendableRecordBatchStream> {
        Ok(execute_stream(plan, self.context.task_ctx())?)
    }
}

pub struct ClusterJobRunner {
    #[allow(dead_code)]
    system: ActorSystem,
    driver: ActorHandle<DriverActor>,
}

impl ClusterJobRunner {
    pub fn start() -> ExecutionResult<Self> {
        let options = DriverOptions {
            enable_tls: false,
            driver_listen_host: "127.0.0.1".to_string(),
            driver_listen_port: 0,
            driver_external_host: "127.0.0.1".to_string(),
            driver_external_port: None,
            worker_count_per_job: 4,
            job_output_buffer: 16,
        };
        // TODO: share actor system across sessions
        let mut system = ActorSystem::new();
        let driver = system.spawn(options);
        Ok(Self { system, driver })
    }
}

#[tonic::async_trait]
impl JobRunner for ClusterJobRunner {
    async fn execute(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> ExecutionResult<SendableRecordBatchStream> {
        let (tx, rx) = oneshot::channel();
        self.driver
            .send(DriverEvent::ExecuteJob { plan, result: tx })
            .await?;
        rx.await.map_err(|e| {
            ExecutionError::InternalError(format!("failed to create job stream: {e}"))
        })?
    }
}
