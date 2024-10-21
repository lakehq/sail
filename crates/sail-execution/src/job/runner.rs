use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::SessionContext;
use sail_server::actor::ActorHandle;
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
        };
        let driver = ActorHandle::new(options);
        Ok(Self { driver })
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
        let stream = rx.await.map_err(|_| {
            ExecutionError::InternalError("failed to create job stream".to_string())
        })?;
        Ok(stream)
    }
}
