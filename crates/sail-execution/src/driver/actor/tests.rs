// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::TaskContext;
use futures::StreamExt;
use futures::future::BoxFuture;
use sail_common::actor::ActorSystem;
use sail_common::config::{AppConfig, SystemCatalogStore};
use sail_common::runtime::RuntimeHandle;
use sail_physical_plan::replay::ReplayExec;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::{Instant, timeout};

use crate::driver::job_scheduler::event_reporter;
use crate::driver::{
    DriverActor, DriverComponents, DriverHandle, DriverMessage, DriverOptions, TaskStatus,
};
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{DriverId, JobId, TaskKey, WorkerId};
use crate::worker_manager::{WorkerLaunchOptions, WorkerManager};

/// The test sends registration and task updates in a controlled order.
struct TestWorkers;

#[tonic::async_trait]
impl WorkerManager for TestWorkers {
    fn launch_worker(
        &self,
        _: &mut ActorSystem,
        _: WorkerId,
        _: WorkerLaunchOptions,
    ) -> BoxFuture<'static, ExecutionResult<()>> {
        Box::pin(std::future::ready(Ok(())))
    }

    async fn stop(&self) -> ExecutionResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn losing_completed_replay_producer_fails_job_output()
-> Result<(), Box<dyn std::error::Error>> {
    let mut config = AppConfig::load()?;
    config.cluster.worker_initial_count = 2;
    config.cluster.worker_max_count = 2;
    config.cluster.worker_task_slots = 1;
    config.cluster.task_max_attempts = 4;
    config.catalog.system.store = SystemCatalogStore::Memory;
    config.telemetry.export_traces = false;
    config.telemetry.export_metrics = false;
    config.telemetry.export_logs = false;
    let handle = tokio::runtime::Handle::current();
    let options = DriverOptions::try_new(
        &config,
        RuntimeHandle::new(handle.clone(), handle),
        "lost-replay-producer".into(),
        DriverId::from(1),
        0,
    )?;
    let mut system = ActorSystem::new();
    let driver = DriverHandle::new(system.spawn::<DriverActor>((
        options,
        DriverComponents {
            worker_manager: Box::new(TestWorkers),
            event_reporter: event_reporter(&config)?,
        },
    )));
    driver.activate().await?;

    // Keep RPCs pending while the test reports worker events to the driver.
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let port = listener.local_addr()?.port();
    let register = |id| {
        let driver = driver.clone();
        async move {
            let (result, rx) = oneshot::channel();
            driver
                .send(DriverMessage::RegisterWorker {
                    worker_id: WorkerId::from(id),
                    host: "127.0.0.1".into(),
                    port,
                    result,
                })
                .await
                .map_err(ExecutionError::from)?;
            rx.await?
        }
    };
    register(1).await?;
    let input = MemorySourceConfig::try_new_exec(&[vec![]], Arc::new(Schema::empty()), None)?;
    let (result, rx) = oneshot::channel();
    driver
        .send(DriverMessage::ExecuteJob {
            plan: Arc::new(ReplayExec::try_new(input)?),
            context: Arc::new(TaskContext::default()),
            result,
        })
        .await
        .map_err(ExecutionError::from)?;
    let mut output = rx.await??;

    // The consumer goes to the second worker when the first releases its slot.
    register(2).await?;
    driver
        .send(DriverMessage::UpdateTask {
            key: TaskKey {
                job_id: JobId::from(1),
                stage: 0,
                partition: 0,
                attempt: 0,
            },
            status: TaskStatus::Succeeded,
            message: None,
            cause: None,
            sequence: Some(1),
        })
        .await
        .map_err(ExecutionError::from)?;
    driver
        .send(DriverMessage::ProbeLostWorker {
            worker_id: WorkerId::from(1),
            instant: Instant::now(),
        })
        .await
        .map_err(ExecutionError::from)?;

    let result = timeout(Duration::from_secs(3), output.next()).await?;
    assert!(
        matches!(result, Some(Err(ref error)) if error.to_string().contains("task failed for lost worker")),
        "{result:?}"
    );
    Ok(())
}
