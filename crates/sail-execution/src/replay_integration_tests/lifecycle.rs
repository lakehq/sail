use datafusion::execution::TaskContext;
use sail_common::actor::ActorHandle;
use tokio::sync::oneshot;

use super::*;
use crate::driver::DriverActor;
use crate::id::{JobId, TaskAttempt, TaskStreamKey};
use crate::proto::{RemoteExecutionCodec, encode_remote_physical_plan};
use crate::shuffle::ShuffleCompression;
use crate::stream::local::{LocalStreamManager, LocalStreamManagerOptions};
use crate::stream::storage::StorageStreamManager;
use crate::task::definition::{
    TaskDefinition, TaskOutput, TaskOutputDistribution, TaskOutputLocator,
};
use crate::task_runner::{
    TaskRunnerActor, TaskRunnerComponents, TaskRunnerExtensions, TaskRunnerMessage,
    TaskRunnerPlacement,
};

async fn send(
    runner: &ActorHandle<TaskRunnerActor>,
    message: TaskRunnerMessage,
) -> ExecutionResult<()> {
    runner.send(message).await?;
    Ok(())
}

async fn close(
    runner: &ActorHandle<TaskRunnerActor>,
    job_id: JobId,
) -> ExecutionResult<oneshot::Receiver<ExecutionResult<()>>> {
    let (result, stopped) = oneshot::channel();
    runner
        .send(TaskRunnerMessage::CloseJob { job_id, result })
        .await?;
    Ok(stopped)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replay_close_waits_for_monitor_and_late_chunk_or_manifest_put()
-> Result<(), Box<dyn std::error::Error>> {
    tokio::time::timeout(Duration::from_secs(30), async {
        for manifest in [false, true] {
            let config = AppConfig::load()?;
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::new(GreedyMemoryPool::new(8 * 1024 * 1024)))
                .build_arc()?;
            let store = Arc::new(super::gated_store::GatedStore::new(manifest));
            runtime.register_object_store(&url::Url::parse("memory:///")?, store.clone());
            let session = SessionContext::new_with_state(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_runtime_env(runtime.clone())
                    .build(),
            );
            let context = session.task_ctx();
            let handle = tokio::runtime::Handle::current();
            let runtime_handle = RuntimeHandle::new(handle.clone(), handle);
            let options = DriverOptions::try_new(
                &config,
                runtime_handle.clone(),
                "replay-fence".into(),
                DriverId::from(1),
                0,
            )?;
            let local_options = LocalStreamManagerOptions::from(&options);
            let mut system = ActorSystem::new();
            let driver = system.spawn::<DriverActor>((
                options,
                DriverComponents {
                    worker_manager: Box::new(LocalWorkerManager::new(
                        runtime_handle,
                        session.clone(),
                    )),
                    event_reporter: test_event_reporter(),
                },
            ));
            let runner = system.spawn::<TaskRunnerActor>(TaskRunnerComponents {
                session_id: "replay-fence".into(),
                extensions: TaskRunnerExtensions {
                    local_streams: LocalStreamManager::new(local_options),
                    storage_streams: Some(StorageStreamManager::new(
                        Some("memory:///replay-tests".into()),
                        "replay-fence".into(),
                        1024 * 1024,
                        ShuffleCompression::None,
                    )),
                    celeborn_streams: None,
                },
                placement: TaskRunnerPlacement::Driver { driver },
            });
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
                vec![Arc::new(Int64Array::from_iter_values(0..1024))],
            )?;
            let schema = batch.schema();
            let source = session.read_batch(batch)?.create_physical_plan().await?;
            let id = ReplayId {
                scope: Default::default(),
                ordinal: 0,
            };
            let definition = Arc::new(TaskDefinition {
                plan: encode_remote_physical_plan(&RemoteExecutionCodec, source)?.into(),
                inputs: vec![],
                output: TaskOutput {
                    distribution: TaskOutputDistribution::RoundRobinBatch { channels: 1 },
                    locator: TaskOutputLocator::ReplayStorage { id },
                },
            });
            let job_id = JobId::from(42);
            let launch = |context: Arc<TaskContext>| {
                let (result, accepted) = oneshot::channel();
                (
                    TaskRunnerMessage::RunTaskBatch {
                        job_id,
                        stage: 0,
                        tasks: vec![TaskAttempt {
                            partition: 0,
                            attempt: 0,
                        }],
                        definition: definition.clone(),
                        context,
                        peers: vec![],
                        result,
                    },
                    accepted,
                )
            };
            let (message, accepted) = launch(context.clone());
            send(&runner, message).await?;
            accepted.await??;
            store.entered.acquire().await?.forget();
            let mut first = close(&runner, job_id).await?;
            let mut duplicate = close(&runner, job_id).await?;
            // Ordered admission requests prove CloseJob has been handled, not just queued.
            let (message, accepted) = launch(context.clone());
            send(&runner, message).await?;
            assert!(accepted.await?.is_err());
            let (result, created) = oneshot::channel();
            send(
                &runner,
                TaskRunnerMessage::CreateReplayStream {
                    key: TaskStreamKey {
                        job_id,
                        stage: 0,
                        partition: 0,
                        attempt: 1,
                        channel: 0,
                    },
                    id,
                    storage: true,
                    schema,
                    context: context.clone(),
                    result,
                },
            )
            .await?;
            assert!(created.await?.is_err());
            assert!(matches!(
                first.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
            assert!(matches!(
                duplicate.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
            // Let the already-issued put finish AFTER cancellation. It must be retracted
            // before either CloseJob acknowledgment, including a late manifest publication.
            store.release.add_permits(1);
            first.await??;
            duplicate.await??;
            assert!(store.list(None).try_collect::<Vec<_>>().await?.is_empty());
            assert_eq!(runtime.memory_pool.reserved(), 0);
            assert_eq!(runtime.disk_manager.used_disk_space(), 0);
            close(&runner, job_id).await?.await??;
            send(&runner, TaskRunnerMessage::Shutdown).await?;
            drop(system);
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })
    .await?
}
