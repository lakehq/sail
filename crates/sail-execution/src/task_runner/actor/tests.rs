use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{AsArray, Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use sail_common::actor::{ActorHandle, ActorSystem};
use sail_common::config::{AppConfig, SystemCatalogStore};
use sail_common::runtime::RuntimeHandle;
use tokio::sync::oneshot;

use crate::driver::job_scheduler::event_reporter;
use crate::driver::{DriverActor, DriverComponents, DriverOptions};
use crate::error::ExecutionResult;
use crate::id::{DriverId, JobId, TaskKey, TaskStreamKeySet};
use crate::stream::local::LocalStreamManager;
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{TaskStreamSink, TaskStreamWriteState};
use crate::task_runner::{
    TaskRunnerActor, TaskRunnerComponents, TaskRunnerExtensions, TaskRunnerMessage,
    TaskRunnerPlacement,
};
use crate::worker_manager::LocalWorkerManager;

type TestError = Box<dyn std::error::Error>;

/// Starts a task runner that gives up on streams that are not created within a second.
fn task_runner(
    system: &mut ActorSystem,
    name: &str,
) -> Result<ActorHandle<TaskRunnerActor>, TestError> {
    let mut config = AppConfig::load()?;
    config.cluster.task_stream_creation_timeout_secs = 1;
    config.catalog.system.store = SystemCatalogStore::Memory;
    config.telemetry.export_traces = false;
    config.telemetry.export_metrics = false;
    config.telemetry.export_logs = false;
    let reporter = event_reporter(&config)?;
    let handle = tokio::runtime::Handle::current();
    let runtime = RuntimeHandle::new(handle.clone(), handle);
    let options = DriverOptions::try_new(
        &config,
        runtime.clone(),
        name.to_string(),
        DriverId::from(1),
        0,
    )?;
    let local_streams = LocalStreamManager::new((&options).into());
    let driver = system.spawn::<DriverActor>((
        options,
        DriverComponents {
            worker_manager: Box::new(LocalWorkerManager::new(runtime, SessionContext::new())),
            event_reporter: reporter,
        },
    ));
    Ok(system.spawn::<TaskRunnerActor>(TaskRunnerComponents {
        session_id: name.to_string(),
        extensions: TaskRunnerExtensions {
            local_streams,
            storage_streams: None,
            celeborn_streams: None,
        },
        placement: TaskRunnerPlacement::Driver { driver },
    }))
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]))
}

fn batch(value: i32) -> Result<RecordBatch, TestError> {
    Ok(RecordBatch::try_new(
        schema(),
        vec![Arc::new(Int32Array::from(vec![value]))],
    )?)
}

fn value(batch: &RecordBatch) -> i32 {
    batch.column(0).as_primitive::<Int32Type>().value(0)
}

fn keys(partitions: &[usize], channels: &[usize]) -> TaskStreamKeySet {
    TaskStreamKeySet {
        job_id: JobId::from(1),
        stage: 0,
        tasks: partitions.iter().map(|&partition| (partition, 0)).collect(),
        channels: channels.to_vec(),
        release: false,
    }
}

/// Creates the streams of a task that are read by one reader each.
async fn create(
    runner: &ActorHandle<TaskRunnerActor>,
    partition: usize,
    channels: usize,
) -> Result<Box<dyn TaskStreamSink>, TestError> {
    let (result, rx) = oneshot::channel();
    let message = TaskRunnerMessage::CreateLocalStream {
        key: TaskKey {
            job_id: JobId::from(1),
            stage: 0,
            partition,
            attempt: 0,
        },
        channels,
        replicas: 1,
        bounded: true,
        schema: schema(),
        result,
    };
    runner.send(message).await.map_err(|e| e.to_string())?;
    Ok(rx.await??)
}

async fn fetch(
    runner: &ActorHandle<TaskRunnerActor>,
    keys: TaskStreamKeySet,
) -> Result<ExecutionResult<TaskStreamSource>, TestError> {
    let (result, rx) = oneshot::channel();
    let message = TaskRunnerMessage::FetchLocalStreams { keys, result };
    runner.send(message).await.map_err(|e| e.to_string())?;
    Ok(rx.await?)
}

async fn with_timeout(test: impl Future<Output = Result<(), TestError>>) -> Result<(), TestError> {
    tokio::time::timeout(Duration::from_secs(30), test).await?
}

#[tokio::test]
async fn streams_of_several_tasks_are_fetched_as_one_source() -> Result<(), TestError> {
    with_timeout(async {
        let mut system = ActorSystem::new();
        let runner = task_runner(&mut system, "fetch-several-tasks")?;
        let mut sinks = vec![];
        for partition in 0..3 {
            sinks.push(create(&runner, partition, 2).await?);
        }
        // The reader of channel 1 gets the data of that channel from all tasks.
        let mut source = fetch(&runner, keys(&[0, 1, 2], &[1])).await??;
        for (partition, sink) in sinks.iter_mut().enumerate() {
            let partition = partition as i32;
            let batches = vec![Some(batch(100 + partition)?), Some(batch(partition)?)];
            assert_eq!(sink.write(batches).await?, TaskStreamWriteState::Active);
        }
        for sink in sinks {
            sink.commit().await?;
        }
        let mut values = vec![];
        while let Some(batch) = source.next().await {
            values.push(value(&batch?));
        }
        values.sort();
        assert_eq!(values, vec![0, 1, 2]);

        // The reader of several channels gets the data of these channels from all tasks.
        let mut sink = create(&runner, 3, 3).await?;
        let mut source = fetch(&runner, keys(&[3], &[0, 2])).await??;
        sink.write(vec![Some(batch(0)?), Some(batch(1)?), Some(batch(2)?)])
            .await?;
        sink.commit().await?;
        let mut values = vec![];
        while let Some(batch) = source.next().await {
            values.push(value(&batch?));
        }
        values.sort();
        assert_eq!(values, vec![0, 2]);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn stream_that_is_created_late_does_not_delay_the_others() -> Result<(), TestError> {
    with_timeout(async {
        let mut system = ActorSystem::new();
        let runner = task_runner(&mut system, "fetch-late-stream")?;
        let mut first = create(&runner, 0, 1).await?;
        let mut source = fetch(&runner, keys(&[0, 1], &[0])).await??;
        first.write(vec![Some(batch(0)?)]).await?;
        assert_eq!(source.next().await.transpose()?.map(|x| value(&x)), Some(0));

        // The task that starts after its reader finds the reader waiting.
        let mut second = create(&runner, 1, 1).await?;
        second.write(vec![Some(batch(1)?)]).await?;
        assert_eq!(source.next().await.transpose()?.map(|x| value(&x)), Some(1));
        second.commit().await?;
        first.write(vec![Some(batch(2)?)]).await?;
        assert_eq!(source.next().await.transpose()?.map(|x| value(&x)), Some(2));
        first.commit().await?;
        assert!(source.next().await.is_none());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn stream_that_is_never_created_fails_as_when_fetched_alone() -> Result<(), TestError> {
    with_timeout(async {
        let mut system = ActorSystem::new();
        let runner = task_runner(&mut system, "fetch-missing-stream")?;
        let mut sink = create(&runner, 0, 1).await?;
        let mut alone = fetch(&runner, keys(&[1], &[0])).await??;
        let mut source = fetch(&runner, keys(&[0, 2], &[0])).await??;

        // The stream that exists delivers its data until the other stream fails.
        sink.write(vec![Some(batch(0)?)]).await?;
        assert_eq!(source.next().await.transpose()?.map(|x| value(&x)), Some(0));
        let Some(Err(expected)) = alone.next().await else {
            return Err("the stream that is fetched alone must fail".into());
        };
        let Some(Err(error)) = source.next().await else {
            return Err("the streams that are fetched together must fail".into());
        };
        assert_eq!(error.to_string(), expected.to_string());
        let message = "local stream is not created within the expected time";
        assert!(error.to_string().contains(message));

        // The stream that has failed fails the fetch, and the reader loses its place
        // in the other streams, as when the streams are fetched one by one.
        let mut before = create(&runner, 3, 1).await?;
        let mut after = create(&runner, 4, 1).await?;
        let Err(expected) = fetch(&runner, keys(&[2], &[0])).await? else {
            return Err("the fetch of the failed stream must fail".into());
        };
        let Err(error) = fetch(&runner, keys(&[3, 2, 4, 5], &[0])).await? else {
            return Err("the fetch with the failed stream must fail".into());
        };
        assert_eq!(error.to_string(), expected.to_string());
        for sink in [&mut before, &mut after] {
            let state = sink.write(vec![Some(batch(0)?)]).await?;
            assert_eq!(state, TaskStreamWriteState::Closed);
        }
        // This also holds for the task that creates its stream later.
        let mut late = create(&runner, 5, 1).await?;
        let state = late.write(vec![Some(batch(0)?)]).await?;
        assert_eq!(state, TaskStreamWriteState::Closed);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn dropped_source_closes_all_its_streams() -> Result<(), TestError> {
    with_timeout(async {
        let mut system = ActorSystem::new();
        let runner = task_runner(&mut system, "fetch-and-drop")?;
        let mut sinks = [create(&runner, 0, 2).await?, create(&runner, 1, 2).await?];
        let source = fetch(&runner, keys(&[0, 1], &[0, 1])).await??;
        for sink in sinks.iter_mut() {
            let batches = vec![Some(batch(0)?), Some(batch(1)?)];
            assert_eq!(sink.write(batches).await?, TaskStreamWriteState::Active);
        }
        drop(source);
        for sink in sinks.iter_mut() {
            let batches = vec![Some(batch(0)?), Some(batch(1)?)];
            assert_eq!(sink.write(batches).await?, TaskStreamWriteState::Closed);
        }
        Ok(())
    })
    .await
}

#[tokio::test]
async fn released_streams_are_closed_without_data_for_the_reader() -> Result<(), TestError> {
    with_timeout(async {
        let mut system = ActorSystem::new();
        let runner = task_runner(&mut system, "release")?;
        let release = |partitions: &[usize], channels: &[usize]| TaskStreamKeySet {
            release: true,
            ..keys(partitions, channels)
        };

        // A stream that has data when it is released.
        let mut written = create(&runner, 0, 1).await?;
        let state = written.write(vec![Some(batch(0)?)]).await?;
        assert_eq!(state, TaskStreamWriteState::Active);
        let mut source = fetch(&runner, release(&[0, 1], &[0])).await??;
        assert!(source.next().await.is_none());
        let state = written.write(vec![Some(batch(1)?)]).await?;
        assert_eq!(state, TaskStreamWriteState::Closed);

        // A stream that is created after it has been released.
        let mut late = create(&runner, 1, 1).await?;
        let state = late.write(vec![Some(batch(0)?)]).await?;
        assert_eq!(state, TaskStreamWriteState::Closed);

        // The other streams of the tasks are not released.
        let mut sink = create(&runner, 2, 2).await?;
        drop(fetch(&runner, release(&[2], &[1])).await??);
        let mut source = fetch(&runner, keys(&[2], &[0])).await??;
        let state = sink.write(vec![Some(batch(7)?), Some(batch(8)?)]).await?;
        assert_eq!(state, TaskStreamWriteState::Active);
        sink.commit().await?;
        let mut values = vec![];
        while let Some(batch) = source.next().await {
            values.push(value(&batch?));
        }
        assert_eq!(values, vec![7]);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn reader_that_joins_a_released_stream_gets_a_creation_timeout_of_its_own()
-> Result<(), TestError> {
    with_timeout(async {
        let mut system = ActorSystem::new();
        let runner = task_runner(&mut system, "fetch-after-release")?;

        // The reader requests the streams of two tasks and leaves, as an input does
        // that is dropped before it has been read.
        drop(fetch(&runner, keys(&[0, 1], &[0])).await??);
        // The creation timeout passes. It leaves the streams alone, since no reader waits.
        tokio::time::sleep(Duration::from_millis(1500)).await;

        // The task that starts now can still create its stream, and finds that its
        // reader has left.
        let mut sink = create(&runner, 0, 1).await?;
        let state = sink.write(vec![Some(batch(0)?)]).await?;
        assert_eq!(state, TaskStreamWriteState::Closed);

        // A reader that requests the other stream now waits for the creation timeout
        // again, and fails only when it has passed.
        let start = tokio::time::Instant::now();
        let mut source = fetch(&runner, keys(&[1], &[0])).await??;
        let Some(Err(error)) = source.next().await else {
            return Err("the reader of a stream that is never created must fail".into());
        };
        let message = "local stream is not created within the expected time";
        assert!(error.to_string().contains(message));
        assert!(start.elapsed() >= Duration::from_millis(900));
        Ok(())
    })
    .await
}
