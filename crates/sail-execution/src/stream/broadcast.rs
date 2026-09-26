use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use datafusion::execution::TaskContext;
use futures::StreamExt;
use futures::future::BoxFuture;
use sail_common::actor::{Actor, ActorContext};
use tokio::task::AbortHandle;

use crate::error::ExecutionResult;
use crate::id::JobId;
use crate::stream::error::TaskStreamError;
use crate::stream::local::memory::MemoryStream;
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::TaskStreamChannelSink;
use crate::task::definition::TaskInputKey;

/// Include every producer attempt, so retries cannot reuse an earlier attempt's data.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BroadcastStreamKey {
    pub job_id: JobId,
    pub stage: usize,
    pub inputs: Vec<TaskInputKey>,
}

#[derive(Default)]
pub(crate) struct BroadcastStreamManager {
    streams: HashMap<BroadcastStreamKey, BroadcastStream>,
}

struct BroadcastStream {
    stream: MemoryStream,
    fetch: AbortHandle,
}

impl Drop for BroadcastStream {
    fn drop(&mut self) {
        self.fetch.abort();
    }
}

impl BroadcastStreamManager {
    pub fn fetch_stream<T: Actor>(
        &mut self,
        ctx: &mut ActorContext<T>,
        key: BroadcastStreamKey,
        fetch: BoxFuture<'static, ExecutionResult<TaskStreamSource>>,
        context: &TaskContext,
        buffer: usize,
    ) -> ExecutionResult<TaskStreamSource> {
        // Existing subscribers retain the original error. A later request may
        // retry a transient fetch failure even if the producer attempt is unchanged.
        if self
            .streams
            .get(&key)
            .is_some_and(|stream| stream.stream.is_failed())
        {
            self.streams.remove(&key);
        }
        let stream = match self.streams.entry(key) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let mut stream = MemoryStream::new(buffer);
                let mut sink = stream.publish(context.runtime_env().disk_manager.clone())?;
                let fetch = ctx.spawn(async move {
                    let output = async {
                        let mut source = fetch
                            .await
                            .map_err(|e| TaskStreamError::External(Arc::new(e)))?;
                        while let Some(batch) = source.next().await {
                            sink.write(batch?)
                                .await
                                .map_err(|e| TaskStreamError::External(Arc::new(e)))?;
                        }
                        Ok::<_, TaskStreamError>(())
                    }
                    .await;
                    match output {
                        Ok(()) => {
                            let _ = Box::new(sink).commit().await;
                        }
                        Err(error) => sink.fail(error),
                    }
                });
                entry.insert(BroadcastStream { stream, fetch })
            }
        };
        Ok(stream.stream.subscribe())
    }

    pub fn remove_streams(&mut self, job_id: JobId, stage: Option<usize>) {
        self.streams
            .retain(|key, _| key.job_id != job_id || stage.is_some_and(|stage| stage != key.stage));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use futures::stream;
    use sail_common::actor::{ActorAction, ActorSystem};

    use super::*;
    use crate::task_runner::TaskRunnerMessage;

    struct TestActor;

    impl Actor for TestActor {
        type Message = TaskRunnerMessage;
        type Options = ();

        fn name() -> &'static str {
            "BroadcastStreamTestActor"
        }
        fn new(_: ()) -> Self {
            Self
        }
        async fn receive(&mut self, _: &mut ActorContext<Self>, _: Self::Message) -> ActorAction {
            ActorAction::Continue
        }
    }

    fn key(job: u64, stage: usize, attempt: usize) -> BroadcastStreamKey {
        BroadcastStreamKey {
            job_id: JobId::from(job),
            stage,
            inputs: vec![TaskInputKey {
                partition: 0,
                attempt,
                channel: 0,
            }],
        }
    }

    fn fetch(count: Arc<AtomicUsize>) -> BoxFuture<'static, ExecutionResult<TaskStreamSource>> {
        Box::pin(async move {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(stream::iter([Err(TaskStreamError::Unknown(
                "fetch failed".into(),
            ))])) as TaskStreamSource)
        })
    }

    #[tokio::test]
    async fn shares_inflight_fetches_and_retries_errors_without_reusing_other_attempts()
    -> ExecutionResult<()> {
        let mut actors = ActorSystem::new();
        let mut ctx = ActorContext::new(&actors.spawn::<TestActor>(()));
        let context = TaskContext::default();
        let mut manager = BroadcastStreamManager::default();
        let count = Arc::new(AtomicUsize::new(0));
        let mut first =
            manager.fetch_stream(&mut ctx, key(1, 2, 0), fetch(count.clone()), &context, 1)?;
        let mut second =
            manager.fetch_stream(&mut ctx, key(1, 2, 0), fetch(count.clone()), &context, 1)?;
        for source in [&mut first, &mut second] {
            assert!(
                matches!(source.next().await, Some(Err(error)) if error.to_string() == "fetch failed")
            );
            assert!(source.next().await.is_none());
        }
        let empty =
            |count: Arc<AtomicUsize>| -> BoxFuture<'static, ExecutionResult<TaskStreamSource>> {
                Box::pin(async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(Box::pin(stream::empty()) as TaskStreamSource)
                })
            };
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let mut recovered =
            manager.fetch_stream(&mut ctx, key(1, 2, 0), empty(count.clone()), &context, 1)?;
        assert!(recovered.next().await.is_none());
        let mut late =
            manager.fetch_stream(&mut ctx, key(1, 2, 0), empty(count.clone()), &context, 1)?;
        assert!(late.next().await.is_none());
        assert_eq!(count.load(Ordering::SeqCst), 2);
        let mut retry =
            manager.fetch_stream(&mut ctx, key(1, 2, 1), empty(count.clone()), &context, 1)?;
        assert!(retry.next().await.is_none());
        assert_eq!(count.load(Ordering::SeqCst), 3);
        Ok(())
    }

    #[tokio::test]
    async fn cleanup_aborts_fetches_and_preserves_other_stages_and_jobs() -> ExecutionResult<()> {
        let mut actors = ActorSystem::new();
        let mut ctx = ActorContext::new(&actors.spawn::<TestActor>(()));
        let context = TaskContext::default();
        let mut manager = BroadcastStreamManager::default();
        for key in [key(1, 2, 0), key(1, 3, 0), key(2, 2, 0)] {
            let _ = manager.fetch_stream(
                &mut ctx,
                key,
                Box::pin(futures::future::pending()),
                &context,
                1,
            )?;
        }
        let pending_fetch = manager.streams[&key(1, 2, 0)].fetch.clone();
        manager.remove_streams(JobId::from(1), Some(2));
        tokio::time::timeout(Duration::from_secs(5), async {
            while !pending_fetch.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|e| crate::error::ExecutionError::InternalError(e.to_string()))?;
        assert_eq!(manager.streams.len(), 2);
        assert!(manager.streams.contains_key(&key(1, 3, 0)));
        assert!(manager.streams.contains_key(&key(2, 2, 0)));
        manager.remove_streams(JobId::from(1), None);
        assert_eq!(manager.streams.len(), 1);
        assert!(manager.streams.contains_key(&key(2, 2, 0)));
        // Cleanup must not permanently close a stage: a new producer attempt
        // can be needed when a pipelined region is retried.
        let count = Arc::new(AtomicUsize::new(0));
        let mut retry =
            manager.fetch_stream(&mut ctx, key(1, 2, 1), fetch(count.clone()), &context, 1)?;
        assert!(retry.next().await.transpose().is_err());
        assert_eq!(count.load(Ordering::SeqCst), 1);
        Ok(())
    }
}
