use std::fmt;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use futures::future::try_join_all;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStreamWriteState {
    Active,
    Closed,
}

#[tonic::async_trait]
pub trait TaskStreamWriter: fmt::Debug + Send + Sync {
    async fn open(&self, partition: usize) -> Result<Box<dyn TaskStreamSink>>;
}

/// A sink for one shuffle task partition and all of its output channels.
#[tonic::async_trait]
pub trait TaskStreamSink: Send {
    /// Write at most one batch per output channel concurrently. Calls are ordered
    /// so that each channel preserves its input batch order.
    async fn write(&mut self, batches: Vec<Option<RecordBatch>>) -> Result<TaskStreamWriteState>;
    async fn commit(self: Box<Self>) -> Result<()>;
    async fn abort(self: Box<Self>) -> Result<()>;
}

/// A physical sink for exactly one channel of the task stream.
#[tonic::async_trait]
pub trait TaskStreamChannelSink: Send {
    async fn write(&mut self, batch: RecordBatch) -> Result<TaskStreamWriteState>;
    async fn commit(self: Box<Self>) -> Result<()>;
    async fn abort(self: Box<Self>) -> Result<()>;
}

pub(crate) struct MultiChannelTaskStreamSink {
    pub(crate) sinks: Vec<Option<Box<dyn TaskStreamChannelSink>>>,
}

#[tonic::async_trait]
impl TaskStreamSink for MultiChannelTaskStreamSink {
    async fn write(&mut self, batches: Vec<Option<RecordBatch>>) -> Result<TaskStreamWriteState> {
        if batches.len() != self.sinks.len() {
            return Err(DataFusionError::Internal(format!(
                "expected {} shuffle output channels, got {}",
                self.sinks.len(),
                batches.len()
            )));
        }
        // Borrow each channel independently so slow I/O on one partition does
        // not prevent writes to other partitions from making progress. Retain
        // the sinks so the caller can abort them if any write fails.
        try_join_all(
            self.sinks
                .iter_mut()
                .zip(batches)
                .map(|(sink, batch)| async move {
                    if let (Some(writer), Some(batch)) = (sink.as_mut(), batch)
                        && writer.write(batch).await? == TaskStreamWriteState::Closed
                    {
                        *sink = None;
                    }
                    Ok::<_, DataFusionError>(())
                }),
        )
        .await?;
        Ok(if self.sinks.iter().any(Option::is_some) {
            TaskStreamWriteState::Active
        } else {
            TaskStreamWriteState::Closed
        })
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        // A consumer may finish without reading one channel (for example, an
        // empty hash-join build partition). Do not let that channel's buffered
        // data prevent other channels from reaching end-of-stream. Once the
        // consumers finish, stage cleanup releases any unread channels.
        try_join_all(self.sinks.into_iter().flatten().map(|sink| sink.commit())).await?;
        Ok(())
    }

    async fn abort(self: Box<Self>) -> Result<()> {
        for sink in self.sinks.into_iter().flatten() {
            sink.abort().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use datafusion::arrow::datatypes::Schema;
    use tokio::sync::Barrier;

    use super::*;

    struct TestSink {
        barrier: Option<Arc<Barrier>>,
        state: TaskStreamWriteState,
        fail: bool,
        aborted: Arc<AtomicUsize>,
    }

    #[tonic::async_trait]
    impl TaskStreamChannelSink for TestSink {
        async fn write(&mut self, _batch: RecordBatch) -> Result<TaskStreamWriteState> {
            if let Some(barrier) = &self.barrier {
                barrier.wait().await;
            }
            if self.fail {
                return Err(DataFusionError::Execution("write failed".to_string()));
            }
            Ok(self.state)
        }

        async fn commit(self: Box<Self>) -> Result<()> {
            Ok(())
        }

        async fn abort(self: Box<Self>) -> Result<()> {
            self.aborted.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn batch() -> Option<RecordBatch> {
        Some(RecordBatch::new_empty(Arc::new(Schema::empty())))
    }

    #[tokio::test]
    async fn writes_channels_concurrently() -> Result<()> {
        let barrier = Arc::new(Barrier::new(2));
        let mut sink = MultiChannelTaskStreamSink {
            sinks: (0..2)
                .map(|_| {
                    Some(Box::new(TestSink {
                        barrier: Some(barrier.clone()),
                        state: TaskStreamWriteState::Active,
                        fail: false,
                        aborted: Arc::new(AtomicUsize::new(0)),
                    }) as Box<dyn TaskStreamChannelSink>)
                })
                .collect(),
        };
        // A sequential writer cannot get past the first channel's barrier.
        for _ in 0..2 {
            let state =
                tokio::time::timeout(Duration::from_secs(5), sink.write(vec![batch(), batch()]))
                    .await
                    .map_err(|error| DataFusionError::External(Box::new(error)))??;
            assert_eq!(state, TaskStreamWriteState::Active);
        }
        Box::new(sink).commit().await
    }

    #[tokio::test]
    async fn stops_only_after_all_channels_close() -> Result<()> {
        let mut sink = MultiChannelTaskStreamSink {
            sinks: (0..2)
                .map(|_| {
                    Some(Box::new(TestSink {
                        barrier: None,
                        state: TaskStreamWriteState::Closed,
                        fail: false,
                        aborted: Arc::new(AtomicUsize::new(0)),
                    }) as Box<dyn TaskStreamChannelSink>)
                })
                .collect(),
        };
        assert_eq!(
            sink.write(vec![batch(), None]).await?,
            TaskStreamWriteState::Active
        );
        assert_eq!(
            sink.write(vec![batch(), batch()]).await?,
            TaskStreamWriteState::Closed
        );
        Ok(())
    }

    #[tokio::test]
    async fn retains_channels_for_abort_after_write_error() -> Result<()> {
        let aborted = Arc::new(AtomicUsize::new(0));
        let mut sink = MultiChannelTaskStreamSink {
            sinks: (0..2)
                .map(|i| {
                    Some(Box::new(TestSink {
                        // This channel remains pending when the other channel fails.
                        barrier: (i == 0).then(|| Arc::new(Barrier::new(2))),
                        state: TaskStreamWriteState::Active,
                        fail: i == 1,
                        aborted: aborted.clone(),
                    }) as Box<dyn TaskStreamChannelSink>)
                })
                .collect(),
        };
        let result =
            tokio::time::timeout(Duration::from_secs(5), sink.write(vec![batch(), batch()]))
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
        assert!(result.is_err());
        Box::new(sink).abort().await?;
        assert_eq!(aborted.load(Ordering::SeqCst), 2);
        Ok(())
    }
}
