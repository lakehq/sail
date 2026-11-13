#[cfg(test)]
mod tests;

use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Result, exec_err};
use datafusion::execution::runtime_env::RuntimeEnv;
use futures::StreamExt;
use sail_common_datafusion::replay::{ReplayId, ReplayPartition, ReplayPartitionBuilder};
use tokio_util::sync::CancellationToken;

use crate::error::{ExecutionError, ExecutionResult};
use crate::stream::error::TaskStreamError;
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{TaskStreamChannelSink, TaskStreamWriteState};

/// Publication and retirement share the same lock. A late sink cannot resurrect
/// data after its manager removes the stream.
pub struct ReplayStream {
    id: ReplayId,
    state: Arc<Mutex<State>>,
    runtime: Arc<RuntimeEnv>,
    cancellation: CancellationToken,
}

enum State {
    Building,
    Sealed(ReplayPartition),
    Failed,
    Retired,
}

impl ReplayStream {
    pub fn create(
        id: ReplayId,
        schema: SchemaRef,
        runtime: Arc<RuntimeEnv>,
        partition: usize,
    ) -> ExecutionResult<(Self, Box<dyn TaskStreamChannelSink>)> {
        let builder = ReplayPartitionBuilder::try_new(schema, runtime.clone(), partition)?;
        let state = Arc::new(Mutex::new(State::Building));
        let cancellation = CancellationToken::new();
        let sink = ReplaySink {
            builder: Some(builder),
            state: state.clone(),
            cancellation: cancellation.clone(),
        };
        Ok((
            Self {
                id,
                state,
                runtime,
                cancellation,
            },
            Box::new(sink),
        ))
    }

    pub fn subscribe(&self) -> ExecutionResult<TaskStreamSource> {
        let state = self.state.lock().map_err(ExecutionError::from)?;
        let State::Sealed(partition) = &*state else {
            return Err(ExecutionError::InternalError(format!(
                "replay {} is not sealed or is retired",
                self.id
            )));
        };
        let reader = partition.open_reader(&self.runtime)?;
        let cancellation = self.cancellation.clone();
        Ok(Box::pin(futures::stream::unfold(
            Some((reader, cancellation)),
            |state| async move {
                let (mut reader, cancellation) = state?;
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => Some((Err(TaskStreamError::Unknown("replay retired".into())), None)),
                    item = reader.next() => item.map(|item| (item.map_err(|e| TaskStreamError::External(Arc::new(e))), Some((reader, cancellation)))),
                }
            },
        )))
    }

    pub fn retire(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *state = State::Retired;
        self.cancellation.cancel();
    }
}

impl Drop for ReplayStream {
    fn drop(&mut self) {
        self.retire();
    }
}

struct ReplaySink {
    builder: Option<ReplayPartitionBuilder>,
    state: Arc<Mutex<State>>,
    cancellation: CancellationToken,
}

#[tonic::async_trait]
impl TaskStreamChannelSink for ReplaySink {
    async fn write(&mut self, batch: RecordBatch) -> Result<TaskStreamWriteState> {
        if self.cancellation.is_cancelled() {
            return exec_err!("replay retired");
        }
        let Some(builder) = &mut self.builder else {
            return exec_err!("replay writer is closed");
        };
        builder.append(batch)?;
        Ok(TaskStreamWriteState::Active)
    }

    async fn commit(mut self: Box<Self>) -> Result<()> {
        let Some(builder) = self.builder.take() else {
            return exec_err!("replay writer is closed");
        };
        let partition = builder.seal()?;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(*state, State::Building) || self.cancellation.is_cancelled() {
            return exec_err!("replay publication closed");
        }
        *state = State::Sealed(partition);
        Ok(())
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        self.builder.take();
        Ok(())
    }
}

impl Drop for ReplaySink {
    fn drop(&mut self) {
        self.builder.take();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(*state, State::Building) {
            *state = State::Failed;
        }
    }
}
