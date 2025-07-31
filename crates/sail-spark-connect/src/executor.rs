use std::collections::VecDeque;
use std::io::Cursor;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::execution::SendableRecordBatchStream;
use futures::stream::{StreamExt, TryStreamExt};
use sail_runtime::RuntimeHandle;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

use crate::error::{SparkError, SparkResult};
use crate::schema::to_spark_schema;
use crate::spark::connect::execute_plan_response::{
    ArrowBatch, Metrics, ObservedMetrics, SqlCommandResult,
};
use crate::spark::connect::DataType;

#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum ExecutorBatch {
    ArrowBatch(ArrowBatch),
    SqlCommandResult(SqlCommandResult),
    Schema(DataType),
    #[allow(dead_code)]
    Metrics(Metrics),
    #[allow(dead_code)]
    ObservedMetrics(Vec<ObservedMetrics>),
    Complete,
}

#[derive(Clone, Debug)]
pub(crate) struct ExecutorOutput {
    pub(crate) id: String,
    pub(crate) batch: ExecutorBatch,
}

impl ExecutorOutput {
    pub(crate) fn new(batch: ExecutorBatch) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            batch,
        }
    }

    pub(crate) fn complete() -> Self {
        Self::new(ExecutorBatch::Complete)
    }
}

struct ExecutorBuffer {
    inner: VecDeque<ExecutorOutput>,
}

impl ExecutorBuffer {
    fn new() -> Self {
        // TODO: use "spark.connect.execute.reattachable.observerRetryBufferSize"
        // TODO: limit the size based on serialized message size instead of element count
        Self {
            inner: VecDeque::with_capacity(128),
        }
    }

    fn add(&mut self, output: ExecutorOutput) {
        if self.inner.len() >= self.inner.capacity() {
            self.inner.pop_front();
        }
        self.inner.push_back(output);
    }

    fn remove_until(&mut self, id: &str) {
        let index = self.inner.iter().position(|x| x.id == id);
        if let Some(index) = index {
            self.inner.drain(0..=index);
        }
    }

    fn iter(&self) -> impl Iterator<Item = &ExecutorOutput> {
        self.inner.iter()
    }
}

pub(crate) struct ExecutorMetadata {
    pub(crate) operation_id: String,
    pub(crate) tags: Vec<String>,
    pub(crate) reattachable: bool,
}

pub(crate) struct Executor {
    pub(crate) metadata: ExecutorMetadata,
    state: Mutex<ExecutorState>,
    runtime: RuntimeHandle,
}

#[allow(clippy::large_enum_variant)]
enum ExecutorState {
    Idle,
    Pending(ExecutorTaskContext),
    Running(ExecutorTask),
    Pausing,
    // FIXME: Rust 1.87 triggers `clippy::large_enum_variant` warning
    Failed(SparkError),
}

struct ExecutorTask {
    notifier: oneshot::Sender<()>,
    handle: JoinHandle<ExecutorTaskResult>,
    buffer: Arc<Mutex<ExecutorBuffer>>,
}

struct ExecutorTaskContext {
    stream: SendableRecordBatchStream,
    buffer: Arc<Mutex<ExecutorBuffer>>,
}

impl ExecutorTaskContext {
    fn new(stream: SendableRecordBatchStream) -> Self {
        Self {
            stream,
            buffer: Arc::new(Mutex::new(ExecutorBuffer::new())),
        }
    }

    fn save_output(&self, output: &ExecutorOutput) -> SparkResult<()> {
        let mut buffer = self.buffer.lock()?;
        buffer.add(output.clone());
        Ok(())
    }

    fn replay_outputs(&self) -> SparkResult<Vec<ExecutorOutput>> {
        let buffer = self.buffer.lock()?;
        Ok(buffer.iter().cloned().collect())
    }
}

#[allow(clippy::large_enum_variant)]
enum ExecutorTaskResult {
    Paused(ExecutorTaskContext),
    // FIXME: Rust 1.87 triggers `clippy::large_enum_variant` warning
    Failed(SparkError),
    Completed,
}

impl Executor {
    pub(crate) fn new(
        metadata: ExecutorMetadata,
        stream: SendableRecordBatchStream,
        runtime: RuntimeHandle,
    ) -> Self {
        Self {
            metadata,
            state: Mutex::new(ExecutorState::Pending(ExecutorTaskContext::new(stream))),
            runtime,
        }
    }

    async fn run_internal(
        context: &mut ExecutorTaskContext,
        tx: mpsc::Sender<ExecutorOutput>,
    ) -> SparkResult<()> {
        for out in context.replay_outputs()? {
            tx.send(out).await?;
        }
        let schema = to_spark_schema(context.stream.schema())?;
        let out = ExecutorOutput::new(ExecutorBatch::Schema(schema));
        context.save_output(&out)?;
        tx.send(out).await?;

        let mut empty = true;
        while let Some(batch) = context.stream.next().await {
            // tokio::task::yield_now().await;
            let batch = batch?;
            let batch = to_arrow_batch(&batch)?;
            let out = ExecutorOutput::new(ExecutorBatch::ArrowBatch(batch));
            context.save_output(&out)?;
            tx.send(out).await?;
            empty = false;
        }
        if empty {
            let batch = RecordBatch::new_empty(context.stream.schema());
            let batch = to_arrow_batch(&batch)?;
            let out = ExecutorOutput::new(ExecutorBatch::ArrowBatch(batch));
            context.save_output(&out)?;
            tx.send(out).await?;
        }

        let out = ExecutorOutput::new(ExecutorBatch::Complete);
        context.save_output(&out)?;
        tx.send(out).await?;
        Ok(())
    }

    async fn run(
        mut context: ExecutorTaskContext,
        listener: oneshot::Receiver<()>,
        tx: mpsc::Sender<ExecutorOutput>,
    ) -> ExecutorTaskResult {
        let out = tokio::select! {
            biased;
            x = Executor::run_internal(&mut context, tx) => x,
            _ = listener => return ExecutorTaskResult::Paused(context),
        };
        match out {
            Ok(()) => ExecutorTaskResult::Completed,
            Err(SparkError::SendError(_)) => ExecutorTaskResult::Paused(context),
            Err(e) => ExecutorTaskResult::Failed(e),
        }
    }

    pub(crate) fn start(&self) -> SparkResult<ReceiverStream<ExecutorOutput>> {
        let mut state = self.state.lock()?;
        let context = match mem::replace(state.deref_mut(), ExecutorState::Idle) {
            ExecutorState::Pending(context) => context,
            ExecutorState::Failed(e) => {
                *state = ExecutorState::Failed(SparkError::internal(
                    "task failed due to a previous error",
                ));
                return Err(e);
            }
            x @ ExecutorState::Idle => {
                *state = x;
                return Err(SparkError::internal("task context not found for operation"));
            }
            x @ ExecutorState::Running(_) => {
                *state = x;
                return Err(SparkError::internal("task is already running"));
            }
            x @ ExecutorState::Pausing => {
                *state = x;
                return Err(SparkError::internal("task is being paused"));
            }
        };
        let (tx, rx) = mpsc::channel(2);
        let (notifier, listener) = oneshot::channel();
        let buffer = Arc::clone(&context.buffer);
        // let handle = self.runtime.primary().clone();
        let handle = self.runtime.cpu().clone();
        let handle = handle.spawn(async move { Executor::run(context, listener, tx).await });
        // let handle = tokio::spawn(async move { Executor::run(context, listener, tx).await });
        *state = ExecutorState::Running(ExecutorTask {
            notifier,
            handle,
            buffer,
        });
        Ok(ReceiverStream::new(rx))
    }

    pub(crate) async fn pause_if_running(&self) -> SparkResult<()> {
        let task = {
            let mut state = self.state.lock()?;
            match mem::replace(state.deref_mut(), ExecutorState::Idle) {
                ExecutorState::Running(task) => {
                    *state = ExecutorState::Pausing;
                    task
                }
                x => {
                    *state = x;
                    return Ok(());
                }
            }
        };
        let _ = task.notifier.send(());
        let state = match task.handle.await? {
            ExecutorTaskResult::Paused(context) => ExecutorState::Pending(context),
            ExecutorTaskResult::Completed => ExecutorState::Idle,
            ExecutorTaskResult::Failed(e) => ExecutorState::Failed(e),
        };
        *(self.state.lock()?) = state;
        Ok(())
    }

    pub(crate) fn release(&self, response_id: Option<String>) -> SparkResult<()> {
        let state = self.state.lock()?;
        let buffer = match state.deref() {
            ExecutorState::Running(task) => &task.buffer,
            ExecutorState::Pending(context) => &context.buffer,
            ExecutorState::Idle | ExecutorState::Failed(_) | ExecutorState::Pausing => {
                return Ok(())
            }
        };
        if let Some(response_id) = response_id {
            buffer.lock()?.remove_until(&response_id);
        }
        Ok(())
    }
}

pub(crate) async fn read_stream(
    stream: SendableRecordBatchStream,
    handle: Handle,
) -> SparkResult<Vec<RecordBatch>> {
    // let mut output = vec![];
    // while let Some(batch) = stream.next().await {
    //     let batch = batch?;
    //     output.push(batch);
    // }
    // Ok(output)

    handle
        .spawn(async move { stream.err_into().try_collect::<Vec<_>>().await })
        .await
        .map_err(|e| SparkError::internal(format!("failed to execute on CPU runtime: {e}")))?
}

// pub(crate) async fn collect_partitioned(
//     streams: Vec<SendableRecordBatchStream>,
// ) -> SparkResult<Vec<Vec<RecordBatch>>> {
//     let mut join_set = datafusion::common::runtime::JoinSet::new();
//     // Execute the plan and collect the results into batches.
//     streams.into_iter().enumerate().for_each(|(idx, stream)| {
//         join_set.spawn(async move {
//             let result: SparkResult<Vec<RecordBatch>> = stream.try_collect().await.map_err(|e| {
//                 SparkError::internal(format!(
//                     "failed to collect partitioned stream at index {idx}: {e}"
//                 ))
//             });
//             (idx, result)
//         });
//     });
//
//     let mut batches = vec![];
//     // Note that currently this doesn't identify the thread that panicked
//     //
//     // TODO: Replace with [join_next_with_id](https://docs.rs/tokio/latest/tokio/task/struct.JoinSet.html#method.join_next_with_id
//     // once it is stable
//     while let Some(result) = join_set.join_next().await {
//         match result {
//             Ok((idx, res)) => batches.push((idx, res?)),
//             Err(e) => {
//                 if e.is_panic() {
//                     std::panic::resume_unwind(e.into_panic());
//                 } else {
//                     unreachable!();
//                 }
//             }
//         }
//     }
//
//     batches.sort_by_key(|(idx, _)| *idx);
//     let batches = batches.into_iter().map(|(_, batch)| batch).collect();
//
//     Ok(batches)
// }

pub(crate) fn to_arrow_batch(batch: &RecordBatch) -> SparkResult<ArrowBatch> {
    let mut output = ArrowBatch::default();
    {
        let cursor = Cursor::new(&mut output.data);
        let mut writer = StreamWriter::try_new(cursor, batch.schema().as_ref())?;
        writer.write(batch)?;
        output.row_count += batch.num_rows() as i64;
        writer.finish()?;
    }
    Ok(output)
}
