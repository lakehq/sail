use std::collections::VecDeque;
use std::io::Cursor;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, FieldRef, Fields, Schema};
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::execution::SendableRecordBatchStream;
use fastrace::future::FutureExt;
use fastrace::Span;
use futures::stream::{StreamExt, TryStreamExt};
use futures::Stream;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

use crate::error::{SparkError, SparkResult};
use crate::schema::to_spark_schema;
use crate::spark::connect::execute_plan_response::{ArrowBatch, SqlCommandResult};
use crate::spark::connect::{
    CheckpointCommandResult, DataType, StreamingQueryCommandResult,
    StreamingQueryManagerCommandResult, WriteStreamOperationStartResult,
};

#[derive(Clone, Debug)]
pub enum ExecutorBatch {
    ArrowBatch(ArrowBatch),
    SqlCommandResult(Box<SqlCommandResult>),
    WriteStreamOperationStartResult(Box<WriteStreamOperationStartResult>),
    StreamingQueryCommandResult(Box<StreamingQueryCommandResult>),
    StreamingQueryManagerCommandResult(Box<StreamingQueryManagerCommandResult>),
    CheckpointCommandResult(Box<CheckpointCommandResult>),
    Schema(Box<DataType>),
    Complete,
}

#[derive(Clone, Debug)]
pub struct ExecutorOutput {
    pub(crate) id: String,
    pub(crate) batch: ExecutorBatch,
}

impl ExecutorOutput {
    pub fn new(batch: ExecutorBatch) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            batch,
        }
    }

    pub fn complete() -> Self {
        Self::new(ExecutorBatch::Complete)
    }
}

pub type ExecutorOutputStream = Pin<Box<dyn Stream<Item = ExecutorOutput> + Send>>;

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
}

enum ExecutorState {
    Idle,
    Pending {
        context: ExecutorTaskContext,
        span: Span,
    },
    Running {
        task: ExecutorTask,
        span: Span,
    },
    Pausing,
    Failed(SparkError),
}

struct ExecutorTask {
    notifier: oneshot::Sender<()>,
    handle: JoinHandle<ExecutorTaskResult>,
    buffer: Arc<Mutex<ExecutorBuffer>>,
}

struct ExecutorTaskContext {
    stream: SendableRecordBatchStream,
    heartbeat_interval: Duration,
    buffer: Arc<Mutex<ExecutorBuffer>>,
}

impl ExecutorTaskContext {
    fn new(stream: SendableRecordBatchStream, heartbeat_interval: Duration) -> Self {
        Self {
            stream,
            heartbeat_interval,
            buffer: Arc::new(Mutex::new(ExecutorBuffer::new())),
        }
    }

    async fn next(&mut self) -> SparkResult<Option<RecordBatch>> {
        let span = Span::enter_with_local_parent("ExecutorTaskContext::next");
        tokio::select! {
            batch = self.stream.next().in_span(span) => Ok(batch.transpose()?),
            _ = tokio::time::sleep(self.heartbeat_interval) => {
                Ok(Some(RecordBatch::new_empty(self.stream.schema())))
            }
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

enum ExecutorTaskResult {
    Paused(ExecutorTaskContext),
    Failed(SparkError),
    Completed,
}

impl Executor {
    pub(crate) fn new(
        metadata: ExecutorMetadata,
        stream: SendableRecordBatchStream,
        heartbeat_interval: Duration,
    ) -> Self {
        Self {
            metadata,
            state: Mutex::new(ExecutorState::Pending {
                context: ExecutorTaskContext::new(stream, heartbeat_interval),
                span: Span::enter_with_local_parent("Executor::new"),
            }),
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
        let out = ExecutorOutput::new(ExecutorBatch::Schema(Box::new(schema)));
        context.save_output(&out)?;
        tx.send(out).await?;

        let mut empty = true;
        while let Some(batch) = context.next().await? {
            let batch = to_spark_execute_arrow_batch(&batch)?;
            let out = ExecutorOutput::new(ExecutorBatch::ArrowBatch(batch));
            context.save_output(&out)?;
            tx.send(out).await?;
            empty = false;
        }
        if empty {
            let batch = RecordBatch::new_empty(context.stream.schema());
            let batch = to_spark_execute_arrow_batch(&batch)?;
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
        let (context, span) = match mem::replace(state.deref_mut(), ExecutorState::Idle) {
            ExecutorState::Pending { context, span } => (context, span),
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
            x @ ExecutorState::Running { .. } => {
                *state = x;
                return Err(SparkError::internal("task is already running"));
            }
            x @ ExecutorState::Pausing => {
                *state = x;
                return Err(SparkError::internal("task is being paused"));
            }
        };
        let (tx, rx) = mpsc::channel(1);
        let (notifier, listener) = oneshot::channel();
        let buffer = Arc::clone(&context.buffer);
        let handle = {
            let span = { Span::enter_with_parent("Executor::run", &span) };
            tokio::spawn(async move { Executor::run(context, listener, tx).in_span(span).await })
        };
        *state = ExecutorState::Running {
            task: ExecutorTask {
                notifier,
                handle,
                buffer,
            },
            span,
        };
        Ok(ReceiverStream::new(rx))
    }

    pub(crate) async fn pause_if_running(&self) -> SparkResult<()> {
        let (task, span) = {
            let mut state = self.state.lock()?;
            match mem::replace(state.deref_mut(), ExecutorState::Idle) {
                ExecutorState::Running { task, span } => {
                    *state = ExecutorState::Pausing;
                    (task, span)
                }
                x => {
                    *state = x;
                    return Ok(());
                }
            }
        };
        let _ = task.notifier.send(());
        let state = match task.handle.await? {
            ExecutorTaskResult::Paused(context) => ExecutorState::Pending { context, span },
            ExecutorTaskResult::Completed => ExecutorState::Idle,
            ExecutorTaskResult::Failed(e) => ExecutorState::Failed(e),
        };
        *(self.state.lock()?) = state;
        Ok(())
    }

    pub(crate) fn release(&self, response_id: Option<String>) -> SparkResult<()> {
        let state = self.state.lock()?;
        let buffer = match state.deref() {
            ExecutorState::Running { task, span: _ } => &task.buffer,
            ExecutorState::Pending { context, span: _ } => &context.buffer,
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
) -> SparkResult<Vec<RecordBatch>> {
    stream.err_into().try_collect::<Vec<_>>().await
}

pub(crate) fn to_arrow_batch(batch: &RecordBatch) -> SparkResult<ArrowBatch> {
    to_arrow_batch_with_schema(batch, batch.schema().as_ref())
}

fn to_spark_execute_arrow_batch(batch: &RecordBatch) -> SparkResult<ArrowBatch> {
    let schema = to_spark_execute_arrow_schema(batch.schema().as_ref());
    let batch = RecordBatch::try_new_with_options(
        schema.into(),
        batch.columns().to_vec(),
        &RecordBatchOptions::new().with_match_field_names(false),
    )?;
    to_arrow_batch_with_schema(&batch, batch.schema().as_ref())
}

fn to_arrow_batch_with_schema(batch: &RecordBatch, schema: &Schema) -> SparkResult<ArrowBatch> {
    let mut output = ArrowBatch::default();
    {
        let cursor = Cursor::new(&mut output.data);
        let mut writer = StreamWriter::try_new(cursor, schema)?;
        writer.write(batch)?;
        output.row_count += batch.num_rows() as i64;
        writer.finish()?;
    }
    Ok(output)
}

fn to_spark_execute_arrow_schema(schema: &Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| sanitize_arrow_field(field, format!("col_{i}")))
        .collect::<Vec<_>>();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

fn sanitize_arrow_fields(fields: &Fields) -> Fields {
    let names = deduplicate_arrow_field_names(fields.iter().map(|field| field.name()));
    fields
        .iter()
        .zip(names)
        .map(|(field, name)| sanitize_arrow_field(field, name))
        .collect::<Vec<_>>()
        .into()
}

fn sanitize_arrow_field(field: &FieldRef, name: String) -> FieldRef {
    Field::new(name, sanitize_arrow_data_type(field.data_type()), field.is_nullable())
        .with_metadata(field.metadata().clone())
        .into()
}

fn sanitize_arrow_data_type(data_type: &ArrowDataType) -> ArrowDataType {
    match data_type {
        ArrowDataType::Struct(fields) => ArrowDataType::Struct(sanitize_arrow_fields(fields)),
        ArrowDataType::List(field) => ArrowDataType::List(sanitize_arrow_field(field, field.name().clone())),
        ArrowDataType::LargeList(field) => {
            ArrowDataType::LargeList(sanitize_arrow_field(field, field.name().clone()))
        }
        ArrowDataType::FixedSizeList(field, size) => {
            ArrowDataType::FixedSizeList(sanitize_arrow_field(field, field.name().clone()), *size)
        }
        ArrowDataType::ListView(field) => {
            ArrowDataType::ListView(sanitize_arrow_field(field, field.name().clone()))
        }
        ArrowDataType::LargeListView(field) => {
            ArrowDataType::LargeListView(sanitize_arrow_field(field, field.name().clone()))
        }
        ArrowDataType::Map(field, keys_sorted) => {
            ArrowDataType::Map(sanitize_arrow_field(field, field.name().clone()), *keys_sorted)
        }
        ArrowDataType::Dictionary(key_type, value_type) => ArrowDataType::Dictionary(
            key_type.clone(),
            Box::new(sanitize_arrow_data_type(value_type.as_ref())),
        ),
        ArrowDataType::RunEndEncoded(run_ends, values) => ArrowDataType::RunEndEncoded(
            sanitize_arrow_field(run_ends, run_ends.name().clone()),
            sanitize_arrow_field(values, values.name().clone()),
        ),
        ArrowDataType::Union(fields, mode) => {
            let sanitized = fields
                .iter()
                .map(|(type_id, field)| (type_id, sanitize_arrow_field(field, field.name().clone())))
                .collect();
            ArrowDataType::Union(sanitized, *mode)
        }
        _ => data_type.clone(),
    }
}

fn deduplicate_arrow_field_names<'a>(names: impl IntoIterator<Item = &'a String>) -> Vec<String> {
    let names = names.into_iter().collect::<Vec<_>>();
    names.iter()
        .enumerate()
        .map(|(i, name)| {
            let count = names.iter().filter(|x| x.as_str() == name.as_str()).count();
            if count > 1 {
                let index = names[..i]
                    .iter()
                    .filter(|x| x.as_str() == name.as_str())
                    .count();
                format!("{name}_{index}")
            } else {
                (*name).clone()
            }
        })
        .collect()
}
