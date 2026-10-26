use std::collections::VecDeque;
use std::io::Cursor;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::arrow::array::{Array, ArrayData, ArrayRef, RecordBatch, make_array};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, FieldRef, Fields, Schema, SchemaRef,
};
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::execution::SendableRecordBatchStream;
use fastrace::Span;
use fastrace::future::FutureExt;
use futures::stream::StreamExt;
use futures::{Stream, stream};
use sail_common_datafusion::schema::{normalize_record_batch, record_batch_has_views};
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
    Heartbeat,
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

pub type ExecutorOutputStream = Pin<Box<dyn Stream<Item = SparkResult<ExecutorOutput>> + Send>>;

struct ExecutorBuffer {
    capacity: usize,
    inner: VecDeque<ExecutorOutput>,
}

// TODO: use "spark.connect.execute.reattachable.observerRetryBufferSize"
// TODO: limit the size based on serialized message size instead of element count
const EXECUTOR_BUFFER_CAPACITY: usize = 128;

impl ExecutorBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            inner: VecDeque::with_capacity(capacity),
        }
    }

    fn add(&mut self, output: ExecutorOutput) {
        if self.capacity == 0 {
            return;
        }
        if self.inner.len() >= self.capacity {
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
    Completed {
        buffer: Arc<Mutex<ExecutorBuffer>>,
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
    state: ExecutorTaskState,
    buffer: Arc<Mutex<ExecutorBuffer>>,
}

pub(crate) enum ExecutorMode {
    Query,
    Command {
        completion: CommandCompletionHandler,
    },
}

impl ExecutorMode {
    pub(crate) fn command() -> Self {
        Self::command_with_completion(|_, _| Ok(None))
    }

    pub(crate) fn command_with_completion(
        completion: impl FnOnce(SchemaRef, Vec<RecordBatch>) -> SparkResult<Option<ExecutorOutput>>
        + Send
        + 'static,
    ) -> Self {
        Self::Command {
            completion: Box::new(completion),
        }
    }
}

pub(crate) enum ExecutorTaskState {
    Query,
    Command {
        batches: Vec<RecordBatch>,
        completion: Option<CommandCompletionHandler>,
    },
    Completed,
}

enum ExecutorTaskItem {
    Batch(Option<RecordBatch>),
    Heartbeat,
}

pub(crate) type CommandCompletionHandler =
    Box<dyn FnOnce(SchemaRef, Vec<RecordBatch>) -> SparkResult<Option<ExecutorOutput>> + Send>;

impl ExecutorTaskContext {
    async fn next(
        stream: &mut SendableRecordBatchStream,
        heartbeat_interval: Duration,
    ) -> SparkResult<ExecutorTaskItem> {
        let span = Span::enter_with_local_parent("ExecutorTaskContext::next");
        tokio::select! {
            batch = stream.next().in_span(span) => Ok(ExecutorTaskItem::Batch(batch.transpose()?)),
            _ = tokio::time::sleep(heartbeat_interval) => {
                // FIXME: non-reattachable clients cannot refresh session activity by releasing heartbeat responses
                Ok(ExecutorTaskItem::Heartbeat)
            }
        }
    }

    async fn send_output(
        buffer: &Arc<Mutex<ExecutorBuffer>>,
        tx: &mpsc::Sender<SparkResult<ExecutorOutput>>,
        output: ExecutorOutput,
    ) -> SparkResult<()> {
        {
            let mut buffer = buffer.lock()?;
            buffer.add(output.clone());
        }
        tx.send(Ok(output)).await?;
        Ok(())
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
        mode: ExecutorMode,
    ) -> Self {
        let state = match mode {
            ExecutorMode::Query => ExecutorTaskState::Query,
            ExecutorMode::Command { completion } => ExecutorTaskState::Command {
                batches: vec![],
                completion: Some(completion),
            },
        };
        let buffer = if metadata.reattachable {
            ExecutorBuffer::new(EXECUTOR_BUFFER_CAPACITY)
        } else {
            ExecutorBuffer::new(0)
        };
        Self {
            metadata,
            state: Mutex::new(ExecutorState::Pending {
                context: ExecutorTaskContext {
                    stream,
                    heartbeat_interval,
                    state,
                    buffer: Arc::new(Mutex::new(buffer)),
                },
                span: Span::enter_with_local_parent("Executor::new"),
            }),
        }
    }

    pub(crate) fn completed(metadata: ExecutorMetadata, outputs: Vec<ExecutorOutput>) -> Self {
        let mut buffer = ExecutorBuffer::new(outputs.len());
        for output in outputs {
            buffer.add(output);
        }
        Self {
            metadata,
            state: Mutex::new(ExecutorState::Completed {
                buffer: Arc::new(Mutex::new(buffer)),
            }),
        }
    }

    async fn run_internal(
        context: &mut ExecutorTaskContext,
        tx: &mpsc::Sender<SparkResult<ExecutorOutput>>,
        reattachable: bool,
    ) -> SparkResult<()> {
        let outputs = {
            let buffer = context.buffer.lock()?;
            buffer.iter().cloned().collect::<Vec<_>>()
        };
        for output in outputs {
            tx.send(Ok(output)).await?;
        }
        match &mut context.state {
            ExecutorTaskState::Query => {
                let schema = to_spark_schema(context.stream.schema())?;
                ExecutorTaskContext::send_output(
                    &context.buffer,
                    tx,
                    ExecutorOutput::new(ExecutorBatch::Schema(Box::new(schema))),
                )
                .await?;

                let mut empty = true;
                loop {
                    match ExecutorTaskContext::next(&mut context.stream, context.heartbeat_interval)
                        .await?
                    {
                        ExecutorTaskItem::Batch(Some(batch)) => {
                            let batch = to_arrow_batch(&batch)?;
                            ExecutorTaskContext::send_output(
                                &context.buffer,
                                tx,
                                ExecutorOutput::new(ExecutorBatch::ArrowBatch(batch)),
                            )
                            .await?;
                            empty = false;
                        }
                        ExecutorTaskItem::Batch(None) => break,
                        ExecutorTaskItem::Heartbeat => {
                            ExecutorTaskContext::send_output(
                                &context.buffer,
                                tx,
                                ExecutorOutput::new(ExecutorBatch::Heartbeat),
                            )
                            .await?;
                        }
                    }
                }
                if empty {
                    let batch = RecordBatch::new_empty(context.stream.schema());
                    let batch = to_arrow_batch(&batch)?;
                    ExecutorTaskContext::send_output(
                        &context.buffer,
                        tx,
                        ExecutorOutput::new(ExecutorBatch::ArrowBatch(batch)),
                    )
                    .await?;
                }
            }
            ExecutorTaskState::Command {
                batches,
                completion,
            } => {
                loop {
                    match ExecutorTaskContext::next(&mut context.stream, context.heartbeat_interval)
                        .await?
                    {
                        ExecutorTaskItem::Batch(Some(batch)) => {
                            batches.push(batch);
                        }
                        ExecutorTaskItem::Batch(None) => break,
                        ExecutorTaskItem::Heartbeat => {
                            ExecutorTaskContext::send_output(
                                &context.buffer,
                                tx,
                                ExecutorOutput::new(ExecutorBatch::Heartbeat),
                            )
                            .await?;
                        }
                    }
                }
                let schema = context.stream.schema();
                let output = completion
                    .take()
                    .map(|completion| completion(schema, mem::take(batches)))
                    .transpose()?
                    .flatten();
                if let Some(output) = output {
                    ExecutorTaskContext::send_output(&context.buffer, tx, output).await?;
                }
            }
            ExecutorTaskState::Completed => return Ok(()),
        }
        // A disconnect while sending completion must only replay the buffered response.
        context.state = ExecutorTaskState::Completed;
        if reattachable {
            ExecutorTaskContext::send_output(&context.buffer, tx, ExecutorOutput::complete())
                .await?;
        }
        Ok(())
    }

    async fn run(
        mut context: ExecutorTaskContext,
        listener: oneshot::Receiver<()>,
        tx: mpsc::Sender<SparkResult<ExecutorOutput>>,
        reattachable: bool,
    ) -> ExecutorTaskResult {
        let out = tokio::select! {
            x = Executor::run_internal(&mut context, &tx, reattachable) => x,
            _ = listener => return ExecutorTaskResult::Paused(context),
        };
        match out {
            Ok(()) => ExecutorTaskResult::Completed,
            Err(SparkError::SendError(_)) => ExecutorTaskResult::Paused(context),
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                // TODO: track the original error in the task result
                ExecutorTaskResult::Failed(SparkError::internal(
                    "task failed while executing the plan",
                ))
            }
        }
    }

    pub(crate) fn start(&self) -> SparkResult<ExecutorOutputStream> {
        let mut state = self.state.lock()?;
        let (context, span) = match mem::replace(state.deref_mut(), ExecutorState::Idle) {
            ExecutorState::Pending { context, span } => (context, span),
            ExecutorState::Completed { buffer } => {
                let outputs = buffer.lock()?.iter().cloned().collect::<Vec<_>>();
                *state = ExecutorState::Completed { buffer };
                return Ok(Box::pin(stream::iter(outputs.into_iter().map(Ok))));
            }
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
        let reattachable = self.metadata.reattachable;
        let handle = {
            let span = { Span::enter_with_parent("Executor::run", &span) };
            tokio::spawn(async move {
                Executor::run(context, listener, tx, reattachable)
                    .in_span(span)
                    .await
            })
        };
        *state = ExecutorState::Running {
            task: ExecutorTask {
                notifier,
                handle,
                buffer,
            },
            span,
        };
        Ok(Box::pin(ReceiverStream::new(rx)))
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
            ExecutorTaskResult::Completed => ExecutorState::Completed {
                buffer: task.buffer,
            },
            ExecutorTaskResult::Failed(e) => ExecutorState::Failed(e),
        };
        *(self.state.lock()?) = state;
        Ok(())
    }

    pub(crate) fn release(&self, response_id: String) -> SparkResult<()> {
        let state = self.state.lock()?;
        let buffer = match state.deref() {
            ExecutorState::Running { task, span: _ } => &task.buffer,
            ExecutorState::Pending { context, span: _ } => &context.buffer,
            ExecutorState::Completed { buffer } => buffer,
            ExecutorState::Idle | ExecutorState::Failed(_) | ExecutorState::Pausing => {
                return Ok(());
            }
        };
        buffer.lock()?.remove_until(&response_id);
        Ok(())
    }
}

pub(crate) fn to_arrow_batch(batch: &RecordBatch) -> SparkResult<ArrowBatch> {
    let batch = &deduplicate_struct_field_names(batch)?;
    let mut output = ArrowBatch::default();
    {
        // Fallback incase view types weren't handled in planning/execution.
        let batch = if record_batch_has_views(batch) {
            // TODO: Look into passing the session's `spark.sql.execution.arrow.useLargeVarTypes` value.
            // TODO: Look into making `large_to_utf8_and_binary` configurable for Spark Connect clients that may not support `LargeUtf8` or `LargeBinary`.
            &normalize_record_batch(batch, false, false)?
        } else {
            batch
        };
        let cursor = Cursor::new(&mut output.data);
        let mut writer = StreamWriter::try_new(cursor, batch.schema().as_ref())?;
        writer.write(batch)?;
        output.row_count += batch.num_rows() as i64;
        writer.finish()?;
    }
    Ok(output)
}

/// Renames duplicate struct field names in the Arrow data sent to the client.
///
/// The Spark Connect client converts a struct value to a row by looking up each field
/// by a deduplicated name (`<name>_<index>` for a name that appears more than once in
/// the struct), and takes the original names from the schema in the response.
/// A struct with duplicate field names cannot be converted otherwise.
fn deduplicate_struct_field_names(batch: &RecordBatch) -> SparkResult<RecordBatch> {
    let schema = batch.schema();
    let fields = schema
        .fields()
        .iter()
        .map(deduplicate_field)
        .collect::<Vec<_>>();
    if fields
        .iter()
        .zip(schema.fields().iter())
        .all(|(new, old)| new == old)
    {
        return Ok(batch.clone());
    }
    let columns = batch
        .columns()
        .iter()
        .zip(fields.iter())
        .map(|(column, field)| relabel_array(column, field.data_type()))
        .collect::<SparkResult<Vec<_>>>()?;
    let schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn deduplicate_field(field: &FieldRef) -> FieldRef {
    let data_type = deduplicate_data_type(field.data_type());
    if &data_type == field.data_type() {
        Arc::clone(field)
    } else {
        Arc::new(field.as_ref().clone().with_data_type(data_type))
    }
}

fn deduplicate_data_type(data_type: &ArrowDataType) -> ArrowDataType {
    match data_type {
        ArrowDataType::Struct(fields) => {
            let names = deduplicate_names(fields);
            let fields = fields
                .iter()
                .zip(names)
                .map(|(field, name)| {
                    let field = deduplicate_field(field);
                    if field.name() == &name {
                        field
                    } else {
                        Arc::new(field.as_ref().clone().with_name(name))
                    }
                })
                .collect::<Fields>();
            ArrowDataType::Struct(fields)
        }
        ArrowDataType::List(field) => ArrowDataType::List(deduplicate_field(field)),
        ArrowDataType::LargeList(field) => ArrowDataType::LargeList(deduplicate_field(field)),
        ArrowDataType::FixedSizeList(field, size) => {
            ArrowDataType::FixedSizeList(deduplicate_field(field), *size)
        }
        ArrowDataType::Map(field, sorted) => ArrowDataType::Map(deduplicate_field(field), *sorted),
        other => other.clone(),
    }
}

/// Follows `_dedup_names` in `pyspark.sql.pandas.types`.
fn deduplicate_names(fields: &Fields) -> Vec<String> {
    let mut seen = std::collections::HashMap::<&str, usize>::new();
    fields
        .iter()
        .map(|field| {
            let total = fields.iter().filter(|f| f.name() == field.name()).count();
            if total > 1 {
                let index = seen.entry(field.name().as_str()).or_insert(0);
                let name = format!("{}_{}", field.name(), index);
                *index += 1;
                name
            } else {
                field.name().clone()
            }
        })
        .collect()
}

/// Gives an array a data type that differs only in field names.
fn relabel_array(array: &ArrayRef, data_type: &ArrowDataType) -> SparkResult<ArrayRef> {
    if array.data_type() == data_type {
        return Ok(Arc::clone(array));
    }
    Ok(make_array(relabel_data(array.to_data(), data_type)?))
}

fn relabel_data(data: ArrayData, data_type: &ArrowDataType) -> SparkResult<ArrayData> {
    let child_types: Vec<&ArrowDataType> = match data_type {
        ArrowDataType::Struct(fields) => fields.iter().map(|f| f.data_type()).collect(),
        ArrowDataType::List(field)
        | ArrowDataType::LargeList(field)
        | ArrowDataType::FixedSizeList(field, _)
        | ArrowDataType::Map(field, _) => vec![field.data_type()],
        _ => vec![],
    };
    let children = data
        .child_data()
        .iter()
        .zip(child_types)
        .map(|(child, child_type)| relabel_data(child.clone(), child_type))
        .collect::<SparkResult<Vec<_>>>()?;
    let data = data
        .into_builder()
        .data_type(data_type.clone())
        .child_data(children)
        .build()?;
    Ok(data)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int32Array, StringArray, StructArray};
    use datafusion::arrow::datatypes::Field;

    use super::*;

    #[test]
    fn deduplicates_nested_struct_field_names() -> SparkResult<()> {
        let fields = Fields::from(vec![
            Field::new("x", ArrowDataType::Utf8, true),
            Field::new("x", ArrowDataType::Int32, true),
            Field::new("y", ArrowDataType::Int32, true),
        ]);
        let column = Arc::new(StructArray::new(
            fields.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(Int32Array::from(vec![2])) as ArrayRef,
            ],
            None,
        )) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            ArrowDataType::Struct(fields),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![column])?;

        let batch = deduplicate_struct_field_names(&batch)?;

        let ArrowDataType::Struct(fields) = batch.schema().field(0).data_type().clone() else {
            return Err(SparkError::internal("expected a struct"));
        };
        let names = fields.iter().map(|f| f.name().as_str()).collect::<Vec<_>>();
        assert_eq!(names, vec!["x_0", "x_1", "y"]);
        assert_eq!(batch.num_rows(), 1);
        Ok(())
    }

    #[test]
    fn keeps_batches_without_duplicate_names() -> SparkResult<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            ArrowDataType::Int32,
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))])?;
        let output = deduplicate_struct_field_names(&batch)?;
        assert_eq!(output.schema(), batch.schema());
        Ok(())
    }
}
