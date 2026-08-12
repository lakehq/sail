use std::collections::VecDeque;
use std::io::Cursor;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, FieldRef, Schema, SchemaRef, TimeUnit,
};
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::execution::SendableRecordBatchStream;
use fastrace::Span;
use fastrace::future::FutureExt;
use futures::Stream;
use futures::stream::StreamExt;
use sail_common::spec::SAIL_SPARK_TIME_PRECISION_METADATA_KEY;
use sail_common_datafusion::array::record_batch::{
    cast_record_batch_positionally, retag_schema_timestamp_timezone,
};
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
    session_timezone: Arc<str>,
    external_schema: Option<SchemaRef>,
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
        session_timezone: Arc<str>,
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
                    session_timezone,
                    external_schema: None,
                    state,
                    buffer: Arc::new(Mutex::new(buffer)),
                },
                span: Span::enter_with_local_parent("Executor::new"),
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

                let external_schema = match &context.external_schema {
                    Some(schema) => Arc::clone(schema),
                    None => {
                        let schema = spark_connect_arrow_schema(
                            context.stream.schema().as_ref(),
                            &context.session_timezone,
                        )?;
                        context.external_schema = Some(Arc::clone(&schema));
                        schema
                    }
                };

                let mut empty = true;
                loop {
                    match ExecutorTaskContext::next(&mut context.stream, context.heartbeat_interval)
                        .await?
                    {
                        ExecutorTaskItem::Batch(Some(batch)) => {
                            let batch = to_arrow_batch_with_schema(&batch, &external_schema)?;
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
                    let batch = to_arrow_batch_with_schema(&batch, &external_schema)?;
                    ExecutorTaskContext::send_output(
                        &context.buffer,
                        tx,
                        ExecutorOutput::new(ExecutorBatch::ArrowBatch(batch)),
                    )
                    .await?;
                }
                if reattachable {
                    ExecutorTaskContext::send_output(
                        &context.buffer,
                        tx,
                        ExecutorOutput::complete(),
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
                if reattachable {
                    ExecutorTaskContext::send_output(
                        &context.buffer,
                        tx,
                        ExecutorOutput::complete(),
                    )
                    .await?;
                }
            }
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
            ExecutorTaskResult::Completed => ExecutorState::Idle,
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
            ExecutorState::Idle | ExecutorState::Failed(_) | ExecutorState::Pausing => {
                return Ok(());
            }
        };
        buffer.lock()?.remove_until(&response_id);
        Ok(())
    }
}

pub(crate) fn to_arrow_batch(
    batch: &RecordBatch,
    session_timezone: &str,
) -> SparkResult<ArrowBatch> {
    let schema = spark_connect_arrow_schema(batch.schema().as_ref(), session_timezone)?;
    to_arrow_batch_with_schema(batch, &schema)
}

fn to_arrow_batch_with_schema(
    batch: &RecordBatch,
    external_schema: &SchemaRef,
) -> SparkResult<ArrowBatch> {
    let batch = if batch.schema_ref().as_ref() == external_schema.as_ref() {
        batch.clone()
    } else {
        cast_record_batch_positionally(batch.clone(), Arc::clone(external_schema))?
    };
    let mut output = ArrowBatch::default();
    {
        let cursor = Cursor::new(&mut output.data);
        let mut writer = StreamWriter::try_new(cursor, batch.schema().as_ref())?;
        writer.write(&batch)?;
        output.row_count += batch.num_rows() as i64;
        writer.finish()?;
    }
    Ok(output)
}

fn spark_connect_arrow_schema(
    input_schema: &Schema,
    session_timezone: &str,
) -> SparkResult<SchemaRef> {
    let input_schema = retag_schema_timestamp_timezone(input_schema, session_timezone)?;
    let fields = input_schema
        .fields()
        .iter()
        .map(spark_connect_arrow_field)
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        input_schema.metadata().clone(),
    )))
}

fn spark_connect_arrow_field(field: &FieldRef) -> FieldRef {
    let mut metadata = field.metadata().clone();
    metadata.remove(SAIL_SPARK_TIME_PRECISION_METADATA_KEY);
    Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(spark_connect_arrow_data_type(field.data_type()))
            .with_metadata(metadata),
    )
}

fn spark_connect_arrow_data_type(data_type: &ArrowDataType) -> ArrowDataType {
    match data_type {
        ArrowDataType::Time32(_) | ArrowDataType::Time64(_) => {
            ArrowDataType::Time64(TimeUnit::Nanosecond)
        }
        ArrowDataType::List(field) => ArrowDataType::List(spark_connect_arrow_field(field)),
        ArrowDataType::LargeList(field) => {
            ArrowDataType::LargeList(spark_connect_arrow_field(field))
        }
        ArrowDataType::FixedSizeList(field, size) => {
            ArrowDataType::FixedSizeList(spark_connect_arrow_field(field), *size)
        }
        ArrowDataType::ListView(field) => ArrowDataType::ListView(spark_connect_arrow_field(field)),
        ArrowDataType::LargeListView(field) => {
            ArrowDataType::LargeListView(spark_connect_arrow_field(field))
        }
        ArrowDataType::Struct(fields) => ArrowDataType::Struct(
            fields
                .iter()
                .map(spark_connect_arrow_field)
                .collect::<Vec<_>>()
                .into(),
        ),
        ArrowDataType::Map(field, sorted) => {
            ArrowDataType::Map(spark_connect_arrow_field(field), *sorted)
        }
        _ => data_type.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::array::{
        ArrayRef, AsArray, StructArray, Time32SecondArray, TimestampMicrosecondArray,
    };
    use datafusion::arrow::datatypes::{
        DataType, Field, Fields, Time64NanosecondType, TimestampMicrosecondType,
    };
    use datafusion::arrow::ipc::reader::StreamReader;

    use super::*;

    fn nested_batch(
        schema: SchemaRef,
        time_seconds: i32,
        timestamp_micros: i64,
    ) -> SparkResult<RecordBatch> {
        let DataType::Struct(fields) = schema.field(0).data_type() else {
            return Err(SparkError::internal("expected nested struct schema"));
        };
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Time32SecondArray::from(vec![time_seconds])),
            Arc::new(TimestampMicrosecondArray::from(vec![timestamp_micros]).with_timezone("UTC")),
        ];
        let nested = StructArray::try_new(fields.clone(), columns, None)?;
        Ok(RecordBatch::try_new(schema, vec![Arc::new(nested)])?)
    }

    fn decode_arrow_batch(batch: ArrowBatch) -> SparkResult<RecordBatch> {
        let mut reader = StreamReader::try_new(Cursor::new(batch.data), None)?;
        reader
            .next()
            .transpose()?
            .ok_or_else(|| SparkError::internal("Arrow batch did not contain a record batch"))
    }

    #[test]
    fn serializes_multiple_batches_with_one_nested_external_schema() -> SparkResult<()> {
        let time_field = Arc::new(
            Field::new("time", DataType::Time32(TimeUnit::Second), false).with_metadata(
                HashMap::from([(
                    SAIL_SPARK_TIME_PRECISION_METADATA_KEY.to_string(),
                    "0".to_string(),
                )]),
            ),
        );
        let timestamp_field = Arc::new(Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            false,
        ));
        let nested_fields: Fields = vec![time_field, timestamp_field].into();
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "nested",
            DataType::Struct(nested_fields),
            false,
        )]));
        let external_schema =
            spark_connect_arrow_schema(input_schema.as_ref(), "America/Los_Angeles")?;

        let DataType::Struct(external_fields) = external_schema.field(0).data_type() else {
            return Err(SparkError::internal("expected nested external schema"));
        };
        assert_eq!(
            external_fields[0].data_type(),
            &DataType::Time64(TimeUnit::Nanosecond)
        );
        assert!(
            !external_fields[0]
                .metadata()
                .contains_key(SAIL_SPARK_TIME_PRECISION_METADATA_KEY)
        );
        assert_eq!(
            external_fields[1].data_type(),
            &DataType::Timestamp(
                TimeUnit::Microsecond,
                Some(Arc::from("America/Los_Angeles"))
            )
        );

        for (time_seconds, timestamp_micros) in [(1, 11), (2, 22)] {
            let input = nested_batch(Arc::clone(&input_schema), time_seconds, timestamp_micros)?;
            let output = to_arrow_batch_with_schema(&input, &external_schema)?;
            assert_eq!(output.row_count, 1);
            let output = decode_arrow_batch(output)?;
            assert_eq!(output.schema_ref().as_ref(), external_schema.as_ref());
            let nested = output.column(0).as_struct();
            assert_eq!(
                nested
                    .column(0)
                    .as_primitive::<Time64NanosecondType>()
                    .value(0),
                i64::from(time_seconds) * 1_000_000_000
            );
            assert_eq!(
                nested
                    .column(1)
                    .as_primitive::<TimestampMicrosecondType>()
                    .value(0),
                timestamp_micros
            );
        }

        let empty = RecordBatch::new_empty(input_schema);
        let output = to_arrow_batch_with_schema(&empty, &external_schema)?;
        assert_eq!(output.row_count, 0);
        let output = decode_arrow_batch(output)?;
        assert_eq!(output.num_rows(), 0);
        assert_eq!(output.schema_ref().as_ref(), external_schema.as_ref());
        Ok(())
    }
}
