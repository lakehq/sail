use std::collections::VecDeque;
use std::io::Cursor;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::arrow::array::{
    Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, MapArray, RecordBatch,
    RecordBatchOptions, StructArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, FieldRef, Fields, Schema};
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
use crate::schema::{deduplicate_field_names, normalize_schema_for_pandas, to_spark_schema};
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
        let schema = to_spark_schema(normalize_schema_for_pandas(context.stream.schema()))?;
        let out = ExecutorOutput::new(ExecutorBatch::Schema(Box::new(schema)));
        context.save_output(&out)?;
        tx.send(out).await?;

        let mut empty = true;
        while let Some(batch) = context.next().await? {
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
    let mut output = ArrowBatch::default();
    let batch = normalize_arrow_batch_for_pandas(batch)?;
    {
        let cursor = Cursor::new(&mut output.data);
        let mut writer = StreamWriter::try_new(cursor, batch.schema().as_ref())?;
        writer.write(&batch)?;
        output.row_count += batch.num_rows() as i64;
        writer.finish()?;
    }
    Ok(output)
}

fn normalize_arrow_batch_for_pandas(batch: &RecordBatch) -> SparkResult<RecordBatch> {
    let fields = normalize_fields_for_pandas(batch.schema().fields(), false);
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| normalize_array_for_pandas(column, field.data_type()))
        .collect::<SparkResult<Vec<_>>>()?;

    if columns.is_empty() {
        Ok(RecordBatch::try_new_with_options(
            schema,
            columns,
            &RecordBatchOptions::default().with_row_count(Some(batch.num_rows())),
        )?)
    } else {
        Ok(RecordBatch::try_new(schema, columns)?)
    }
}

fn normalize_fields_for_pandas(fields: &Fields, deduplicate: bool) -> Fields {
    let names = if deduplicate {
        deduplicate_field_names(fields)
    } else {
        fields
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>()
    };
    fields
        .iter()
        .zip(names)
        .map(|(field, name)| normalize_field_for_pandas(field, name))
        .collect::<Vec<_>>()
        .into()
}

fn normalize_field_for_pandas(field: &FieldRef, name: String) -> FieldRef {
    Arc::new(
        field
            .as_ref()
            .clone()
            .with_name(name)
            .with_data_type(normalize_data_type_for_pandas(field.data_type())),
    )
}

fn normalize_data_type_for_pandas(data_type: &ArrowDataType) -> ArrowDataType {
    match data_type {
        ArrowDataType::Struct(fields) => {
            ArrowDataType::Struct(normalize_fields_for_pandas(fields, true))
        }
        ArrowDataType::List(field) => {
            ArrowDataType::List(normalize_field_for_pandas(field, field.name().clone()))
        }
        ArrowDataType::LargeList(field) => {
            ArrowDataType::LargeList(normalize_field_for_pandas(field, field.name().clone()))
        }
        ArrowDataType::FixedSizeList(field, size) => ArrowDataType::FixedSizeList(
            normalize_field_for_pandas(field, field.name().clone()),
            *size,
        ),
        ArrowDataType::Map(field, sorted) => ArrowDataType::Map(
            normalize_field_for_pandas(field, field.name().clone()),
            *sorted,
        ),
        _ => data_type.clone(),
    }
}

fn normalize_array_for_pandas(
    column: &ArrayRef,
    target_type: &ArrowDataType,
) -> SparkResult<ArrayRef> {
    if column.data_type() == target_type {
        return Ok(column.clone());
    }

    match (column.data_type(), target_type) {
        (ArrowDataType::Struct(_), ArrowDataType::Struct(fields)) => {
            normalize_struct_array_for_pandas(column, fields)
        }
        (ArrowDataType::List(_), ArrowDataType::List(field)) => {
            normalize_list_array_for_pandas(column, field)
        }
        (ArrowDataType::LargeList(_), ArrowDataType::LargeList(field)) => {
            normalize_large_list_array_for_pandas(column, field)
        }
        (ArrowDataType::FixedSizeList(_, _), ArrowDataType::FixedSizeList(field, size)) => {
            normalize_fixed_size_list_array_for_pandas(column, field, *size)
        }
        (ArrowDataType::Map(_, _), ArrowDataType::Map(field, sorted)) => {
            normalize_map_array_for_pandas(column, field, *sorted)
        }
        _ => Ok(cast(column, target_type)?),
    }
}

fn normalize_struct_array_for_pandas(column: &ArrayRef, fields: &Fields) -> SparkResult<ArrayRef> {
    let struct_array = column
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| SparkError::internal("expected struct array"))?;
    let columns = struct_array
        .columns()
        .iter()
        .zip(fields)
        .map(|(child, field)| normalize_array_for_pandas(child, field.data_type()))
        .collect::<SparkResult<Vec<_>>>()?;
    Ok(Arc::new(StructArray::try_new(
        fields.clone(),
        columns,
        struct_array.nulls().cloned(),
    )?))
}

fn normalize_list_array_for_pandas(column: &ArrayRef, field: &FieldRef) -> SparkResult<ArrayRef> {
    let list_array = column
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| SparkError::internal("expected list array"))?;
    let values = normalize_array_for_pandas(list_array.values(), field.data_type())?;
    Ok(Arc::new(ListArray::try_new(
        field.clone(),
        list_array.offsets().clone(),
        values,
        list_array.nulls().cloned(),
    )?))
}

fn normalize_large_list_array_for_pandas(
    column: &ArrayRef,
    field: &FieldRef,
) -> SparkResult<ArrayRef> {
    let list_array = column
        .as_any()
        .downcast_ref::<LargeListArray>()
        .ok_or_else(|| SparkError::internal("expected large list array"))?;
    let values = normalize_array_for_pandas(list_array.values(), field.data_type())?;
    Ok(Arc::new(LargeListArray::try_new(
        field.clone(),
        list_array.offsets().clone(),
        values,
        list_array.nulls().cloned(),
    )?))
}

fn normalize_fixed_size_list_array_for_pandas(
    column: &ArrayRef,
    field: &FieldRef,
    size: i32,
) -> SparkResult<ArrayRef> {
    let list_array = column
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| SparkError::internal("expected fixed-size list array"))?;
    let values = normalize_array_for_pandas(list_array.values(), field.data_type())?;
    Ok(Arc::new(FixedSizeListArray::try_new(
        field.clone(),
        size,
        values,
        list_array.nulls().cloned(),
    )?))
}

fn normalize_map_array_for_pandas(
    column: &ArrayRef,
    field: &FieldRef,
    sorted: bool,
) -> SparkResult<ArrayRef> {
    let map_array = column
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| SparkError::internal("expected map array"))?;
    let entries: ArrayRef = Arc::new(map_array.entries().clone());
    let entries = normalize_array_for_pandas(&entries, field.data_type())?;
    let entries = entries
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| SparkError::internal("expected map entries struct array"))?
        .clone();
    Ok(Arc::new(MapArray::try_new(
        field.clone(),
        map_array.offsets().clone(),
        entries,
        map_array.nulls().cloned(),
        sorted,
    )?))
}
