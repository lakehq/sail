use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Cursor;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, FieldRef, Schema, SchemaRef};
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::execution::SendableRecordBatchStream;
use fastrace::Span;
use fastrace::future::FutureExt;
use futures::stream::StreamExt;
use futures::{Stream, stream};
use sail_common_datafusion::array::record_batch::cast_record_batch_positionally;
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

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]
    #![expect(clippy::panic, reason = "Assert typed response shapes in tests")]

    use datafusion::arrow::array::{
        Array, ArrayRef, FixedSizeListArray, Int32Array, LargeListArray, ListArray, MapArray,
        RecordBatchOptions, StringArray, StringViewArray, StructArray,
    };
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
    use datafusion::arrow::datatypes::{DataType as DT, Field};
    use datafusion::arrow::ipc::reader::StreamReader;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

    use super::*;

    fn payload(nulls: Option<NullBuffer>) -> ArrayRef {
        let fields = vec![
            Field::new("", DT::Int32, false)
                .with_metadata(HashMap::from([("tag".into(), "first".into())])),
            Field::new("x", DT::Int32, true),
            Field::new("x", DT::Utf8, false)
                .with_metadata(HashMap::from([("tag".into(), "last".into())])),
        ];
        Arc::new(StructArray::new(
            fields.into(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int32Array::from(vec![Some(10), None, Some(30), Some(40)])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            ],
            nulls,
        ))
    }

    fn batch(columns: Vec<ArrayRef>) -> RecordBatch {
        let fields = columns
            .iter()
            .map(|column| {
                Field::new("s", column.data_type().clone(), true)
                    .with_metadata(HashMap::from([("column".into(), "kept".into())]))
            })
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            Arc::new(Schema::new_with_metadata(
                fields,
                HashMap::from([("schema".into(), "kept".into())]),
            )),
            columns,
        )
        .unwrap()
    }

    fn decode(batch: ArrowBatch) -> RecordBatch {
        let mut reader = StreamReader::try_new(Cursor::new(batch.data), None).unwrap();
        let decoded = reader.next().unwrap().unwrap();
        assert_eq!(decoded.num_rows() as i64, batch.row_count);
        assert!(reader.next().is_none());
        decoded
    }

    // Renaming must not change any data buffer, offset, length or validity bit.
    fn assert_same_data(
        before: &datafusion::arrow::array::ArrayData,
        after: &datafusion::arrow::array::ArrayData,
    ) {
        assert_eq!(before.len(), after.len());
        assert_eq!(before.offset(), after.offset());
        assert_eq!(before.nulls(), after.nulls());
        assert_eq!(before.buffers(), after.buffers());
        for (left, right) in before.buffers().iter().zip(after.buffers()) {
            assert_eq!(left.as_ptr(), right.as_ptr());
        }
        assert_eq!(before.child_data().len(), after.child_data().len());
        for (left, right) in before.child_data().iter().zip(after.child_data()) {
            assert_same_data(left, right);
        }
    }

    fn assert_field_properties(before: &Field, after: &Field) {
        assert_eq!(before.is_nullable(), after.is_nullable());
        assert_eq!(before.metadata(), after.metadata());
        match (before.data_type(), after.data_type()) {
            (DT::Struct(left), DT::Struct(right)) => {
                assert_eq!(left.len(), right.len());
                for (left, right) in left.iter().zip(right.iter()) {
                    assert_field_properties(left, right);
                }
            }
            (DT::List(left), DT::List(right)) | (DT::LargeList(left), DT::LargeList(right)) => {
                assert_eq!(left.name(), right.name());
                assert_field_properties(left, right);
            }
            (DT::FixedSizeList(left, a), DT::FixedSizeList(right, b)) => {
                assert_eq!(a, b);
                assert_field_properties(left, right);
            }
            (DT::Map(left, a), DT::Map(right, b)) => {
                assert_eq!(a, b);
                assert_eq!(left.name(), right.name());
                assert_field_properties(left, right);
            }
            (left, right) => assert_eq!(left, right),
        }
    }

    #[test]
    fn query_transport_preserves_positions_metadata_and_top_level_names() {
        let input = batch(vec![payload(None).slice(1, 2), payload(None).slice(1, 2)]);
        let original_schema = input.schema();
        let output = normalize_query_result(&input).unwrap();
        assert_eq!(input.schema(), original_schema);
        assert_eq!(output.schema().metadata(), original_schema.metadata());
        assert_eq!(
            output
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            vec!["s", "s"]
        );
        for index in 0..2 {
            assert_field_properties(original_schema.field(index), output.schema().field(index));
            assert_same_data(
                &input.column(index).to_data(),
                &output.column(index).to_data(),
            );
            let value = output
                .column(index)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            assert_eq!(
                value
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>(),
                vec!["", "x_0", "x_1"]
            );
            assert_eq!(
                value
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(0),
                2
            );
            assert!(value.column(1).is_null(0));
            assert_eq!(
                value
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                "b"
            );
        }
        let wire = decode(to_query_arrow_batch(&input).unwrap());
        assert_eq!(wire, output);
        // Commands with schema=None rely on the generic encoder retaining logical names.
        assert_eq!(decode(to_arrow_batch(&input).unwrap()), input);
    }

    #[test]
    fn query_transport_rebinds_sliced_nested_containers_without_copying_data() {
        let values = payload(Some(NullBuffer::from(vec![true, false, true, true])));
        let item = Arc::new(
            Field::new("item", values.data_type().clone(), true)
                .with_metadata(HashMap::from([("item".into(), "kept".into())])),
        );
        let nulls = Some(NullBuffer::from(vec![true, false, true, true]));
        let entries = StructArray::new(
            vec![
                Field::new("key", payload(None).data_type().clone(), false),
                Field::new("value", values.data_type().clone(), true),
            ]
            .into(),
            vec![payload(None), values.clone()],
            None,
        );
        let arrays: Vec<ArrayRef> = vec![
            values.clone(),
            Arc::new(StructArray::new(
                vec![item.clone()].into(),
                vec![values.clone()],
                nulls.clone(),
            )),
            Arc::new(ListArray::new(
                item.clone(),
                OffsetBuffer::new(vec![0i32, 1, 3, 3, 4].into()),
                values.clone(),
                nulls.clone(),
            )),
            Arc::new(LargeListArray::new(
                item.clone(),
                OffsetBuffer::new(vec![0i64, 1, 3, 3, 4].into()),
                values.clone(),
                nulls.clone(),
            )),
            Arc::new(FixedSizeListArray::new(
                item.clone(),
                1,
                values.clone(),
                nulls.clone(),
            )),
            Arc::new(
                FixedSizeListArray::try_new_with_length(item, 0, values.slice(0, 0), None, 4)
                    .unwrap(),
            ),
            Arc::new(MapArray::new(
                Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                OffsetBuffer::new(vec![0i32, 1, 3, 3, 4].into()),
                entries,
                nulls,
                true,
            )),
        ];
        for array in arrays {
            let input = batch(vec![array.slice(1, 2)]);
            let output = normalize_query_result(&input).unwrap();
            assert_ne!(input.schema(), output.schema());
            assert_field_properties(input.schema().field(0), output.schema().field(0));
            assert_same_data(&input.column(0).to_data(), &output.column(0).to_data());
            assert_eq!(decode(to_query_arrow_batch(&input).unwrap()), output);
        }
    }

    #[test]
    fn query_transport_normalizes_views_before_positional_rename() {
        let input = batch(vec![Arc::new(StructArray::new(
            vec![
                Field::new("x", DT::Utf8View, true),
                Field::new("x", DT::Int32, false),
            ]
            .into(),
            vec![
                Arc::new(StringViewArray::from(vec![
                    Some("long enough to use an external buffer"),
                    None,
                ])),
                Arc::new(Int32Array::from(vec![10, 20])),
            ],
            None,
        ))]);
        let output = decode(to_query_arrow_batch(&input).unwrap());
        let value = output
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(value.fields()[0].name(), "x_0");
        assert_eq!(value.fields()[1].name(), "x_1");
        assert_eq!(
            value
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap(),
            &StringArray::from(vec![Some("long enough to use an external buffer"), None])
        );
        assert_eq!(
            value
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap(),
            &Int32Array::from(vec![10, 20])
        );
        assert_eq!(
            input.schema().field(0).data_type(),
            &DT::Struct(
                vec![
                    Field::new("x", DT::Utf8View, true),
                    Field::new("x", DT::Int32, false)
                ]
                .into()
            )
        );
    }

    #[test]
    fn query_transport_preserves_zero_column_row_count() {
        let input = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::default().with_row_count(Some(3)),
        )
        .unwrap();
        assert_eq!(decode(to_query_arrow_batch(&input).unwrap()).num_rows(), 3);
    }

    #[tokio::test]
    async fn query_executor_emits_logical_schema_and_transport_batches_even_when_empty() {
        let input = batch(vec![payload(None)]);
        for batches in [
            vec![],
            vec![input.slice(0, 0)],
            vec![input.slice(0, 2), input.slice(2, 2)],
        ] {
            let expected = if batches.is_empty() {
                vec![input.slice(0, 0)]
            } else {
                batches.clone()
            };
            let mut context = ExecutorTaskContext {
                stream: Box::pin(RecordBatchStreamAdapter::new(
                    input.schema(),
                    stream::iter(batches.into_iter().map(Ok)),
                )),
                heartbeat_interval: Duration::from_secs(60),
                state: ExecutorTaskState::Query,
                buffer: Arc::new(Mutex::new(ExecutorBuffer::new(0))),
            };
            let (tx, mut rx) = mpsc::channel(16);
            Executor::run_internal(&mut context, &tx, true)
                .await
                .unwrap();
            drop(tx);
            let output = rx.recv().await.unwrap().unwrap();
            let ExecutorBatch::Schema(schema) = output.batch else {
                panic!("expected schema")
            };
            assert_eq!(*schema, to_spark_schema(input.schema()).unwrap());
            for expected in expected {
                let output = rx.recv().await.unwrap().unwrap();
                let ExecutorBatch::ArrowBatch(wire) = output.batch else {
                    panic!("expected Arrow batch")
                };
                assert_eq!(decode(wire), normalize_query_result(&expected).unwrap());
            }
            assert!(matches!(
                rx.recv().await.unwrap().unwrap().batch,
                ExecutorBatch::Complete
            ));
            assert!(rx.recv().await.is_none());
        }
    }
}

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
                            let batch = to_query_arrow_batch(&batch)?;
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
                    let batch = to_query_arrow_batch(&batch)?;
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

// Query responses carry the original Spark schema separately. Command LocalRelations may
// not, so their generic Arrow encoder must retain the logical field names.
fn to_query_arrow_batch(batch: &RecordBatch) -> SparkResult<ArrowBatch> {
    to_arrow_batch(&normalize_query_result(batch)?)
}

fn normalize_query_result(batch: &RecordBatch) -> SparkResult<RecordBatch> {
    // Normalize views before renaming so the positional rebind sees ordinary containers.
    let batch = if record_batch_has_views(batch) {
        normalize_record_batch(batch, false, false)?
    } else {
        batch.clone()
    };
    let schema = batch.schema();
    let fields = schema
        .fields()
        .iter()
        .map(query_transport_field)
        .collect::<SparkResult<Vec<_>>>()?;
    let transport_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    if schema == transport_schema {
        Ok(batch)
    } else {
        Ok(cast_record_batch_positionally(batch, transport_schema)?)
    }
}

fn query_transport_field(field: &FieldRef) -> SparkResult<FieldRef> {
    Ok(Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(query_transport_type(field.data_type())?),
    ))
}

fn query_transport_type(data_type: &ArrowDataType) -> SparkResult<ArrowDataType> {
    Ok(match data_type {
        ArrowDataType::Struct(fields) => {
            // Match Spark ArrowUtils.deduplicateFieldNames / PySpark _dedup_names:
            // suffix every occurrence of a repeated name, not just later occurrences.
            let mut counts = HashMap::<&str, usize>::new();
            for field in fields {
                *counts.entry(field.name()).or_default() += 1;
            }
            let mut indices = HashMap::<&str, usize>::new();
            let mut names = HashSet::new();
            let fields = fields
                .iter()
                .map(|field| {
                    let name = field.name();
                    let name = if counts[name.as_str()] > 1 {
                        let index = indices.entry(name).or_default();
                        let name = format!("{name}_{index}");
                        *index += 1;
                        name
                    } else {
                        name.clone()
                    };
                    // The client looks up these exact names. A different unique-name
                    // scheme would silently lose values for e.g. x, x, x_0.
                    if !names.insert(name.clone()) {
                        return Err(SparkError::unsupported(format!(
                            "nested struct field names collide after Spark Arrow transport naming: {name:?}"
                        )));
                    }
                    Ok(Arc::new(query_transport_field(field)?.as_ref().clone().with_name(name)))
                })
                .collect::<SparkResult<Vec<_>>>()?;
            ArrowDataType::Struct(fields.into())
        }
        ArrowDataType::List(field) => ArrowDataType::List(query_transport_field(field)?),
        ArrowDataType::LargeList(field) => ArrowDataType::LargeList(query_transport_field(field)?),
        ArrowDataType::FixedSizeList(field, size) => {
            ArrowDataType::FixedSizeList(query_transport_field(field)?, *size)
        }
        ArrowDataType::Map(field, sorted) => {
            ArrowDataType::Map(query_transport_field(field)?, *sorted)
        }
        _ => data_type.clone(),
    })
}

pub(crate) fn to_arrow_batch(batch: &RecordBatch) -> SparkResult<ArrowBatch> {
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
