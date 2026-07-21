use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, LargeBinaryArray, RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{DataFusionError, Result, internal_datafusion_err};
use futures::{StreamExt, stream};
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};
use sail_common_datafusion::array::record_batch::{
    write_record_batches, write_record_batches_file,
};
use uuid::Uuid;

const PARTITION_COLUMN: &str = "partition";
const SEQUENCE_COLUMN: &str = "sequence";
const DATA_COLUMN: &str = "data";
const LOCATION_COLUMN: &str = "location";

#[derive(Debug)]
pub struct LocalCheckpointExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl LocalCheckpointExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new(PARTITION_COLUMN, DataType::UInt64, false),
            Field::new(SEQUENCE_COLUMN, DataType::UInt64, false),
            Field::new(DATA_COLUMN, DataType::LargeBinary, false),
        ]));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Incremental,
            input.boundedness(),
        ));
        Self { input, properties }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for LocalCheckpointExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "LocalCheckpointExec")
    }
}

impl ExecutionPlan for LocalCheckpointExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(internal_datafusion_err!(
                "LocalCheckpointExec must have exactly one child"
            ));
        }
        Ok(Arc::new(Self::new(Arc::clone(&children[0]))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let input_schema = self.input.schema();
        let output_schema = self.schema();
        let stream_schema = Arc::clone(&output_schema);
        let stream = stream::unfold(
            LocalCheckpointStreamState {
                input,
                sequence: 0,
                emitted: false,
                finished: false,
            },
            move |mut state| {
                let input_schema = Arc::clone(&input_schema);
                let output_schema = Arc::clone(&output_schema);
                async move {
                    if state.finished {
                        return None;
                    }
                    let result = match state.input.next().await {
                        Some(Ok(batch)) => {
                            let bytes = write_record_batches(&[batch], input_schema.as_ref());
                            bytes.and_then(|bytes| {
                                local_checkpoint_batch(
                                    partition,
                                    state.sequence,
                                    &bytes,
                                    output_schema,
                                )
                            })
                        }
                        Some(Err(error)) => {
                            state.finished = true;
                            Err(error)
                        }
                        None if !state.emitted => {
                            state.finished = true;
                            let bytes = write_record_batches(&[], input_schema.as_ref());
                            bytes.and_then(|bytes| {
                                local_checkpoint_batch(partition, 0, &bytes, output_schema)
                            })
                        }
                        None => return None,
                    };
                    if result.is_err() {
                        state.finished = true;
                    }
                    state.sequence += 1;
                    state.emitted = true;
                    Some((result, state))
                }
            },
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            stream,
        )))
    }
}

struct LocalCheckpointStreamState {
    input: SendableRecordBatchStream,
    sequence: u64,
    emitted: bool,
    finished: bool,
}

fn local_checkpoint_batch(
    partition: usize,
    sequence: u64,
    bytes: &[u8],
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let partition = u64::try_from(partition)
        .map_err(|_| internal_datafusion_err!("checkpoint partition index is too large"))?;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(vec![partition])),
        Arc::new(UInt64Array::from(vec![sequence])),
        Arc::new(LargeBinaryArray::from_vec(vec![bytes])),
    ];
    Ok(RecordBatch::try_new(schema, columns)?)
}

#[derive(Debug)]
pub struct ReliableCheckpointExec {
    input: Arc<dyn ExecutionPlan>,
    object_store_url: ObjectStoreUrl,
    path: Path,
    properties: Arc<PlanProperties>,
}

impl ReliableCheckpointExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        object_store_url: ObjectStoreUrl,
        path: Path,
    ) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new(PARTITION_COLUMN, DataType::UInt64, false),
            Field::new(LOCATION_COLUMN, DataType::Utf8, false),
        ]));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Final,
            input.boundedness(),
        ));
        Self {
            input,
            object_store_url,
            path,
            properties,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn object_store_url(&self) -> &ObjectStoreUrl {
        &self.object_store_url
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl DisplayAs for ReliableCheckpointExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "ReliableCheckpointExec: object_store_url={}, path={}",
            self.object_store_url, self.path
        )
    }
}

impl ExecutionPlan for ReliableCheckpointExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(internal_datafusion_err!(
                "ReliableCheckpointExec must have exactly one child"
            ));
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.object_store_url.clone(),
            self.path.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut input = self.input.execute(partition, Arc::clone(&context))?;
        let input_schema = self.input.schema();
        let output_schema = self.schema();
        let stream_schema = Arc::clone(&output_schema);
        let store = context.runtime_env().object_store(&self.object_store_url)?;
        let location = self
            .path
            .clone()
            .join("_temporary")
            .join(Uuid::new_v4().to_string())
            .join(format!("part-{partition:05}.arrow"));
        let output = stream::once(async move {
            // FIXME: Stream Arrow IPC to object storage instead of buffering an entire partition.
            let mut batches = vec![];
            while let Some(batch) = input.next().await {
                batches.push(batch?);
            }
            let bytes = write_record_batches_file(&batches, input_schema.as_ref())?;
            store
                .put(&location, PutPayload::from(bytes))
                .await
                .map_err(|error| DataFusionError::ObjectStore(Box::new(error)))?;
            let partition = u64::try_from(partition)
                .map_err(|_| internal_datafusion_err!("checkpoint partition index is too large"))?;
            let columns: Vec<ArrayRef> = vec![
                Arc::new(UInt64Array::from(vec![partition])),
                Arc::new(StringArray::from(vec![location.as_ref()])),
            ];
            Ok(RecordBatch::try_new(output_schema, columns)?)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            output,
        )))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::object_store::memory::InMemory;
    use datafusion::prelude::SessionContext;

    use super::*;

    async fn execute_checkpoint_attempt(
        checkpoint: &ReliableCheckpointExec,
        context: Arc<TaskContext>,
    ) -> Result<Path> {
        let mut stream = checkpoint.execute(0, context)?;
        let batch = stream.next().await.ok_or_else(|| {
            internal_datafusion_err!("reliable checkpoint attempt returned no metadata")
        })??;
        let locations = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal_datafusion_err!("invalid checkpoint location column"))?;
        if locations.is_null(0) {
            return Err(internal_datafusion_err!(
                "checkpoint attempt returned null location"
            ));
        }
        Path::parse(locations.value(0))
            .map_err(|error| internal_datafusion_err!("invalid checkpoint location: {error}"))
    }

    #[tokio::test]
    async fn reliable_checkpoint_attempts_write_distinct_temporary_objects() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        let input: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)?;
        let object_store_url = ObjectStoreUrl::parse("memory://")?;
        let checkpoint =
            ReliableCheckpointExec::new(input, object_store_url.clone(), Path::from("checkpoint"));
        let ctx = SessionContext::new();
        ctx.runtime_env()
            .register_object_store(object_store_url.as_ref(), Arc::new(InMemory::new()));

        let first = execute_checkpoint_attempt(&checkpoint, ctx.task_ctx()).await?;
        let second = execute_checkpoint_attempt(&checkpoint, ctx.task_ctx()).await?;

        assert_ne!(first, second);
        let temporary = Path::from("checkpoint/_temporary");
        assert!(first.prefix_matches(&temporary));
        assert!(second.prefix_matches(&temporary));
        let store = ctx.runtime_env().object_store(&object_store_url)?;
        store
            .head(&first)
            .await
            .map_err(|error| DataFusionError::ObjectStore(Box::new(error)))?;
        store
            .head(&second)
            .await
            .map_err(|error| DataFusionError::ObjectStore(Box::new(error)))?;
        Ok(())
    }
}
