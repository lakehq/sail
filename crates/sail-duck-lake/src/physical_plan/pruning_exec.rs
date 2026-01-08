use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{Distribution, EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_common::{internal_err, DataFusionError, Result as DataFusionResult};
use futures::{stream, StreamExt};
use serde_arrow::from_record_batch;

use crate::datasource::pruning::prune_files_with_physical_predicate;
use crate::metadata::file_info_fields;
use crate::spec::{FileInfo, PartitionFieldInfo};

/// Physical node that prunes DuckLake data files (metadata) using DataFusion pruning predicates.
///
/// Input/Output: Arrow RecordBatches representing `Vec<FileInfo>` (serde_arrow schema).
#[derive(Debug, Clone)]
pub struct DuckLakePruningExec {
    input: Arc<dyn ExecutionPlan>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    prune_schema: SchemaRef,
    partition_fields: Vec<PartitionFieldInfo>,
    limit: Option<usize>,
    output_schema: SchemaRef,
    cache: PlanProperties,
}

impl DuckLakePruningExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        prune_schema: SchemaRef,
        partition_fields: Vec<PartitionFieldInfo>,
        limit: Option<usize>,
    ) -> DataFusionResult<Self> {
        let output_schema = input.schema();
        let cache = Self::compute_properties(output_schema.clone());
        Ok(Self {
            input,
            predicate,
            prune_schema,
            partition_fields,
            limit,
            output_schema,
            cache,
        })
    }

    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn predicate(&self) -> &Option<Arc<dyn PhysicalExpr>> {
        &self.predicate
    }

    pub fn prune_schema(&self) -> &SchemaRef {
        &self.prune_schema
    }

    pub fn partition_fields(&self) -> &[PartitionFieldInfo] {
        &self.partition_fields
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }
}

#[async_trait]
impl ExecutionPlan for DuckLakePruningExec {
    fn name(&self) -> &'static str {
        "DuckLakePruningExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("DuckLakePruningExec requires exactly one child");
        }
        Ok(Arc::new(Self::try_new(
            children[0].clone(),
            self.predicate.clone(),
            self.prune_schema.clone(),
            self.partition_fields.clone(),
            self.limit,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("DuckLakePruningExec can only be executed in a single partition");
        }

        let input_stream = self.input.execute(0, context)?;
        let predicate = self.predicate.clone();
        let prune_schema = self.prune_schema.clone();
        let partition_fields = self.partition_fields.clone();
        let remaining_limit: Option<u64> = self.limit.map(|x| x as u64);
        let output_schema = self.output_schema.clone();

        // Use serde_arrow schema for re-encoding filtered FileInfo back to RecordBatch.
        let fields = file_info_fields()?;

        struct PruningState {
            input_stream: SendableRecordBatchStream,
            predicate: Option<Arc<dyn PhysicalExpr>>,
            prune_schema: SchemaRef,
            partition_fields: Vec<PartitionFieldInfo>,
            remaining_limit: Option<u64>,
            output_schema: SchemaRef,
            fields: Vec<datafusion::arrow::datatypes::FieldRef>,
            done: bool,
        }

        let state = PruningState {
            input_stream,
            predicate,
            prune_schema,
            partition_fields,
            remaining_limit,
            output_schema,
            fields,
            done: false,
        };

        // Demand-driven (lazy) stream: does no work until polled.
        let stream = stream::unfold(state, |mut state| async move {
            if state.done || state.remaining_limit.is_some_and(|x| x == 0) {
                return None;
            }

            loop {
                let batch_result = match state.input_stream.next().await {
                    None => return None,
                    Some(v) => v,
                };

                let batch = match batch_result {
                    Ok(b) => b,
                    Err(e) => {
                        state.done = true;
                        return Some((Err(e), state));
                    }
                };

                if batch.num_rows() == 0 {
                    continue;
                }

                if state.remaining_limit.is_some_and(|x| x == 0) {
                    return None;
                }

                let files: Vec<FileInfo> = match from_record_batch(&batch) {
                    Ok(v) => v,
                    Err(e) => {
                        state.done = true;
                        return Some((Err(DataFusionError::External(Box::new(e))), state));
                    }
                };

                let batch_limit = state
                    .remaining_limit
                    .map(|x| x.min(usize::MAX as u64) as usize)
                    .filter(|x| *x > 0);

                let (kept, _mask) = match prune_files_with_physical_predicate(
                    state.predicate.clone(),
                    batch_limit,
                    state.prune_schema.clone(),
                    files,
                    &state.partition_fields,
                ) {
                    Ok(v) => v,
                    Err(e) => {
                        state.done = true;
                        return Some((Err(e), state));
                    }
                };

                if let Some(rem) = &mut state.remaining_limit {
                    let kept_rows = kept
                        .iter()
                        .fold(0u64, |acc, f| acc.saturating_add(f.record_count));
                    *rem = rem.saturating_sub(kept_rows);
                }

                let out_batch = if kept.is_empty() {
                    RecordBatch::new_empty(state.output_schema.clone())
                } else {
                    match serde_arrow::to_record_batch(&state.fields, &kept) {
                        Ok(b) => b,
                        Err(e) => {
                            state.done = true;
                            return Some((Err(DataFusionError::External(Box::new(e))), state));
                        }
                    }
                };

                return Some((Ok(out_batch), state));
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

impl DisplayAs for DuckLakePruningExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DuckLakePruningExec")
            }
            DisplayFormatType::TreeRender => write!(f, "DuckLakePruningExec"),
        }
    }
}
