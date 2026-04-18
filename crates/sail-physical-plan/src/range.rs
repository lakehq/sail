use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{exec_err, internal_err, plan_err, Result};
use sail_logical_plan::range::Range;

const RANGE_BATCH_SIZE: usize = 1024;

#[derive(Debug, Clone)]
pub struct RangeExec {
    range: Range,
    num_partitions: usize,
    original_schema: SchemaRef,
    projected_schema: SchemaRef,
    projection: Vec<usize>,
    properties: Arc<PlanProperties>,
}

impl RangeExec {
    /// Creates a new execution plan for the range source.
    /// The schema should be the original schema before projection.
    pub fn try_new(
        range: Range,
        num_partitions: usize,
        schema: SchemaRef,
        projection: Vec<usize>,
    ) -> Result<Self> {
        let projected_schema = Arc::new(schema.project(&projection)?);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::RoundRobinBatch(num_partitions),
            EmissionType::Both,
            Boundedness::Bounded,
        ));
        Ok(Self {
            range,
            num_partitions,
            original_schema: schema,
            projected_schema,
            projection,
            properties,
        })
    }

    pub fn range(&self) -> &Range {
        &self.range
    }

    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    pub fn original_schema(&self) -> &SchemaRef {
        &self.original_schema
    }

    pub fn projection(&self) -> &[usize] {
        &self.projection
    }
}

impl DisplayAs for RangeExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "RangeExec")
    }
}

impl ExecutionPlan for RangeExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("RangeExec should have no children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.num_partitions {
            return exec_err!("partition index out of range: {}", partition);
        }
        let mut iter = self
            .range
            .partition(partition, self.num_partitions)
            .into_iter();
        let projected_schema = self.projected_schema.clone();
        let projection = self.projection.clone();
        let chunks = std::iter::from_fn(move || {
            Some(iter.by_ref().take(RANGE_BATCH_SIZE).collect::<Vec<i64>>())
                .filter(|x| !x.is_empty())
                .map(|x| -> Result<RecordBatch> {
                    let num_rows = x.len();
                    if projection.is_empty() {
                        return Ok(RecordBatch::try_new_with_options(
                            projected_schema.clone(),
                            vec![],
                            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
                        )?);
                    }
                    let id_array: ArrayRef = Arc::new(Int64Array::from(x));
                    let columns: Vec<ArrayRef> = projection
                        .iter()
                        .map(|&i| match i {
                            0 => Ok(id_array.clone()),
                            _ => plan_err!("invalid projection index {i} for range table"),
                        })
                        .collect::<Result<_>>()?;
                    Ok(RecordBatch::try_new(projected_schema.clone(), columns)?)
                })
        });
        let stream = tokio_stream::iter(chunks);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.projected_schema.clone(),
            stream,
        )))
    }
}
