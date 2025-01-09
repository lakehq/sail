use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{exec_err, internal_err, Result};

use crate::extension::logical::Range;

const RANGE_BATCH_SIZE: usize = 1024;

#[derive(Debug, Clone)]
pub struct RangeExec {
    range: Range,
    num_partitions: usize,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl RangeExec {
    pub fn new(range: Range, num_partitions: usize, schema: SchemaRef) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::RoundRobinBatch(num_partitions),
            EmissionType::Both,
            Boundedness::Bounded,
        );
        Self {
            range,
            num_partitions,
            schema,
            properties,
        }
    }

    pub fn range(&self) -> &Range {
        &self.range
    }

    pub fn num_partitions(&self) -> usize {
        self.num_partitions
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

    fn properties(&self) -> &PlanProperties {
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
        let schema = self.schema.clone();
        let chunks = std::iter::from_fn(move || {
            Some(iter.by_ref().take(RANGE_BATCH_SIZE).collect::<Vec<i64>>())
                .filter(|x| !x.is_empty())
                .map(|x| -> Result<RecordBatch> {
                    let array = Arc::new(Int64Array::from(x));
                    Ok(RecordBatch::try_new(schema.clone(), vec![array])?)
                })
        });
        let stream = tokio_stream::iter(chunks);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}
