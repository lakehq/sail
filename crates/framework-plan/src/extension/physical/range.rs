use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use tokio_stream;

use crate::extension::logical::{Range, RangeNode};

const RANGE_BATCH_SIZE: usize = 1024;

#[derive(Debug)]
struct RangeExec {
    range: Range,
    num_partitions: u32,
    schema: SchemaRef,
    cache: PlanProperties,
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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(datafusion::error::DataFusionError::Internal(
                "RangeExec should have no children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        if partition >= self.num_partitions as usize {
            return Err(datafusion::error::DataFusionError::Plan(
                "partition index out of range".to_string(),
            ));
        }
        let mut iter = self
            .range
            .partition(partition as u32, self.num_partitions)
            .into_iter();
        let schema = self.schema.clone();
        let chunks = std::iter::from_fn(move || {
            Some(iter.by_ref().take(RANGE_BATCH_SIZE).collect::<Vec<i64>>())
                .filter(|x| !x.is_empty())
                .map(|x| -> datafusion::common::Result<RecordBatch> {
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

pub(crate) struct RangePlanner {}

#[async_trait]
impl ExtensionPlanner for RangePlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> datafusion::common::Result<Option<Arc<dyn ExecutionPlan>>> {
        let node = node.as_any().downcast_ref::<RangeNode>().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(
                "RangePlanner can only handle RangeNode".to_string(),
            )
        })?;
        let schema: SchemaRef = Arc::new(UserDefinedLogicalNode::schema(node).as_ref().into());
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(node.num_partitions() as usize),
            ExecutionMode::Bounded,
        );
        Ok(Some(Arc::new(RangeExec {
            range: node.range().clone(),
            num_partitions: node.num_partitions(),
            schema,
            cache,
        })))
    }
}
