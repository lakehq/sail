//! Physical Operations for DataFusion
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result as DataFusionResult;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
};
use deltalake::DeltaTableError;
use futures::{Stream, StreamExt};

// Metric Observer is used to update DataFusion metrics from a record batch.
// Typically the null count for a particular column is pulled after performing a
// projection since this count is easy to obtain

pub(crate) type MetricObserverFunction = fn(&RecordBatch, &ExecutionPlanMetricsSet) -> ();

pub(crate) struct MetricObserverExec {
    parent: Arc<dyn ExecutionPlan>,
    id: String,
    metrics: ExecutionPlanMetricsSet,
    update: MetricObserverFunction,
}

impl MetricObserverExec {
    pub fn new(id: String, parent: Arc<dyn ExecutionPlan>, f: MetricObserverFunction) -> Self {
        MetricObserverExec {
            parent,
            id,
            metrics: ExecutionPlanMetricsSet::new(),
            update: f,
        }
    }

    pub fn try_new(
        id: String,
        inputs: &[Arc<dyn ExecutionPlan>],
        f: MetricObserverFunction,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        match inputs {
            [input] => Ok(Arc::new(MetricObserverExec::new(id, input.clone(), f))),
            _ => Err(datafusion::common::DataFusionError::External(Box::new(
                DeltaTableError::Generic("MetricObserverExec expects only one child".into()),
            ))),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }
}

impl std::fmt::Debug for MetricObserverExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricObserverExec")
            .field("id", &self.id)
            .field("metrics", &self.metrics)
            .finish()
    }
}

impl DisplayAs for MetricObserverExec {
    fn fmt_as(
        &self,
        _: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "MetricObserverExec id={}", self.id)
    }
}

impl ExecutionPlan for MetricObserverExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.parent.schema()
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        self.parent.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.parent]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::context::TaskContext>,
    ) -> datafusion::common::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let res = self.parent.execute(partition, context)?;
        Ok(Box::pin(MetricObserverStream {
            schema: self.schema(),
            input: res,
            metrics: self.metrics.clone(),
            update: self.update,
        }))
    }

    fn statistics(&self) -> DataFusionResult<datafusion::common::Statistics> {
        self.parent.partition_statistics(None)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        MetricObserverExec::try_new(self.id.clone(), &children, self.update)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct MetricObserverStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    metrics: ExecutionPlanMetricsSet,
    update: MetricObserverFunction,
}

impl Stream for MetricObserverStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.input.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(batch)) => {
                (self.update)(&batch, &self.metrics);
                Some(Ok(batch))
            }
            other => other,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.input.size_hint()
    }
}

impl RecordBatchStream for MetricObserverStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub(crate) fn find_metric_node(
    id: &str,
    parent: &Arc<dyn ExecutionPlan>,
) -> Option<Arc<dyn ExecutionPlan>> {
    //! Used to locate the physical MetricCountExec Node after the planner converts the logical node
    if let Some(metric) = parent.as_any().downcast_ref::<MetricObserverExec>() {
        if metric.id().eq(id) {
            return Some(parent.to_owned());
        }
    }

    for child in &parent.children() {
        let res = find_metric_node(id, child);
        if res.is_some() {
            return res;
        }
    }

    None
}

pub(crate) fn get_metric(metrics: &MetricsSet, name: &str) -> usize {
    metrics.sum_by_name(name).map(|m| m.as_usize()).unwrap_or(0)
}
