use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::{Result, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{StreamExt, TryStreamExt};

use crate::stream::reader::TaskStreamReader;

#[derive(Debug, Clone)]
pub struct ShuffleReadExec {
    properties: Arc<PlanProperties>,
    reader: Arc<dyn TaskStreamReader>,
}

impl ShuffleReadExec {
    pub fn new(reader: Arc<dyn TaskStreamReader>, properties: Arc<PlanProperties>) -> Self {
        Self { properties, reader }
    }
}

impl DisplayAs for ShuffleReadExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "ShuffleReadExec: partitioning={}",
            self.properties.output_partitioning(),
        )
    }
}

impl ExecutionPlan for ShuffleReadExec {
    fn name(&self) -> &str {
        "ShuffleReadExec"
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
            return internal_err!("ShuffleReadExec does not accept children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let reader = self.reader.clone();
        let output = futures::stream::once(async move {
            let source = reader.open(partition).await?;
            Ok::<_, datafusion::error::DataFusionError>(source.map(|item| {
                item.map_err(|error| datafusion::error::DataFusionError::External(Box::new(error)))
            }))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }
}
