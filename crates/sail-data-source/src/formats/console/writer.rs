use std::any::Any;
use std::io::Write;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{plan_err, Result};
use futures::StreamExt;

#[derive(Debug)]
pub struct ConsoleSinkExec {
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl ConsoleSinkExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::new(Schema::empty())),
            Partitioning::UnknownPartitioning(
                input.properties().output_partitioning().partition_count(),
            ),
            EmissionType::Final,
            // The node returns no data, so it is bounded.
            Boundedness::Bounded,
        );
        Self { input, properties }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for ConsoleSinkExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl ExecutionPlan for ConsoleSinkExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match (children.pop(), children.is_empty()) {
            (Some(child), true) => Ok(Arc::new(ConsoleSinkExec::new(child))),
            _ => plan_err!("{} should have exactly one child", self.name()),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        let output = futures::stream::once(async move {
            stream
                .enumerate()
                .for_each(|(i, batch)| async move {
                    let text = match batch {
                        Ok(batch) => match pretty_format_batches(&[batch]) {
                            Ok(batch) => format!("{batch}"),
                            Err(e) => {
                                format!("error formatting batch: {e}")
                            }
                        },
                        Err(e) => {
                            format!("error: {e}")
                        }
                    };
                    let mut stdout = std::io::stdout().lock();
                    let _ = writeln!(stdout, "partition {partition} batch {i}");
                    let _ = writeln!(stdout, "{text}");
                })
                .await;
            futures::stream::empty()
        })
        .flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }
}
