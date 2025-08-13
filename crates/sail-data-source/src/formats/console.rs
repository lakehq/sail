use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::util::pretty::print_batches;
use datafusion::catalog::{Session, TableProvider};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{not_impl_err, plan_err, Result};
use futures::StreamExt;
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat};

#[derive(Debug)]
pub struct ConsoleTableFormat;

#[async_trait]
impl TableFormat for ConsoleTableFormat {
    fn name(&self) -> &str {
        "console"
    }

    async fn create_provider(
        &self,
        _ctx: &dyn Session,
        _info: SourceInfo,
    ) -> Result<Arc<dyn TableProvider>> {
        not_impl_err!("console table format does not support reading")
    }

    async fn create_writer(
        &self,
        _ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ConsoleExec::new(info.input)))
    }
}

#[derive(Debug)]
struct ConsoleExec {
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl ConsoleExec {
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
}

impl DisplayAs for ConsoleExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl ExecutionPlan for ConsoleExec {
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
            (Some(child), true) => Ok(Arc::new(ConsoleExec::new(child))),
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
                    println!("partition {partition} batch {i}");
                    match batch {
                        Ok(batch) => match print_batches(&[batch]) {
                            Ok(_) => {}
                            Err(e) => {
                                println!("error printing batch: {e}");
                            }
                        },
                        Err(e) => {
                            println!("error: {e}");
                        }
                    }
                })
                .await;
            futures::stream::empty()
        })
        .flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.properties.eq_properties.schema().clone(),
            output,
        )))
    }
}
