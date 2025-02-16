use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{internal_datafusion_err, Result};
use sail_common_datafusion::udf::StreamUDF;
use sail_common_datafusion::utils::record_batch_with_schema;
use tokio_stream::StreamExt;

use crate::utils::ItemTaker;

#[derive(Debug, Clone)]
pub struct MapPartitionsExec {
    input: Arc<dyn ExecutionPlan>,
    udf: Arc<dyn StreamUDF>,
    properties: PlanProperties,
}

impl MapPartitionsExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, udf: Arc<dyn StreamUDF>, schema: SchemaRef) -> Self {
        // The plan output schema can be different from the output schema of the UDF
        // due to field renaming.
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        );
        Self {
            input,
            udf,
            properties,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn udf(&self) -> &Arc<dyn StreamUDF> {
        &self.udf
    }
}

impl DisplayAs for MapPartitionsExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "MapPartitionsExec")
    }
}

impl ExecutionPlan for MapPartitionsExec {
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = children.one().map_err(|_| {
            internal_datafusion_err!("MapPartitionsExec must have exactly one child")
        })?;
        Ok(Arc::new(Self {
            input,
            ..self.as_ref().clone()
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        let output = self.udf.invoke(stream)?;
        let schema = self.schema();
        let output =
            output.map(move |x| x.and_then(|batch| record_batch_with_schema(batch, &schema)));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }
}
