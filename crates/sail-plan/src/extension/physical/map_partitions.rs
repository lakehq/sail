use std::any::Any;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{internal_datafusion_err, Result};
use sail_common::udf::MapIterUDF;
use sail_common::utils::rename_physical_plan;
use tokio_stream::StreamExt;

use crate::utils::ItemTaker;

#[derive(Debug, Clone)]
pub struct MapPartitionsExec {
    input: Arc<dyn ExecutionPlan>,
    input_names: Vec<String>,
    udf: Arc<dyn MapIterUDF>,
    properties: PlanProperties,
}

impl MapPartitionsExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        input_names: Vec<String>,
        udf: Arc<dyn MapIterUDF>,
        schema: SchemaRef,
    ) -> Self {
        // The plan output schema can be different from the output schema of the UDF
        // due to field renaming.
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            input.output_partitioning().clone(),
            input.execution_mode(),
        );
        Self {
            input,
            input_names,
            udf,
            properties,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn input_names(&self) -> &[String] {
        &self.input_names
    }

    pub fn udf(&self) -> &Arc<dyn MapIterUDF> {
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
        let input = rename_physical_plan(self.input.clone(), &self.input_names)?;
        let stream = input.execute(partition, context)?;
        let output = self.udf.invoke(stream)?;
        let schema = self.schema().clone();
        let output = output.map(move |x| {
            x.and_then(|batch| {
                Ok(RecordBatch::try_new(
                    schema.clone(),
                    batch.columns().to_vec(),
                )?)
            })
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }
}
