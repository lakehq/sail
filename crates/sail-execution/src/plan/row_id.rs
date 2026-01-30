use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{exec_datafusion_err, plan_err, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures::StreamExt;

pub const BUILD_ROW_ID_COLUMN: &str = "__sail_build_row_id";

#[derive(Debug, Clone)]
pub struct AddRowIdExec {
    input: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    properties: PlanProperties,
}

impl AddRowIdExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        let mut fields = input.schema().fields().to_vec();
        fields.push(Arc::new(Field::new(
            BUILD_ROW_ID_COLUMN,
            DataType::UInt64,
            // Nullable: downstream operators may represent "no corresponding build-side row"
            // (e.g. outer-join paths) using NULL.
            true,
        )));
        let output_schema = Arc::new(datafusion::arrow::datatypes::Schema::new(fields));
        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            input.output_partitioning().clone(),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Ok(Self {
            input,
            output_schema,
            properties,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for AddRowIdExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "AddRowIdExec")
    }
}

impl ExecutionPlan for AddRowIdExec {
    fn name(&self) -> &str {
        "AddRowIdExec"
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
        let child = children.pop();
        match (child, children.is_empty()) {
            (Some(input), true) => Ok(Arc::new(Self::try_new(input)?)),
            _ => plan_err!("AddRowIdExec should have one child"),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = self.output_schema.clone();
        let output =
            // TODO: This currently generates row-ids starting from 1 per input partition. 
            // Correctness for distributed collect-left join relies on upstream planning to ensure 
            // a single partition before `AddRowIdExec` (e.g. via `CoalescePartitionsExec`), otherwise 
            // row-ids can collide across partitions.
            //
            // Consider encoding partition/stage identity into the row-id (or generating a stable
            // hash-based id) so we can avoid coalescing wide inputs just to assign unique row-ids.
            // With that in place, we can also push down projection earlier (while retaining
            // join keys + `row_id`) to reduce shuffle/broadcast width for wide tables.
            // Start from 1 and reserve 0 as a sentinel value. This keeps the row-id namespace
            // distinct from any "missing" / default materialization that may appear downstream.
            futures::stream::unfold((input, 1u64, schema.clone()), move |state| async move {
                let (mut stream, mut offset, schema) = state;
                let batch = stream.next().await?;
                let batch = match batch {
                    Ok(batch) => batch,
                    Err(err) => return Some((Err(err), (stream, offset, schema))),
                };
                let num_rows = batch.num_rows();
                let row_ids = UInt64Array::from_iter_values(offset..(offset + num_rows as u64));
                offset += num_rows as u64;
                let mut columns = batch.columns().to_vec();
                columns.push(Arc::new(row_ids));
                let batch = match RecordBatch::try_new(schema.clone(), columns) {
                    Ok(batch) => batch,
                    Err(err) => {
                        return Some((
                            Err(exec_datafusion_err!("failed to build row id batch: {err}")),
                            (stream, offset, schema),
                        ))
                    }
                };
                Some((Ok(batch), (stream, offset, schema)))
            });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, output)))
    }
}
