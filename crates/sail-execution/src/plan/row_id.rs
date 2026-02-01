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
const ROW_ID_OFFSET_BITS: u32 = 40;
const ROW_ID_OFFSET_MASK: u64 = (1u64 << ROW_ID_OFFSET_BITS) - 1;
const ROW_ID_PARTITION_BITS: u32 = 64 - ROW_ID_OFFSET_BITS;
const ROW_ID_PARTITION_LIMIT: u64 = 1u64 << ROW_ID_PARTITION_BITS;

// NOTE: RowID encodes (partition_id, local_offset) as:
//   row_id = (partition_id << ROW_ID_OFFSET_BITS) | local_offset
// This assumes a fixed 40/24 split (offset/partition). If needed, we can make the
// split configurable (e.g. 32/32) once we have a cluster-level config surface.

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
        let partition_id = partition as u64;
        if partition_id >= ROW_ID_PARTITION_LIMIT {
            return Err(exec_datafusion_err!(
                "partition id {} exceeds row id partition limit {}",
                partition_id,
                ROW_ID_PARTITION_LIMIT - 1
            ));
        }
        let row_id_base = partition_id << ROW_ID_OFFSET_BITS;
        let input = self.input.execute(partition, context)?;
        let schema = self.output_schema.clone();
        let output =
            // Row IDs are partition-unique via:
            //   row_id = (partition_id << ROW_ID_OFFSET_BITS) | local_offset
            // where `local_offset` starts from 1. We reserve 0 as a sentinel value (e.g. "missing")
            // to keep the row-id namespace distinct from downstream default materialization.
            //
            // NOTE: This is unique within a single execution, but not stable across changes in
            // partitioning / ordering.
            futures::stream::unfold((input, 1u64, schema.clone()), move |state| async move {
                let (mut stream, mut offset, schema) = state;
                let batch = stream.next().await?;
                let batch = match batch {
                    Ok(batch) => batch,
                    Err(err) => return Some((Err(err), (stream, offset, schema))),
                };
                let num_rows = batch.num_rows() as u64;
                let end_offset = offset.saturating_add(num_rows).saturating_sub(1);
                if offset == 0 || end_offset > ROW_ID_OFFSET_MASK {
                    return Some((
                        Err(exec_datafusion_err!(
                            "row id offset {} exceeds limit {}",
                            end_offset,
                            ROW_ID_OFFSET_MASK
                        )),
                        (stream, offset, schema),
                    ));
                }
                let start = match row_id_base.checked_add(offset) {
                    Some(start) => start,
                    None => {
                        return Some((
                            Err(exec_datafusion_err!("row id base overflow for offset")),
                            (stream, offset, schema),
                        ))
                    }
                };
                let end = match start.checked_add(num_rows) {
                    Some(end) => end,
                    None => {
                        return Some((
                            Err(exec_datafusion_err!("row id range overflow for batch")),
                            (stream, offset, schema),
                        ))
                    }
                };
                let row_ids = UInt64Array::from_iter_values(start..end);
                offset = offset.saturating_add(num_rows);
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

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests use unwrap for brevity")]
mod tests {
    use std::collections::HashSet;

    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::test::TestMemoryExec;

    use super::*;

    #[tokio::test]
    async fn add_row_id_is_partition_unique() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))])
                .unwrap();
        let data = vec![vec![batch.clone()], vec![batch]];
        let input = TestMemoryExec::try_new_exec(&data, schema, None).unwrap();
        let exec = AddRowIdExec::try_new(input).unwrap();

        let ctx = SessionContext::new();
        let batches = collect(Arc::new(exec), ctx.task_ctx()).await.unwrap();
        let mut ids = HashSet::new();
        for batch in batches {
            let row_ids = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for value in row_ids.iter().flatten() {
                ids.insert(value);
            }
        }

        let base_p0 = 0u64 << ROW_ID_OFFSET_BITS;
        let base_p1 = 1u64 << ROW_ID_OFFSET_BITS;
        let expected = HashSet::from([base_p0 + 1, base_p0 + 2, base_p1 + 1, base_p1 + 2]);
        assert_eq!(ids, expected);
    }
}
