use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;

/// `DeltaProjectExec` is a factory for creating `ProjectionExec` instances that reorder
/// columns so that partition columns are placed at the end of the `RecordBatch`.
pub struct DeltaProjectExec;

impl DeltaProjectExec {
    pub fn create_projection(
        input: Arc<dyn ExecutionPlan>,
        partition_columns: Vec<String>,
    ) -> DFResult<Arc<ProjectionExec>> {
        let input_schema = input.schema();
        let projection_exprs =
            Self::create_projection_expressions(&input_schema, &partition_columns)?;

        Ok(Arc::new(ProjectionExec::try_new(projection_exprs, input)?))
    }

    fn create_projection_expressions(
        input_schema: &ArrowSchema,
        partition_columns: &[String],
    ) -> DFResult<Vec<(Arc<dyn PhysicalExpr>, String)>> {
        let mut projection_exprs = Vec::new();
        let mut partition_indices_map = std::collections::HashMap::new();

        let partition_set: HashSet<&String> = partition_columns.iter().collect();

        // First, add all non-partition columns
        for (i, field) in input_schema.fields().iter().enumerate() {
            if partition_set.contains(field.name()) {
                partition_indices_map.insert(field.name().clone(), i);
            } else {
                let column_expr = Arc::new(Column::new(field.name(), i)) as Arc<dyn PhysicalExpr>;
                projection_exprs.push((column_expr, field.name().clone()));
            }
        }

        // Then, add partition columns in the specified order
        for col_name in partition_columns {
            let idx = *partition_indices_map.get(col_name).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Partition column '{}' not found in schema",
                    col_name
                ))
            })?;
            let column_expr = Arc::new(Column::new(col_name, idx)) as Arc<dyn PhysicalExpr>;
            projection_exprs.push((column_expr, col_name.clone()));
        }

        Ok(projection_exprs)
    }
}
