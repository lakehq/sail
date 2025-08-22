use std::sync::Arc;

use datafusion::arrow::compute::SortOptions;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::{LexOrdering, LexRequirement, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_physical_expr::expressions::Column;

/// `DeltaSortExec` is a factory for creating `SortExec` instances
/// for Delta Lake data sorting.
pub struct DeltaSortExec;

impl DeltaSortExec {
    pub fn create_sort(
        input: Arc<dyn ExecutionPlan>,
        partition_columns: Vec<String>,
        sort_order: Option<LexRequirement>,
    ) -> Result<Arc<SortExec>> {
        let schema = input.schema();
        let num_cols = schema.fields().len();
        let num_part_cols = partition_columns.len();

        let mut sort_exprs: Vec<PhysicalSortExpr> = (num_cols - num_part_cols..num_cols)
            .zip(partition_columns.iter())
            .map(|(idx, name)| PhysicalSortExpr {
                expr: Arc::new(Column::new(name, idx)) as Arc<dyn PhysicalExpr>,
                options: SortOptions::default(), // Default ascending
            })
            .collect();

        // Add user-specified sort columns
        if let Some(ref user_sort_order) = sort_order {
            for req in user_sort_order {
                sort_exprs.push(PhysicalSortExpr {
                    expr: req.expr.clone(),
                    options: req.options.unwrap_or_default(),
                });
            }
        }

        let sort_exec = if !sort_exprs.is_empty() {
            let lex_ordering = LexOrdering::new(sort_exprs).ok_or_else(|| {
                DataFusionError::Internal("Failed to create LexOrdering".to_string())
            })?;
            // We've already partitioned data in the Repartition node,
            // sorting is only within each physical partition, so we must preserve partitioning.
            Arc::new(SortExec::new(lex_ordering, input).with_preserve_partitioning(true))
        } else {
            // No sorting needed, create a minimal SortExec with empty ordering
            use datafusion::common::ScalarValue;
            use datafusion_physical_expr::expressions::lit;

            let dummy_expr = PhysicalSortExpr {
                expr: lit(ScalarValue::Int32(Some(1))),
                options: SortOptions::default(),
            };
            let lex_ordering = LexOrdering::new(vec![dummy_expr]).ok_or_else(|| {
                DataFusionError::Internal("Failed to create dummy LexOrdering".to_string())
            })?;
            Arc::new(SortExec::new(lex_ordering, input))
        };

        Ok(sort_exec)
    }
}
