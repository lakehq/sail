use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, LexRequirement, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_physical_expr::expressions::Column as PhysicalColumn;
use deltalake::kernel::Action;
use deltalake::protocol::DeltaOperation;
use serde::{Deserialize, Serialize};

mod commit_exec;
mod plan_builder;
mod writer_exec;

pub use commit_exec::DeltaCommitExec;
pub use plan_builder::DeltaPlanBuilder;
pub use writer_exec::DeltaWriterExec;

/// Create a `ProjectionExec` instance that reorders columns so that partition columns
/// are placed at the end of the `RecordBatch`.
pub fn create_projection(
    input: Arc<dyn ExecutionPlan>,
    partition_columns: Vec<String>,
) -> Result<Arc<ProjectionExec>> {
    let input_schema = input.schema();
    let projection_exprs = create_projection_expressions(&input_schema, &partition_columns)?;

    Ok(Arc::new(ProjectionExec::try_new(projection_exprs, input)?))
}

fn create_projection_expressions(
    input_schema: &ArrowSchema,
    partition_columns: &[String],
) -> Result<Vec<(Arc<dyn PhysicalExpr>, String)>> {
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
            DataFusionError::Plan(format!("Partition column '{col_name}' not found in schema"))
        })?;
        let column_expr = Arc::new(Column::new(col_name, idx)) as Arc<dyn PhysicalExpr>;
        projection_exprs.push((column_expr, col_name.clone()));
    }

    Ok(projection_exprs)
}

/// Create a `SortExec` instance for Delta Lake data sorting.
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
            expr: Arc::new(PhysicalColumn::new(name, idx)) as Arc<dyn PhysicalExpr>,
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
        let lex_ordering = LexOrdering::new(sort_exprs)
            .ok_or_else(|| DataFusionError::Internal("Failed to create LexOrdering".to_string()))?;
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

/// Create a `RepartitionExec` instance for Delta Lake data repartitioning.
pub fn create_repartition(
    input: Arc<dyn ExecutionPlan>,
    partition_columns: Vec<String>,
) -> Result<Arc<RepartitionExec>> {
    let partitioning = if partition_columns.is_empty() {
        // No partition columns, ensure some parallelism
        // TODO: Make partition count configurable
        Partitioning::RoundRobinBatch(4)
    } else {
        // Since create_projection moves partition columns to the end, we can rely on their positions.
        let schema = input.schema();
        let num_cols = schema.fields().len();
        let num_part_cols = partition_columns.len();

        // TODO: Investigate repartitioning behavior for "bucketing" with overlapping partition columns
        // Current implementation may not handle the desired output structure where multiple writers
        // can create files within the same partition directory. For example:
        // year=2024/
        //     part-00000.parquet (created by writer 1)
        //     part-00001.parquet (created by writer 2)
        //     part-00002.parquet (created by writer 3)
        //     part-00003.parquet (created by writer 4)
        // year=2025/
        //     part-00000.parquet (created by writer 1)
        //     part-00001.parquet (created by writer 2)
        //     part-00002.parquet (created by writer 3)
        //     part-00003.parquet (created by writer 4)
        let partition_exprs: Vec<Arc<dyn PhysicalExpr>> = (num_cols - num_part_cols..num_cols)
            .zip(partition_columns.iter())
            .map(|(idx, name)| Arc::new(PhysicalColumn::new(name, idx)) as Arc<dyn PhysicalExpr>)
            .collect();

        // TODO: Partition count should be configurable
        let num_partitions = 4;
        Partitioning::Hash(partition_exprs, num_partitions)
    };

    Ok(Arc::new(RepartitionExec::try_new(input, partitioning)?))
}

/// Helper struct for serializing commit information into a single JSON field
#[derive(Serialize, Deserialize)]
pub struct CommitInfo {
    pub row_count: u64,
    pub add_actions: Vec<deltalake::kernel::Add>,
    pub schema_actions: Vec<Action>,
    pub initial_actions: Vec<Action>,
    pub operation: Option<DeltaOperation>,
}
