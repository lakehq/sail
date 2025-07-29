use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::Schema;
use datafusion::execution::SessionState;
use datafusion::physical_expr::{
    LexOrdering, LexRequirement, PhysicalSortExpr, PhysicalSortRequirement,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::PhysicalPlanner;
use datafusion_common::{plan_err, Result};
use datafusion_expr::expr::Sort;
use datafusion_expr::{Expr, LogicalPlan};
use sail_common_datafusion::datasource::{PhysicalSinkMode, SinkInfo, SinkMode};
use sail_data_source::default_registry;

use crate::extension::logical::FileWriteOptions;

pub async fn create_file_write_physical_plan(
    ctx: &SessionState,
    planner: &dyn PhysicalPlanner,
    logical_input: &LogicalPlan,
    physical_input: Arc<dyn ExecutionPlan>,
    options: FileWriteOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let FileWriteOptions {
        path,
        format,
        mode,
        partition_by,
        sort_by,
        bucket_by,
        options,
    } = options;
    let mode = match mode {
        SinkMode::ErrorIfExists => PhysicalSinkMode::ErrorIfExists,
        SinkMode::IgnoreIfExists => PhysicalSinkMode::IgnoreIfExists,
        SinkMode::Append => PhysicalSinkMode::Append,
        SinkMode::Overwrite => PhysicalSinkMode::Overwrite,
        SinkMode::OverwriteIf { condition } => {
            let condition =
                planner.create_physical_expr(&condition, logical_input.schema(), ctx)?;
            PhysicalSinkMode::OverwriteIf { condition }
        }
        SinkMode::OverwritePartitions => PhysicalSinkMode::OverwritePartitions,
    };
    let sort_order = if sort_by.is_empty() {
        None
    } else {
        Some(create_sort_order(sort_by, &physical_input.schema())?)
    };
    let info = SinkInfo {
        input: physical_input,
        path,
        mode,
        partition_by,
        bucket_by,
        sort_order,
        options: options.into_iter().collect(),
    };
    default_registry()
        .get_format(&format)?
        .create_writer(ctx, info)
        .await
}

fn create_sort_order(sort_by: Vec<Sort>, schema: &Schema) -> Result<LexRequirement> {
    let mut ordering = LexOrdering::default();
    for sort in sort_by.iter() {
        match &sort.expr {
            Expr::Column(c) => {
                let expr = datafusion_physical_expr::expressions::col(c.name(), schema)?;
                ordering.push(PhysicalSortExpr {
                    expr,
                    options: SortOptions {
                        descending: !sort.asc,
                        nulls_first: sort.nulls_first,
                    },
                });
            }
            _ => return plan_err!("expected column expression in sort order: {sort_by:?}"),
        }
    }
    Ok(LexRequirement::new(
        ordering
            .into_iter()
            .map(PhysicalSortRequirement::from)
            .collect::<Vec<_>>(),
    ))
}
