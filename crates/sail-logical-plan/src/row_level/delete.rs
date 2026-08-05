use std::sync::Arc;

use datafusion_common::{DFSchema, Result};
use datafusion_expr::{Expr, LogicalPlanBuilder, col, lit, when};
use sail_common_datafusion::datasource::{
    DeleteInfo, ROW_ACTION_COLUMN, ROW_ACTION_ORIGIN_COLUMN, RowAction, RowActionOrigin,
};
use sail_common_datafusion::logical_expr::ExprWithSource;

use super::target::{normalize_row_level_target, rewrite_target_expr};
use super::{ExpandedRowLevelOperation, RowLevelCommitInfo, RowLevelEffect};

pub fn expand_delete(
    info: DeleteInfo,
    path_column: &str,
    row_index_column: Option<&str>,
) -> Result<ExpandedRowLevelOperation> {
    let normalized = normalize_row_level_target(
        info.target_plan.as_ref().clone(),
        &info.input_schema,
        &info.resolved_target_field_names,
        path_column,
        row_index_column,
    )?;
    let condition = info
        .condition
        .map(|condition| -> Result<_> {
            Ok(ExprWithSource::new(
                rewrite_target_expr(condition.expr, &normalized.rename_map)?,
                condition.source,
            ))
        })
        .transpose()?;
    let predicate = condition
        .as_ref()
        .map(|condition| condition.expr.clone())
        .unwrap_or_else(|| lit(true));

    let mut write_projection = normalized
        .field_names
        .iter()
        .map(|name| col(name).alias(name))
        .collect::<Vec<_>>();
    write_projection.push(col(path_column).alias(path_column));
    write_projection.push(
        when(predicate.clone(), lit(RowAction::Delete.as_i32()))
            .otherwise(lit(RowAction::Copy.as_i32()))?
            .alias(ROW_ACTION_COLUMN),
    );
    write_projection.push(lit(RowActionOrigin::Direct.as_i32()).alias(ROW_ACTION_ORIGIN_COLUMN));
    let write_rows = LogicalPlanBuilder::from(normalized.plan.clone())
        .project(write_projection)?
        .build()?;

    let touched_files = LogicalPlanBuilder::from(normalized.plan.clone())
        .filter(predicate.clone())?
        .aggregate(vec![col(path_column)], Vec::<Expr>::new())?
        .project(vec![col(path_column).alias(path_column)])?
        .build()?;

    let mut effects = vec![
        RowLevelEffect::WriteRows(Arc::new(write_rows)),
        RowLevelEffect::TouchFiles(Arc::new(touched_files)),
    ];
    if let Some(row_index_column) = row_index_column {
        let delete_rows = LogicalPlanBuilder::from(normalized.plan)
            .filter(predicate)?
            .project(vec![
                col(path_column).alias(path_column),
                col(row_index_column).alias(row_index_column),
            ])?
            .build()?;
        effects.push(RowLevelEffect::DeleteRows(Arc::new(delete_rows)));
    }

    ExpandedRowLevelOperation::try_new(
        info.target,
        effects,
        RowLevelCommitInfo::Delete {
            predicate: condition,
        },
        Arc::new(DFSchema::empty()),
    )
}
