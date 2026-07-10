use std::sync::Arc;

use datafusion::logical_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, Result};
use datafusion_expr::logical_plan::Extension;
use datafusion_expr::{Expr, LogicalPlan, TableScan, TableSource};
use log::trace;
use sail_common_datafusion::datasource::{
    MERGE_FILE_COLUMN, MERGE_ROW_INDEX_COLUMN, MergeCapableSource, MergeInfo, MergeMatchedAction,
    MergeNotMatchedBySourceAction,
};
use sail_logical_plan::merge::{RowLevelWriteNode, expand_merge};

use crate::logical::table_source::IcebergTableSource;

/// Expand MERGE information into a unified row-level write node for Iceberg.
///
/// Iceberg MERGE is planned as merge-on-read: target rows affected by DELETE or
/// UPDATE clauses are represented by position deletes, and UPDATE/INSERT output
/// rows are appended as new data files.
pub fn expand_merge_node(info: MergeInfo) -> Result<LogicalPlan> {
    let expected_snapshot_id = Some(merge_target_snapshot_id(info.target.as_ref())?);
    let row_index_column = merge_needs_position_deletes(&info).then_some(MERGE_ROW_INDEX_COLUMN);
    let mut target_plan = ensure_merge_metadata_columns(
        info.target.as_ref().clone(),
        MERGE_FILE_COLUMN,
        row_index_column,
    )?;
    let target_fields: Vec<String> = target_plan
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    trace!(
        "iceberg merge target schema after metadata columns: {:?}",
        target_fields
    );
    if !target_fields.iter().any(|n| n == MERGE_FILE_COLUMN)
        || row_index_column.is_some_and(|c| !target_fields.iter().any(|n| n == c))
    {
        let mut exprs: Vec<Expr> = target_fields
            .iter()
            .map(|name| Expr::Column(Column::from_name(name.clone())))
            .collect();
        if !target_fields.iter().any(|n| n == MERGE_FILE_COLUMN) {
            exprs.push(Expr::Column(Column::from_name(MERGE_FILE_COLUMN)).alias(MERGE_FILE_COLUMN));
        }
        if let Some(row_index_column) = row_index_column
            && !target_fields.iter().any(|n| n == row_index_column)
        {
            exprs.push(Expr::Column(Column::from_name(row_index_column)).alias(row_index_column));
        }
        target_plan = LogicalPlanBuilder::from(target_plan)
            .project(exprs)?
            .build()?;
    }

    let info = MergeInfo {
        target: Arc::new(target_plan),
        source: info.source,
        options: info.options,
        input_schema: info.input_schema,
    };
    let raw_target = Arc::clone(&info.target);
    let raw_source = Arc::clone(&info.source);
    let raw_input_schema = info.input_schema.clone();
    let expansion = expand_merge(info, MERGE_FILE_COLUMN, row_index_column)?;
    let write_node = RowLevelWriteNode::new_merge(
        raw_target,
        raw_source,
        raw_input_schema,
        Arc::new(expansion.write_plan),
        Arc::new(expansion.touched_files_plan),
        expansion.row_index_delete_plan.map(Arc::new),
        expansion.options,
        expansion.output_schema,
    )
    .with_expected_snapshot_id(expected_snapshot_id);

    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(write_node),
    }))
}

fn merge_target_snapshot_id(plan: &LogicalPlan) -> Result<Option<i64>> {
    let mut snapshot_id = None;
    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && let Some(source) = scan.source.downcast_ref::<IcebergTableSource>()
        {
            snapshot_id = source
                .provider()
                .current_snapshot()
                .map(|snapshot| snapshot.snapshot_id());
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(snapshot_id)
}

fn merge_needs_position_deletes(info: &MergeInfo) -> bool {
    info.options.matched_clauses.iter().any(|clause| {
        matches!(
            clause.action,
            MergeMatchedAction::Delete
                | MergeMatchedAction::UpdateAll
                | MergeMatchedAction::UpdateSet(_)
        )
    }) || info
        .options
        .not_matched_by_source_clauses
        .iter()
        .any(|clause| {
            matches!(
                clause.action,
                MergeNotMatchedBySourceAction::Delete | MergeNotMatchedBySourceAction::UpdateSet(_)
            )
        })
}

fn try_enable_merge_metadata_columns(
    source: &Arc<dyn TableSource>,
    file_col: &str,
    row_index_col: Option<&str>,
) -> Result<
    Option<(
        Arc<dyn TableSource>,
        datafusion::arrow::datatypes::SchemaRef,
    )>,
> {
    let Some(iceberg_source) = source.downcast_ref::<IcebergTableSource>() else {
        return Ok(None);
    };
    let mut new_source = Arc::clone(source);
    let mut changed = false;

    if iceberg_source.file_column_name().is_none() {
        new_source = iceberg_source.with_file_column(file_col)?;
        changed = true;
    }
    if let (Some(row_index_col), Some(iceberg_source)) = (
        row_index_col,
        new_source.downcast_ref::<IcebergTableSource>(),
    ) && iceberg_source.row_index_column_name().is_none()
    {
        new_source = iceberg_source.with_row_index_column(row_index_col)?;
        changed = true;
    }
    if changed {
        let schema = new_source.schema();
        return Ok(Some((new_source, schema)));
    }
    Ok(None)
}

fn ensure_merge_metadata_columns(
    plan: LogicalPlan,
    file_col: &str,
    row_index_col: Option<&str>,
) -> Result<LogicalPlan> {
    let mut metadata_cols = vec![file_col];
    if let Some(row_index_col) = row_index_col {
        metadata_cols.push(row_index_col);
    }

    let transformed = plan
        .transform_up(|plan| {
            if let LogicalPlan::TableScan(scan) = &plan
                && let Some((new_source, schema)) =
                    try_enable_merge_metadata_columns(&scan.source, file_col, row_index_col)?
            {
                let mut projection: Option<Vec<usize>> = scan.projection.clone();
                if projection.is_none() {
                    projection = Some((0..schema.fields().len()).collect::<Vec<usize>>());
                }
                if let Some(proj) = projection.as_mut() {
                    for col in &metadata_cols {
                        if let Some(idx) = schema.column_with_name(col).map(|(idx, _)| idx)
                            && !proj.contains(&idx)
                        {
                            proj.push(idx);
                        }
                    }
                }

                let new_scan = LogicalPlan::TableScan(TableScan::try_new(
                    scan.table_name.clone(),
                    new_source,
                    projection,
                    scan.filters.clone(),
                    scan.fetch,
                )?);
                return Ok(Transformed::yes(new_scan));
            }

            if let LogicalPlan::Projection(proj) = &plan {
                let input_schema = proj.input.schema();
                let mut new_exprs = proj.expr.clone();
                let mut changed = false;
                for col in &metadata_cols {
                    let has_in_input = input_schema.fields().iter().any(|f| f.name() == *col);
                    let has_in_projection = proj.expr.iter().any(|e| match e {
                        Expr::Column(c) => c.name == *col,
                        Expr::Alias(a) => a.name == *col,
                        _ => false,
                    });
                    if has_in_input && !has_in_projection {
                        new_exprs.push(Expr::Column(Column::from_name(*col)).alias(*col));
                        changed = true;
                    }
                }
                if changed {
                    let new_proj = LogicalPlanBuilder::from(proj.input.as_ref().clone())
                        .project(new_exprs)?
                        .build()?;
                    return Ok(Transformed::yes(new_proj));
                }
            }

            Ok(Transformed::no(plan))
        })
        .map(|t| t.data)?;

    let mut transformed = transformed;
    if let LogicalPlan::SubqueryAlias(sa) = &transformed {
        let missing_in_alias = metadata_cols.iter().any(|col| {
            let has_in_child = sa.input.schema().fields().iter().any(|f| f.name() == *col);
            let has_in_alias = sa.schema.fields().iter().any(|f| f.name() == *col);
            has_in_child && !has_in_alias
        });
        if missing_in_alias {
            transformed =
                LogicalPlan::SubqueryAlias(datafusion_expr::logical_plan::SubqueryAlias::try_new(
                    sa.input.clone(),
                    sa.alias.clone(),
                )?);
        }
    }

    Ok(transformed)
}
