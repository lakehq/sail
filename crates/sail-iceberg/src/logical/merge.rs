use std::sync::Arc;

use datafusion::logical_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, Result, ScalarValue, not_impl_err};
use datafusion_expr::logical_plan::Extension;
use datafusion_expr::utils::conjunction;
use datafusion_expr::{Expr, LogicalPlan, TableScanBuilder, TableSource, lit};
use log::trace;
use sail_common_datafusion::datasource::{
    MERGE_FILE_COLUMN, MERGE_ROW_INDEX_COLUMN, MergeCapableSource, MergeInfo, MergeMatchedAction,
    MergeNotMatchedBySourceAction, RowLevelCommand, RowLevelWriteMode,
};
use sail_common_datafusion::logical_expr::ExprWithSource;
use sail_logical_plan::merge::{
    MergePlanRequirements, expand_merge, validate_merge_internal_columns,
};
use sail_logical_plan::row_level::{
    RowLevelEffectPlans, RowLevelEffectRequirements, RowLevelWriteNode,
};

use crate::logical::table_source::IcebergTableSource;
use crate::row_level_metadata::{MERGE_FILE_METADATA_COLUMN, MERGE_PARTITION_SPEC_ID_COLUMN};

/// Expand MERGE information into a unified row-level write node for Iceberg.
///
/// The table mode selects either affected-file rewrites or position deletes.
pub fn expand_merge_node(info: MergeInfo) -> Result<LogicalPlan> {
    // TODO: Add Iceberg MERGE schema evolution support.
    if info.options.with_schema_evolution {
        return not_impl_err!("Iceberg MERGE WITH SCHEMA EVOLUTION is not supported");
    }
    validate_merge_internal_columns(
        &info,
        &[
            MERGE_FILE_COLUMN,
            MERGE_ROW_INDEX_COLUMN,
            MERGE_PARTITION_SPEC_ID_COLUMN,
            MERGE_FILE_METADATA_COLUMN,
            crate::row_lineage::ROW_ID_COLUMN,
            crate::row_lineage::LAST_UPDATED_SEQUENCE_COLUMN,
        ],
    )?;
    let (mode, snapshot_id) =
        super::row_level::target_write_state(&info.target, RowLevelCommand::Merge)?;
    let expected_snapshot_id = Some(snapshot_id);
    let row_index_column = merge_needs_position_deletes(&info).then_some(MERGE_ROW_INDEX_COLUMN);
    let mut target_plan = ensure_merge_metadata_columns(
        info.target.as_ref().clone(),
        MERGE_FILE_COLUMN,
        row_index_column,
    )?;
    if mode == RowLevelWriteMode::CopyOnWrite
        && info.options.not_matched_by_source_clauses.is_empty()
        && let Some(predicate) = conjunction(info.options.target_only_predicates.clone())
    {
        let predicate = sail_logical_plan::row_level::rewrite_row_level_target_condition(
            Some(ExprWithSource::new(predicate, None)),
            &info.options.resolved_target_schema,
            info.target.schema(),
            &info.options.resolved_target_field_names,
        )?
        .ok_or_else(|| {
            datafusion_common::internal_datafusion_err!("Missing MERGE target predicate")
        })?;
        target_plan =
            super::row_level::select_copy_on_write_candidates(target_plan, predicate.expr)?;
    }
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
    let mut row_metadata_columns = vec![MERGE_PARTITION_SPEC_ID_COLUMN, MERGE_FILE_METADATA_COLUMN];
    row_metadata_columns.extend(super::row_level::lineage_columns(&target_plan)?);
    let mut required_metadata_columns = vec![
        MERGE_FILE_COLUMN,
        MERGE_PARTITION_SPEC_ID_COLUMN,
        MERGE_FILE_METADATA_COLUMN,
    ];
    required_metadata_columns.extend(super::row_level::lineage_columns(&target_plan)?);
    if let Some(row_index_column) = row_index_column {
        required_metadata_columns.push(row_index_column);
    }
    if required_metadata_columns
        .iter()
        .any(|column| !target_fields.iter().any(|name| name == column))
    {
        let mut exprs: Vec<Expr> = target_fields
            .iter()
            .map(|name| Expr::Column(Column::from_name(name.clone())))
            .collect();
        for metadata_column in required_metadata_columns {
            if !target_fields.iter().any(|name| name == metadata_column) {
                exprs.push(Expr::Column(Column::from_name(metadata_column)).alias(metadata_column));
            }
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
    let expansion = expand_merge(
        info,
        MERGE_FILE_COLUMN,
        row_index_column,
        &row_metadata_columns,
        MergePlanRequirements {
            preserve_unmodified_target_rows: mode == RowLevelWriteMode::CopyOnWrite,
            source_metrics: false,
            effects: RowLevelEffectRequirements::default(),
        },
    )?;
    let write_plan = match (mode, row_index_column) {
        (RowLevelWriteMode::CopyOnWrite, Some(_)) => {
            super::row_level::select_copy_on_write_rows(expansion.write_plan)?
        }
        (RowLevelWriteMode::CopyOnWrite, None) => {
            let mut projection = expansion
                .write_plan
                .schema()
                .columns()
                .into_iter()
                .map(Expr::Column)
                .collect::<Vec<_>>();
            projection.push(lit(ScalarValue::Utf8(None)).alias(MERGE_FILE_COLUMN));
            LogicalPlanBuilder::from(expansion.write_plan)
                .project(projection)?
                .build()?
        }
        _ => expansion.write_plan,
    };
    let effects = RowLevelEffectPlans::new(Some(Arc::new(write_plan)), None, None);
    let write_node = RowLevelWriteNode::new_merge(
        raw_target,
        mode,
        effects,
        expansion.options,
        expansion.output_schema,
    )
    .with_expected_snapshot_id(expected_snapshot_id);

    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(write_node),
    }))
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

pub(crate) fn ensure_merge_metadata_columns(
    plan: LogicalPlan,
    file_col: &str,
    row_index_col: Option<&str>,
) -> Result<LogicalPlan> {
    let mut metadata_cols = vec![
        file_col,
        MERGE_PARTITION_SPEC_ID_COLUMN,
        MERGE_FILE_METADATA_COLUMN,
        crate::row_lineage::ROW_ID_COLUMN,
        crate::row_lineage::LAST_UPDATED_SEQUENCE_COLUMN,
    ];
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

                let new_scan = LogicalPlan::TableScan(
                    TableScanBuilder::new(scan.table_name.clone(), new_source)
                        .with_projection(projection)
                        .with_filters(scan.filters.clone())
                        .with_fetch(scan.fetch)
                        .with_statistics_requests(scan.statistics_requests.clone())
                        .build()?,
                );
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
