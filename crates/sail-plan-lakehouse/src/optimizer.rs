use std::fmt::Debug;
use std::sync::Arc;

use datafusion::logical_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, Result};
use datafusion_expr::logical_plan::Extension;
use datafusion_expr::{Expr, LogicalPlan, TableScan, TableSource};
use log::trace;
use sail_common_datafusion::datasource::{
    is_lakehouse_format, MergeCapableSource, MERGE_FILE_COLUMN, MERGE_ROW_INDEX_COLUMN,
};
use sail_delta_lake::DeltaTableSource;
use sail_logical_plan::file_delete::FileDeleteNode;
use sail_logical_plan::merge::{
    expand_merge, MergeIntoNode, MergeMatchedAction, MergeNotMatchedBySourceAction,
    RowLevelWriteNode,
};

/// Optimizer rule that expands row-level operations (DELETE, UPDATE, MERGE)
/// into `RowLevelWriteNode` for lakehouse formats.
#[derive(Debug, Clone, Default)]
pub struct ExpandRowLevelOp;

impl ExpandRowLevelOp {
    pub fn new() -> Self {
        Self
    }
}

impl OptimizerRule for ExpandRowLevelOp {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(|plan| {
            if let LogicalPlan::Extension(ext) = &plan {
                // MERGE expansion
                if let Some(node) = ext.node.as_any().downcast_ref::<MergeIntoNode>() {
                    if !is_lakehouse_format(&node.options().target.format) {
                        return Ok(Transformed::no(plan));
                    }
                    return expand_merge_node(node);
                }

                // DELETE → RowLevelWriteNode (delegates physical plan to format)
                if let Some(node) = ext.node.as_any().downcast_ref::<FileDeleteNode>() {
                    if !is_lakehouse_format(node.options().format.as_str()) {
                        return Ok(Transformed::no(plan));
                    }
                    return expand_delete_node(node);
                }
            }
            Ok(Transformed::no(plan))
        })
    }

    fn name(&self) -> &str {
        "expand_row_level_op"
    }
}

/// Expand `MergeIntoNode` → `RowLevelWriteNode(Merge)`.
fn expand_merge_node(node: &MergeIntoNode) -> Result<Transformed<LogicalPlan>> {
    let row_index_column = (merge_has_delete_actions(node)
        && merge_target_supports_deletion_vectors(node.target().as_ref())?)
    .then_some(MERGE_ROW_INDEX_COLUMN);
    let mut target_plan = ensure_merge_metadata_columns(
        node.target().as_ref().clone(),
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
        "rewrite target_plan schema after ensure_merge_metadata_columns: {:?}",
        &target_fields
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
        if let Some(row_index_column) = row_index_column {
            if !target_fields.iter().any(|n| n == row_index_column) {
                exprs.push(
                    Expr::Column(Column::from_name(row_index_column)).alias(row_index_column),
                );
            }
        }
        target_plan = LogicalPlanBuilder::from(target_plan)
            .project(exprs)?
            .build()?;
        trace!(
            "rewrite target_plan schema after patch projection: {:?}",
            target_plan
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        );
    }
    let node = MergeIntoNode::new(
        Arc::new(target_plan),
        node.source().clone(),
        node.options().clone(),
        node.input_schema().clone(),
    );

    let expansion = expand_merge(&node, MERGE_FILE_COLUMN, row_index_column)?;
    trace!(
        "ExpandRowLevelOp write_plan schema fields: {:?}",
        expansion
            .write_plan
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>()
    );
    let write_node = RowLevelWriteNode::new_merge(
        Arc::clone(node.target()),
        Arc::clone(node.source()),
        node.input_schema().clone(),
        Arc::new(expansion.write_plan),
        Arc::new(expansion.touched_files_plan),
        expansion.deletion_vector_plan.map(Arc::new),
        expansion.options,
        expansion.output_schema,
    );

    Ok(Transformed::yes(LogicalPlan::Extension(Extension {
        node: Arc::new(write_node),
    })))
}

/// Convert `FileDeleteNode` → `RowLevelWriteNode(Delete)`.
///
/// DELETE's physical plan is built by the format's `create_row_level_writer`
/// (e.g., Delta's `build_delete_plan`), so we simply wrap the parameters.
fn expand_delete_node(node: &FileDeleteNode) -> Result<Transformed<LogicalPlan>> {
    let opts = node.options();
    let write_node = RowLevelWriteNode::new_delete(
        Arc::new(LogicalPlan::EmptyRelation(
            datafusion_expr::logical_plan::EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(DFSchema::empty()),
            },
        )),
        Arc::new(DFSchema::empty()),
        opts.condition.clone(),
        opts.format.clone(),
        opts.path.clone(),
        opts.table_name.clone(),
        opts.options.clone(),
    );

    Ok(Transformed::yes(LogicalPlan::Extension(Extension {
        node: Arc::new(write_node),
    })))
}

fn merge_has_delete_actions(node: &MergeIntoNode) -> bool {
    node.options()
        .matched_clauses
        .iter()
        .any(|clause| matches!(clause.action, MergeMatchedAction::Delete))
        || node
            .options()
            .not_matched_by_source_clauses
            .iter()
            .any(|clause| matches!(clause.action, MergeNotMatchedBySourceAction::Delete))
}

fn merge_target_supports_deletion_vectors(plan: &LogicalPlan) -> Result<bool> {
    let mut supports = false;
    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            if let Some(delta_source) = scan.source.as_any().downcast_ref::<DeltaTableSource>() {
                supports = delta_source.snapshot().verify_deletion_vectors().is_ok();
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(supports)
}

/// Attempts to enable MERGE metadata columns on a table source via the [`MergeCapableSource`] trait.
/// Returns `Some((new_source, schema))` if reconfigured, or `None` if unsupported.
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
    // Try Delta Lake source
    if let Some(delta_source) = source.as_any().downcast_ref::<DeltaTableSource>() {
        let mut new_source = Arc::clone(source);
        let mut changed = false;

        if delta_source.file_column_name().is_none() {
            new_source = delta_source.with_file_column(file_col)?;
            changed = true;
        }
        if let (Some(row_index_col), Some(delta_source)) = (
            row_index_col,
            new_source.as_any().downcast_ref::<DeltaTableSource>(),
        ) {
            if delta_source.row_index_column_name().is_none() {
                new_source = delta_source.with_row_index_column(row_index_col)?;
                changed = true;
            }
        }
        if changed {
            let schema = new_source.schema();
            return Ok(Some((new_source, schema)));
        }
        return Ok(None);
    }

    // Future: try Iceberg source when it implements MergeCapableSource
    // if let Some(iceberg_source) = source.as_any().downcast_ref::<IcebergTableSource>() {
    //     ...
    // }

    Ok(None)
}

/// Traverses a logical plan to ensure that merge-capable table scans expose the
/// file path and optional file-local row-index columns, and that parent projections propagate them.
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
            // First, configure table scans to expose the file path column.
            if let LogicalPlan::TableScan(scan) = &plan {
                if let Some((new_source, schema)) =
                    try_enable_merge_metadata_columns(&scan.source, file_col, row_index_col)?
                {
                    trace!(
                        "ensure_merge_metadata_columns (scan) before - table_name: {:?}, projection: {:?}",
                        &scan.table_name,
                        &scan.projection
                    );

                    let mut projection: Option<Vec<usize>> = scan.projection.clone();
                    if projection.is_none() {
                        projection = Some((0..schema.fields().len()).collect::<Vec<usize>>());
                    }
                    if let Some(proj) = projection.as_mut() {
                        for col in &metadata_cols {
                            if let Some(idx) = schema.column_with_name(col).map(|(idx, _)| idx) {
                                if !proj.contains(&idx) {
                                    proj.push(idx);
                                }
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
                    trace!(
                        "ensure_merge_metadata_columns (scan) after - schema_fields: {:?}",
                        new_scan
                            .schema()
                            .fields()
                            .iter()
                            .map(|f| f.name().clone())
                            .collect::<Vec<_>>(),
                    );

                    return Ok(Transformed::yes(new_scan));
                }
            }

            // Then ensure parent projections keep metadata columns if present in input.
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
                    trace!(
                        "ensure_merge_metadata_columns (proj) add - exprs: {:?}, input_schema_fields: {:?}",
                        proj.expr.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
                        input_schema
                            .fields()
                            .iter()
                            .map(|f| f.name().clone())
                            .collect::<Vec<_>>()
                    );
                    let new_proj = LogicalPlanBuilder::from(proj.input.as_ref().clone())
                        .project(new_exprs)?
                        .build()?;
                    trace!(
                        "ensure_merge_metadata_columns (proj) after: {:?}",
                        new_proj
                            .schema()
                            .fields()
                            .iter()
                            .map(|f| f.name().clone())
                            .collect::<Vec<_>>()
                    );
                    return Ok(Transformed::yes(new_proj));
                }
            }

            Ok(Transformed::no(plan))
        })
        .map(|t| t.data)?;

    let mut transformed = transformed;

    // If the root is a SubqueryAlias whose schema was computed before we added metadata,
    // rebuild the alias so its schema picks up the new column.
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

    trace!(
        "ensure_merge_metadata_columns (final) schema: {:?}",
        transformed
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>()
    );

    Ok(transformed)
}

pub fn lakehouse_optimizer_rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    vec![Arc::new(ExpandRowLevelOp::new())]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lakehouse_rules_include_expand_row_level_op() {
        let rules = lakehouse_optimizer_rules();
        assert!(rules
            .iter()
            .any(|rule| rule.name() == "expand_row_level_op"));
    }
}
