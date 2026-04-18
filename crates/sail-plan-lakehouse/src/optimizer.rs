use std::fmt::Debug;
use std::sync::Arc;

use datafusion::logical_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DFSchema, Result};
use datafusion_expr::logical_plan::Extension;
use datafusion_expr::{Expr, LogicalPlan, TableScan, TableSource};
use log::trace;
use sail_common_datafusion::datasource::{
    is_lakehouse_format, MergeCapableSource, MERGE_FILE_COLUMN,
};
use sail_delta_lake::DeltaTableSource;
use sail_logical_plan::file_delete::FileDeleteNode;
use sail_logical_plan::merge::{expand_merge, MergeIntoNode, RowLevelWriteNode};

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
    let mut target_plan = ensure_file_column(node.target().as_ref().clone(), MERGE_FILE_COLUMN)?;
    let target_fields: Vec<String> = target_plan
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    trace!(
        "rewrite target_plan schema after ensure_file_column: {:?}",
        &target_fields
    );
    if !target_fields.iter().any(|n| n == MERGE_FILE_COLUMN) {
        let mut exprs: Vec<Expr> = target_fields
            .iter()
            .map(|name| Expr::Column(Column::from_name(name.clone())))
            .collect();
        exprs.push(Expr::Column(Column::from_name(MERGE_FILE_COLUMN)).alias(MERGE_FILE_COLUMN));
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

    let expansion = expand_merge(&node, MERGE_FILE_COLUMN)?;
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

/// Attempts to enable the file column on a table source via the [`MergeCapableSource`] trait.
/// Returns `Some((new_source, schema))` if reconfigured, or `None` if unsupported.
fn try_enable_file_column(
    source: &Arc<dyn TableSource>,
    file_col: &str,
) -> Result<
    Option<(
        Arc<dyn TableSource>,
        datafusion::arrow::datatypes::SchemaRef,
    )>,
> {
    // Try Delta Lake source
    if let Some(delta_source) = source.as_any().downcast_ref::<DeltaTableSource>() {
        if delta_source.file_column_name().is_none() {
            let new_source = delta_source.with_file_column(file_col)?;
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
/// file path column, and that parent projections propagate it.
fn ensure_file_column(plan: LogicalPlan, file_col: &str) -> Result<LogicalPlan> {
    let transformed = plan
        .transform_up(|plan| {
            // First, configure table scans to expose the file path column.
            if let LogicalPlan::TableScan(scan) = &plan {
                if let Some((new_source, schema)) = try_enable_file_column(&scan.source, file_col)? {
                    trace!(
                        "ensure_file_column (scan) before - table_name: {:?}, projection: {:?}",
                        &scan.table_name,
                        &scan.projection
                    );

                    let file_idx = schema.column_with_name(file_col).map(|(idx, _)| idx);

                    let mut projection: Option<Vec<usize>> = scan.projection.clone();
                    if projection.is_none() {
                        projection = Some((0..schema.fields().len()).collect::<Vec<usize>>());
                    }
                    if let (Some(idx), Some(proj)) = (file_idx, projection.as_mut()) {
                        if !proj.contains(&idx) {
                            proj.push(idx);
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
                        "ensure_file_column (scan) after - schema_fields: {:?}",
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

            // Then ensure parent projections keep the file column if present in input.
            if let LogicalPlan::Projection(proj) = &plan {
                let input_schema = proj.input.schema();
                let has_path_in_input = input_schema
                    .fields()
                    .iter()
                    .any(|f| f.name() == file_col);
                if has_path_in_input {
                    let has_path = proj.expr.iter().any(|e| match e {
                        Expr::Column(c) => c.name == file_col,
                        Expr::Alias(a) => a.name == file_col,
                        _ => false,
                    });

                    if !has_path {
                        trace!(
                            "ensure_file_column (proj) add - exprs: {:?}, input_schema_fields: {:?}",
                            proj.expr.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
                            input_schema
                                .fields()
                                .iter()
                                .map(|f| f.name().clone())
                                .collect::<Vec<_>>()
                        );
                        let mut new_exprs = proj.expr.clone();
                        new_exprs.push(
                            Expr::Column(Column::from_name(file_col)).alias(file_col),
                        );
                        let new_proj = LogicalPlanBuilder::from(proj.input.as_ref().clone())
                            .project(new_exprs)?
                            .build()?;
                        trace!(
                            "ensure_file_column (proj) after: {:?}",
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
            }

            Ok(Transformed::no(plan))
        })
        .map(|t| t.data)?;

    let mut transformed = transformed;

    // If the root is a SubqueryAlias whose schema was computed before we added the path,
    // rebuild the alias so its schema picks up the new column.
    if let LogicalPlan::SubqueryAlias(sa) = &transformed {
        let has_path_in_child = sa
            .input
            .schema()
            .fields()
            .iter()
            .any(|f| f.name() == file_col);
        let has_path_in_alias = sa.schema.fields().iter().any(|f| f.name() == file_col);
        if has_path_in_child && !has_path_in_alias {
            transformed =
                LogicalPlan::SubqueryAlias(datafusion_expr::logical_plan::SubqueryAlias::try_new(
                    sa.input.clone(),
                    sa.alias.clone(),
                )?);
        }
    }

    trace!(
        "ensure_file_column (final) schema: {:?}",
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
