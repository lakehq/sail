use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::datasource::{provider_as_source, source_as_provider, TableProvider};
use datafusion::logical_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, Result};
use datafusion_expr::logical_plan::Extension;
use datafusion_expr::{Expr, LogicalPlan};
use log::trace;
use sail_delta_lake::datasource::DeltaTableProvider;
use sail_logical_plan::merge::{expand_merge, MergeIntoNode, MergeIntoWriteNode};

const PATH_COLUMN_NAME: &str = "__sail_file_path";

#[derive(Clone, Default)]
pub struct ExpandMerge;

impl ExpandMerge {
    pub fn new() -> Self {
        Self
    }
}

impl OptimizerRule for ExpandMerge {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up(|plan| {
            if let LogicalPlan::Extension(ext) = &plan {
                if let Some(node) = ext.node.as_any().downcast_ref::<MergeIntoNode>() {
                    if !node.options().target.format.eq_ignore_ascii_case("delta") {
                        return Ok(Transformed::no(plan));
                    }

                    // Ensure the target scan exposes the file path column for touched-files plan
                    let mut target_plan = ensure_file_column(node.target().as_ref().clone())?;
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
                    if !target_fields.iter().any(|n| n == PATH_COLUMN_NAME) {
                        let mut exprs: Vec<Expr> = target_fields
                            .iter()
                            .map(|name| Expr::Column(Column::from_name(name.clone())))
                            .collect();
                        exprs.push(
                            Expr::Column(Column::from_name(PATH_COLUMN_NAME))
                                .alias(PATH_COLUMN_NAME),
                        );
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

                    let expansion = expand_merge(&node, PATH_COLUMN_NAME)?;
                    trace!(
                        "ExpandMergeRule write_plan schema fields: {:?}",
                        expansion
                            .write_plan
                            .schema()
                            .fields()
                            .iter()
                            .map(|f| f.name().clone())
                            .collect::<Vec<_>>()
                    );
                    let write_node = MergeIntoWriteNode::new(
                        Arc::clone(node.target()),
                        Arc::clone(node.source()),
                        node.input_schema().clone(),
                        Arc::new(expansion.write_plan),
                        Arc::new(expansion.touched_files_plan),
                        expansion.options,
                        expansion.output_schema,
                    );

                    return Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                        node: Arc::new(write_node),
                    })));
                }
            }
            Ok(Transformed::no(plan))
        })
    }

    fn name(&self) -> &str {
        "expand_merge"
    }
}

impl Debug for ExpandMerge {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

fn ensure_file_column(plan: LogicalPlan) -> Result<LogicalPlan> {
    let transformed = plan
        .transform_up(|plan| {
            // First, make sure Delta table scans expose the path column.
            if let LogicalPlan::TableScan(scan) = &plan {
                if let Ok(provider) = source_as_provider(&scan.source) {
                    if let Some(delta_provider) =
                        provider.as_any().downcast_ref::<DeltaTableProvider>()
                    {
                        trace!(
                            "ensure_file_column (scan) before - table_name: {:?}, schema_fields: {:?}, projection: {:?}",
                            &scan.table_name,
                            provider
                                .schema()
                                .fields()
                                .iter()
                                .map(|f| f.name().clone())
                                .collect::<Vec<_>>(),
                            &scan.projection
                        );
                        if delta_provider.config().file_column_name.is_none() {
                            let mut new_config = delta_provider.config().clone();
                            new_config.file_column_name = Some(PATH_COLUMN_NAME.to_string());

                            let new_provider = Arc::new(DeltaTableProvider::try_new(
                                delta_provider.snapshot().clone(),
                                delta_provider.log_store().clone(),
                                new_config,
                            )?);

                            let schema = new_provider.schema();
                            let file_idx = schema
                                .column_with_name(PATH_COLUMN_NAME)
                                .map(|(idx, _)| idx);

                            let mut projection = scan.projection.clone();
                            if projection.is_none() {
                                projection =
                                    Some((0..schema.fields().len()).collect::<Vec<usize>>());
                            }
                            if let (Some(idx), Some(proj)) = (file_idx, projection.as_mut()) {
                                if !proj.contains(&idx) {
                                    proj.push(idx);
                                }
                            }

                            let new_scan = LogicalPlanBuilder::scan(
                                scan.table_name.clone(),
                                provider_as_source(new_provider as Arc<dyn TableProvider>),
                                projection,
                            )?
                            .build()?;
                            trace!(
                                "ensure_file_column (scan) after - schema_fields: {:?}, scan: {:?}",
                                new_scan
                                    .schema()
                                    .fields()
                                    .iter()
                                    .map(|f| f.name().clone())
                                    .collect::<Vec<_>>(),
                                &new_scan
                            );

                            return Ok(Transformed::yes(new_scan));
                        }
                    }
                }
            }

            // Then ensure parent projections keep the path column if present in input.
            if let LogicalPlan::Projection(proj) = &plan {
                let input_schema = proj.input.schema();
                let has_path_in_input = input_schema
                    .fields()
                    .iter()
                    .any(|f| f.name() == PATH_COLUMN_NAME);
                if has_path_in_input {
                    let has_path = proj.expr.iter().any(|e| match e {
                        Expr::Column(c) => c.name == PATH_COLUMN_NAME,
                        Expr::Alias(a) => a.name == PATH_COLUMN_NAME,
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
                            Expr::Column(Column::from_name(PATH_COLUMN_NAME))
                                .alias(PATH_COLUMN_NAME),
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
            .any(|f| f.name() == PATH_COLUMN_NAME);
        let has_path_in_alias = sa
            .schema
            .fields()
            .iter()
            .any(|f| f.name() == PATH_COLUMN_NAME);
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
    vec![Arc::new(ExpandMerge::new())]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lakehouse_rules_include_expand_merge() {
        let rules = lakehouse_optimizer_rules();
        assert!(rules.iter().any(|rule| rule.name() == "expand_merge"));
    }
}
