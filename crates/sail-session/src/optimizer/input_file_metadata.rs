use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, Result, plan_err};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{Expr, LogicalPlan, Projection, Window};
use datafusion::optimizer::AnalyzerRule;
use sail_common_datafusion::input_file::is_input_file_metadata_function;
use sail_data_source::listing::table::ListingTableSource;
use sail_delta_lake::logical::table_source::DeltaTableSource;
use sail_iceberg::logical::table_source::IcebergTableSource;

/// Validates file provenance and materializes window metadata before window shuffles.
#[derive(Debug)]
pub(super) struct ResolveInputFileMetadata;

impl AnalyzerRule for ResolveInputFileMetadata {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let mut names = HashSet::new();
        plan.apply_with_subqueries(|node| {
            names.extend(node.schema().fields().iter().map(|field| field.name().clone()));
            for expression in node.expressions() {
                expression.apply(|expression| {
                    if let Expr::ScalarFunction(function) = expression
                        && is_input_file_metadata_function(&function.func)
                        && file_source_count(node) > 1
                    {
                        return plan_err!(
                            "[MULTI_SOURCES_UNSUPPORTED_FOR_EXPRESSION] The expression {}() does not support more than one source",
                            function.func.name()
                        );
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;

        let mut next_alias = 0;
        plan.transform_up_with_subqueries(|plan| {
            let LogicalPlan::Window(window) = &plan else {
                return Ok(Transformed::no(plan));
            };
            let mut metadata: Vec<(Expr, String)> = Vec::new();
            let mut expressions = Vec::with_capacity(window.window_expr.len());
            for expression in &window.window_expr {
                let original_name = expression.schema_name().to_string();
                let rewritten = expression.clone().transform_up(|expression| {
                    let Expr::ScalarFunction(function) = &expression else {
                        return Ok(Transformed::no(expression));
                    };
                    if !is_input_file_metadata_function(&function.func) {
                        return Ok(Transformed::no(expression));
                    }
                    let existing = metadata
                        .iter()
                        .find(|(candidate, _)| candidate == &expression);
                    let alias = if let Some((_, alias)) = existing {
                        alias.clone()
                    } else {
                        let alias = loop {
                            let alias = format!("__sail_input_file_metadata_{next_alias}");
                            next_alias += 1;
                            if names.insert(alias.clone()) {
                                break alias;
                            }
                        };
                        metadata.push((expression, alias.clone()));
                        alias
                    };
                    Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                        alias,
                    ))))
                })?;
                expressions.push(rewritten.data.alias_if_changed(original_name)?);
            }
            if metadata.is_empty() {
                return Ok(Transformed::no(plan));
            }

            let projection = window
                .input
                .schema()
                .columns()
                .into_iter()
                .map(Expr::Column)
                .chain(
                    metadata
                        .into_iter()
                        .map(|(expression, alias)| expression.alias(alias)),
                )
                .collect();
            let input = Arc::new(LogicalPlan::Projection(Projection::try_new(
                projection,
                Arc::clone(&window.input),
            )?));
            let rewritten = LogicalPlan::Window(Window::try_new(expressions, input)?);
            let recovery = Projection::try_new_with_schema(
                window
                    .schema
                    .columns()
                    .into_iter()
                    .map(Expr::Column)
                    .collect(),
                Arc::new(rewritten),
                Arc::clone(&window.schema),
            )?;
            Ok(Transformed::yes(LogicalPlan::Projection(recovery)))
        })
        .map(|result| result.data)
    }

    fn name(&self) -> &str {
        "resolve_input_file_metadata"
    }
}

fn file_source_count(plan: &LogicalPlan) -> usize {
    match plan {
        LogicalPlan::TableScan(scan) => usize::from(
            scan.source.is::<ListingTableSource>()
                || scan.source.is::<DeltaTableSource>()
                || scan.source.is::<IcebergTableSource>(),
        ),
        // Union inputs are consumed independently and can each establish a file context.
        LogicalPlan::Union(union) => union
            .inputs
            .iter()
            .map(|input| file_source_count(input))
            .max()
            .unwrap_or(0),
        _ => plan.inputs().into_iter().map(file_source_count).sum(),
    }
}
