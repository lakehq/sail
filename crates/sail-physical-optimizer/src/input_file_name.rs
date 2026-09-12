use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Constraints, Result, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::{FileScanConfig, FileScanConfigBuilder, FileSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr::expressions::{Column, LambdaVariable};
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::filter::{FilterExec, FilterExecBuilder};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, PhysicalExpr};
use sail_common_datafusion::input_file::{
    InputFileMetadata, InputFileMetadataSource, expression_references_input_file_metadata,
    is_input_file_metadata_function, projection_references_input_file_metadata,
    rewrite_input_file_metadata_projection,
};
use sail_physical_plan::repartition::ExplicitRepartitionExec;

use crate::projection_pushdown::LambdaSafeProjectionPushdown;

/// Installs a per-file projection boundary before DataFusion pushes scan-metadata functions down.
#[derive(Debug, Default)]
pub struct WrapInputFileMetadataSource;

impl WrapInputFileMetadataSource {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for WrapInputFileMetadataSource {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !plan_references_input_file_metadata(&plan)? {
            return Ok(plan);
        }

        plan.transform_up(|plan| {
            let Some(scan) = plan.downcast_ref::<DataSourceExec>() else {
                return Ok(Transformed::no(plan));
            };
            let Some(config) = scan.data_source().downcast_ref::<FileScanConfig>() else {
                return Ok(Transformed::no(plan));
            };
            if config.file_source.is::<InputFileMetadataSource>() {
                return Ok(Transformed::no(plan));
            }

            let source = Arc::new(InputFileMetadataSource::try_new(Arc::clone(
                &config.file_source,
            ))?) as Arc<dyn FileSource>;
            let statistics = Statistics::new_unknown(source.table_schema().table_schema());
            let mut config = FileScanConfigBuilder::from(config.clone())
                .with_source(source)
                .with_statistics(statistics)
                .with_output_ordering(Vec::new())
                .with_output_partitioning(None)
                .build();
            config.constraints = Constraints::default();
            let scan = scan.clone().with_data_source(Arc::new(config));
            Ok(Transformed::yes(Arc::new(scan) as Arc<dyn ExecutionPlan>))
        })
        .map(|result| result.data)
    }

    fn name(&self) -> &str {
        "WrapInputFileMetadataSource"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Pushes scan-metadata projections through operators before repartitioning is enforced.
#[derive(Debug, Default)]
pub struct PushDownInputFileMetadata {
    projection_pushdown: LambdaSafeProjectionPushdown,
}

impl PushDownInputFileMetadata {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalOptimizerRule for PushDownInputFileMetadata {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !plan_references_input_file_metadata(&plan)? {
            return Ok(plan);
        }
        let mut plan = plan.transform_up(extract_metadata_projection)?.data;
        loop {
            plan = self.projection_pushdown.optimize(plan, config)?;
            let pushed = plan.transform_down(push_metadata_through_file_context)?;
            plan = pushed.data;
            if !pushed.transformed {
                return Ok(plan);
            }
        }
    }

    fn name(&self) -> &str {
        "PushDownInputFileMetadata"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn is_metadata_expression(expression: &Arc<dyn PhysicalExpr>) -> bool {
    expression
        .downcast_ref::<ScalarFunctionExpr>()
        .is_some_and(|function| is_input_file_metadata_function(function.fun()))
}

/// Keep source-dependent expressions below projections that contain lambda variables.
fn extract_metadata_projection(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(projection) = plan.downcast_ref::<ProjectionExec>() else {
        return Ok(Transformed::no(plan));
    };
    if !projection_references_input_file_metadata(projection.projection_expr())
        || projection.expr().iter().all(|expression| {
            expression.expr.is::<Column>() || is_metadata_expression(&expression.expr)
        })
    {
        return Ok(Transformed::no(plan));
    }
    let (expressions, extracted) = extract_metadata_expressions(projection)?;
    let schema = projection.input().schema();
    let identity =
        ProjectionExprs::from_indices(&(0..schema.fields().len()).collect::<Vec<_>>(), &schema);
    let input = Arc::new(ProjectionExec::try_new(
        identity.iter().cloned().chain(extracted),
        Arc::clone(projection.input()),
    )?) as Arc<dyn ExecutionPlan>;
    let rewritten = ProjectionExec::try_new_with_schema_metadata(
        expressions.iter().cloned(),
        input,
        projection.schema().as_ref(),
    )?;
    Ok(Transformed::yes(Arc::new(rewritten)))
}

fn extract_metadata_expressions(
    projection: &ProjectionExec,
) -> Result<(ProjectionExprs, Vec<ProjectionExpr>)> {
    let schema = projection.input().schema();
    let mut extracted: Vec<ProjectionExpr> = Vec::new();
    let expressions = projection
        .projection_expr()
        .clone()
        .try_map_exprs(|expression| {
            expression
                .transform_up(|expression| {
                    if !is_metadata_expression(&expression) {
                        return Ok(Transformed::no(expression));
                    }
                    let index = match extracted
                        .iter()
                        .position(|candidate| candidate.expr.eq(&expression))
                    {
                        Some(index) => index,
                        None => {
                            let mut suffix = extracted.len();
                            let alias = loop {
                                let alias = format!("__sail_input_file_metadata_{suffix}");
                                if schema.field_with_name(&alias).is_err()
                                    && !extracted.iter().any(|expression| expression.alias == alias)
                                {
                                    break alias;
                                }
                                suffix += 1;
                            };
                            extracted.push(ProjectionExpr::new(expression, alias));
                            extracted.len() - 1
                        }
                    };
                    Ok(Transformed::yes(Arc::new(Column::new(
                        &extracted[index].alias,
                        schema.fields().len() + index,
                    ))
                        as Arc<dyn PhysicalExpr>))
                })
                .map(|result| result.data)
        })?;
    // Lambda parameters follow the input columns in the physical evaluation schema.
    let expressions = expressions.try_map_exprs(|expression| {
        expression
            .transform_up(|expression| {
                if let Some(variable) = expression.downcast_ref::<LambdaVariable>() {
                    Ok(Transformed::yes(Arc::new(LambdaVariable::new(
                        variable.index() + extracted.len(),
                        Arc::clone(variable.field()),
                    ))
                        as Arc<dyn PhysicalExpr>))
                } else {
                    Ok(Transformed::no(expression))
                }
            })
            .map(|result| result.data)
    })?;
    Ok((expressions, extracted))
}

fn push_metadata_through_file_context(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(projection) = plan.downcast_ref::<ProjectionExec>() else {
        return Ok(Transformed::no(plan));
    };
    if !projection_references_input_file_metadata(projection.projection_expr()) {
        return Ok(Transformed::no(plan));
    }
    if let Some(filter) = projection.input().downcast_ref::<FilterExec>() {
        let (expressions, metadata) = extract_metadata_expressions(projection)?;
        let schema = filter.input().schema();
        let column_count = schema.fields().len();
        let identity =
            ProjectionExprs::from_indices(&(0..column_count).collect::<Vec<_>>(), &schema);
        let indices = filter
            .projection()
            .as_ref()
            .map_or_else(
                || (0..column_count).collect::<Vec<_>>(),
                |projection| projection.to_vec(),
            )
            .into_iter()
            .chain(column_count..column_count + metadata.len())
            .collect();
        let input = Arc::new(ProjectionExec::try_new(
            identity.iter().cloned().chain(metadata),
            Arc::clone(filter.input()),
        )?) as Arc<dyn ExecutionPlan>;
        let filter = FilterExecBuilder::from(filter)
            .with_input(input)
            .apply_projection(None)?
            .apply_projection(Some(indices))?
            .build()?;
        let rewritten = ProjectionExec::try_new_with_schema_metadata(
            expressions.iter().cloned(),
            Arc::new(filter),
            projection.schema().as_ref(),
        )?;
        return Ok(Transformed::yes(Arc::new(rewritten)));
    }
    let project = |input: &Arc<dyn ExecutionPlan>| -> Result<Arc<dyn ExecutionPlan>> {
        let schema = input.schema();
        let expressions = projection
            .projection_expr()
            .clone()
            .try_map_exprs(|expression| {
                expression
                    .transform_up(|expression| {
                        if let Some(column) = expression.downcast_ref::<Column>() {
                            Ok(Transformed::yes(Arc::new(Column::new(
                                schema.field(column.index()).name(),
                                column.index(),
                            ))
                                as Arc<dyn PhysicalExpr>))
                        } else {
                            Ok(Transformed::no(expression))
                        }
                    })
                    .map(|result| result.data)
            })?;
        Ok(Arc::new(ProjectionExec::try_new_with_schema_metadata(
            expressions.iter().cloned(),
            Arc::clone(input),
            projection.schema().as_ref(),
        )?))
    };
    if let Some(union) = projection.input().downcast_ref::<UnionExec>() {
        let inputs = union
            .inputs()
            .iter()
            .map(project)
            .collect::<Result<Vec<_>>>()?;
        return Ok(Transformed::yes(UnionExec::try_new(inputs)?));
    }
    if let Some(repartition) = projection.input().downcast_ref::<ExplicitRepartitionExec>()
        && matches!(
            repartition.properties().output_partitioning(),
            Partitioning::UnknownPartitioning(_)
        )
    {
        return Ok(Transformed::yes(Arc::new(ExplicitRepartitionExec::new(
            project(repartition.input())?,
            repartition.properties().output_partitioning().clone(),
        ))));
    }
    Ok(Transformed::no(plan))
}

fn plan_references_input_file_metadata(plan: &Arc<dyn ExecutionPlan>) -> Result<bool> {
    let mut found = false;
    plan.apply(|node| {
        node.apply_expressions(&mut |expression| {
            if expression_references_input_file_metadata(expression) {
                found = true;
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
}

/// Replaces scan-metadata functions that did not reach a file source with Spark defaults.
#[derive(Debug, Default)]
pub struct RewriteInputFileMetadataFallback;

impl RewriteInputFileMetadataFallback {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for RewriteInputFileMetadataFallback {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            let Some(projection) = plan.downcast_ref::<ProjectionExec>() else {
                return Ok(Transformed::no(plan));
            };
            let expressions = rewrite_input_file_metadata_projection(
                projection.projection_expr().clone(),
                &InputFileMetadata::fallback(),
            )?;
            if expressions == *projection.projection_expr() {
                return Ok(Transformed::no(plan));
            }
            let projection = ProjectionExec::try_new_with_schema_metadata(
                expressions.as_ref().iter().cloned(),
                Arc::clone(projection.input()),
                projection.schema().as_ref(),
            )?;
            Ok(Transformed::yes(
                Arc::new(projection) as Arc<dyn ExecutionPlan>
            ))
        })
        .map(|result| result.data)
    }

    fn name(&self) -> &str {
        "RewriteInputFileMetadataFallback"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::datatypes::Schema;
    use datafusion::common::{DFSchema, ScalarValue};
    use datafusion::functions::core::coalesce::CoalesceFunc;
    use datafusion::functions::core::input_file_name::InputFileNameFunc;
    use datafusion::functions::expr_fn;
    use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
    use datafusion::logical_expr::{ScalarUDF, lit};
    use datafusion::physical_expr::ScalarFunctionExpr;
    use datafusion::physical_expr::execution_props::ExecutionProps;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion::physical_expr_adapter::rewrite::expr_references_scalar_udf;
    use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
    use datafusion::physical_plan::projection::ProjectionExpr;
    use datafusion_physical_expr::create_physical_exprs;
    use sail_common_datafusion::input_file::{InputFileBlockLengthFunc, InputFileBlockStartFunc};

    use super::*;

    #[test]
    fn rewrites_unresolved_input_file_metadata_to_spark_defaults() {
        let schema = Arc::new(Schema::empty());
        let df_schema = DFSchema::try_from(Arc::clone(&schema)).unwrap();
        let block_start = datafusion::logical_expr::Expr::ScalarFunction(
            datafusion::logical_expr::expr::ScalarFunction::new_udf(
                Arc::new(ScalarUDF::from(InputFileBlockStartFunc::new())),
                vec![],
            ),
        );
        let block_length = datafusion::logical_expr::Expr::ScalarFunction(
            datafusion::logical_expr::expr::ScalarFunction::new_udf(
                Arc::new(ScalarUDF::from(InputFileBlockLengthFunc::new())),
                vec![],
            ),
        );
        let expressions = [
            expr_fn::coalesce(vec![expr_fn::input_file_name(), lit("")]),
            block_start,
            block_length,
        ];
        let expressions = create_physical_exprs(
            &expressions,
            &df_schema,
            &ExecutionProps::default(),
            &PhysicalPlanningContext::default(),
        )
        .unwrap();
        let input: Arc<dyn ExecutionPlan> = Arc::new(PlaceholderRowExec::new(schema));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            ProjectionExec::try_new(
                expressions
                    .into_iter()
                    .enumerate()
                    .map(|(index, expression)| {
                        ProjectionExpr::new(expression, format!("c{index}"))
                    }),
                input,
            )
            .unwrap(),
        );
        let original_schema = plan.schema();

        let optimized = RewriteInputFileMetadataFallback::new()
            .optimize(plan, &ConfigOptions::default())
            .unwrap();

        assert_eq!(optimized.schema(), original_schema);
        let projection = optimized.downcast_ref::<ProjectionExec>().unwrap();
        let file_name = &projection.expr()[0].expr;
        assert!(!expr_references_scalar_udf::<InputFileNameFunc>(file_name));
        let coalesce = file_name.downcast_ref::<ScalarFunctionExpr>().unwrap();
        assert!(coalesce.fun().inner().is::<CoalesceFunc>());
        let fallback = coalesce.args()[0].downcast_ref::<Literal>().unwrap();
        assert_eq!(fallback.value(), &ScalarValue::Utf8(Some(String::new())));
        for expression in [&projection.expr()[1].expr, &projection.expr()[2].expr] {
            let literal = expression.downcast_ref::<Literal>().unwrap();
            assert_eq!(literal.value(), &ScalarValue::Int64(Some(-1)));
        }
    }
}
