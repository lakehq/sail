use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Constraints, Result, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::{FileScanConfig, FileScanConfigBuilder, FileSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use sail_common_datafusion::input_file::{
    InputFileMetadata, InputFileMetadataSource, expression_references_input_file_metadata,
    rewrite_input_file_metadata_projection,
};

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
        self.projection_pushdown.optimize(plan, config)
    }

    fn name(&self) -> &str {
        "PushDownInputFileMetadata"
    }

    fn schema_check(&self) -> bool {
        true
    }
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
