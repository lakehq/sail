use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_expr_adapter::rewrite::rewrite_input_file_name_in_projection;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;

/// Replaces `input_file_name()` expressions that were not pushed into a file scan.
#[derive(Debug, Default)]
pub struct RewriteInputFileNameFallback;

impl RewriteInputFileNameFallback {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for RewriteInputFileNameFallback {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            let Some(projection) = plan.downcast_ref::<ProjectionExec>() else {
                return Ok(Transformed::no(plan));
            };
            let expressions =
                rewrite_input_file_name_in_projection(projection.projection_expr().clone(), "")?;
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
        "RewriteInputFileNameFallback"
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
    use datafusion::logical_expr::lit;
    use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
    use datafusion::physical_expr::ScalarFunctionExpr;
    use datafusion::physical_expr::execution_props::ExecutionProps;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion::physical_expr_adapter::rewrite::expr_references_scalar_udf;
    use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
    use datafusion::physical_plan::projection::ProjectionExpr;
    use datafusion_physical_expr::create_physical_exprs;

    use super::*;

    #[test]
    fn rewrites_unresolved_input_file_name_to_empty_string() {
        let schema = Arc::new(Schema::empty());
        let df_schema = DFSchema::try_from(Arc::clone(&schema)).unwrap();
        let expression = expr_fn::coalesce(vec![expr_fn::input_file_name(), lit("")]);
        let [expression] = create_physical_exprs(
            [&expression],
            &df_schema,
            &ExecutionProps::default(),
            &PhysicalPlanningContext::default(),
        )
        .unwrap()
        .try_into()
        .unwrap();
        let input: Arc<dyn ExecutionPlan> = Arc::new(PlaceholderRowExec::new(schema));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            ProjectionExec::try_new([ProjectionExpr::new(expression, "file_name")], input).unwrap(),
        );
        let original_schema = plan.schema();

        let optimized = RewriteInputFileNameFallback::new()
            .optimize(plan, &ConfigOptions::default())
            .unwrap();

        assert_eq!(optimized.schema(), original_schema);
        assert!(!optimized.schema().field(0).is_nullable());
        let projection = optimized.downcast_ref::<ProjectionExec>().unwrap();
        let expression = &projection.expr()[0].expr;
        assert!(!expr_references_scalar_udf::<InputFileNameFunc>(expression));
        let coalesce = expression.downcast_ref::<ScalarFunctionExpr>().unwrap();
        assert!(coalesce.fun().inner().is::<CoalesceFunc>());
        let fallback = coalesce.args()[0].downcast_ref::<Literal>().unwrap();
        assert_eq!(fallback.value(), &ScalarValue::Utf8(Some(String::new())));
    }
}
