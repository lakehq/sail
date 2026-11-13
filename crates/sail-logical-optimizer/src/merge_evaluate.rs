use datafusion::optimizer::scalar_subquery_to_join::ScalarSubqueryToJoin;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::Transformed;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{LogicalPlan, Projection};
use sail_logical_plan::merge_evaluate::{EvaluateOnceNode, evaluate_once};

/// Reuse DataFusion's decorrelation without exposing a projection to other rules.
#[derive(Debug)]
pub(crate) struct EvaluateOnceScalarSubquery;

impl OptimizerRule for EvaluateOnceScalarSubquery {
    fn name(&self) -> &str {
        "scalar_subquery_to_join"
    }
    fn supports_rewrite(&self) -> bool {
        true
    }
    fn apply_order(&self) -> Option<ApplyOrder> {
        ScalarSubqueryToJoin::new().apply_order()
    }
    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Extension(extension) = &plan else {
            return ScalarSubqueryToJoin::new().rewrite(plan, config);
        };
        let Some(node) = extension.node.as_any().downcast_ref::<EvaluateOnceNode>() else {
            return ScalarSubqueryToJoin::new().rewrite(plan, config);
        };
        let projection = LogicalPlan::Projection(Projection::try_new(
            node.expr().to_vec(),
            node.input().clone(),
        )?);
        let rewritten = ScalarSubqueryToJoin::new().rewrite(projection, config)?;
        if !rewritten.transformed {
            return Ok(Transformed::no(plan));
        }
        let LogicalPlan::Projection(projection) = rewritten.data else {
            return internal_err!(
                "scalar subquery lowering must retain the EvaluateOnce projection"
            );
        };
        Ok(Transformed::yes(evaluate_once(
            projection.input.as_ref().clone(),
            projection.expr,
        )?))
    }
}
