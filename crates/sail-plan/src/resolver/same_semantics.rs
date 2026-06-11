use datafusion_common::display::{PlanType, ToStringifiedPlan};
use datafusion_common::DataFusionError;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_expr::expr::Alias;
use datafusion_expr::{Expr, LogicalPlan};
use sail_common::spec;

use crate::error::PlanResult;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {

    fn transform_expression(expr: Expr) -> Result<Transformed<Expr>, DataFusionError> {
       expr.transform_up(|e| match e {
            Expr::Alias(Alias {
                expr,
                relation,
                name: _,
                metadata
            }) => Ok(Transformed::yes( Expr::Alias( Alias {
                expr,
                relation,
                name: String::new(),
                metadata
            }))),
            other =>  Ok(Transformed::no(other))
       })
    }

    fn canonicalize_plan(plan: LogicalPlan)-> Result<LogicalPlan, DataFusionError> {
        plan.transform_up(|node| {
            node.map_expressions(|expr| Self::transform_expression(expr))
        })
        .data()
    }

    async fn resolve_canonical_plan_string(&self, plan: spec::QueryPlan) -> PlanResult<String> {
        let mut state = PlanResolverState::new();
        let initial_logical = self.resolve_query_plan(plan, &mut state).await?;

        let session_state = self.ctx.state();
        let config_options = session_state.config_options();

        let analyzed_logical = session_state.analyzer().execute_and_check(
            initial_logical,
            config_options.as_ref(),
            |_, _| {}
        )?;

        let canonical = Self::canonicalize_plan(analyzed_logical)?;

        Ok(canonical.to_stringified(PlanType::FinalAnalyzedLogicalPlan).plan.to_string())
    }

    pub async fn resolve_same_semantics(&self, target_plan: spec::QueryPlan, other_plan: spec::QueryPlan) -> PlanResult<bool> {
        let target_plan_str = self.resolve_canonical_plan_string(target_plan).await?;
        let other_plan_str = self.resolve_canonical_plan_string(other_plan).await?;
        Ok(target_plan_str == other_plan_str)
        // Ok(true)
    }
}
