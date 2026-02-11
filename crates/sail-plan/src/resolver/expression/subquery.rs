use std::sync::Arc;

use datafusion_common::DFSchemaRef;
use datafusion_expr::expr_fn;
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_expression_in_subquery(
        &self,
        expr: spec::Expr,
        subquery: spec::QueryPlan,
        negated: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        let subquery = {
            let mut scope = state.enter_query_scope(Arc::clone(schema));
            self.resolve_query_plan(subquery, scope.state()).await?
        };
        let in_subquery = if !negated {
            expr_fn::in_subquery(expr, Arc::new(subquery))
        } else {
            expr_fn::not_in_subquery(expr, Arc::new(subquery))
        };
        Ok(NamedExpr::new(vec!["in_subquery".to_string()], in_subquery))
    }

    pub(super) async fn resolve_expression_scalar_subquery(
        &self,
        subquery: spec::QueryPlan,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let subquery = {
            let mut scope = state.enter_query_scope(Arc::clone(schema));
            self.resolve_query_plan(subquery, scope.state()).await?
        };
        Ok(NamedExpr::new(
            vec!["subquery".to_string()],
            expr_fn::scalar_subquery(Arc::new(subquery)),
        ))
    }

    pub(super) async fn resolve_expression_exists(
        &self,
        subquery: spec::QueryPlan,
        negated: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let subquery = {
            let mut scope = state.enter_query_scope(Arc::clone(schema));
            self.resolve_query_plan(subquery, scope.state()).await?
        };
        let exists = if !negated {
            expr_fn::exists(Arc::new(subquery))
        } else {
            expr_fn::not_exists(Arc::new(subquery))
        };
        Ok(NamedExpr::new(vec!["exists".to_string()], exists))
    }

    /// Resolves a Subquery by looking up the referenced plan from state and delegating to existing subquery resolvers.
    pub(super) async fn resolve_expression_subquery(
        &self,
        plan_id: i64,
        subquery_type: spec::SubqueryType,
        in_subquery_values: Vec<spec::Expr>,
        negated: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let subquery_plan = state.get_subquery_reference(plan_id).ok_or_else(|| {
            PlanError::invalid(format!(
                "subquery plan_id {plan_id} not found in WithRelations references"
            ))
        })?;

        match subquery_type {
            spec::SubqueryType::In => {
                if in_subquery_values.len() > 1 {
                    return Err(PlanError::unsupported(
                        "multi-column IN subquery is not yet supported",
                    ));
                }
                let expr = in_subquery_values
                    .into_iter()
                    .next()
                    .ok_or_else(|| PlanError::invalid("IN subquery missing value expression"))?;
                self.resolve_expression_in_subquery(expr, subquery_plan, negated, schema, state)
                    .await
            }
            spec::SubqueryType::Scalar => {
                self.resolve_expression_scalar_subquery(subquery_plan, schema, state)
                    .await
            }
            spec::SubqueryType::Exists => {
                self.resolve_expression_exists(subquery_plan, negated, schema, state)
                    .await
            }
        }
    }
}
