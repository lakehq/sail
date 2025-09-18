use std::sync::Arc;

use datafusion_expr::{col, Extension, LogicalPlan};
use sail_common::spec;
use sail_logical_plan::repartition::ExplicitRepartitionNode;

use crate::error::PlanResult;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_repartition(
        &self,
        input: spec::QueryPlan,
        num_partitions: usize,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        let expr = input.schema().columns().into_iter().map(col).collect();
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(ExplicitRepartitionNode::new(
                Arc::new(input),
                Some(num_partitions),
                expr,
            )),
        }))
    }

    pub(super) async fn resolve_query_repartition_by_expression(
        &self,
        input: spec::QueryPlan,
        partition_expressions: Vec<spec::Expr>,
        num_partitions: Option<usize>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        let schema = input.schema();
        let expr = self
            .resolve_expressions(partition_expressions, schema, state)
            .await?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(ExplicitRepartitionNode::new(
                Arc::new(input),
                num_partitions,
                expr,
            )),
        }))
    }
}
