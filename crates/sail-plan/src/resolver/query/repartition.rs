use std::sync::Arc;

use datafusion_expr::{LogicalPlan, Partitioning, Repartition};
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
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
        // TODO: handle shuffle partition
        Ok(LogicalPlan::Repartition(Repartition {
            input: Arc::new(input),
            partitioning_scheme: Partitioning::RoundRobinBatch(num_partitions),
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
        let num_partitions = num_partitions
            .ok_or_else(|| PlanError::todo("rebalance partitioning by expression"))?;
        Ok(LogicalPlan::Repartition(Repartition {
            input: Arc::new(input),
            partitioning_scheme: Partitioning::Hash(expr, num_partitions),
        }))
    }
}
