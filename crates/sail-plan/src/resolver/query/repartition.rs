use std::sync::Arc;

use datafusion_expr::{Extension, LogicalPlan};
use sail_common::spec;
use sail_logical_plan::repartition::{ExplicitRepartitionKind, ExplicitRepartitionNode};

use crate::error::PlanResult;
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_repartition(
        &self,
        input: spec::QueryPlan,
        num_partitions: usize,
        shuffle: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        let kind = if shuffle {
            ExplicitRepartitionKind::RoundRobin
        } else {
            ExplicitRepartitionKind::Coalesce
        };
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(ExplicitRepartitionNode::new(
                Arc::new(input),
                Some(num_partitions),
                kind,
                vec![],
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
                ExplicitRepartitionKind::Hash,
                expr,
            )),
        }))
    }
}
