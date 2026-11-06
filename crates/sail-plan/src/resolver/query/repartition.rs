use std::sync::Arc;

use datafusion_expr::{Extension, LogicalPlan};
use sail_common::spec;
use sail_logical_plan::repartition::{ExplicitRepartitionKind, ExplicitRepartitionNode};

use crate::error::{PlanError, PlanResult};
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
        self.resolve_repartition_by_expression(input, partition_expressions, num_partitions, state)
            .await
    }

    /// Plans range repartitioning when every expression is a sort order,
    /// and hash repartitioning when none is, as Spark does.
    pub(super) async fn resolve_repartition_by_expression(
        &self,
        input: LogicalPlan,
        partition_expressions: Vec<spec::Expr>,
        num_partitions: Option<usize>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let num_sort_orders = partition_expressions
            .iter()
            .filter(|expr| matches!(expr, spec::Expr::SortOrder(_)))
            .count();
        let is_range = num_sort_orders > 0 && num_sort_orders == partition_expressions.len();
        // Spark plans a single partition before it checks the expression kinds.
        let is_single = num_partitions.unwrap_or(self.config.shuffle_partitions) == 1;
        if !is_range && num_sort_orders > 0 && !is_single {
            return Err(PlanError::invalid(
                "requirement failed: RepartitionByExpression expects that either all its `partitionExpressions` are of type `SortOrder`, which means `RangePartitioning`, or none of them are `SortOrder`, which means `HashPartitioning`",
            ));
        }
        let (partition_expressions, sort_options): (Vec<_>, Vec<_>) = partition_expressions
            .into_iter()
            .map(|expr| match expr {
                spec::Expr::SortOrder(spec::SortOrder {
                    child,
                    direction,
                    null_ordering,
                }) => (*child, Some((direction, null_ordering))),
                expr => (expr, None),
            })
            .unzip();
        let output_schema = Arc::clone(input.schema());
        // Like a filter, Spark recovers partitioning attributes removed by the input.
        let (expr, input) = self
            .resolve_expressions_with_missing_inputs(partition_expressions, input, state)
            .await?;
        let (input, expr) = self.rewrite_partition_expressions(input, expr, state)?;
        let kind = if is_range {
            ExplicitRepartitionKind::Range(
                expr.into_iter()
                    .zip(sort_options.into_iter().flatten())
                    .map(|(expr, (direction, null_ordering))| {
                        Self::sort_with_options(expr, direction, null_ordering)
                    })
                    .collect(),
            )
        } else {
            ExplicitRepartitionKind::Hash(expr)
        };
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(ExplicitRepartitionNode::new(
                Arc::new(input),
                num_partitions,
                kind,
            )),
        });
        Self::restore_missing_input_output(plan, output_schema)
    }
}
