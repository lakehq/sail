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
        self.resolve_repartition_by_expression(input, partition_expressions, num_partitions, state)
            .await
    }

    pub(super) async fn resolve_repartition_by_expression(
        &self,
        input: LogicalPlan,
        partition_expressions: Vec<spec::Expr>,
        num_partitions: Option<usize>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let output_schema = Arc::clone(input.schema());
        let sort_options = partition_expressions
            .iter()
            .map(|expr| match expr {
                spec::Expr::SortOrder(sort) => {
                    Some((sort.direction.clone(), sort.null_ordering.clone()))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let range = sort_options.iter().any(Option::is_some);
        let num_partitions = if range {
            Some(num_partitions.unwrap_or(self.config.shuffle_partitions))
        } else {
            num_partitions
        };
        if range && sort_options.iter().any(Option::is_none) {
            return Err(PlanError::analysis(
                "range partitioning requires sort orders for all partition expressions",
            ));
        }
        // Like a filter, Spark recovers partitioning attributes removed by the input.
        let (expr, input) = self
            .resolve_expressions_with_missing_inputs(partition_expressions, input, state)
            .await?;
        let (input, expr) = self.rewrite_partition_expressions(input, expr, state)?;
        let ordering = if range {
            expr.iter()
                .cloned()
                .zip(sort_options)
                .map(|(expr, options)| {
                    let (direction, null_ordering) =
                        options.ok_or_else(|| PlanError::internal("missing range sort order"))?;
                    let sort = Self::sort_with_options(expr, direction, null_ordering);
                    Ok((!sort.asc, sort.nulls_first))
                })
                .collect::<PlanResult<Vec<_>>>()?
        } else {
            vec![]
        };
        let kind = if range {
            ExplicitRepartitionKind::Range
        } else {
            ExplicitRepartitionKind::Hash
        };
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(
                ExplicitRepartitionNode::new(Arc::new(input), num_partitions, kind, expr)
                    .with_range_ordering(ordering),
            ),
        });
        Self::restore_missing_input_output(plan, output_schema)
    }
}
