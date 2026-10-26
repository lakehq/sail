use std::sync::Arc;

use datafusion::functions_window::expr_fn::percent_rank;
use datafusion_common::Column;
use datafusion_expr::{Expr, ExprFunctionExt, Extension, LogicalPlan, LogicalPlanBuilder};
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
        let (mut input, mut expr) = self.rewrite_partition_expressions(input, expr, state)?;
        let kind = if range {
            let sorts = expr
                .into_iter()
                .zip(sort_options)
                .map(|(expr, options)| {
                    let (direction, null_ordering) =
                        options.ok_or_else(|| PlanError::internal("missing range sort order"))?;
                    Ok(Self::sort_with_options(expr, direction, null_ordering))
                })
                .collect::<PlanResult<Vec<_>>>()?;
            // Exact percentiles give data-dependent range boundaries and keep equal
            // keys together. The physical shuffle partitions this temporary column.
            let name = state.register_field_name("__range_percentile");
            let rank = percent_rank().order_by(sorts).build()?.alias(&name);
            input = LogicalPlanBuilder::from(input)
                .window(vec![rank])?
                .build()?;
            expr = vec![Expr::Column(Column::from_name(name))];
            ExplicitRepartitionKind::Range
        } else {
            ExplicitRepartitionKind::Hash
        };
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(ExplicitRepartitionNode::new(
                Arc::new(input),
                num_partitions,
                kind,
                expr,
            )),
        });
        Self::restore_missing_input_output(plan, output_schema)
    }
}
