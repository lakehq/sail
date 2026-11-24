// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, IntervalUnit};
use datafusion_expr::{ExprSchemable, Extension, LogicalPlan, SortExpr};
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;
use sail_logical_plan::repartition::{ExplicitRepartitionKind, ExplicitRepartitionNode};

use crate::error::{PlanError, PlanResult};
use crate::function::common::normalize_range_floats;
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::seeded_random::SeededRandomScope;

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
        // Without a count, Spark repartitions into `spark.sql.shuffle.partitions`, as the
        // partitioning hints do.
        let num_partitions = num_partitions.unwrap_or(self.config.shuffle_partitions);
        // Spark plans a single partition before it checks the expression kinds.
        let is_single = num_partitions == 1;
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
        let (input, expr) =
            self.rewrite_seeded_random_expressions(input, expr, SeededRandomScope::Call, state)?;
        let kind = if is_range {
            let mut sorts: Vec<SortExpr> = vec![];
            for (expr, (direction, null_ordering)) in
                expr.into_iter().zip(sort_options.into_iter().flatten())
            {
                // A repeated key cannot break a tie, so only its first order counts.
                if sorts.iter().any(|sort| sort.expr == expr) {
                    continue;
                }
                sorts.push(Self::sort_with_options(expr, direction, null_ordering));
            }
            let sorts = sorts
                .into_iter()
                .map(|sort| {
                    let data_type = sort.expr.get_type(input.schema())?;
                    // Spark rejects a key it cannot order before it reads any row.
                    if !is_orderable(&data_type) {
                        let service = self.ctx.extension::<PlanService>()?;
                        let data_type = service
                            .plan_formatter()
                            .data_type_to_simple_string(&data_type)?;
                        return Err(PlanError::AnalysisError(format!(
                            "[DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] The `sortorder` does not support ordering on type \"{data_type}\"."
                        )));
                    }
                    // Spark's ordering compares every NaN as one value, and -0.0 as 0.0.
                    let expr = normalize_range_floats(sort.expr.clone(), &data_type);
                    Ok(sort.with_expr(expr))
                })
                .collect::<PlanResult<Vec<_>>>()?;
            ExplicitRepartitionKind::Range {
                sorts,
                sample_size_per_partition: self.config.range_exchange_sample_size_per_partition,
            }
        } else {
            ExplicitRepartitionKind::Hash(expr)
        };
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(ExplicitRepartitionNode::new(
                Arc::new(input),
                Some(num_partitions),
                kind,
            )),
        });
        Self::restore_missing_input_output(plan, output_schema)
    }
}

/// Whether Spark can order values of the type, as in `RowOrdering.isOrderable`.
fn is_orderable(data_type: &DataType) -> bool {
    match data_type {
        DataType::Map(_, _) | DataType::Interval(IntervalUnit::MonthDayNano) => false,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => is_orderable(field.data_type()),
        DataType::Struct(fields) => fields.iter().all(|field| is_orderable(field.data_type())),
        DataType::Dictionary(_, value) => is_orderable(value),
        _ => true,
    }
}
