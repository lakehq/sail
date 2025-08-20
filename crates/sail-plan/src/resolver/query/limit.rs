use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::Int64Type;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion_common::utils::expr::COUNT_STAR_EXPANSION;
use datafusion_common::ScalarValue;
use datafusion_expr::expr::AggregateFunctionParams;
use datafusion_expr::{expr, lit, Aggregate, Expr, Limit, LogicalPlan};
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_limit(
        &self,
        input: spec::QueryPlan,
        skip: Option<spec::Expr>,
        limit: Option<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        let skip = if let Some(skip) = skip {
            Some(self.resolve_expression(skip, input.schema(), state).await?)
        } else {
            None
        };
        let limit = if let Some(limit) = limit {
            Some(
                self.resolve_expression(limit, input.schema(), state)
                    .await?,
            )
        } else {
            None
        };
        Ok(LogicalPlan::Limit(Limit {
            skip: skip.map(Box::new),
            fetch: limit.map(Box::new),
            input: Arc::new(input),
        }))
    }

    pub(super) async fn resolve_query_tail(
        &self,
        input: spec::QueryPlan,
        limit: spec::Expr,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        let limit = self
            .resolve_expression(limit, input.schema(), state)
            .await?;
        let limit_num = match &limit {
            Expr::Literal(ScalarValue::Int8(Some(value)), _metadata) => Ok(*value as i64),
            Expr::Literal(ScalarValue::Int16(Some(value)), _metadata) => Ok(*value as i64),
            Expr::Literal(ScalarValue::Int32(Some(value)), _metadata) => Ok(*value as i64),
            Expr::Literal(ScalarValue::Int64(Some(value)), _metadata) => Ok(*value),
            Expr::Literal(ScalarValue::UInt8(Some(value)), _metadata) => Ok(*value as i64),
            Expr::Literal(ScalarValue::UInt16(Some(value)), _metadata) => Ok(*value as i64),
            Expr::Literal(ScalarValue::UInt32(Some(value)), _metadata) => Ok(*value as i64),
            Expr::Literal(ScalarValue::UInt64(Some(value)), _metadata) => Ok(*value as i64),
            _ => Err(PlanError::invalid("`tail` limit must be an integer")),
        }?;
        // TODO: This can be expensive for large input datasets
        //  According to Spark's docs:
        //    Running tail requires moving data into the application's driver process, and doing so
        //    with a very large `num` can crash the driver process with OutOfMemoryError.
        let count_alias = state.register_field_name("COUNT(*)");
        let count_expr = Expr::AggregateFunction(expr::AggregateFunction {
            func: count_udaf(),
            params: AggregateFunctionParams {
                args: vec![Expr::Literal(COUNT_STAR_EXPANSION, None)],
                distinct: false,
                filter: None,
                order_by: vec![],
                null_treatment: None,
            },
        })
        .alias(count_alias);
        let count_plan = LogicalPlan::Aggregate(Aggregate::try_new(
            Arc::new(input.clone()),
            vec![],
            vec![count_expr],
        )?);
        let count_batches = self
            .ctx
            .execute_logical_plan(count_plan)
            .await?
            .collect()
            .await?;
        let count = count_batches[0]
            .column(0)
            .as_primitive::<Int64Type>()
            .value(0);
        Ok(LogicalPlan::Limit(Limit {
            skip: Some(Box::new(lit(0i64.max(count - limit_num)))),
            fetch: Some(Box::new(limit)),
            input: Arc::new(input),
        }))
    }
}
