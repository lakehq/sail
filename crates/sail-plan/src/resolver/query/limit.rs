use std::sync::Arc;

use datafusion_common::ScalarValue;
use datafusion_expr::{Expr, Extension, Limit, LogicalPlan};
use sail_common::spec;
use sail_logical_plan::tail::TailNode;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

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
        let Ok(fetch) = usize::try_from(limit_num) else {
            return Err(PlanError::AnalysisError(format!(
                "The limit like expression \"{limit_num}\" is invalid. \
                 The tail expression must be equal to or greater than 0, but got {limit_num}."
            )));
        };
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(TailNode::new(Arc::new(input), fetch)),
        }))
    }
}
