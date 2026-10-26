use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use sail_common::spec;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::analyzer::typing;
use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn limit_expression(
        &mut self,
        expression: Option<spec::Expr>,
    ) -> PlanResult<Option<Box<Expr>>> {
        let Some(expression) = expression else {
            return Ok(None);
        };
        let expression = self
            .resolve_named_expression(expression, &QuerySchema::default())
            .await?
            .expression;
        if let ExprKind::Literal(value) = expression.kind.resolved("limit expression")? {
            let value = value.cast_to(&DataType::Int64)?;
            if !matches!(value, ScalarValue::Int64(Some(value)) if value >= 0) {
                return Err(PlanError::analysis("limit must be a nonnegative integer"));
            }
        }
        Ok(Some(Box::new(typing::coerce(
            expression,
            &DataType::Int64,
            &mut self.context.ids,
        )?)))
    }
}

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn resolve_query_limit(
        &mut self,
        input: spec::QueryPlan,
        skip: Option<spec::Expr>,
        limit: Option<spec::Expr>,
    ) -> PlanResult<Query> {
        Ok({
            let input = self.resolve_query_plan(input).await?;
            let schema = input.schema.resolved("limit input")?.clone();
            let skip = self.limit_expression(skip).await?;
            let fetch = self.limit_expression(limit).await?;
            Query {
                kind: Deferred::Resolved(QueryKind::Limit {
                    input: Box::new(input),
                    skip,
                    fetch,
                }),
                schema: Deferred::Resolved(schema),
            }
        })
    }
}
