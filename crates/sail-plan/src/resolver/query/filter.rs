use arrow_schema::DataType;
use sail_common::spec;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::analyzer::typing;
use crate::error::PlanResult;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn resolve_query_filter(
        &mut self,
        input: spec::QueryPlan,
        condition: spec::Expr,
    ) -> PlanResult<Query> {
        Ok({
            let input = self.resolve_query_plan(input).await?;
            let output = input.schema.resolved("filter output")?.clone();
            let (input, condition) = self.recover_expression(input, condition, false).await?;
            let condition = typing::coerce(
                condition.expression,
                &DataType::Boolean,
                &mut self.state.ids,
            )?;
            let schema = input.schema.clone();
            let result = Query {
                kind: Deferred::Resolved(QueryKind::Filter {
                    input: Box::new(input),
                    condition: Box::new(condition),
                }),
                schema,
            };
            self.restore_output(result, output)?
        })
    }
}
