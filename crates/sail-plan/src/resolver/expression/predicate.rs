use arrow_schema::DataType;
use sail_common::spec;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::analyzer::typing;
use crate::error::PlanResult;
use crate::resolver::{NamedExpression, PlanResolver};

impl PlanResolver<'_> {
    #[async_recursion::async_recursion]
    pub(in crate::resolver) async fn truth_test(
        &mut self,
        id: ExprId,
        expression: spec::Expr,
        value: Option<bool>,
        negated: bool,
        schema: &QuerySchema,
    ) -> PlanResult<NamedExpression> {
        let inner = self.resolve_named_expression(expression, schema).await?;
        let expression =
            typing::coerce(inner.expression, &DataType::Boolean, &mut self.context.ids)?;
        Ok(NamedExpression {
            name: format!(
                "({} IS {}{})",
                inner.name,
                if negated { "NOT " } else { "" },
                match value {
                    Some(true) => "TRUE",
                    Some(false) => "FALSE",
                    None => "UNKNOWN",
                }
            ),
            expression: Expr {
                id,
                kind: Deferred::Resolved(ExprKind::TruthTest {
                    expression: Box::new(expression),
                    value,
                    negated,
                }),
                result_type: Deferred::Resolved(ExprType::new(DataType::Boolean, false)),
            },
        })
    }
}
