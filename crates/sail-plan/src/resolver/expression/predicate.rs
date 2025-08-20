use datafusion_common::DFSchemaRef;
use datafusion_expr::{expr, BinaryExpr};
use datafusion_expr_common::operator::Operator;
use sail_common::spec;

use crate::error::PlanResult;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    // TODO: Construct better names for the expression (e.g. a IN (b, c)) for all functions below.

    pub(super) async fn resolve_expression_in_list(
        &self,
        expr: spec::Expr,
        list: Vec<spec::Expr>,
        negated: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = Box::new(self.resolve_expression(expr, schema, state).await?);
        let list = self.resolve_expressions(list, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["in_list".to_string()],
            expr::Expr::InList(expr::InList::new(expr, list, negated)),
        ))
    }

    pub(super) async fn resolve_expression_is_false(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_false".to_string()],
            expr::Expr::IsFalse(Box::new(expr)),
        ))
    }

    pub(super) async fn resolve_expression_is_not_false(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_not_false".to_string()],
            expr::Expr::IsNotFalse(Box::new(expr)),
        ))
    }

    pub(super) async fn resolve_expression_is_true(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_true".to_string()],
            expr::Expr::IsTrue(Box::new(expr)),
        ))
    }

    pub(super) async fn resolve_expression_is_not_true(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_not_true".to_string()],
            expr::Expr::IsNotTrue(Box::new(expr)),
        ))
    }

    pub(super) async fn resolve_expression_is_null(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_null".to_string()],
            expr::Expr::IsNull(Box::new(expr)),
        ))
    }

    pub(super) async fn resolve_expression_is_not_null(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_not_null".to_string()],
            expr::Expr::IsNotNull(Box::new(expr)),
        ))
    }

    pub(super) async fn resolve_expression_is_unknown(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_unknown".to_string()],
            expr::Expr::IsUnknown(Box::new(expr)),
        ))
    }

    pub(super) async fn resolve_expression_is_not_unknown(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_not_unknown".to_string()],
            expr::Expr::IsNotUnknown(Box::new(expr)),
        ))
    }

    pub(super) async fn resolve_expression_between(
        &self,
        expr: spec::Expr,
        negated: bool,
        low: spec::Expr,
        high: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        let low = self.resolve_expression(low, schema, state).await?;
        let high = self.resolve_expression(high, schema, state).await?;

        // DataFusion's BETWEEN operator has a bug, so we construct the expression manually.
        let greater_eq = expr::Expr::BinaryExpr(BinaryExpr::new(
            Box::new(expr.clone()),
            Operator::GtEq,
            Box::new(low),
        ));
        let less_eq = expr::Expr::BinaryExpr(BinaryExpr::new(
            Box::new(expr),
            Operator::LtEq,
            Box::new(high),
        ));
        let between_expr = expr::Expr::BinaryExpr(BinaryExpr::new(
            Box::new(greater_eq),
            Operator::And,
            Box::new(less_eq),
        ));
        let between_expr = if negated {
            expr::Expr::Not(Box::new(between_expr))
        } else {
            between_expr
        };
        Ok(NamedExpr::new(vec!["between".to_string()], between_expr))
    }

    pub(super) async fn resolve_expression_is_distinct_from(
        &self,
        left: spec::Expr,
        right: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let left = self.resolve_expression(left, schema, state).await?;
        let right = self.resolve_expression(right, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_distinct_from".to_string()],
            expr::Expr::BinaryExpr(BinaryExpr {
                left: Box::new(left),
                op: Operator::IsDistinctFrom,
                right: Box::new(right),
            }),
        ))
    }

    pub(super) async fn resolve_expression_is_not_distinct_from(
        &self,
        left: spec::Expr,
        right: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let left = self.resolve_expression(left, schema, state).await?;
        let right = self.resolve_expression(right, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["is_not_distinct_from".to_string()],
            expr::Expr::BinaryExpr(BinaryExpr {
                left: Box::new(left),
                op: Operator::IsNotDistinctFrom,
                right: Box::new(right),
            }),
        ))
    }

    pub(super) async fn resolve_expression_similar_to(
        &self,
        expr: spec::Expr,
        pattern: spec::Expr,
        negated: bool,
        escape_char: Option<char>,
        case_insensitive: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let expr = self.resolve_expression(expr, schema, state).await?;
        let pattern = self.resolve_expression(pattern, schema, state).await?;
        Ok(NamedExpr::new(
            vec!["similar_to".to_string()],
            expr::Expr::SimilarTo(expr::Like::new(
                negated,
                Box::new(expr),
                Box::new(pattern),
                escape_char,
                case_insensitive,
            )),
        ))
    }
}
