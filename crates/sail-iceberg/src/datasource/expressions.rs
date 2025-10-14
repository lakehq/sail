use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::DFSchema;
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::physical_expr::PhysicalExpr;

pub fn simplify_expr(
    session: &dyn Session,
    df_schema: &DFSchema,
    expr: Expr,
) -> Arc<dyn PhysicalExpr> {
    let props = ExecutionProps::new();
    let simplify_context = SimplifyContext::new(&props).with_schema(df_schema.clone().into());
    let simplifier = ExprSimplifier::new(simplify_context).with_max_cycles(10);
    #[allow(clippy::expect_used)]
    let simplified = simplifier
        .simplify(expr)
        .expect("Failed to simplify expression");
    #[allow(clippy::expect_used)]
    session
        .create_physical_expr(simplified, df_schema)
        .expect("Failed to create physical expression")
}

pub fn get_pushdown_filters(
    filter: &[&Expr],
    _partition_cols: &[String],
) -> Vec<TableProviderFilterPushDown> {
    filter
        .iter()
        .map(|expr| match expr {
            Expr::BinaryExpr(be) => match be.op {
                datafusion::logical_expr::Operator::Eq
                | datafusion::logical_expr::Operator::Lt
                | datafusion::logical_expr::Operator::LtEq
                | datafusion::logical_expr::Operator::Gt
                | datafusion::logical_expr::Operator::GtEq
                | datafusion::logical_expr::Operator::And
                | datafusion::logical_expr::Operator::Or => TableProviderFilterPushDown::Inexact,
                _ => TableProviderFilterPushDown::Unsupported,
            },
            Expr::InList(_) => TableProviderFilterPushDown::Inexact,
            _ => TableProviderFilterPushDown::Unsupported,
        })
        .collect()
}
