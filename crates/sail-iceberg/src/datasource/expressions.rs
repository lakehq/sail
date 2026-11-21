// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::simplify::SimplifyContextBuilder;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::physical_expr::PhysicalExpr;

pub fn simplify_expr(
    session: &dyn Session,
    df_schema: &DFSchema,
    expr: Expr,
) -> Result<Arc<dyn PhysicalExpr>> {
    let simplify_context = SimplifyContextBuilder::default()
        .with_schema(df_schema.clone().into())
        .build();
    let simplifier = ExprSimplifier::new(simplify_context).with_max_cycles(10);
    let simplified = simplifier.simplify(expr)?;
    session.create_physical_expr(simplified, df_schema)
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
