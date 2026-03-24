// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::physical_expr::PhysicalExpr;

pub fn simplify_expr(
    session: &dyn Session,
    df_schema: &DFSchema,
    expr: Expr,
) -> Result<Arc<dyn PhysicalExpr>> {
    let props = ExecutionProps::new();
    let simplify_context = SimplifyContext::new(&props).with_schema(df_schema.clone().into());
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
