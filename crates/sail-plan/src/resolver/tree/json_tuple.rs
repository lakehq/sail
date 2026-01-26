use datafusion::common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion::common::Result;
use datafusion::functions::core::expr_ext::FieldAccessor;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{Expr, LogicalPlan, ScalarUDF};
use sail_function::scalar::json::SparkJsonTuple;
use sail_function::scalar::multi_expr::MultiExpr;

use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::PlanRewriter;

/// Rewriter that transforms SparkJsonTuple expressions into MultiExpr expressions
/// containing field access expressions for each output column.
pub(crate) struct JsonTupleRewriter<'s> {
    plan: LogicalPlan,
    #[allow(dead_code)]
    state: &'s mut PlanResolverState,
}

impl<'s> PlanRewriter<'s> for JsonTupleRewriter<'s> {
    fn new_from_plan(plan: LogicalPlan, state: &'s mut PlanResolverState) -> Self {
        Self { plan, state }
    }

    fn into_plan(self) -> LogicalPlan {
        self.plan
    }
}

impl TreeNodeRewriter for JsonTupleRewriter<'_> {
    type Node = Expr;

    fn f_up(&mut self, node: Expr) -> Result<Transformed<Expr>> {
        let (func, args) = match node {
            Expr::ScalarFunction(ScalarFunction { func, args }) => (func, args),
            _ => return Ok(Transformed::no(node)),
        };

        // Check if this is a SparkJsonTuple call
        let is_json_tuple = func.inner().as_any().is::<SparkJsonTuple>();
        if !is_json_tuple {
            return Ok(Transformed::no(func.call(args)));
        }

        // SparkJsonTuple returns a struct with fields c0, c1, c2, ...
        // We need to extract each field and wrap them in MultiExpr
        let num_keys = args.len().saturating_sub(1);
        if num_keys == 0 {
            return Ok(Transformed::no(func.call(args)));
        }

        // Recreate the json_tuple call as the struct expression
        let struct_expr = func.call(args);

        // Create field extraction expressions for each output column
        let field_exprs: Vec<Expr> = (0..num_keys)
            .map(|i| {
                let field_name = format!("c{i}");
                struct_expr.clone().field(field_name)
            })
            .collect();

        // If there's only one field, return it directly
        // Otherwise wrap in MultiExpr
        let result = if field_exprs.len() == 1 {
            field_exprs.into_iter().next().ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "Expected at least one field expression".to_string(),
                )
            })?
        } else {
            ScalarUDF::from(MultiExpr::new()).call(field_exprs)
        };

        Ok(Transformed::yes(result))
    }
}
