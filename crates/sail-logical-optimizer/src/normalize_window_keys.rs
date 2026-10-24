use datafusion::optimizer::AnalyzerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{DFSchema, Result};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan, ScalarUDF, Window};
use sail_function::scalar::normalize_float_keys::NormalizeFloatKeys;

/// Give ordinary and decorrelated windows the same floating comparison keys.
#[derive(Debug)]
pub struct NormalizeWindowKeys;

impl AnalyzerRule for NormalizeWindowKeys {
    fn name(&self) -> &str {
        "normalize_window_keys"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        // Run once after coercion; later CSE can bind completed normalized keys.
        plan.transform_up_with_subqueries(normalize_window).data()
    }
}

fn normalize_window(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let names = NamePreserver::new(&plan);
    let LogicalPlan::Window(window) = plan else {
        return Ok(Transformed::no(plan));
    };
    let mut changed = false;
    let expressions = window
        .window_expr
        .iter()
        .map(|expression| {
            let name = names.save(expression);
            let result = expression.clone().transform_up(|expression| {
                let Expr::WindowFunction(mut function) = expression else {
                    return Ok(Transformed::no(expression));
                };
                let mut normalized = false;
                for key in function.params.partition_by.iter_mut().chain(
                    function
                        .params
                        .order_by
                        .iter_mut()
                        .map(|sort| &mut sort.expr),
                ) {
                    normalized |= normalize_key(key, window.input.schema())?;
                }
                Ok(Transformed::new_transformed(
                    Expr::WindowFunction(function),
                    normalized,
                ))
            })?;
            changed |= result.transformed;
            Ok(name.restore(result.data))
        })
        .collect::<Result<Vec<_>>>()?;
    if changed {
        Ok(Transformed::yes(LogicalPlan::Window(Window::try_new(
            expressions,
            window.input,
        )?)))
    } else {
        Ok(Transformed::no(LogicalPlan::Window(window)))
    }
}

fn normalize_key(key: &mut Expr, schema: &DFSchema) -> Result<bool> {
    if !NormalizeFloatKeys::has_float(&key.get_type(schema)?)
        || matches!(key, Expr::ScalarFunction(function) if function.func.inner().is::<NormalizeFloatKeys>())
    {
        return Ok(false);
    }
    // The completed key is one argument; do not duplicate volatile operands or change raw values.
    *key = ScalarUDF::from(NormalizeFloatKeys::new()).call(vec![key.clone()]);
    Ok(true)
}
