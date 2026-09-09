use std::sync::Arc;

use datafusion::functions::expr_fn;
use datafusion_expr::{Expr, ScalarUDF, expr};
use sail_function::scalar::struct_function::StructFunction;

use crate::error::PlanResult;
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn r#struct(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let field_names: Vec<String> = input
        .arguments
        .iter()
        .zip(input.function_context.argument_display_names)
        .enumerate()
        .map(|(i, (expr, name))| match expr {
            Expr::Column(_) | Expr::Alias(_) => name.clone(),
            _ => format!("col{}", i + 1),
        })
        .collect();
    Ok(Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(StructFunction::new(field_names))),
        args: input.arguments,
    }))
}

pub(super) fn list_built_in_struct_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("named_struct", F::var_arg(expr_fn::named_struct)),
        ("struct", F::custom(r#struct)),
    ]
}
