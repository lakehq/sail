use std::sync::Arc;

use datafusion::functions::expr_fn;
use datafusion_expr::{expr, Expr, ScalarUDF};

use crate::error::{PlanError, PlanResult};
use crate::extension::function::struct_function::StructFunction;
use crate::function::common::{Function, FunctionInput};

fn r#struct(input: FunctionInput) -> PlanResult<Expr> {
    let field_names: Vec<String> = input
        .arguments
        .iter()
        .zip(input.argument_names)
        .enumerate()
        .map(|(i, (expr, name))| -> PlanResult<_> {
            match expr {
                Expr::Column(_) | Expr::Alias(_) => Ok(name.clone()),
                Expr::Wildcard { .. } => Err(PlanError::invalid(
                    "wildcard is not yet supported in struct",
                )),
                _ => Ok(format!("col{}", i + 1)),
            }
        })
        .collect::<PlanResult<_>>()?;
    Ok(Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(StructFunction::new(field_names))),
        args: input.arguments,
    }))
}

pub(super) fn list_built_in_struct_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("named_struct", F::var_arg(expr_fn::named_struct)),
        ("struct", F::custom(r#struct)),
    ]
}
