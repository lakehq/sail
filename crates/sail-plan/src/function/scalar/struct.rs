use std::sync::Arc;

use datafusion::functions::expr_fn;
use datafusion_expr::{Expr, ScalarUDF, expr};
use sail_function::scalar::struct_function::StructFunction;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn r#struct(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let field_names: Vec<String> = input
        .arguments
        .iter()
        .zip(input.function_context.argument_display_names)
        .enumerate()
        .map(|(i, (expr, name))| -> PlanResult<_> {
            match expr {
                Expr::Column(_) => Ok(name.clone()),
                Expr::Alias(alias) => Ok(alias.name.clone()),
                #[expect(deprecated)]
                Expr::Wildcard { .. } => Err(PlanError::internal(
                    "wildcard should have been expanded before struct",
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

// CreateNamedStruct.nullable is false; each field retains its value's
// nullability (complexTypeCreator.scala:457-477).
fn named_struct(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let args = input.arguments;
    let names = args
        .chunks(2)
        .map(|pair| match pair {
            [Expr::Literal(name, _), _] => name.try_as_str().flatten().map(str::to_owned),
            _ => None,
        })
        .collect::<Option<Vec<_>>>();
    match names {
        Some(names)
            if !names.is_empty()
                && names
                    .iter()
                    .enumerate()
                    .all(|(i, name)| !names[..i].contains(name)) =>
        {
            let values = args.into_iter().skip(1).step_by(2).collect();
            Ok(ScalarUDF::from(StructFunction::new(names)).call(values))
        }
        _ => Ok(expr_fn::named_struct(args)),
    }
}

pub(super) fn list_built_in_struct_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("named_struct", F::custom(named_struct)),
        ("struct", F::custom(r#struct)),
    ]
}
