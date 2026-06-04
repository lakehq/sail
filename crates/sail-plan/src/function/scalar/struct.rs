use std::sync::Arc;

use datafusion_common::ScalarValue;
use datafusion_expr::{expr, Expr, ScalarUDF};
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
                Expr::Column(_) | Expr::Alias(_) => Ok(name.clone()),
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

fn named_struct(input: ScalarFunctionInput) -> PlanResult<Expr> {
    if input.arguments.is_empty() || input.arguments.len() % 2 != 0 {
        return Err(PlanError::invalid(
            "named_struct requires an even number of arguments",
        ));
    }

    let mut field_names = Vec::with_capacity(input.arguments.len() / 2);
    let mut values = Vec::with_capacity(input.arguments.len() / 2);
    let mut arguments = input.arguments.into_iter();

    while let Some(name) = arguments.next() {
        let value = arguments.next().ok_or_else(|| {
            PlanError::invalid("named_struct requires an even number of arguments")
        })?;
        let Expr::Literal(value_name, _) = name else {
            return Err(PlanError::invalid(
                "named_struct field names must be string literals",
            ));
        };
        let value_name = match value_name {
            ScalarValue::Utf8(Some(name))
            | ScalarValue::LargeUtf8(Some(name))
            | ScalarValue::Utf8View(Some(name)) => name,
            _ => {
                return Err(PlanError::invalid(
                    "named_struct field names must be non-null string literals",
                ));
            }
        };
        field_names.push(value_name);
        values.push(value);
    }

    Ok(Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(StructFunction::new(field_names))),
        args: values,
    }))
}

pub(super) fn list_built_in_struct_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("named_struct", F::custom(named_struct)),
        ("struct", F::custom(r#struct)),
    ]
}
