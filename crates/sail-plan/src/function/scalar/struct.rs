use std::sync::Arc;

use datafusion::functions::expr_fn;
use datafusion_common::DFSchema;
use datafusion_expr::{Expr, ScalarUDF, expr};
use sail_common_datafusion::interval::IntervalRange;
use sail_function::scalar::struct_function::StructFunction;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};
use crate::function::interval::{interval_range_of, with_struct_field_interval_ranges};

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
    // `CreateNamedStruct.dataType` builds each `StructField` from its own value
    // (`complexTypeCreator.scala:465-476`), so every field keeps the range of the value it holds;
    // there is no common range across the fields.
    let ranges = field_interval_ranges(&input.arguments, input.function_context.schema);
    let result = Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(StructFunction::new(field_names))),
        args: input.arguments,
    });
    with_struct_field_interval_ranges(result, &ranges, input.function_context.schema)
}

fn field_interval_ranges(values: &[Expr], schema: &DFSchema) -> Vec<Option<IntervalRange>> {
    values
        .iter()
        .map(|value| interval_range_of(value, schema))
        .collect()
}

fn named_struct(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let schema = function_context.schema;
    let values: Vec<Expr> = arguments.iter().skip(1).step_by(2).cloned().collect();
    let ranges = field_interval_ranges(&values, schema);
    let result = expr_fn::named_struct(arguments);
    with_struct_field_interval_ranges(result, &ranges, schema)
}

pub(super) fn list_built_in_struct_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("named_struct", F::custom(named_struct)),
        ("struct", F::custom(r#struct)),
    ]
}
