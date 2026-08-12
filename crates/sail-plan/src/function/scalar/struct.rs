use std::sync::Arc;

use datafusion_expr::{Expr, ScalarUDF, expr};
use sail_common::spec::SAIL_SPARK_TIME_PRECISION_METADATA_KEY;
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_function::scalar::struct_function::StructFunction;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn is_internal_time_precision_alias(alias: &expr::Alias) -> bool {
    alias.metadata.as_ref().is_some_and(|metadata| {
        metadata
            .inner()
            .contains_key(SAIL_SPARK_TIME_PRECISION_METADATA_KEY)
    }) && !matches!(alias.expr.as_ref(), Expr::Alias(_))
}

fn r#struct(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let field_names: Vec<String> = input
        .arguments
        .iter()
        .zip(input.function_context.argument_display_names)
        .enumerate()
        .map(|(i, (expr, name))| -> PlanResult<_> {
            match expr {
                Expr::Column(_) => Ok(name.clone()),
                Expr::Alias(alias) if !is_internal_time_precision_alias(alias) => Ok(name.clone()),
                #[expect(deprecated)]
                Expr::Wildcard { .. } => Err(PlanError::internal(
                    "wildcard should have been expanded before struct",
                )),
                _ => Ok(format!("col{}", i + 1)),
            }
        })
        .collect::<PlanResult<_>>()?;
    Ok(make_struct(field_names, input.arguments))
}

fn named_struct(input: ScalarFunctionInput) -> PlanResult<Expr> {
    if input.arguments.is_empty() || !input.arguments.len().is_multiple_of(2) {
        return Err(PlanError::invalid(format!(
            "named_struct requires a non-empty even number of arguments, got {}",
            input.arguments.len()
        )));
    }
    let mut field_names = Vec::with_capacity(input.arguments.len() / 2);
    let mut values = Vec::with_capacity(input.arguments.len() / 2);
    let mut arguments = input.arguments.into_iter();
    let evaluator = LiteralEvaluator::new();
    while let Some(name) = arguments.next() {
        let name = evaluator.evaluate(&name).map_err(|error| {
            PlanError::invalid(format!("named_struct field name must be foldable: {error}"))
        })?;
        let Some(name) = name.try_as_str().flatten() else {
            return Err(PlanError::invalid(
                "named_struct field name must be a non-null string",
            ));
        };
        field_names.push(name.to_string());
        values.push(
            arguments
                .next()
                .ok_or_else(|| PlanError::internal("named_struct value is missing"))?,
        );
    }
    Ok(make_struct(field_names, values))
}

fn make_struct(field_names: Vec<String>, arguments: Vec<Expr>) -> Expr {
    Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(StructFunction::new(field_names))),
        args: arguments,
    })
}

pub(super) fn list_built_in_struct_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("named_struct", F::custom(named_struct)),
        ("struct", F::custom(r#struct)),
    ]
}
