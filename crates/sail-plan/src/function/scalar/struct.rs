use std::sync::Arc;

use datafusion::functions::expr_fn;
use datafusion_common::DFSchemaRef;
use datafusion_expr::{Expr, ScalarUDF, expr};
use sail_function::scalar::struct_function::StructFunction;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};
use crate::resolver::PlanResolver;

/// A field of a struct reports the metadata of what it holds only where that is a name: an
/// attribute, another alias or the field of a struct (`CreateNamedStruct.dataType`). Anything
/// else, a cast for one, reports none, so the metadata it carries is hidden here -- and only
/// where there is something to hide, since an override of its own is part of the Arrow type and
/// a signature that matches a struct exactly would no longer accept it.
fn struct_field_values(names: &[String], values: Vec<Expr>, schema: &DFSchemaRef) -> Vec<Expr> {
    values
        .into_iter()
        .zip(names)
        .map(|(value, name)| {
            if PlanResolver::inherits_metadata(&value)
                || !PlanResolver::has_spark_metadata(&value, schema)
            {
                value
            } else {
                value.alias_with_metadata(name, PlanResolver::empty_spark_metadata())
            }
        })
        .collect()
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
                Expr::Alias(alias) => Ok(alias.name.clone()),
                #[expect(deprecated)]
                Expr::Wildcard { .. } => Err(PlanError::internal(
                    "wildcard should have been expanded before struct",
                )),
                _ => Ok(format!("col{}", i + 1)),
            }
        })
        .collect::<PlanResult<_>>()?;
    let args = struct_field_values(&field_names, input.arguments, &input.function_context.schema);
    Ok(Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(StructFunction::new(field_names))),
        args,
    }))
}

/// `named_struct` is `struct` with the names given, so it is built the same way when every name is
/// a string literal, which reports the struct as not nullable and each field as nullable where its
/// value is (`CreateNamedStruct`). Any other argument list goes to DataFusion, which rejects it.
fn named_struct(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let schema = input.function_context.schema;
    let args = input.arguments;
    let names = args
        .chunks(2)
        .map(|pair| match pair {
            [Expr::Literal(name, _), _] => name.try_as_str().flatten().map(|name| name.to_string()),
            _ => None,
        })
        .collect::<Option<Vec<_>>>();
    // A name the struct holds twice is left to DataFusion, since `StructFunction` builds the
    // Arrow fields itself and Arrow refuses a struct whose field names repeat.
    let repeated = |names: &[String]| {
        names
            .iter()
            .enumerate()
            .any(|(i, name)| names[..i].contains(name))
    };
    match names {
        Some(names) if !names.is_empty() && !repeated(&names) => {
            let values = args.into_iter().skip(1).step_by(2).collect();
            let values = struct_field_values(&names, values, &schema);
            Ok(Expr::ScalarFunction(expr::ScalarFunction {
                func: Arc::new(ScalarUDF::from(StructFunction::new(names))),
                args: values,
            }))
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
