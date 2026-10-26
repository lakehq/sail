use std::sync::Arc;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::analyzer::typing;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionBuilder as F, ScalarFunctionInput};

fn resolve_named_struct(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let values = input.arguments;
    Ok({
        if values.is_empty() || !values.len().is_multiple_of(2) {
            return Err(PlanError::analysis(
                "named_struct requires name/value pairs",
            ));
        }
        let mut fields = vec![];
        let mut args = vec![];
        let mut values = values.into_iter();
        while let Some(name) = values.next() {
            let name = crate::analyzer::lowering::evaluate_constant(&name)?;
            fields.push(
                name.try_as_str()
                    .flatten()
                    .ok_or_else(|| {
                        PlanError::analysis("named_struct field name must be a string literal")
                    })?
                    .to_string(),
            );
            args.push(
                values
                    .next()
                    .ok_or_else(|| PlanError::internal("missing struct value"))?,
            );
        }
        input.context.scalar_udf(
            Arc::new(datafusion_expr::ScalarUDF::from(
                sail_function::scalar::struct_function::StructFunction::new(fields),
            )),
            args,
        )?
    })
}
fn resolve_struct(input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let values = input.arguments;
    let names = input.argument_display_names;
    let id = input.context.ids.expression();
    Ok({
        let field_names = values
                .iter()
                .zip(names)
                .enumerate()
                .map(|(i, (value, name))| {
                    if matches!(
                        value.kind,
                        Deferred::Resolved(
                            ExprKind::Attribute(_)
                                | ExprKind::OuterAttribute { .. }
                                | ExprKind::WithMetadata(_)
                                | ExprKind::LambdaVariable(_)
                        )
                    ) || matches!(&value.kind, Deferred::Resolved(ExprKind::ScalarFunction { function, .. }) if function.name() == "get_field") {
                        name.clone()
                    } else {
                        format!("col{}", i + 1)
                    }
                })
                .collect();
        let function = datafusion_expr::ScalarUDF::from(
            sail_function::scalar::struct_function::StructFunction::new(field_names),
        );
        typing::resolve_scalar_call(id, Arc::new(function), values, input.context.ids)?
    })
}

pub(super) fn list_built_in_struct_functions() -> Vec<(&'static str, ScalarFunction)> {
    vec![
        ("struct", F::custom(resolve_struct)),
        ("named_struct", F::custom(resolve_named_struct)),
    ]
}
