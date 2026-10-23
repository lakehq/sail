//! Resolve argument coercion and the result field as one operation. This API
//! consumes fields and literal information, not backend expression trees.

use arrow_schema::FieldRef;
use datafusion_common::{Result, ScalarValue, plan_err};
use datafusion_expr::type_coercion::functions::fields_with_udf;
use datafusion_expr::{ReturnFieldArgs, ScalarUDF};

#[derive(Debug)]
pub struct ScalarTyping {
    pub arguments: Vec<FieldRef>,
    pub result: FieldRef,
}

pub fn resolve(
    function: &ScalarUDF,
    arguments: &[FieldRef],
    literals: &[Option<&ScalarValue>],
) -> Result<ScalarTyping> {
    if arguments.len() != literals.len() {
        return plan_err!("argument fields and literal information have different lengths");
    }
    let mut targets = if function.name() == "coalesce" {
        let common = crate::coercion::common_type(
            &arguments
                .iter()
                .map(|f| f.data_type().clone())
                .collect::<Vec<_>>(),
        )?;
        arguments
            .iter()
            .map(|field| crate::coercion::cast_field(field, &common))
            .collect()
    } else {
        fields_with_udf(arguments, function)?
    };
    if function.name() == "levenshtein" && targets.len() >= 2 {
        let target = crate::coercion::common_type(&[
            targets[0].data_type().clone(),
            targets[1].data_type().clone(),
        ])?;
        targets[0] = crate::coercion::cast_field(&targets[0], &target);
        targets[1] = crate::coercion::cast_field(&targets[1], &target);
    }
    if targets.len() != arguments.len() {
        return plan_err!("{} returned an invalid coercion arity", function.name());
    }
    // An implicit cast changes the physical type but preserves source
    // nullability. Extension metadata belongs to the original physical type.
    let targets = targets
        .into_iter()
        .zip(arguments)
        .map(|(target, source)| crate::coercion::cast_field(source, target.data_type()))
        .collect::<Vec<_>>();
    // Analysis materializes successful literal casts. Retain failed casts as
    // nonliteral expressions so lazy evaluation still controls their errors.
    let coerced_literals = literals
        .iter()
        .zip(arguments)
        .zip(&targets)
        .map(|((literal, source), target)| {
            literal.and_then(|value| {
                if source.data_type() == target.data_type() {
                    Some(value.clone())
                } else {
                    value.cast_to(target.data_type()).ok()
                }
            })
        })
        .collect::<Vec<_>>();
    let literals = coerced_literals
        .iter()
        .map(Option::as_ref)
        .collect::<Vec<_>>();
    let result = function.return_field_from_args(ReturnFieldArgs {
        arg_fields: &targets,
        scalar_arguments: &literals,
    })?;
    Ok(ScalarTyping {
        arguments: targets,
        result,
    })
}

/// String formatting preserves the value's nullability; an optional interval
/// qualifier is analysis metadata and does not affect the result field.
pub fn formatted_string_field(
    name: &str,
    kind: arrow_schema::DataType,
    args: ReturnFieldArgs,
) -> Result<FieldRef> {
    let ([value] | [value, _]) = args.arg_fields else {
        return plan_err!(
            "{name} expects one or two arguments, got {}",
            args.arg_fields.len()
        );
    };
    Ok(std::sync::Arc::new(arrow_schema::Field::new(
        name,
        kind,
        value.is_nullable(),
    )))
}

pub fn length_field(name: &str, args: ReturnFieldArgs) -> Result<FieldRef> {
    Ok(std::sync::Arc::new(arrow_schema::Field::new(
        name,
        arrow_schema::DataType::Int32,
        args.arg_fields.iter().any(|f| f.is_nullable()),
    )))
}
