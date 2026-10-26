//! Resolve higher-order calls through their UDF typing hooks.

use arrow_schema::FieldRef;
use datafusion_common::{Result, ScalarValue, plan_err};
use datafusion_expr::type_coercion::functions::{
    value_fields_with_higher_order_udf, value_fields_with_higher_order_udf_and_lambdas,
};
use datafusion_expr::{HigherOrderReturnFieldArgs, HigherOrderUDF, ValueOrLambda};

pub fn value_fields<L: Clone>(
    function: &HigherOrderUDF,
    fields: &[ValueOrLambda<FieldRef, L>],
) -> Result<Vec<ValueOrLambda<FieldRef, L>>> {
    let targets = value_fields_with_higher_order_udf(fields, function)?;
    normalize(fields, targets)
}

fn normalize<L>(
    sources: &[ValueOrLambda<FieldRef, L>],
    targets: Vec<ValueOrLambda<FieldRef, L>>,
) -> Result<Vec<ValueOrLambda<FieldRef, L>>> {
    super::coercion::validate_arity("higher-order coercion", targets.len(), sources.len())?;
    targets
        .into_iter()
        .zip(sources)
        .map(|(target, source)| {
            Ok(match (target, source) {
                (ValueOrLambda::Value(target), ValueOrLambda::Value(source)) => {
                    ValueOrLambda::Value(super::coercion::cast_field(source, target.data_type()))
                }
                (ValueOrLambda::Lambda(target), ValueOrLambda::Lambda(_)) => {
                    ValueOrLambda::Lambda(target)
                }
                _ => return plan_err!("higher-order coercion changed argument kind"),
            })
        })
        .collect()
}

pub struct HigherOrderTyping {
    pub arguments: Vec<ValueOrLambda<FieldRef, FieldRef>>,
    pub result: FieldRef,
}

pub fn resolve(
    function: &HigherOrderUDF,
    fields: &[ValueOrLambda<FieldRef, FieldRef>],
    literals: &[Option<&ScalarValue>],
) -> Result<HigherOrderTyping> {
    super::coercion::validate_arity(function.name(), literals.len(), fields.len())?;
    let arguments = normalize(
        fields,
        value_fields_with_higher_order_udf_and_lambdas(fields, function)?,
    )?;
    let literals = literals
        .iter()
        .zip(fields)
        .zip(&arguments)
        .map(|((literal, source), target)| match (source, target) {
            (ValueOrLambda::Value(source), ValueOrLambda::Value(target))
                if source.data_type() == target.data_type() =>
            {
                *literal
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let result = function.return_field_from_args(HigherOrderReturnFieldArgs {
        arg_fields: &arguments,
        scalar_arguments: &literals,
    })?;
    Ok(HigherOrderTyping { arguments, result })
}
