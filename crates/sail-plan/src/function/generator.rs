use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion_common::ScalarValue;
use datafusion_expr::{cast, lit, Expr, ExprSchemable, ScalarUDF};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::array::arrays_zip::ArraysZip;
use sail_function::scalar::array::spark_array::SparkArray;
use sail_function::scalar::explode::{Explode, ExplodeKind};
use sail_function::scalar::json_to_struct::JsonToStruct;
use sail_function::scalar::variant::spark_variant_explode::SparkVariantExplodeUdf;

use crate::error::PlanError;
use crate::function::common::{ScalarFunction, ScalarFunctionInput};
use crate::PlanResult;

fn stack(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;

    let (n_expr, mut args) = arguments.at_least_one()?;

    let err_int = || {
        Err(PlanError::invalid(
            "stack expects integer literal as first argument",
        ))
    };

    let Expr::Literal(n_scalar, _) = n_expr else {
        return err_int();
    };

    if !n_scalar.data_type().is_integer() {
        return err_int();
    }

    let n = match n_scalar.cast_to(&DataType::Int32)? {
        ScalarValue::Int32(Some(n)) if n > 0 => n as usize,
        wrong_value => {
            return Err(PlanError::invalid(format!(
                "stack expects first argument to be between (0, INT32::MAX], got {wrong_value}"
            )))
        }
    };

    let num_cols = args.len().div_ceil(n);
    args.resize(num_cols * n, lit(ScalarValue::Null));

    let arrays = (0..num_cols)
        .map(|i| args.iter().skip(i).step_by(num_cols).cloned().collect())
        .map(|col| ScalarUDF::from(SparkArray::new()).call(col))
        .collect::<Vec<_>>();

    let zipped = ScalarUDF::from(ArraysZip::new(vec![])).call(arrays);

    let err_struct = || {
        Err(PlanError::internal(
            "stack: arrays_zip call should return array<struct>",
        ))
    };

    let DataType::List(field) = zipped.get_type(function_context.schema)? else {
        return err_struct();
    };

    let DataType::Struct(fields) = field.data_type() else {
        return err_struct();
    };

    let res_type = DataType::List(Arc::new(Field::new(
        field.name(),
        DataType::Struct(
            fields
                .iter()
                .map(|field| {
                    field
                        .as_ref()
                        .clone()
                        .with_name(format!("col{}", field.name()))
                })
                .collect(),
        ),
        field.is_nullable(),
    )));

    Ok(ScalarUDF::from(Explode::new(ExplodeKind::Inline)).call(vec![cast(zipped, res_type)]))
}

fn json_tuple(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput { arguments, .. } = input;

    // Split into (json_expr, [key_expr, key_expr, ...])
    let (json_expr, key_exprs) = arguments.at_least_one()?;

    if key_exprs.is_empty() {
        return Err(PlanError::invalid(
            "json_tuple requires at least one field name argument",
        ));
    }

    // Every key must be a string literal validated at planning time
    let field_names: Vec<String> = key_exprs
        .iter()
        .map(|e| match e {
            Expr::Literal(ScalarValue::Utf8(Some(s)), _) => Ok(s.clone()),
            _ => Err(PlanError::invalid(
                "json_tuple field names must be string literals",
            )),
        })
        .collect::<PlanResult<_>>()?;

    // Step 1 — parse the JSON string into a Struct<c0: Utf8, c1: Utf8, …>
    let struct_expr = ScalarUDF::from(JsonToStruct::new(field_names)).call(vec![json_expr]);

    // Step 2 — wrap in a single-element array so Inline can iterate over it
    let array_expr = ScalarUDF::from(SparkArray::new()).call(vec![struct_expr]);

    // Step 3 — inline-explode the array<struct> into flat output columns
    Ok(ScalarUDF::from(Explode::new(ExplodeKind::Inline)).call(vec![array_expr]))
}

fn variant_explode(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput { arguments, .. } = input;
    let arg = arguments.one()?;
    let explode_arr = ScalarUDF::from(SparkVariantExplodeUdf::new()).call(vec![arg]);
    Ok(ScalarUDF::from(Explode::new(ExplodeKind::Inline)).call(vec![explode_arr]))
}

pub(super) fn list_built_in_generator_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("explode", F::udf(Explode::new(ExplodeKind::Explode))),
        (
            "explode_outer",
            F::udf(Explode::new(ExplodeKind::ExplodeOuter)),
        ),
        ("inline", F::udf(Explode::new(ExplodeKind::Inline))),
        (
            "inline_outer",
            F::udf(Explode::new(ExplodeKind::InlineOuter)),
        ),
        ("posexplode", F::udf(Explode::new(ExplodeKind::PosExplode))),
        (
            "posexplode_outer",
            F::udf(Explode::new(ExplodeKind::PosExplodeOuter)),
        ),
        ("stack", F::custom(stack)),
        ("json_tuple", F::custom(json_tuple)),
        ("variant_explode", F::custom(variant_explode)),
        ("variant_explode_outer", F::custom(variant_explode)),
    ]
}

pub fn get_outer_built_in_generator_functions(name: &str) -> &str {
    match name.to_lowercase().as_str() {
        "explode" => "explode_outer",
        "inline" => "inline_outer",
        "posexplode" => "posexplode_outer",
        "variant_explode" => "variant_explode_outer",
        _ => name,
    }
}
