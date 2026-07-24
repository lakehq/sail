use datafusion::arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{Expr, ScalarUDF, lit};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::array::arrays_zip::ArraysZip;
use sail_function::scalar::array::spark_array::SparkArray;
use sail_function::scalar::explode::{Explode, ExplodeKind};
use sail_function::scalar::variant::spark_variant_explode::SparkVariantExplodeUdf;

use crate::PlanResult;
use crate::error::PlanError;
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn stack(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context: _,
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
            )));
        }
    };

    let num_cols = args.len().div_ceil(n);
    args.resize(num_cols * n, lit(ScalarValue::Null));

    let field_names = (0..num_cols).map(|i| format!("col{i}")).collect::<Vec<_>>();

    let arrays = (0..num_cols)
        .map(|i| args.iter().skip(i).step_by(num_cols).cloned().collect())
        .map(|col| ScalarUDF::from(SparkArray::new()).call(col))
        .collect::<Vec<_>>();

    let zipped = ScalarUDF::from(ArraysZip::new(field_names)).call(arrays);

    Ok(ScalarUDF::from(Explode::new(ExplodeKind::Inline)).call(vec![zipped]))
}

fn variant_explode(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput { arguments, .. } = input;
    let arg = arguments.one()?;
    let explode_arr = ScalarUDF::from(SparkVariantExplodeUdf::new()).call(vec![arg]);
    Ok(ScalarUDF::from(Explode::new(ExplodeKind::Inline)).call(vec![explode_arr]))
}

fn variant_explode_outer(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput { arguments, .. } = input;
    let arg = arguments.one()?;
    let explode_arr = ScalarUDF::from(SparkVariantExplodeUdf::new()).call(vec![arg]);
    Ok(ScalarUDF::from(Explode::new(ExplodeKind::InlineOuter)).call(vec![explode_arr]))
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
        ("variant_explode", F::custom(variant_explode)),
        ("variant_explode_outer", F::custom(variant_explode_outer)),
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
