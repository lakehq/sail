use datafusion::arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{cast, expr, lit, when, ExprSchemable, ScalarUDF};
use datafusion_functions::math::expr_fn::abs;
use datafusion_functions_nested::expr_fn;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::collection::spark_concat::SparkConcat;
use sail_function::scalar::collection::spark_reverse::SparkReverse;
use sail_function::scalar::misc::raise_error::RaiseError;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn size(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let value = input.arguments.one()?;

    match value.get_type(input.function_context.schema)? {
        DataType::List(_)
        | DataType::ListView(_)
        | DataType::FixedSizeList(..)
        | DataType::LargeList(_)
        | DataType::LargeListView(_) => Ok(cast(expr_fn::array_length(value), DataType::Int32)),
        DataType::Map(..) => Ok(cast(expr_fn::cardinality(value), DataType::Int32)),
        wrong_type => Err(PlanError::InvalidArgument(format!(
            "size expects List or Map as argument, got {wrong_type:?}"
        ))),
    }
}

fn element_at(input: ScalarFunctionInput, is_try: bool) -> PlanResult<expr::Expr> {
    let (collection, element) = input.arguments.two()?;
    let (name, null_or_out_of_bounds) = if is_try {
        ("try_element_at", lit(ScalarValue::Null))
    } else {
        (
            "element_at",
            // TODO: respect spark.sql.ansi.enabled=false: https://spark.apache.org/docs/latest/api/sql/index.html#element_at
            ScalarUDF::from(RaiseError::new())
                .call(vec![lit("element_at: the index is out of bounds")]),
        )
    };

    Ok(match collection.get_type(input.function_context.schema)? {
        DataType::List(_)
        | DataType::ListView(_)
        | DataType::FixedSizeList(..)
        | DataType::LargeList(_)
        | DataType::LargeListView(_) => {
            when(element.clone().eq(lit(0)), ScalarUDF::from(RaiseError::new()).call(
            vec![lit(format!("{name}: the index 0 is invalid. An index shall be either < 0 or > 0 (the first element has index 1)"))]
                )).when( abs(element.clone()).not_between(lit(1), expr_fn::array_length(collection.clone())),
                null_or_out_of_bounds
                )
                .when(lit(true), expr_fn::array_element(collection, element)).end()?
        }
        DataType::Map(..) => {
            expr_fn::array_element(expr_fn::map_extract(collection, element), lit(1))
        }
        wrong_type => ScalarUDF::from(RaiseError::new()).call(vec![lit(format!(
            "{name} expects List or Map type as first argument, got {wrong_type:?}",
        ))]),
    })
}

pub(super) fn list_built_in_collection_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        // TODO: coalesce(result, -1)
        // if spark.sql.ansi.enabled is false and spark.sql.legacy.sizeOfNull is true
        // https://spark.apache.org/docs/latest/api/sql/index.html#cardinality
        ("cardinality", F::custom(size)),
        ("deep_size", F::unary(expr_fn::cardinality)),
        ("element_at", F::custom(|input| element_at(input, false))),
        ("size", F::custom(size)),
        ("array_concat", F::udf(SparkConcat::new())),
        ("concat", F::udf(SparkConcat::new())),
        ("reverse", F::udf(SparkReverse::new())),
        ("try_element_at", F::custom(|input| element_at(input, true))),
    ]
}
