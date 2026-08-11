use datafusion::arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{ExprSchemable, ScalarUDF, cast, expr, lit, when};
use datafusion_functions::math::expr_fn::abs;
use datafusion_functions_nested::expr_fn;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::collection::spark_concat::SparkConcat;
use sail_function::scalar::collection::spark_reverse::SparkReverse;
use sail_function::scalar::misc::raise_error::RaiseError;
use sail_function::scalar::spark_to_string::SparkToUtf8;

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
            let (collection, element) = super::conditional::coerce_temporal_collection_value(
                collection,
                element,
                &input.function_context,
            )?;
            expr_fn::array_element(expr_fn::map_extract(collection, element), lit(1))
        }
        wrong_type => ScalarUDF::from(RaiseError::new()).call(vec![lit(format!(
            "{name} expects List or Map type as first argument, got {wrong_type:?}",
        ))]),
    })
}

fn concat(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let all_nested = arguments.iter().try_fold(true, |all_nested, argument| {
        Ok::<_, PlanError>(all_nested && argument.get_type(function_context.schema)?.is_nested())
    })?;
    let arguments = if all_nested {
        super::conditional::coerce_temporal_collection_values(arguments, &function_context)?
    } else {
        arguments
            .into_iter()
            .map(|argument| {
                if matches!(
                    argument.get_type(function_context.schema),
                    Ok(DataType::Timestamp(_, Some(_)))
                ) {
                    ScalarUDF::from(SparkToUtf8::new(
                        function_context.plan_config.session_timezone.clone(),
                    ))
                    .call(vec![argument])
                } else {
                    argument
                }
            })
            .collect()
    };
    Ok(ScalarUDF::from(SparkConcat::new()).call(arguments))
}

fn array_concat(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let arguments = super::conditional::coerce_temporal_collection_values(
        input.arguments,
        &input.function_context,
    )?;
    Ok(ScalarUDF::from(SparkConcat::new()).call(arguments))
}

fn reverse(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let argument = arguments.one()?;
    let argument = if matches!(
        argument.get_type(function_context.schema)?,
        DataType::Timestamp(_, Some(_))
    ) {
        ScalarUDF::from(SparkToUtf8::new(
            function_context.plan_config.session_timezone.clone(),
        ))
        .call(vec![argument])
    } else {
        argument
    };
    Ok(ScalarUDF::from(SparkReverse::new()).call(vec![argument]))
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
        ("array_concat", F::custom(array_concat)),
        ("concat", F::custom(concat)),
        ("reverse", F::custom(reverse)),
        ("try_element_at", F::custom(|input| element_at(input, true))),
    ]
}
