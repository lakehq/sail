use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::functions::{core, math};
use datafusion::functions_nested;
use datafusion_common::ScalarValue;
use datafusion_expr::{Operator, ScalarUDF};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::collection::spark_concat::SparkConcat;
use sail_function::scalar::collection::spark_reverse::SparkReverse;
use sail_function::scalar::misc::raise_error::RaiseError;

use crate::analyzer::model::Expr;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{FunctionContextInput, ScalarFunction, ScalarFunctionInput};

fn size(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let value = values.one()?;
    let data_type = &value.result_type.resolved("size input")?.data_type;
    let function = if data_type.is_list() {
        functions_nested::length::array_length_udf()
    } else if matches!(data_type, DataType::Map(..)) {
        functions_nested::cardinality::cardinality_udf()
    } else {
        return Err(PlanError::invalid(format!(
            "size expects List or Map as argument, got {data_type:?}"
        )));
    };
    let result = context.scalar_udf(function, vec![value])?;
    let result = context.cast(result, DataType::Int32)?;
    Ok(
        if context.config.legacy_size_of_null && !context.config.ansi_mode {
            let fallback = context.literal(ScalarValue::Int32(Some(-1)));
            context.scalar_udf(core::coalesce(), vec![result, fallback])?
        } else {
            result
        },
    )
}

fn element_at(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let (collection, element) = values.two()?;
    let data_type = &collection
        .result_type
        .resolved("element_at input")?
        .data_type;
    Ok(if data_type.is_list() {
        let index = context.copy_expression(&element)?;
        let zero = context.literal(ScalarValue::Int32(Some(0)));
        let invalid = context.binary(index, Operator::Eq, zero)?;
        let error = raise_error(
            context,
            format!(
                "{name}: the index 0 is invalid. An index shall be either < 0 or > 0 (the first element has index 1)"
            ),
        )?;
        let index = context.copy_expression(&element)?;
        let absolute = context.scalar_udf(math::abs(), vec![index])?;
        let absolute_copy = context.copy_expression(&absolute)?;
        let one = context.literal(ScalarValue::Int64(Some(1)));
        let below = context.binary(absolute_copy, Operator::Lt, one)?;
        let array = context.copy_expression(&collection)?;
        let length =
            context.scalar_udf(functions_nested::length::array_length_udf(), vec![array])?;
        let above = context.binary(absolute, Operator::Gt, length)?;
        let outside = context.binary(below, Operator::Or, above)?;
        let fallback = if name == "try_element_at" {
            context.literal(ScalarValue::Null)
        } else {
            raise_error(context, "element_at: the index is out of bounds".into())?
        };
        let value = context.scalar_udf(
            functions_nested::extract::array_element_udf(),
            vec![collection, element],
        )?;
        let always = context.literal(ScalarValue::Boolean(Some(true)));
        context.case(
            vec![(invalid, error), (outside, fallback), (always, value)],
            None,
        )?
    } else if matches!(data_type, DataType::Map(..)) {
        let values = context.scalar_udf(
            functions_nested::map_extract::map_extract_udf(),
            vec![collection, element],
        )?;
        let first = context.literal(ScalarValue::Int64(Some(1)));
        context.scalar_udf(
            functions_nested::extract::array_element_udf(),
            vec![values, first],
        )?
    } else {
        raise_error(
            context,
            format!("{name} expects List or Map type as first argument, got {data_type:?}"),
        )?
    })
}

fn resolve_deep_size(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    context.scalar_udf(
        functions_nested::cardinality::cardinality_udf(),
        vec![values.one()?],
    )
}

pub(crate) fn raise_error(
    context: &mut FunctionContextInput<'_>,
    message: String,
) -> PlanResult<Expr> {
    let message = context.literal(ScalarValue::Utf8(Some(message)));
    context.scalar_udf(Arc::new(ScalarUDF::from(RaiseError::new())), vec![message])
}

pub(super) fn list_built_in_collection_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("cardinality", F::custom(size)),
        ("deep_size", F::custom(resolve_deep_size)),
        (
            "element_at",
            F::custom(|input| element_at(input, "element_at")),
        ),
        ("size", F::custom(size)),
        ("array_concat", F::udf(SparkConcat::new())),
        ("concat", F::udf(SparkConcat::new())),
        ("reverse", F::udf(SparkReverse::new())),
        (
            "try_element_at",
            F::custom(|input| element_at(input, "try_element_at")),
        ),
        ("||", F::unknown("||")),
    ]
}
