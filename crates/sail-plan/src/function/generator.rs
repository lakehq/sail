use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::ScalarUDF;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::array::arrays_zip::ArraysZip;
use sail_function::scalar::array::spark_array::SparkArray;
use sail_function::scalar::array::spark_array_item_with_position::ArrayItemWithPosition;

use crate::analyzer::model::*;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    FunctionContextInput, ScalarFunctionBuilder as F, ScalarFunctionInput,
};

fn explode_argument(
    context: &mut FunctionContextInput<'_>,
    mut argument: Expr,
    position: bool,
    inline: bool,
) -> PlanResult<Expr> {
    let map = matches!(
        argument
            .result_type
            .resolved("generator argument")?
            .data_type,
        DataType::Map(_, _)
    );
    if inline && map {
        return Err(PlanError::analysis("inline requires an array of structs"));
    }
    if map {
        argument = context.scalar("map_entries", vec![argument])?;
    }
    if position {
        argument = context.scalar_udf(
            Arc::new(ScalarUDF::from(ArrayItemWithPosition::new())),
            vec![argument],
        )?;
    }
    Ok(argument)
}

pub(crate) fn stack_rows(
    context: &mut FunctionContextInput<'_>,
    arguments: Vec<Expr>,
) -> PlanResult<Expr> {
    let (rows, mut values) = arguments.at_least_one()?;
    let crate::analyzer::deferred::Deferred::Resolved(ExprKind::Literal(rows)) = rows.kind else {
        return Err(PlanError::invalid(
            "stack expects integer literal as first argument",
        ));
    };
    if !rows.data_type().is_integer() {
        return Err(PlanError::invalid(
            "stack expects integer literal as first argument",
        ));
    }
    let rows = match rows.cast_to(&DataType::Int32)? {
        ScalarValue::Int32(Some(rows)) if rows > 0 => rows as usize,
        value => {
            return Err(PlanError::invalid(format!(
                "stack expects first argument to be between (0, INT32::MAX], got {value}"
            )));
        }
    };
    let width = values.len().div_ceil(rows);
    values.resize_with(width * rows, || context.literal(ScalarValue::Null));
    let mut columns = (0..width).map(|_| vec![]).collect::<Vec<_>>();
    for (index, value) in values.into_iter().enumerate() {
        columns[index % width].push(value);
    }
    let arrays = columns
        .into_iter()
        .map(|column| context.scalar_udf(Arc::new(ScalarUDF::from(SparkArray::new())), column))
        .collect::<PlanResult<Vec<_>>>()?;
    let fields = (0..width).map(|index| format!("col{index}")).collect();
    context.scalar_udf(Arc::new(ScalarUDF::from(ArraysZip::new(fields))), arrays)
}

pub(crate) fn list_built_in_generator_functions()
-> Vec<(&'static str, crate::function::common::ScalarFunction)> {
    vec![
        (
            "explode",
            F::custom(|mut input| {
                explode_argument(&mut input.context, input.arguments.one()?, false, false)
            }),
        ),
        (
            "explode_outer",
            F::custom(|mut input| {
                explode_argument(&mut input.context, input.arguments.one()?, false, false)
            }),
        ),
        (
            "posexplode",
            F::custom(|mut input| {
                explode_argument(&mut input.context, input.arguments.one()?, true, false)
            }),
        ),
        (
            "posexplode_outer",
            F::custom(|mut input| {
                explode_argument(&mut input.context, input.arguments.one()?, true, false)
            }),
        ),
        (
            "inline",
            F::custom(|mut input| {
                explode_argument(&mut input.context, input.arguments.one()?, false, true)
            }),
        ),
        (
            "inline_outer",
            F::custom(|mut input| {
                explode_argument(&mut input.context, input.arguments.one()?, false, true)
            }),
        ),
        (
            "variant_explode",
            F::udf(
                sail_function::scalar::variant::spark_variant_explode::SparkVariantExplodeUdf::new(
                ),
            ),
        ),
        (
            "variant_explode_outer",
            F::udf(
                sail_function::scalar::variant::spark_variant_explode::SparkVariantExplodeUdf::new(
                ),
            ),
        ),
        (
            "window",
            F::custom(|mut input| {
                crate::function::scalar::datetime::time_window::window_buckets(
                    &mut input.context,
                    input.arguments,
                )
            }),
        ),
        ("json_tuple", F::custom(json_tuple)),
        (
            "stack",
            F::custom(|mut input| stack_rows(&mut input.context, input.arguments)),
        ),
    ]
}

fn json_tuple(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let arguments = input.arguments;
    let argument = {
        if arguments.len() < 2 {
            return Err(PlanError::invalid(
                "json_tuple requires at least 2 arguments",
            ));
        }
        for field in &arguments[1..] {
            if !matches!(
                field.kind,
                crate::analyzer::deferred::Deferred::Resolved(ExprKind::Literal(
                    ScalarValue::Utf8(Some(_))
                ))
            ) {
                return Err(PlanError::invalid(
                    "json_tuple field names must be string literals",
                ));
            }
        }
        let tuple =
            context.scalar_udf(datafusion_spark::function::json::json_tuple(), arguments)?;
        context.scalar_udf(
            Arc::new(ScalarUDF::from(
                sail_function::scalar::array::spark_array::SparkArray::new(),
            )),
            vec![tuple],
        )?
    };
    explode_argument(context, argument, false, true)
}
