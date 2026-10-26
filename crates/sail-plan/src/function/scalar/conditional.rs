use std::sync::Arc;

use arrow_schema::{DataType, TimeUnit};
use datafusion::functions::{core, math};
use datafusion_common::ScalarValue;
use datafusion_expr::{Operator, ScalarUDF};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_cast_string_to_int32::SparkCastStringToInt32;
use sail_function::scalar::spark_to_string::SparkToUtf8;

use crate::analyzer::model::Expr;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{FunctionContextInput, ScalarFunction, ScalarFunctionInput};

fn case(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let mut conditions = vec![];
    let mut branches = vec![];
    let mut values = values.into_iter();
    while let Some(condition) = values.next() {
        if let Some(value) = values.next() {
            conditions.push(condition);
            branches.push(value);
        } else {
            conditions.push(context.literal(ScalarValue::Boolean(Some(true))));
            branches.push(condition);
        }
    }
    let branches = temporal_branches(context, branches)?;
    context.case(conditions.into_iter().zip(branches).collect(), None)
}

fn if_expr(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let (condition, yes, no) = values.three()?;
    let (yes, no) = temporal_branches(context, vec![yes, no])?.two()?;
    context.case(vec![(condition, yes)], Some(no))
}

fn coalesce(mut input: ScalarFunctionInput<'_>, name: &str) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    if name != "coalesce" && values.len() != 2 {
        return Err(PlanError::analysis(format!("{name} requires 2 parameters")));
    }
    let values = temporal_branches(context, values)?;
    context.scalar_udf(core::coalesce(), values)
}

fn nullifzero(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let value = values.one()?;
    let data_type = &value.result_type.resolved("nullifzero input")?.data_type;
    let copy = context.copy_expression(&value)?;
    let condition = if data_type.is_string() {
        let copy = if context.config.ansi_mode {
            context.cast(copy, DataType::Int64)?
        } else {
            context.scalar_udf(
                Arc::new(ScalarUDF::from(SparkCastStringToInt32::new())),
                vec![copy],
            )?
        };
        let zero = context.literal(ScalarValue::Int32(Some(0)));
        context.binary(copy, Operator::Eq, zero)?
    } else if *data_type == DataType::Boolean && !context.config.ansi_mode {
        let no = context.literal(ScalarValue::Boolean(Some(false)));
        context.binary(copy, Operator::Eq, no)?
    } else if data_type.is_numeric() || data_type.is_null() {
        let zero = context.literal(ScalarValue::Int32(Some(0)));
        context.binary(copy, Operator::Eq, zero)?
    } else {
        return Err(PlanError::invalid(format!(
            "nullifzero cannot compare data type {data_type} with an integer zero"
        )));
    };
    let null = context.literal(ScalarValue::Null);
    context.case(vec![(condition, null)], Some(value))
}

fn zeroifnull(mut input: ScalarFunctionInput<'_>) -> PlanResult<Expr> {
    let context = &mut input.context;
    let values = input.arguments;

    let value = values.one()?;
    let data_type = value
        .result_type
        .resolved("zeroifnull input")?
        .data_type
        .clone();
    let ansi = context.config.ansi_mode;
    let mut zero = context.literal(ScalarValue::Int32(Some(0)));
    let value = match data_type {
        DataType::Float32 if ansi => context.cast(value, DataType::Float64)?,
        data_type if data_type.is_string() && ansi => context.cast(value, DataType::Int64)?,
        data_type if data_type.is_string() => {
            zero = context.literal(ScalarValue::Utf8(Some("0".into())));
            value
        }
        DataType::Decimal128(precision, scale) => {
            let integer_digits = (i16::from(precision) - i16::from(scale)).max(10);
            let scale = i16::from(scale).max(0);
            let precision = (integer_digits + scale).min(38) as u8;
            let scale = scale.min((38 - integer_digits).max(0)) as i8;
            let target = DataType::Decimal128(precision, scale);
            if ansi {
                context.cast(value, target)?
            } else {
                context.try_cast(value, target)?
            }
        }
        data_type if data_type.is_numeric() || data_type.is_null() => value,
        data_type => {
            return Err(PlanError::invalid(format!(
                "zeroifnull cannot combine data type {data_type} with an integer zero"
            )));
        }
    };
    context.scalar_udf(core::coalesce(), vec![value, zero])
}

pub(crate) fn temporal_branches(
    context: &mut FunctionContextInput<'_>,
    values: Vec<Expr>,
) -> PlanResult<Vec<Expr>> {
    let types = values
        .iter()
        .map(|v| {
            Ok(v.result_type
                .resolved("conditional result")?
                .data_type
                .clone())
        })
        .collect::<PlanResult<Vec<_>>>()?;
    if !types.iter().any(DataType::is_string) {
        return Ok(values);
    }
    let Some(target) = common_temporal_type(&types, &context.config.session_timezone) else {
        return Ok(values);
    };
    values
        .into_iter()
        .zip(types)
        .map(|(value, source)| {
            let temporal = matches!(
                source,
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(..)
            );
            if context.config.ansi_mode {
                if source == target {
                    return Ok(value);
                }
                if source.is_string() {
                    let function = match &target {
                        DataType::Date32 => ScalarUDF::from(SparkDate::new(false)),
                        DataType::Timestamp(_, timezone) => {
                            ScalarUDF::from(SparkTimestamp::try_new(timezone.clone(), true, false)?)
                        }
                        _ => return context.cast(value, target.clone()),
                    };
                    context.scalar_udf(Arc::new(function), vec![value])
                } else if temporal {
                    context.cast(value, target.clone())
                } else {
                    Ok(value)
                }
            } else if temporal {
                context.scalar_udf(Arc::new(ScalarUDF::from(SparkToUtf8::new())), vec![value])
            } else {
                Ok(value)
            }
        })
        .collect()
}

fn common_temporal_type(types: &[DataType], timezone: &Arc<str>) -> Option<DataType> {
    if types
        .iter()
        .any(|t| matches!(t, DataType::Timestamp(_, Some(_))))
    {
        Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(timezone.clone()),
        ))
    } else if types
        .iter()
        .any(|t| matches!(t, DataType::Timestamp(_, None)))
    {
        Some(DataType::Timestamp(TimeUnit::Microsecond, None))
    } else {
        types
            .iter()
            .any(|t| matches!(t, DataType::Date32 | DataType::Date64))
            .then_some(DataType::Date32)
    }
}

pub(super) fn list_built_in_conditional_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("coalesce", F::custom(|input| coalesce(input, "coalesce"))),
        ("if", F::custom(if_expr)),
        ("ifnull", F::custom(|input| coalesce(input, "ifnull"))),
        ("nanvl", F::scalar_udf(math::nanvl)),
        ("nullif", F::scalar_udf(core::nullif)),
        ("nullifzero", F::custom(nullifzero)),
        ("nvl", F::custom(|input| coalesce(input, "nvl"))),
        ("nvl2", F::scalar_udf(core::nvl2)),
        ("zeroifnull", F::custom(zeroifnull)),
        ("when", F::custom(case)),
        ("case", F::custom(case)),
    ]
}
