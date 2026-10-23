use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, IntervalUnit, TimeUnit};
use datafusion_common::{Result, exec_datafusion_err, exec_err};
use datafusion_expr::{
    HigherOrderReturnFieldArgs, LambdaParametersProgress, ReturnFieldArgs, ScalarUDFImpl,
    ValueOrLambda,
};
pub fn check_lazy_sequence_args<V, L>(args: &[ValueOrLambda<V, L>]) -> Result<()> {
    if !matches!(args.len(), 2 | 3) {
        return invalid_sequence_arity(args.len());
    }
    if args
        .iter()
        .any(|arg| matches!(arg, ValueOrLambda::Value(_)))
    {
        return exec_err!("spark_sequence expected lambda arguments");
    }
    Ok(())
}
pub fn lambda_parameters(
    _step: usize,
    fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
) -> Result<LambdaParametersProgress> {
    check_lazy_sequence_args(fields)?;
    let dummy = Arc::new(Field::new("", DataType::Null, true));
    Ok(LambdaParametersProgress::Complete(
        fields.iter().map(|_| vec![Arc::clone(&dummy)]).collect(),
    ))
}
pub fn return_field_from_args(
    inner: &dyn ScalarUDFImpl,
    args: HigherOrderReturnFieldArgs,
) -> Result<FieldRef> {
    check_lazy_sequence_args(args.arg_fields)?;
    let fields = args
        .arg_fields
        .iter()
        .map(|arg| match arg {
            ValueOrLambda::Lambda(field) => Ok(Arc::clone(field)),
            ValueOrLambda::Value(_) => exec_err!("spark_sequence expected lambda arguments"),
        })
        .collect::<Result<Vec<_>>>()?;

    ScalarUDFImpl::return_field_from_args(
        inner,
        ReturnFieldArgs {
            arg_fields: &fields,
            scalar_arguments: args.scalar_arguments,
        },
    )
}
use crate::policies::scalar_array_spark_sequence_typing::invalid_sequence_arity;
pub fn sequence_wrong_input_types_error(
    arg_types: &[DataType],
) -> datafusion_common::DataFusionError {
    exec_datafusion_err!(
        "[DATATYPE_MISMATCH.SEQUENCE_WRONG_INPUT_TYPES] Spark `sequence` function cannot accept argument types ({})",
        arg_types
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    )
}
pub fn wrong_sequence_input_types<T>(arg_types: &[DataType]) -> Result<T> {
    Err(sequence_wrong_input_types_error(arg_types))
}
pub fn normalize_sequence_type(
    data_type: &DataType,
    session_timezone: &Arc<str>,
) -> Result<DataType> {
    match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Null => {
            Ok(data_type.clone())
        }
        DataType::UInt8 => Ok(DataType::Int16),
        DataType::UInt16 => Ok(DataType::Int32),
        DataType::UInt32 | DataType::UInt64 => Ok(DataType::Int64),
        DataType::Date32 | DataType::Date64 => Ok(DataType::Date32),
        DataType::Timestamp(_, None) => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        DataType::Timestamp(_, Some(_)) => Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::clone(session_timezone)),
        )),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(DataType::Utf8),
        DataType::Duration(_) => Ok(DataType::Duration(TimeUnit::Microsecond)),
        DataType::Interval(_) => Ok(data_type.clone()),
        _ => wrong_sequence_input_types(std::slice::from_ref(data_type)),
    }
}
pub fn wider_sequence_type(left: &DataType, right: &DataType, ansi_mode: bool) -> Option<DataType> {
    if left == right {
        return Some(left.clone());
    }
    if left.is_null() {
        return Some(right.clone());
    }
    if right.is_null() {
        return Some(left.clone());
    }

    if let (Some(left_rank), Some(right_rank)) = (integral_rank(left), integral_rank(right)) {
        return Some(integral_type(left_rank.max(right_rank)));
    }

    match (left, right) {
        (DataType::Date32, DataType::Timestamp(TimeUnit::Microsecond, timezone))
        | (DataType::Timestamp(TimeUnit::Microsecond, timezone), DataType::Date32) => {
            Some(DataType::Timestamp(TimeUnit::Microsecond, timezone.clone()))
        }
        (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)),
        )
        | (
            DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ) => Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::clone(timezone)),
        )),
        (DataType::Utf8, other) | (other, DataType::Utf8) => {
            if !ansi_mode {
                return Some(DataType::Utf8);
            }
            if integral_rank(other).is_some() {
                Some(DataType::Int64)
            } else {
                match other {
                    DataType::Date32 => Some(DataType::Date32),
                    DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
                        Some(DataType::Timestamp(TimeUnit::Microsecond, timezone.clone()))
                    }
                    _ => None,
                }
            }
        }
        _ => None,
    }
}
pub fn integral_rank(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Int8 => Some(0),
        DataType::Int16 => Some(1),
        DataType::Int32 => Some(2),
        DataType::Int64 => Some(3),
        _ => None,
    }
}
pub fn integral_type(rank: u8) -> DataType {
    match rank {
        0 => DataType::Int8,
        1 => DataType::Int16,
        2 => DataType::Int32,
        _ => DataType::Int64,
    }
}
pub fn is_temporal_bound_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32 | DataType::Timestamp(TimeUnit::Microsecond, _)
    )
}
pub fn is_temporal_step_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Interval(
            IntervalUnit::MonthDayNano | IntervalUnit::YearMonth | IntervalUnit::DayTime
        ) | DataType::Duration(TimeUnit::Microsecond)
    )
}
pub fn validate_sequence_types(arg_types: &[DataType]) -> Result<()> {
    let [start, stop, step @ ..] = arg_types else {
        return invalid_sequence_arity(arg_types.len());
    };
    if start != stop {
        return wrong_sequence_input_types(arg_types);
    }

    if integral_rank(start).is_some() {
        if step.is_empty() || step.first() == Some(start) {
            return Ok(());
        }
    } else if is_temporal_bound_type(start)
        && (step.is_empty() || step.first().is_some_and(is_temporal_step_type))
    {
        return Ok(());
    }

    wrong_sequence_input_types(arg_types)
}
pub fn coerce_types(
    session_timezone: &Arc<str>,
    ansi_mode: bool,
    arg_types: &[DataType],
) -> Result<Vec<DataType>> {
    if !matches!(arg_types.len(), 2 | 3) {
        return invalid_sequence_arity(arg_types.len());
    }

    let mut coerced = arg_types
        .iter()
        .map(|data_type| normalize_sequence_type(data_type, session_timezone))
        .collect::<Result<Vec<_>>>()?;

    let mut selected = vec![0, 1];
    if coerced.len() == 3 && !is_temporal_step_type(&coerced[2]) {
        selected.push(2);
    }

    let mut common_type = coerced[selected[0]].clone();
    for index in selected.iter().copied().skip(1) {
        common_type = wider_sequence_type(&common_type, &coerced[index], ansi_mode)
            .ok_or_else(|| sequence_wrong_input_types_error(&coerced))?;
    }
    for index in selected {
        coerced[index] = common_type.clone();
    }

    validate_sequence_types(&coerced)?;
    Ok(coerced)
}
