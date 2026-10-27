use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef, Fields, TimeUnit};
use datafusion_common::DFSchema;
use datafusion_expr::{ExprSchemable, ScalarUDF, cast, expr, lit};
use sail_common::utils::datetime::is_fixed_zero_offset_timezone;
use sail_function::scalar::datetime::convert_tz::ConvertTz;

use crate::error::PlanResult;
use crate::function::common::ScalarFunctionInput;

/// Spark's cast of TIMESTAMP_NTZ to TIMESTAMP, which reads the value as a wall-clock time in
/// `timezone` and resolves DST gaps and overlaps like Spark.
pub(crate) fn cast_timestamp_ntz_to_ltz(
    expression: expr::Expr,
    timezone: &Arc<str>,
    is_try: bool,
) -> expr::Expr {
    if is_fixed_zero_offset_timezone(timezone) {
        // The wall-clock time is the instant. Arrow's cast would compute it with Chrono, which
        // does not represent Spark's full range.
        let micros = cast(
            cast(expression, DataType::Timestamp(TimeUnit::Microsecond, None)),
            DataType::Int64,
        );
        return cast(
            micros,
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::clone(timezone))),
        );
    }
    ScalarUDF::from(ConvertTz::cast(is_try).with_output_timezone(Some(Arc::clone(timezone)))).call(
        vec![
            lit(timezone.to_string()),
            lit("UTC"),
            cast(expression, DataType::Timestamp(TimeUnit::Microsecond, None)),
        ],
    )
}

/// Spark's cast of TIMESTAMP to TIMESTAMP_NTZ, which takes the wall-clock time of the value in
/// `timezone`. Arrow would take the wall-clock time in UTC.
pub(crate) fn cast_timestamp_ltz_to_ntz(
    expression: expr::Expr,
    timezone: &Arc<str>,
    is_try: bool,
) -> expr::Expr {
    let utc = cast(expression, DataType::Timestamp(TimeUnit::Microsecond, None));
    if is_fixed_zero_offset_timezone(timezone) {
        return utc;
    }
    ScalarUDF::from(ConvertTz::cast(is_try)).call(vec![lit("UTC"), lit(timezone.to_string()), utc])
}

/// Spark's common type for TIMESTAMP_NTZ and TIMESTAMP values is TIMESTAMP, which Sail labels with
/// the session time zone. DataFusion would read TIMESTAMP_NTZ values as UTC and keep the TIMESTAMP
/// values' own zone label, which other functions read them in.
pub(crate) fn coerce_timestamp_ntz_values(
    values: Vec<expr::Expr>,
    schema: &DFSchema,
    session_timezone: &Arc<str>,
) -> PlanResult<Vec<expr::Expr>> {
    let types = values
        .iter()
        .map(|value| value.get_type(schema))
        .collect::<Result<Vec<_>, _>>()?;
    if is_timestamp_ntz_mix(&types) {
        return Ok(values
            .into_iter()
            .zip(types)
            .map(|(value, data_type)| {
                cast_to_session_timestamp(value, &data_type, session_timezone)
            })
            .collect());
    }
    // Nested values mix TIMESTAMP_NTZ and TIMESTAMP in the same position.
    let mut common = None::<(DataType, bool)>;
    for data_type in types.iter().filter(|data_type| !data_type.is_null()) {
        common = Some(match common {
            None => (data_type.clone(), false),
            Some((common, mixed)) => {
                let Some((common, other)) =
                    common_timestamp_type(&common, data_type, session_timezone)
                else {
                    return Ok(values);
                };
                (common, mixed || other)
            }
        });
    }
    let Some((target, true)) = common else {
        return Ok(values);
    };
    Ok(values
        .into_iter()
        .zip(types)
        .map(|(value, data_type)| {
            cast_to_common_timestamp_type(value, &data_type, &target, session_timezone)
        })
        .collect())
}

/// Whether the types mix TIMESTAMP_NTZ with TIMESTAMP.
pub(crate) fn is_timestamp_ntz_mix(types: &[DataType]) -> bool {
    types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, None)))
        && types
            .iter()
            .any(|data_type| matches!(data_type, DataType::Timestamp(_, Some(_))))
}

/// Casts a TIMESTAMP_NTZ or TIMESTAMP value to TIMESTAMP in the session time zone, leaving other
/// values to type coercion.
pub(crate) fn cast_to_session_timestamp(
    value: expr::Expr,
    data_type: &DataType,
    session_timezone: &Arc<str>,
) -> expr::Expr {
    let target = DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::clone(session_timezone)));
    match data_type {
        DataType::Timestamp(_, None) => cast_timestamp_ntz_to_ltz(value, session_timezone, false),
        DataType::Timestamp(_, Some(_)) if data_type != &target => cast(value, target),
        _ => value,
    }
}

/// A variadic function whose TIMESTAMP_NTZ and TIMESTAMP arguments are cast to their common type.
pub(crate) fn with_timestamp_ntz_coercion<F>(
    f: F,
) -> impl Fn(ScalarFunctionInput) -> PlanResult<expr::Expr> + Send + Sync + 'static
where
    F: Fn(Vec<expr::Expr>) -> expr::Expr + Send + Sync + 'static,
{
    move |ScalarFunctionInput {
              arguments,
              function_context,
          }| {
        Ok(f(coerce_timestamp_ntz_values(
            arguments,
            function_context.schema,
            &function_context.plan_config.session_timezone,
        )?))
    }
}

/// Spark's common type for TIMESTAMP_NTZ and TIMESTAMP values in the same position of two types,
/// such as the elements of two arrays, which is TIMESTAMP in the session time zone. Returns `None`
/// when no position mixes them.
pub(crate) fn common_timestamp_ntz_type(
    left: &DataType,
    right: &DataType,
    session_timezone: &Arc<str>,
) -> Option<DataType> {
    match common_timestamp_type(left, right, session_timezone)? {
        (data_type, true) => Some(data_type),
        (_, false) => None,
    }
}

/// The common type of two types whose TIMESTAMP values are in the session time zone, and whether
/// a position mixes TIMESTAMP_NTZ and TIMESTAMP.
fn common_timestamp_type(
    left: &DataType,
    right: &DataType,
    session_timezone: &Arc<str>,
) -> Option<(DataType, bool)> {
    let session_timestamp =
        || DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::clone(session_timezone)));
    let field = |left: &FieldRef, right: &FieldRef| {
        common_timestamp_type(left.data_type(), right.data_type(), session_timezone).map(
            |(data_type, mixed)| {
                (
                    Arc::new(left.as_ref().clone().with_data_type(data_type)),
                    mixed,
                )
            },
        )
    };
    match (left, right) {
        _ if left == right => Some((left.clone(), false)),
        (DataType::Timestamp(_, None), DataType::Timestamp(_, Some(_)))
        | (DataType::Timestamp(_, Some(_)), DataType::Timestamp(_, None)) => {
            Some((session_timestamp(), true))
        }
        (DataType::Timestamp(_, Some(_)), DataType::Timestamp(_, Some(_))) => {
            Some((session_timestamp(), false))
        }
        (DataType::List(left), DataType::List(right)) => {
            field(left, right).map(|(field, mixed)| (DataType::List(field), mixed))
        }
        (DataType::LargeList(left), DataType::LargeList(right)) => {
            field(left, right).map(|(field, mixed)| (DataType::LargeList(field), mixed))
        }
        (DataType::Map(left, sorted), DataType::Map(right, _)) => {
            field(left, right).map(|(field, mixed)| (DataType::Map(field, *sorted), mixed))
        }
        (DataType::Struct(left), DataType::Struct(right)) if left.len() == right.len() => {
            let fields = left
                .iter()
                .zip(right.iter())
                .map(|(left, right)| (left.name() == right.name()).then(|| field(left, right))?)
                .collect::<Option<Vec<_>>>()?;
            let mixed = fields.iter().any(|(_, mixed)| *mixed);
            Some((
                DataType::Struct(
                    fields
                        .into_iter()
                        .map(|(field, _)| field)
                        .collect::<Fields>(),
                ),
                mixed,
            ))
        }
        _ => None,
    }
}

/// Casts a value to its common type with TIMESTAMP_NTZ and TIMESTAMP values.
pub(crate) fn cast_to_common_timestamp_type(
    value: expr::Expr,
    data_type: &DataType,
    target: &DataType,
    session_timezone: &Arc<str>,
) -> expr::Expr {
    match data_type {
        DataType::Timestamp(_, _) => cast_to_session_timestamp(value, data_type, session_timezone),
        _ if data_type == target => value,
        _ => cast(value, target.clone()),
    }
}

/// Casts two arrays whose elements mix TIMESTAMP_NTZ and TIMESTAMP values to their common type.
pub(crate) fn coerce_timestamp_ntz_arrays(
    left: expr::Expr,
    right: expr::Expr,
    schema: &DFSchema,
    session_timezone: &Arc<str>,
) -> PlanResult<(expr::Expr, expr::Expr)> {
    let (left_type, right_type) = (left.get_type(schema)?, right.get_type(schema)?);
    let Some(target) = common_timestamp_ntz_type(&left_type, &right_type, session_timezone) else {
        return Ok((left, right));
    };
    Ok((
        cast_to_common_timestamp_type(left, &left_type, &target, session_timezone),
        cast_to_common_timestamp_type(right, &right_type, &target, session_timezone),
    ))
}

/// Casts an array and an element that mix TIMESTAMP_NTZ and TIMESTAMP values to their common type.
pub(crate) fn coerce_timestamp_ntz_array_element(
    array: expr::Expr,
    element: expr::Expr,
    schema: &DFSchema,
    session_timezone: &Arc<str>,
) -> PlanResult<(expr::Expr, expr::Expr)> {
    let array_type = array.get_type(schema)?;
    let (DataType::List(field) | DataType::LargeList(field)) = &array_type else {
        return Ok((array, element));
    };
    let element_type = element.get_type(schema)?;
    let Some(target) =
        common_timestamp_ntz_type(field.data_type(), &element_type, session_timezone)
    else {
        return Ok((array, element));
    };
    let field = Arc::new(field.as_ref().clone().with_data_type(target.clone()));
    let array_target = match array_type {
        DataType::LargeList(_) => DataType::LargeList(field),
        _ => DataType::List(field),
    };
    Ok((
        cast(array, array_target),
        cast_to_common_timestamp_type(element, &element_type, &target, session_timezone),
    ))
}

/// Labels a TIMESTAMP value with the session time zone, in which Spark reads its fields.
pub(crate) fn with_session_timezone_label(
    value: expr::Expr,
    schema: &DFSchema,
    session_timezone: &Arc<str>,
) -> PlanResult<expr::Expr> {
    Ok(match value.get_type(schema)? {
        DataType::Timestamp(unit, Some(timezone)) if timezone != *session_timezone => cast(
            value,
            DataType::Timestamp(unit, Some(Arc::clone(session_timezone))),
        ),
        _ => value,
    })
}
