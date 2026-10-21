use std::collections::HashSet;
use std::sync::Arc;

use chrono::{Offset, TimeZone};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use datafusion_common::{Result as DataFusionResult, ScalarValue};
use datafusion_expr::expr::{HigherOrderFunction, Lambda, LambdaVariable};
use datafusion_expr::type_coercion::binary::comparison_coercion;
use datafusion_expr::{ExprSchemable, HigherOrderUDF, ScalarUDF, cast, expr, lit, try_cast};
use sail_common::spec;
use sail_common::utils::datetime::is_fixed_zero_offset_timezone;
use sail_function::scalar::array::spark_array_transform::SparkArrayTransform;
use sail_function::scalar::datetime::convert_tz::ConvertTz;
use sail_function::scalar::datetime::spark_start_of_day::SparkStartOfDay;
use sail_function::scalar::datetime::spark_timestamp_to_number::SparkTimestampToNumber;
use sail_function::scalar::map::map_entries::SparkMapEntries;
use sail_function::scalar::map::map_from::SparkMapFromEntries;
use sail_function::scalar::map::spark_transform_keys::SparkTransformKeys;
use sail_function::scalar::map::spark_transform_values::SparkTransformValues;
use sail_function::scalar::spark_struct_cast::SparkStructCast;
use sail_function::scalar::spark_struct_rename::SparkStructRename;

use crate::config::MapKeyDedupPolicy;
use crate::error::PlanResult;
use crate::function::common::FunctionContextInput;
use crate::function::is_spark_compatible_arrow_fixed_offset;

/// Cast temporal values using the session zone and Spark's gap/overlap resolution.
pub(crate) fn cast_temporal(
    expression: expr::Expr,
    from: &DataType,
    to: DataType,
    session_timezone: &Arc<str>,
    is_try: bool,
    map_key_dedup_policy: Option<MapKeyDedupPolicy>,
) -> DataFusionResult<expr::Expr> {
    if from == &to {
        // Pin nested types through folding; physical planning elides matching native casts.
        return Ok(if to.is_nested() {
            if supports_nested_identity_cast(&to) {
                cast(expression, to)
            } else {
                ScalarUDF::from(SparkStructRename::new(to)).call(vec![expression])
            }
        } else {
            expression
        });
    }
    match (from, &to) {
        (DataType::Map(source, _), DataType::Map(target, _)) => {
            let (DataType::Struct(source), DataType::Struct(target)) =
                (source.data_type(), target.data_type())
            else {
                return Ok(cast(expression, to));
            };
            let mut result = expression;
            let mut converted = false;
            for (index, (source, target)) in source.iter().zip(target).enumerate() {
                if source.data_type() == target.data_type() {
                    continue;
                }
                converted |= with_session_timezone(source.data_type(), session_timezone)
                    != with_session_timezone(target.data_type(), session_timezone);
                let parameters = vec![
                    "__sail_timestamp_key".to_string(),
                    "__sail_timestamp_value".to_string(),
                ];
                let variable = expr::Expr::LambdaVariable(LambdaVariable::new(
                    parameters[index].clone(),
                    Some(Arc::clone(source)),
                ));
                let body = cast_temporal(
                    variable,
                    source.data_type(),
                    target.data_type().clone(),
                    session_timezone,
                    is_try,
                    map_key_dedup_policy,
                )?;
                let function = if index == 0 {
                    HigherOrderUDF::new_from_impl(
                        SparkTransformKeys::new(false).with_preserve_duplicates(true),
                    )
                } else {
                    HigherOrderUDF::new_from_impl(SparkTransformValues::new())
                };
                result = expr::Expr::HigherOrderFunction(HigherOrderFunction::new(
                    Arc::new(function),
                    vec![result, expr::Expr::Lambda(Lambda::new(parameters, body))],
                ));
            }
            if let Some(policy) = map_key_dedup_policy.filter(|_| converted) {
                // Writes validate keys after all values are converted, including discarded entries.
                let entries = ScalarUDF::from(SparkMapEntries::new()).call(vec![result]);
                result = ScalarUDF::from(SparkMapFromEntries::new(
                    policy == MapKeyDedupPolicy::LastWin,
                ))
                .call(vec![entries]);
            }
            Ok(cast(result, to))
        }
        (
            DataType::List(source) | DataType::LargeList(source),
            DataType::List(target) | DataType::LargeList(target),
        ) if common_temporal_type(
            &[source.data_type().clone(), target.data_type().clone()],
            session_timezone,
        )
        .is_some() =>
        {
            let parameter = "__sail_timestamp_element".to_string();
            let variable = expr::Expr::LambdaVariable(LambdaVariable::new(
                parameter.clone(),
                Some(Arc::clone(source)),
            ));
            let body = cast_temporal(
                variable,
                source.data_type(),
                target.data_type().clone(),
                session_timezone,
                is_try,
                map_key_dedup_policy,
            )?;
            let transformed = expr::Expr::HigherOrderFunction(HigherOrderFunction::new(
                Arc::new(HigherOrderUDF::new_from_impl(SparkArrayTransform::new())),
                vec![
                    expression,
                    expr::Expr::Lambda(Lambda::new(vec![parameter], body)),
                ],
            ));
            if matches!(
                (from, &to),
                (DataType::List(_), DataType::List(_))
                    | (DataType::LargeList(_), DataType::LargeList(_))
            ) {
                // A name-based nested cast would collapse duplicate struct fields.
                Ok(ScalarUDF::from(SparkStructRename::new(to)).call(vec![transformed]))
            } else {
                Ok(cast(transformed, to))
            }
        }
        (DataType::Struct(source), DataType::Struct(target))
            if source.len() == target.len()
                && source
                    .iter()
                    .zip(target)
                    .all(|(source, target)| source.name() == target.name()) =>
        {
            let mut arguments = vec![expression];
            for (source, target) in source.iter().zip(target) {
                let parameter = "field".to_string();
                let variable = expr::Expr::LambdaVariable(LambdaVariable::new(
                    parameter.clone(),
                    Some(Arc::clone(source)),
                ));
                let body = cast_temporal(
                    variable,
                    source.data_type(),
                    target.data_type().clone(),
                    session_timezone,
                    is_try,
                    map_key_dedup_policy,
                )?;
                arguments.push(expr::Expr::Lambda(Lambda::new(vec![parameter], body)));
            }
            Ok(expr::Expr::HigherOrderFunction(HigherOrderFunction::new(
                Arc::new(HigherOrderUDF::new_from_impl(SparkStructCast::new(
                    target.clone(),
                ))),
                arguments,
            )))
        }
        (
            DataType::Timestamp(_, None) | DataType::Date32 | DataType::Date64,
            DataType::Timestamp(_, Some(_)),
        ) => {
            // TRY casts must handle overflow when promoting dates or seconds/milliseconds to micros.
            let expression = if is_try
                && matches!(
                    from,
                    DataType::Date32
                        | DataType::Date64
                        | DataType::Timestamp(TimeUnit::Second | TimeUnit::Millisecond, None)
                ) {
                try_cast(expression, DataType::Timestamp(TimeUnit::Microsecond, None))
            } else {
                expression
            };
            let instant =
                cast_timestamp_ntz_to_ltz(expression, Arc::clone(session_timezone), is_try)?;
            let instant = if from == &DataType::Date32
                && !is_spark_compatible_arrow_fixed_offset(session_timezone)
            {
                // A midnight inside a gap maps to the day's first valid time, not a shifted midnight.
                ScalarUDF::from(SparkStartOfDay::new()).call(vec![instant])
            } else {
                instant
            };
            Ok(cast(instant, to))
        }
        (
            DataType::Timestamp(_, Some(_)),
            DataType::Timestamp(_, None) | DataType::Date32 | DataType::Date64,
        ) => {
            let localized = cast(
                shift_timestamp_timezone(
                    expression,
                    session_timezone,
                    is_try,
                    false,
                    matches!(to, DataType::Timestamp(_, None)),
                )?,
                DataType::Timestamp(TimeUnit::Microsecond, None),
            );
            Ok(cast(localized, to))
        }
        _ if is_try => Ok(try_cast(expression, to)),
        _ => Ok(cast(expression, to)),
    }
}

fn supports_nested_identity_cast(data_type: &DataType) -> bool {
    match data_type {
        DataType::Struct(fields) => {
            // Native nested casts match by name and reject empty structs.
            let mut names = HashSet::new();
            !fields.is_empty()
                && fields.iter().all(|field| {
                    names.insert(field.name()) && supports_nested_identity_cast(field.data_type())
                })
        }
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::Map(field, _) => supports_nested_identity_cast(field.data_type()),
        _ => !data_type.is_nested(),
    }
}

/// The common type for existing date/timestamp operands, including array elements.
/// Other coercions (notably ANSI/legacy string promotion) remain with their callers.
pub(crate) fn common_temporal_type(
    data_types: &[DataType],
    session_timezone: &Arc<str>,
) -> Option<DataType> {
    let types = data_types
        .iter()
        .filter(|data_type| !data_type.is_null())
        .collect::<Vec<_>>();
    if types.is_empty() {
        return None;
    }
    if let DataType::Map(first, _) = types[0] {
        let entries = types
            .iter()
            .map(|data_type| match data_type {
                DataType::Map(entries, _) => Some(entries.data_type().clone()),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        let entries = common_temporal_type(&entries, session_timezone)?;
        return Some(DataType::Map(
            Arc::new(first.as_ref().clone().with_data_type(entries)),
            false,
        ));
    }
    if let DataType::Struct(first) = types[0] {
        let structs = types
            .iter()
            .map(|data_type| match data_type {
                DataType::Struct(fields) if fields.len() == first.len() => Some(fields),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        let mut has_temporal = false;
        let fields = first
            .iter()
            .enumerate()
            .map(|(index, first)| {
                if structs
                    .iter()
                    .any(|fields| fields[index].name() != first.name())
                {
                    return None;
                }
                let field_types = structs
                    .iter()
                    .map(|fields| fields[index].data_type().clone())
                    .filter(|data_type| !data_type.is_null())
                    .collect::<Vec<_>>();
                let first_type = field_types.first().unwrap_or(first.data_type());
                let target =
                    if let Some(target) = common_temporal_type(&field_types, session_timezone) {
                        has_temporal = true;
                        target
                    } else if field_types.iter().all(|data_type| data_type == first_type) {
                        first_type.clone()
                    } else if field_types.iter().all(DataType::is_numeric) {
                        field_types
                            .iter()
                            .skip(1)
                            .try_fold(field_types[0].clone(), |previous, current| {
                                comparison_coercion(&previous, current)
                            })?
                    } else {
                        // TODO: Preserve temporal conversion when sibling fields need string coercion.
                        return None;
                    };
                let mut field = first
                    .as_ref()
                    .clone()
                    .with_data_type(target)
                    .with_nullable(structs.iter().any(|fields| fields[index].is_nullable()));
                if matches!(field.data_type(), DataType::Duration(TimeUnit::Microsecond)) {
                    let key = spec::SAIL_SPARK_INTERVAL_METADATA_KEY;
                    let mut qualifier: Option<spec::SparkIntervalMetadata> = None;
                    let mut all_qualified = true;
                    for fields in &structs {
                        let input = &fields[index];
                        if input.data_type().is_null() {
                            continue;
                        }
                        let Some(value) = input.metadata().get(key) else {
                            all_qualified = false;
                            continue;
                        };
                        let current = spec::SparkIntervalMetadata::from_json(value).ok()?;
                        qualifier = Some(match qualifier {
                            Some(previous) => previous.wider(current)?,
                            None => current,
                        });
                    }
                    field.metadata_mut().remove(key);
                    if let Some(qualifier) = qualifier.filter(|_| all_qualified) {
                        field
                            .metadata_mut()
                            .insert(key.to_owned(), qualifier.to_json().ok()?);
                    }
                }
                Some(Arc::new(field))
            })
            .collect::<Option<Fields>>()?;
        return has_temporal.then_some(DataType::Struct(fields));
    }
    if types
        .iter()
        .all(|data_type| matches!(data_type, DataType::List(_) | DataType::LargeList(_)))
    {
        let nullable = types.iter().any(|data_type| matches!(data_type, DataType::List(field) | DataType::LargeList(field) if field.is_nullable()));
        let elements = types
            .iter()
            .filter_map(|data_type| match data_type {
                DataType::List(field) | DataType::LargeList(field) => {
                    Some(field.data_type().clone())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        return common_temporal_type(&elements, session_timezone)
            .map(|element| DataType::List(Arc::new(Field::new("item", element, nullable))));
    }
    if !types.iter().all(|data_type| {
        matches!(
            data_type,
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
        )
    }) {
        return None;
    }
    if types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, Some(_))))
    {
        Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::clone(session_timezone)),
        ))
    } else if types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, None)))
    {
        Some(DataType::Timestamp(TimeUnit::Microsecond, None))
    } else {
        None
    }
}

/// Arrow carries display zones in nested fields; Spark uses the session zone for every LTZ value.
pub(crate) fn with_session_timezone(data_type: &DataType, timezone: &Arc<str>) -> DataType {
    let field = |field: &Arc<Field>| {
        Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(with_session_timezone(field.data_type(), timezone)),
        )
    };
    match data_type {
        DataType::Timestamp(unit, Some(_)) => {
            DataType::Timestamp(*unit, Some(Arc::clone(timezone)))
        }
        DataType::List(value) => DataType::List(field(value)),
        DataType::LargeList(value) => DataType::LargeList(field(value)),
        DataType::FixedSizeList(value, size) => DataType::FixedSizeList(field(value), *size),
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(field).collect()),
        DataType::Map(entries, sorted) => DataType::Map(field(entries), *sorted),
        _ => data_type.clone(),
    }
}

pub(crate) fn coerce_temporal_arguments(
    arguments: Vec<expr::Expr>,
    context: &FunctionContextInput<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let types = arguments
        .iter()
        .map(|argument| argument.get_type(context.schema))
        .collect::<DataFusionResult<Vec<_>>>()?;
    let timezone = &context.plan_config.session_timezone;
    let Some(target) = common_temporal_type(&types, timezone) else {
        return Ok(arguments);
    };
    arguments
        .into_iter()
        .zip(types)
        .map(|(argument, source)| {
            Ok(cast_temporal(
                argument,
                &source,
                target.clone(),
                timezone,
                false,
                None,
            )?)
        })
        .collect()
}

pub(crate) fn timestamp_epoch_units(
    value: expr::Expr,
    divisor: i64,
) -> DataFusionResult<expr::Expr> {
    let value = cast(value, DataType::Int64);
    if divisor == 1 {
        return Ok(value);
    }
    // Spark floors epoch conversions while evaluating the timestamp only once.
    Ok(ScalarUDF::from(SparkTimestampToNumber::new()).call(vec![value, lit(divisor)]))
}

pub(crate) fn cast_timestamp_ntz_to_ltz(
    expression: expr::Expr,
    timezone: Arc<str>,
    is_try: bool,
) -> DataFusionResult<expr::Expr> {
    Ok(cast(
        shift_timestamp_timezone(expression, &timezone, is_try, true, false)?,
        DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)),
    ))
}

pub(crate) fn shift_timestamp_timezone(
    expression: expr::Expr,
    timezone: &str,
    is_try: bool,
    to_utc: bool,
    ltz_to_ntz: bool,
) -> DataFusionResult<expr::Expr> {
    let timestamp_ntz = cast(expression, DataType::Timestamp(TimeUnit::Microsecond, None));
    // A plain timezone cast can be unwrapped into a numeric comparison by DataFusion,
    // losing the local-time conversion. Keep the conversion explicit before retagging.
    Ok(if is_fixed_zero_offset_timezone(timezone) {
        cast(timestamp_ntz, DataType::Int64)
    } else if is_spark_compatible_arrow_fixed_offset(timezone) {
        let offset = timezone
            .parse::<Tz>()?
            .offset_from_utc_datetime(&chrono::DateTime::UNIX_EPOCH.naive_utc())
            .fix()
            .local_minus_utc();
        // Widen before shifting so the final cast detects i64 overflow instead of wrapping.
        let micros = cast(
            cast(timestamp_ntz, DataType::Int64),
            DataType::Decimal128(20, 0),
        );
        let offset = lit(ScalarValue::Decimal128(
            Some(i128::from(offset) * 1_000_000),
            20,
            0,
        ));
        let shifted = if to_utc {
            micros - offset
        } else {
            micros + offset
        };
        if is_try {
            try_cast(shifted, DataType::Int64)
        } else {
            cast(shifted, DataType::Int64)
        }
    } else {
        // TODO: Support second-resolution fixed offsets rejected by Arrow and ConvertTz.
        let conversion = ConvertTz::new(false);
        let conversion = if ltz_to_ntz {
            conversion.with_ltz_to_ntz()
        } else {
            conversion
        };
        let converted = ScalarUDF::from(conversion).call(vec![
            lit(if to_utc { timezone } else { "UTC" }),
            lit(if to_utc { "UTC" } else { timezone }),
            timestamp_ntz,
        ]);
        cast(converted, DataType::Int64)
    })
}

pub(crate) fn coerce_temporal_array_element(
    array: expr::Expr,
    element: expr::Expr,
    context: &FunctionContextInput<'_>,
) -> PlanResult<(expr::Expr, expr::Expr)> {
    let array_type = array.get_type(context.schema)?;
    let element_type = element.get_type(context.schema)?;
    let field = match &array_type {
        DataType::List(field) | DataType::LargeList(field) => field,
        _ => return Ok((array, element)),
    };
    let timezone = &context.plan_config.session_timezone;
    let Some(target) =
        common_temporal_type(&[field.data_type().clone(), element_type.clone()], timezone)
    else {
        return Ok((array, element));
    };
    let array_target = DataType::List(Arc::new(
        field.as_ref().clone().with_data_type(target.clone()),
    ));
    Ok((
        cast_temporal(array, &array_type, array_target, timezone, false, None)?,
        cast_temporal(element, &element_type, target, timezone, false, None)?,
    ))
}
