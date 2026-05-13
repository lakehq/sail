use std::ops::{Div, Mul};
use std::sync::Arc;

use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::{cast, expr, lit, try_cast, ExprSchemable, ScalarUDF};
use sail_common::datetime::time_unit_to_multiplier;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_interval::{
    SparkCalendarInterval, SparkDayTimeInterval, SparkYearMonthInterval,
};
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_to_string::{SparkToLargeUtf8, SparkToUtf8, SparkToUtf8View};
use sail_function::scalar::variant::spark_cast_to_variant::SparkCastToVariant;

use crate::error::{PlanError, PlanResult};
use crate::resolver::data_type::spark_interval_field_value;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_expression_cast(
        &self,
        expr: spec::Expr,
        cast_to_type: spec::DataType,
        rename: bool,
        is_try: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        // CAST(expr AS VARIANT) → rewrite to SparkCastToVariant UDF
        // Must intercept before resolve_data_type converts Variant to Struct.
        if matches!(cast_to_type, spec::DataType::Variant) {
            let NamedExpr { expr, name, .. } =
                self.resolve_named_expression(expr, schema, state).await?;
            let name = if rename && need_rename_cast(&expr) {
                let prefix = if is_try { "TRY_" } else { "" };
                vec![format!("{}CAST({} AS VARIANT)", prefix, name.one()?)]
            } else {
                name
            };
            let expr = ScalarUDF::new_from_impl(SparkCastToVariant::new()).call(vec![expr]);
            return Ok(NamedExpr::new(name, expr));
        }

        // Extract the DayTimeInterval field unit before resolving to Arrow type,
        // since it determines the multiplier for numeric-to-interval casts.
        // Spark uses the end field (or start field for single-field intervals)
        // to interpret the numeric value: e.g. DayTimeIntervalType(DAY, DAY) treats
        // the value as days, while DayTimeIntervalType(DAY, SECOND) treats it as seconds.
        let interval_metadata = interval_field_metadata(&cast_to_type)?;
        let interval_literal_name = if !rename {
            interval_literal_display_name(&expr, &cast_to_type)
        } else {
            None
        };
        let day_time_interval_field = match &cast_to_type {
            spec::DataType::Interval {
                interval_unit: spec::IntervalUnit::DayTime,
                start_field,
                end_field,
            } => end_field.or(*start_field),
            _ => None,
        };
        let cast_to_type = self.resolve_data_type(&cast_to_type, state)?;
        let NamedExpr { expr, name, .. } =
            self.resolve_named_expression(expr, schema, state).await?;
        let expr_type = expr.get_type(schema)?;
        let name = if let Some(name) = interval_literal_name {
            vec![name]
        } else if rename && need_rename_cast(&expr) {
            let service = self.ctx.extension::<PlanService>()?;
            let data_type_string = service
                .plan_formatter()
                .data_type_to_simple_string(&cast_to_type)?;
            vec![format!(
                "{}CAST({} AS {})",
                if is_try { "TRY_" } else { "" },
                name.one()?,
                data_type_string.to_ascii_uppercase()
            )]
        } else {
            name
        };
        let override_string_cast = matches!(
            expr_type,
            DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Duration(_)
                | DataType::Interval(_)
                | DataType::Timestamp(_, _)
                | DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(_, _)
                | DataType::ListView(_)
                | DataType::LargeListView(_)
                | DataType::Struct(_)
                | DataType::Map(_, _)
        );
        let expr = match (expr_type, cast_to_type.clone(), is_try) {
            (from, DataType::Timestamp(time_unit, _) | DataType::Duration(time_unit), _)
                if from.is_numeric() =>
            {
                let multiplier = match (day_time_interval_field, &cast_to_type) {
                    (Some(field), DataType::Duration(_)) => day_time_field_to_microseconds(field),
                    _ => time_unit_to_multiplier(&time_unit),
                };
                cast(expr.mul(lit(multiplier)), cast_to_type)
            }
            (DataType::Timestamp(time_unit, _) | DataType::Duration(time_unit), to, _)
                if to.is_numeric() =>
            {
                cast(
                    lit(1.0)
                        .div(lit(time_unit_to_multiplier(&time_unit)))
                        .mul(cast(expr, DataType::Int64)),
                    to,
                )
            }
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Interval(IntervalUnit::YearMonth),
                _,
            ) => ScalarUDF::new_from_impl(SparkYearMonthInterval::new()).call(vec![expr]),
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Duration(TimeUnit::Microsecond),
                _,
            ) => ScalarUDF::new_from_impl(SparkDayTimeInterval::new()).call(vec![expr]),
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Interval(IntervalUnit::MonthDayNano),
                _,
            ) => ScalarUDF::new_from_impl(SparkCalendarInterval::new()).call(vec![expr]),
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Date32,
                is_try,
            ) => ScalarUDF::new_from_impl(SparkDate::new(is_try)).call(vec![expr]),
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Timestamp(TimeUnit::Microsecond, tz),
                is_try,
            ) => Arc::new(ScalarUDF::new_from_impl(SparkTimestamp::try_new(
                tz, is_try,
            )?))
            .call(vec![expr]),
            (_, DataType::Utf8, _) if override_string_cast => {
                ScalarUDF::new_from_impl(SparkToUtf8::new()).call(vec![expr])
            }
            (_, DataType::LargeUtf8, _) if override_string_cast => {
                ScalarUDF::new_from_impl(SparkToLargeUtf8::new()).call(vec![expr])
            }
            (_, DataType::Utf8View, _) if override_string_cast => {
                ScalarUDF::new_from_impl(SparkToUtf8View::new()).call(vec![expr])
            }
            (DataType::Date32 | DataType::Date64, to, _)
                if to.is_numeric() || matches!(to, DataType::Boolean) =>
            {
                if !is_try && self.config.ansi_mode {
                    return Err(PlanError::invalid(format!("cannot cast date to {to}")));
                }
                lit(ScalarValue::try_from(&to)?)
            }
            (_, to, true) => try_cast(expr, to),
            (_, to, _) => cast(expr, to),
        };
        let mut expr = NamedExpr::new(name, expr);
        expr.metadata.extend(interval_metadata);
        Ok(expr)
    }
}

/// Extracts Spark interval qualifier metadata from an interval data type.
///
/// The returned key-value pairs use Sail's reserved Arrow field metadata keys and
/// Spark's numeric interval field representation for start and end qualifiers.
fn interval_field_metadata(data_type: &spec::DataType) -> PlanResult<Vec<(String, String)>> {
    let spec::DataType::Interval {
        interval_unit,
        start_field,
        end_field,
    } = data_type
    else {
        return Ok(vec![]);
    };
    let mut metadata = vec![];
    if let Some(start_field) = start_field {
        metadata.push((
            spec::SAIL_INTERVAL_START_FIELD_KEY.to_string(),
            spark_interval_field_value(interval_unit, start_field)?.to_string(),
        ));
    }
    if let Some(end_field) = end_field {
        metadata.push((
            spec::SAIL_INTERVAL_END_FIELD_KEY.to_string(),
            spark_interval_field_value(interval_unit, end_field)?.to_string(),
        ));
    }
    Ok(metadata)
}

fn interval_literal_display_name(expr: &spec::Expr, data_type: &spec::DataType) -> Option<String> {
    let spec::Expr::Literal(literal) = expr else {
        return None;
    };
    let spec::DataType::Interval {
        interval_unit,
        start_field: Some(start_field),
        end_field,
    } = data_type
    else {
        return None;
    };
    let value = match (literal, interval_unit) {
        (
            spec::Literal::IntervalYearMonth {
                months: Some(months),
            },
            spec::IntervalUnit::YearMonth,
        ) => year_month_interval_literal(*months, start_field, end_field.as_ref())?,
        (
            spec::Literal::DurationMicrosecond {
                microseconds: Some(microseconds),
            },
            spec::IntervalUnit::DayTime,
        ) => day_time_interval_literal(*microseconds, start_field, end_field.as_ref())?,
        _ => return None,
    };
    let qualifier = interval_qualifier(start_field, end_field.as_ref());
    Some(format!("INTERVAL '{value}' {qualifier}"))
}

fn year_month_interval_literal(
    months: i32,
    start_field: &spec::IntervalFieldType,
    end_field: Option<&spec::IntervalFieldType>,
) -> Option<String> {
    let negative = months < 0;
    let months = i64::from(months).abs();
    let sign = if negative { "-" } else { "" };
    match (start_field, end_field) {
        (spec::IntervalFieldType::Year, None) => Some(format!("{sign}{}", months / 12)),
        (spec::IntervalFieldType::Year, Some(spec::IntervalFieldType::Month)) => {
            Some(format!("{sign}{}-{}", months / 12, months % 12))
        }
        (spec::IntervalFieldType::Month, None) => Some(format!("{sign}{months}")),
        _ => None,
    }
}

fn day_time_interval_literal(
    microseconds: i64,
    start_field: &spec::IntervalFieldType,
    end_field: Option<&spec::IntervalFieldType>,
) -> Option<String> {
    const MICROS_PER_SECOND: i128 = 1_000_000;
    const MICROS_PER_MINUTE: i128 = 60 * MICROS_PER_SECOND;
    const MICROS_PER_HOUR: i128 = 60 * MICROS_PER_MINUTE;
    const MICROS_PER_DAY: i128 = 24 * MICROS_PER_HOUR;

    let negative = microseconds < 0;
    let microseconds = i128::from(microseconds).abs();
    let sign = if negative { "-" } else { "" };
    let days = microseconds / MICROS_PER_DAY;
    let day_remainder = microseconds % MICROS_PER_DAY;
    let hours = day_remainder / MICROS_PER_HOUR;
    let hour_remainder = day_remainder % MICROS_PER_HOUR;
    let minutes = hour_remainder / MICROS_PER_MINUTE;
    let minute_remainder = hour_remainder % MICROS_PER_MINUTE;
    let total_hours = microseconds / MICROS_PER_HOUR;
    let total_minutes = microseconds / MICROS_PER_MINUTE;

    match (start_field, end_field) {
        (spec::IntervalFieldType::Day, None) => Some(format!("{sign}{days}")),
        (spec::IntervalFieldType::Day, Some(spec::IntervalFieldType::Hour)) => {
            Some(format!("{sign}{days} {hours:02}"))
        }
        (spec::IntervalFieldType::Day, Some(spec::IntervalFieldType::Minute)) => {
            Some(format!("{sign}{days} {hours:02}:{minutes:02}"))
        }
        (spec::IntervalFieldType::Day, Some(spec::IntervalFieldType::Second)) => Some(format!(
            "{sign}{days} {hours:02}:{minutes:02}:{}",
            second_literal(minute_remainder, true)
        )),
        (spec::IntervalFieldType::Hour, None) => Some(format!("{sign}{total_hours}")),
        (spec::IntervalFieldType::Hour, Some(spec::IntervalFieldType::Minute)) => {
            Some(format!("{sign}{total_hours}:{minutes:02}"))
        }
        (spec::IntervalFieldType::Hour, Some(spec::IntervalFieldType::Second)) => Some(format!(
            "{sign}{total_hours}:{minutes:02}:{}",
            second_literal(minute_remainder, true)
        )),
        (spec::IntervalFieldType::Minute, None) => Some(format!("{sign}{total_minutes}")),
        (spec::IntervalFieldType::Minute, Some(spec::IntervalFieldType::Second)) => Some(format!(
            "{sign}{total_minutes}:{}",
            second_literal(minute_remainder, true)
        )),
        (spec::IntervalFieldType::Second, None) => {
            Some(format!("{sign}{}", second_literal(microseconds, false)))
        }
        _ => None,
    }
}

fn second_literal(microseconds: i128, pad_seconds: bool) -> String {
    let seconds = microseconds / 1_000_000;
    let fraction = microseconds % 1_000_000;
    let seconds = if pad_seconds {
        format!("{seconds:02}")
    } else {
        seconds.to_string()
    };
    if fraction == 0 {
        seconds
    } else {
        format!("{seconds}.{fraction:06}")
            .trim_end_matches('0')
            .to_string()
    }
}

fn interval_qualifier(
    start_field: &spec::IntervalFieldType,
    end_field: Option<&spec::IntervalFieldType>,
) -> String {
    let start = interval_field_name(start_field);
    if let Some(end_field) = end_field {
        format!("{start} TO {}", interval_field_name(end_field))
    } else {
        start.to_string()
    }
}

fn interval_field_name(field: &spec::IntervalFieldType) -> &'static str {
    match field {
        spec::IntervalFieldType::Year => "YEAR",
        spec::IntervalFieldType::Month => "MONTH",
        spec::IntervalFieldType::Day => "DAY",
        spec::IntervalFieldType::Hour => "HOUR",
        spec::IntervalFieldType::Minute => "MINUTE",
        spec::IntervalFieldType::Second => "SECOND",
    }
}

fn day_time_field_to_microseconds(field: spec::IntervalFieldType) -> i64 {
    match field {
        spec::IntervalFieldType::Day => 86_400_000_000,
        spec::IntervalFieldType::Hour => 3_600_000_000,
        spec::IntervalFieldType::Minute => 60_000_000,
        // Second, or Year/Month (shouldn't appear for DayTime intervals)
        _ => 1_000_000,
    }
}

fn need_rename_cast(expr: &expr::Expr) -> bool {
    match expr {
        expr::Expr::Alias(_) | expr::Expr::Column(_) | expr::Expr::OuterReferenceColumn(..) => {
            false
        }
        expr::Expr::Cast(cast) => need_rename_cast(cast.expr.as_ref()),
        expr::Expr::TryCast(try_cast) => need_rename_cast(try_cast.expr.as_ref()),
        _ => true,
    }
}
