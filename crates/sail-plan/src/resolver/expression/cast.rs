use std::ops::{Div, Mul};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, IntervalUnit, TimeUnit};
use datafusion::functions::expr_fn::lpad;
use datafusion::functions::string::expr_fn::concat;
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::{ExprSchemable, ScalarUDF, cast, expr, lit, try_cast};
use sail_common::spec;
use sail_common::utils::datetime::time_unit_to_multiplier;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_common_datafusion::variant::is_variant_storage_field;
use sail_function::scalar::datetime::convert_tz::ConvertTz;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_string_to_time::SparkStringToTime;
use sail_function::scalar::datetime::spark_interval::{
    SparkCalendarInterval, SparkDayTimeInterval, SparkDayTimeIntervalFromInt64,
    SparkYearMonthInterval, SparkYearMonthIntervalFromInt64, YearMonthIntervalMonths,
};
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::misc::raise_error::RaiseError;
use sail_function::scalar::spark_cast_string_to_int32::SparkCastStringToInt32;
use sail_function::scalar::spark_struct_rename::SparkStructRename;
use sail_function::scalar::spark_to_string::{SparkToLargeUtf8, SparkToUtf8, SparkToUtf8View};
use sail_function::scalar::variant::spark_cast_to_variant::SparkCastToVariant;
use sail_function::scalar::variant::spark_variant_get::SparkVariantGet;
use sail_function::scalar::variant::spark_variant_to_json::SparkVariantToJsonUdf;

use crate::error::{PlanError, PlanResult};
use crate::function::is_spark_compatible_arrow_fixed_offset;
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::expression::predicate::spark_interval_metadata_for_expression;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_expression_cast(
        &self,
        expr: spec::Expr,
        cast_to_type: spec::DataType,
        _rename: bool,
        is_try: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        // CAST(expr AS VARIANT) → rewrite to SparkCastToVariant UDF
        // Must intercept before resolve_data_type converts Variant to Struct.
        if matches!(cast_to_type, spec::DataType::Variant) {
            let NamedExpr { expr, name, .. } =
                self.resolve_named_expression(expr, schema, state).await?;
            let name = if need_rename_cast(&expr) {
                let prefix = if is_try { "TRY_" } else { "" };
                vec![format!("{}CAST({} AS VARIANT)", prefix, name.one()?)]
            } else {
                name
            };
            let source_field = expr.to_field(schema)?.1;
            // Cast.castInternal returns its input when both types are VARIANT.
            // Passing the storage struct to cast_to_variant would reject it.
            let expr = if is_variant_storage_field(&source_field) {
                expr
            } else {
                let expr = ScalarUDF::new_from_impl(SparkCastToVariant::new()).call(vec![expr]);
                if is_try && !expr.nullable(schema)? {
                    datafusion_expr::when(lit(true), expr).end()?
                } else {
                    expr
                }
            };
            return Ok(NamedExpr::new(name, expr));
        }

        // Extract the DayTimeInterval field unit before resolving to Arrow type,
        // since it determines the multiplier for numeric-to-interval casts.
        // Spark uses the end field (or start field for single-field intervals)
        // to interpret the numeric value: e.g. DayTimeIntervalType(DAY, DAY) treats
        // the value as days, while DayTimeIntervalType(DAY, SECOND) treats it as seconds.
        let day_time_interval_field = match &cast_to_type {
            spec::DataType::Interval {
                interval_unit: spec::IntervalUnit::DayTime,
                start_field,
                end_field,
            } => end_field.or(*start_field),
            _ => None,
        };
        // Same reasoning as above, but for YearMonthIntervalType: the end field
        // (or start field) says whether a numeric value means years or months.
        let year_month_interval_field = match &cast_to_type {
            spec::DataType::Interval {
                interval_unit: spec::IntervalUnit::YearMonth,
                start_field,
                end_field,
            } => end_field.or(*start_field),
            _ => None,
        };
        let spark_interval_metadata = match &cast_to_type {
            spec::DataType::Interval {
                interval_unit,
                start_field,
                end_field,
            } => spec::SparkIntervalMetadata::try_new(*interval_unit, *start_field, *end_field)?
                .map(spec::SparkIntervalMetadata::to_json)
                .transpose()?,
            _ => None,
        };
        // The exact (start, end) field range `CAST(string AS INTERVAL ...)`
        // must parse the source string against -- e.g. `DAY TO SECOND` accepts
        // `"1 02:03:04"` but `HOUR` alone only accepts a signed integer. Reuse
        // `SparkIntervalMetadata`'s own default-filling (a bare `INTERVAL DAY`
        // means `(Day, Day)`, not `(Day, None)`) rather than re-deriving it.
        let day_time_interval_qualifier = match &cast_to_type {
            spec::DataType::Interval {
                interval_unit: spec::IntervalUnit::DayTime,
                start_field,
                end_field,
            } => spec::SparkIntervalMetadata::try_new(
                spec::IntervalUnit::DayTime,
                *start_field,
                *end_field,
            )?
            .map(|metadata| (metadata.start_field(), metadata.end_field())),
            _ => None,
        };
        let cast_to_type = self.resolve_data_type(&cast_to_type, state)?;
        let NamedExpr { expr, name, .. } =
            self.resolve_named_expression(expr, schema, state).await?;
        let expr_field = expr.to_field(schema)?.1;
        let expr_type = expr_field.data_type().clone();
        let expr_is_variant = is_variant_storage_field(expr_field.as_ref());
        // VARIANT is stored as a Struct, but casting it to/from TIME has its own
        // dedicated error (INVALID_VARIANT_CAST), not the generic TIME rejection.
        if !expr_is_variant && spark_rejects_time_cast(&expr_type, &cast_to_type) {
            let service = self.ctx.extension::<PlanService>()?;
            let formatter = service.plan_formatter();
            let source_name = formatter.data_type_to_simple_string(&expr_type)?;
            let target_name = formatter.data_type_to_simple_string(&cast_to_type)?;
            return Err(PlanError::invalid(format!(
                "[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION] cannot cast \"{}\" to \"{}\"",
                source_name.to_ascii_uppercase(),
                target_name.to_ascii_uppercase()
            )));
        }
        let force_nullable =
            !is_try && spark_cast_force_nullable(&expr_type, &cast_to_type, expr_is_variant);

        let name = if need_rename_cast(&expr) {
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
                // Spark's cast to string wraps raw bytes with no UTF-8 validation
                // (UTF8String.fromBytes); Arrow's own cast kernel rejects invalid
                // sequences, so this needs the same lenient UDF-based path.
                | DataType::Binary
                | DataType::LargeBinary
                // Java's Double/Float.toString (scientific-notation thresholds,
                // "Infinity"/"NaN" spelling) differs from Arrow's own float
                // formatting; `ArrayFormatter` already implements it correctly
                // (used by `.show()`), but only this path routes CAST through it.
                | DataType::Float32
                | DataType::Float64
                | DataType::BinaryView
        );
        let expr = match (expr_type, cast_to_type.clone(), is_try) {
            (
                DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _),
                to @ (DataType::Float32 | DataType::Float64),
                is_try,
            ) => {
                // Decimal.scala:245-247 rounds directly from the exact decimal.
                // Arrow's unscaled-integer -> float -> division path can round
                // twice. Preserve every decimal digit before parsing the float.
                let exact = cast(expr, DataType::Utf8View);
                if is_try {
                    try_cast(exact, to)
                } else {
                    cast(exact, to)
                }
            }
            (
                DataType::Timestamp(_, Some(_)),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                _,
            ) => {
                // Cast.scala:787-788: the NTZ value is the wall clock in the
                // session zone, not the UTC microsecond count with its zone erased.
                let instant = cast(expr, DataType::Timestamp(TimeUnit::Microsecond, None));
                let timezone = self.config.session_timezone.as_ref();
                if is_spark_compatible_arrow_fixed_offset(timezone) {
                    let normalized = if timezone.len() == 3 {
                        format!("{timezone}:00")
                    } else {
                        timezone.to_string()
                    };
                    let offset = normalized.parse::<chrono::FixedOffset>().map_err(|error| {
                        PlanError::invalid(format!("invalid timezone {timezone}: {error}"))
                    })?;
                    let micros = i64::from(offset.local_minus_utc()) * 1_000_000;
                    cast(cast(instant, DataType::Int64) + lit(micros), cast_to_type)
                } else {
                    ScalarUDF::from(ConvertTz::new(false)).call(vec![
                        lit("UTC"),
                        lit(timezone),
                        instant,
                    ])
                }
            }
            (
                DataType::Timestamp(_, None),
                DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)),
                is_try,
            ) => {
                if is_spark_compatible_arrow_fixed_offset(timezone.as_ref()) {
                    if is_try {
                        try_cast(expr, cast_to_type)
                    } else {
                        cast(expr, cast_to_type)
                    }
                } else {
                    let timestamp_ntz =
                        cast(expr, DataType::Timestamp(TimeUnit::Microsecond, None));
                    let instant = ScalarUDF::from(ConvertTz::new(false)).call(vec![
                        lit(timezone.to_string()),
                        lit("UTC"),
                        timestamp_ntz,
                    ]);
                    cast(cast(instant, DataType::Int64), cast_to_type)
                }
            }
            (_, DataType::Utf8, _) if expr_is_variant => cast(
                ScalarUDF::new_from_impl(SparkVariantToJsonUdf::new_cast())
                    .call(vec![expr, lit(self.config.session_timezone.to_string())]),
                DataType::Utf8,
            ),
            (_, DataType::LargeUtf8, _) if expr_is_variant => cast(
                ScalarUDF::new_from_impl(SparkVariantToJsonUdf::new_cast())
                    .call(vec![expr, lit(self.config.session_timezone.to_string())]),
                DataType::LargeUtf8,
            ),
            (_, DataType::Utf8View, _) if expr_is_variant => {
                ScalarUDF::new_from_impl(SparkVariantToJsonUdf::new_cast())
                    .call(vec![expr, lit(self.config.session_timezone.to_string())])
            }
            (_, to, is_try) if expr_is_variant => {
                // The internal extractor accepts Arrow type syntax. A Spark SQL
                // type string loses timestamp zones and cannot represent nested
                // targets in its primitive-only Spark-name parser.
                let data_type_string = to.to_string();
                ScalarUDF::new_from_impl(SparkVariantGet::new(is_try)).call(vec![
                    expr,
                    lit("$"),
                    lit(data_type_string),
                ])
            }
            (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View, DataType::Int32, false)
                if !self.config.ansi_mode =>
            {
                ScalarUDF::new_from_impl(SparkCastStringToInt32::new()).call(vec![expr])
            }
            (
                DataType::Time32(unit) | DataType::Time64(unit),
                to @ (DataType::Decimal32(precision, scale)
                | DataType::Decimal64(precision, scale)
                | DataType::Decimal128(precision, scale)
                | DataType::Decimal256(precision, scale)),
                is_try,
            ) => {
                // Cast.scala: TimeType -> Decimal reads the raw value as seconds
                // (Decimal.apply(t, 14, 9)): the fractional seconds must be exact,
                // so build it through a string -- decimal division's inferred result
                // scale drops digits, and float division rounds in binary.
                let raw = cast(expr, DataType::Int64);
                let multiplier = time_unit_to_multiplier(&unit);
                let digits: i32 = match unit {
                    TimeUnit::Second => 0,
                    TimeUnit::Millisecond => 3,
                    TimeUnit::Microsecond => 6,
                    TimeUnit::Nanosecond => 9,
                };
                let whole = raw.clone().div(lit(multiplier));
                let exact = if digits == 0 {
                    cast(whole.clone(), DataType::Utf8)
                } else {
                    let fraction = raw % lit(multiplier);
                    let fraction_str = lpad(vec![
                        cast(fraction, DataType::Utf8),
                        lit(digits),
                        lit("0"),
                    ]);
                    concat(vec![cast(whole.clone(), DataType::Utf8), lit("."), fraction_str])
                };
                let exact = cast(exact, DataType::Decimal128(38, digits.min(38) as i8));
                if is_try || !self.config.ansi_mode {
                    try_cast(exact, to)
                } else {
                    // Spark's NUMERIC_VALUE_OUT_OF_RANGE, unlike CAST_OVERFLOW, is
                    // decimal-precision-specific: the value's integer part alone
                    // (TIME is never negative) doesn't fit the target precision/scale.
                    let integer_digits = i64::from(precision) - i64::from(scale);
                    let bound = 10_i64.pow(u32::try_from(integer_digits.max(0)).unwrap_or(0));
                    let overflow = whole.gt_eq(lit(bound));
                    let target_name = {
                        let service = self.ctx.extension::<PlanService>()?;
                        service
                            .plan_formatter()
                            .data_type_to_simple_string(&to)?
                            .to_ascii_uppercase()
                    };
                    let message = lit(format!(
                        "[NUMERIC_VALUE_OUT_OF_RANGE] value out of range for \"{target_name}\"."
                    ));
                    datafusion_expr::when(
                        overflow,
                        ScalarUDF::from(RaiseError::new()).call(vec![message]),
                    )
                    // Same reasoning as the other CAST_OVERFLOW/RaiseError guards
                    // in this file: constant folding may still visit this branch,
                    // so it must not be able to throw its own, different error.
                    .otherwise(try_cast(exact, to))?
                }
            }
            (
                DataType::Time32(unit)
                | DataType::Time64(unit)
                | DataType::Timestamp(unit, Some(_)),
                to @ (DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64),
                is_try,
            ) => {
                let is_timestamp = matches!(expr_field.data_type(), DataType::Timestamp(..));
                let raw = cast(expr.clone(), DataType::Int64);
                let divisor = time_unit_to_multiplier(&unit);
                // Cast.scala timestampToLong/timeToLong use floorDiv, not a
                // floating-point conversion or truncation toward zero.
                let seconds = if divisor == 1 {
                    raw
                } else {
                    let quotient = raw.clone() / lit(divisor);
                    let adjustment =
                        datafusion_expr::when((raw % lit(divisor)).lt(lit(0_i64)), lit(1_i64))
                            .otherwise(lit(0_i64))?;
                    quotient - adjustment
                };
                let bounds = match to {
                    DataType::Int8 => Some((i64::from(i8::MIN), i64::from(i8::MAX))),
                    DataType::Int16 => Some((i64::from(i16::MIN), i64::from(i16::MAX))),
                    DataType::Int32 if is_timestamp => {
                        Some((i64::from(i32::MIN), i64::from(i32::MAX)))
                    }
                    _ => None,
                };
                if is_try || !self.config.ansi_mode {
                    // TIMESTAMP -> BIGINT cannot overflow and Spark treats it
                    // as an upcast, including TRY_CAST's analyzed nullability.
                    if is_timestamp && to == DataType::Int64 {
                        cast(seconds, to)
                    } else if is_try || bounds.is_some() {
                        try_cast(seconds, to)
                    } else {
                        cast(seconds, to)
                    }
                } else if let Some((min, max)) = bounds {
                    let service = self.ctx.extension::<PlanService>()?;
                    let formatter = service.plan_formatter();
                    let source_name = formatter
                        .data_type_to_simple_string(expr_field.data_type())?
                        .to_ascii_uppercase();
                    let target_name = formatter
                        .data_type_to_simple_string(&to)?
                        .to_ascii_uppercase();
                    let literal_type = if is_timestamp { "TIMESTAMP" } else { "TIME" };
                    let value_string = ScalarUDF::from(SparkToUtf8View::new()).call(vec![expr]);
                    let message = concat(vec![
                        lit(format!("[CAST_OVERFLOW] The value {literal_type} '")),
                        value_string,
                        lit(format!(
                            "' of the type \"{source_name}\" cannot be cast to \"{target_name}\" due to an overflow. Use `try_cast` to tolerate overflow and return NULL instead."
                        )),
                    ]);
                    let overflow = seconds
                        .clone()
                        .lt(lit(min))
                        .or(seconds.clone().gt(lit(max)));
                    datafusion_expr::when(
                        overflow,
                        ScalarUDF::from(RaiseError::new()).call(vec![message]),
                    )
                    // Constant folding may visit the unselected branch. Keep
                    // it non-throwing; the explicit guard owns CAST_OVERFLOW.
                    .otherwise(try_cast(seconds, to))?
                } else {
                    cast(seconds, to)
                }
            }
            (from, DataType::Duration(_), is_try)
                if from.is_integer() && day_time_interval_field.is_some() =>
            {
                let Some(field) = day_time_interval_field else {
                    unreachable!("guarded by the match arm above")
                };
                let multiplier = day_time_field_to_microseconds(field);
                ScalarUDF::from(SparkDayTimeIntervalFromInt64::new(multiplier, is_try))
                    .call(vec![cast(expr, DataType::Int64)])
            }
            (from, DataType::Timestamp(time_unit, _) | DataType::Duration(time_unit), _)
                if from.is_numeric() =>
            {
                // DECIMAL (and any other non-integral numeric) keeps the fractional
                // part through the multiply -- Spark's `decimalToDayTimeInterval`
                // is a separate, fraction-preserving path from the integral one
                // above, unlike the integral path's whole-unit overflow check.
                let multiplier = match (day_time_interval_field, &cast_to_type) {
                    (Some(field), DataType::Duration(_)) => day_time_field_to_microseconds(field),
                    _ => time_unit_to_multiplier(&time_unit),
                };
                cast(expr.mul(lit(multiplier)), cast_to_type)
            }
            (from, DataType::Interval(IntervalUnit::YearMonth), is_try) if from.is_numeric() => {
                // Spark interprets a numeric-to-YearMonthIntervalType cast using the
                // end (or start) field: YEAR means the number counts years, so it
                // must be scaled to months before storing (months is the only
                // representation Catalyst/Arrow have for this interval).
                let multiplier = match year_month_interval_field {
                    Some(spec::IntervalFieldType::Year) => 12_i64,
                    _ => 1_i64,
                };
                ScalarUDF::from(SparkYearMonthIntervalFromInt64::new(multiplier, is_try))
                    .call(vec![cast(expr, DataType::Int64)])
            }
            (
                DataType::Interval(IntervalUnit::YearMonth),
                DataType::Interval(IntervalUnit::YearMonth),
                _,
            ) if matches!(
                year_month_interval_field,
                Some(spec::IntervalFieldType::Year)
            ) =>
            {
                // Spark truncates the month remainder when narrowing to a
                // YEAR-only field, rather than only relabeling the display range.
                // Arrow refuses a direct Interval(YearMonth) -> Int32 cast, so
                // months must be read out through the dedicated UDF.
                let months = ScalarUDF::from(YearMonthIntervalMonths::new()).call(vec![expr]);
                let truncated = months.div(lit(12_i32)).mul(lit(12_i32));
                cast(truncated, DataType::Interval(IntervalUnit::YearMonth))
            }
            (
                DataType::Duration(TimeUnit::Microsecond),
                DataType::Duration(TimeUnit::Microsecond),
                _,
            ) if matches!(
                day_time_interval_field,
                Some(field) if field != spec::IntervalFieldType::Second
            ) =>
            {
                // Same truncation as above, but for the day-time interval fields
                // coarser than SECOND (which already matches microsecond storage).
                let Some(field) = day_time_interval_field else {
                    unreachable!("guarded by the match arm above")
                };
                let unit_micros = day_time_field_to_microseconds(field);
                let micros = cast(expr, DataType::Int64);
                let truncated = micros.clone().div(lit(unit_micros)).mul(lit(unit_micros));
                cast(truncated, DataType::Duration(TimeUnit::Microsecond))
            }
            (DataType::Duration(TimeUnit::Microsecond), to, is_try) if to.is_integer() => {
                // Spark IntervalUtils.dayTimeIntervalToLong divides by the
                // interval's end field, truncating toward zero. Keep integer
                // arithmetic: f64 loses precision near whole-second boundaries.
                let divisor = match spark_interval_metadata_for_expression(&expr, schema)? {
                    Some(spec::SparkIntervalMetadata::DayTime { end_field, .. }) => match end_field
                    {
                        spec::DayTimeIntervalField::Day => 86_400_000_000_i64,
                        spec::DayTimeIntervalField::Hour => 3_600_000_000_i64,
                        spec::DayTimeIntervalField::Minute => 60_000_000_i64,
                        spec::DayTimeIntervalField::Second => 1_000_000_i64,
                    },
                    _ => 1_000_000_i64,
                };
                let value = cast(expr, DataType::Int64) / lit(divisor);
                if is_try {
                    try_cast(value, to)
                } else {
                    cast(value, to)
                }
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
            (DataType::Interval(IntervalUnit::YearMonth), to, is_try) if to.is_integer() => {
                let interval_metadata = expr_field
                    .metadata()
                    .get(spec::SAIL_SPARK_INTERVAL_METADATA_KEY)
                    .map(|value| spec::SparkIntervalMetadata::from_json(value))
                    .transpose()?;
                let months = ScalarUDF::from(YearMonthIntervalMonths::new()).call(vec![expr]);
                let value = if matches!(
                    interval_metadata,
                    Some(spec::SparkIntervalMetadata::YearMonth {
                        end_field: spec::YearMonthIntervalField::Year,
                        ..
                    })
                ) {
                    months / lit(12_i32)
                } else {
                    months
                };
                if is_try {
                    try_cast(value, to)
                } else {
                    cast(value, to)
                }
            }
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Interval(IntervalUnit::YearMonth),
                _,
            ) => ScalarUDF::new_from_impl(SparkYearMonthInterval::new()).call(vec![expr]),
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Duration(TimeUnit::Microsecond),
                is_try,
            ) => {
                let Some((start_field, end_field)) = day_time_interval_qualifier else {
                    return Err(PlanError::internal(
                        "expected day-time interval qualifier for STRING -> INTERVAL cast",
                    ));
                };
                ScalarUDF::new_from_impl(SparkDayTimeInterval::new(start_field, end_field, is_try))
                    .call(vec![expr])
            }
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Interval(IntervalUnit::MonthDayNano),
                _,
            ) => ScalarUDF::new_from_impl(SparkCalendarInterval::new()).call(vec![expr]),
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Date32,
                is_try,
            ) => ScalarUDF::new_from_impl(SparkDate::new(is_try || !self.config.ansi_mode))
                .call(vec![expr]),
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                to @ (DataType::Time32(_) | DataType::Time64(_)),
                is_try,
            ) => {
                // Always parses to Time64(Microsecond); a further cast truncates to the
                // requested precision the same way an existing TIME -> TIME(p) cast does.
                let parsed = ScalarUDF::new_from_impl(SparkStringToTime::new(
                    is_try || !self.config.ansi_mode,
                ))
                .call(vec![expr]);
                cast(parsed, to)
            }
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Timestamp(TimeUnit::Microsecond, tz),
                is_try,
            ) => Arc::new(ScalarUDF::new_from_impl(SparkTimestamp::try_new(
                tz,
                self.config.ansi_mode,
                is_try,
            )?))
            .call(vec![expr]),
            (_, DataType::Utf8, _) if override_string_cast => {
                ScalarUDF::new_from_impl(SparkToUtf8::new())
                    .call(spark_string_cast_arguments(expr, schema)?)
            }
            (_, DataType::LargeUtf8, _) if override_string_cast => {
                ScalarUDF::new_from_impl(SparkToLargeUtf8::new())
                    .call(spark_string_cast_arguments(expr, schema)?)
            }
            (_, DataType::Utf8View, _) if override_string_cast => {
                ScalarUDF::new_from_impl(SparkToUtf8View::new())
                    .call(spark_string_cast_arguments(expr, schema)?)
            }
            (DataType::Date32 | DataType::Date64, to, _)
                if to.is_numeric() || matches!(to, DataType::Boolean) =>
            {
                if !is_try && self.config.ansi_mode {
                    return Err(PlanError::invalid(format!("cannot cast date to {to}")));
                }
                lit(ScalarValue::try_from(&to)?)
            }
            (from, to, _) if needs_struct_field_rename(&from, &to) => {
                // Pre-rename the source struct fields positionally so the cast
                // becomes a no-op or a valid name-matched one (see
                // `needs_struct_field_rename`).
                let renamed_target = build_rename_target_type(&from, &to);
                let renamed =
                    ScalarUDF::new_from_impl(SparkStructRename::new(renamed_target.clone()))
                        .call(vec![expr]);
                if renamed_target == to {
                    renamed
                } else if is_try {
                    try_cast(renamed, to)
                } else {
                    cast(renamed, to)
                }
            }
            (
                from,
                DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale),
                _,
            ) if from.is_numeric()
                && !self.config.ansi_mode
                && (is_try || decimal_cast_can_overflow(&from, precision, scale)) =>
            {
                try_cast(expr, cast_to_type)
            }
            (_, to, true) => try_cast(expr, to),
            (_, to, _) => cast(expr, to),
        };
        // Spark Cast.nullable includes forceNullable even in ANSI mode, where
        // conversion errors throw instead of returning NULL. DataFusion's CAST
        // inherits only its input's nullability. A CASE with an implicit NULL
        // alternative preserves Spark's analyzed schema without changing values;
        // the optimizer can remove its constant condition before execution.
        let expr = if force_nullable && !expr.nullable(schema)? {
            datafusion_expr::when(lit(true), expr).end()?
        } else {
            expr
        };
        Ok(match spark_interval_metadata {
            Some(metadata) => {
                // Nested expressions consume the Expr without its NamedExpr metadata.
                // Keep the target qualifier on the cast field as well as the projection.
                let field = expr.to_field(schema)?.1;
                let mut field_metadata = field.metadata().clone();
                field_metadata.insert(
                    spec::SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(),
                    metadata.clone(),
                );
                let field = Arc::new(field.as_ref().clone().with_metadata(field_metadata));
                let expr = match expr {
                    expr::Expr::Cast(cast) => {
                        expr::Expr::Cast(expr::Cast::new_from_field(cast.expr, field))
                    }
                    expr::Expr::TryCast(cast) => {
                        expr::Expr::TryCast(expr::TryCast::new_from_field(cast.expr, field))
                    }
                    expr => expr::Expr::Cast(expr::Cast::new_from_field(Box::new(expr), field)),
                };
                NamedExpr::new(name, expr).with_metadata(vec![(
                    spec::SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(),
                    metadata,
                )])
            }
            None => NamedExpr::new(name, expr),
        })
    }
}

fn decimal_cast_can_overflow(from: &DataType, precision: u8, scale: i8) -> bool {
    let integer_digits = i16::from(precision) - i16::from(scale);
    match from {
        DataType::Decimal128(from_precision, from_scale)
        | DataType::Decimal256(from_precision, from_scale) => {
            let source_integer_digits = i16::from(*from_precision) - i16::from(*from_scale);
            integer_digits < source_integer_digits
                || (integer_digits == source_integer_digits && scale < *from_scale)
        }
        DataType::Int8 | DataType::UInt8 => scale < 0 || integer_digits < 3,
        DataType::Int16 | DataType::UInt16 => scale < 0 || integer_digits < 5,
        DataType::Int32 | DataType::UInt32 => scale < 0 || integer_digits < 10,
        DataType::Int64 | DataType::UInt64 => scale < 0 || integer_digits < 20,
        _ => true,
    }
}

fn spark_string_cast_arguments(
    expr: expr::Expr,
    schema: &DFSchemaRef,
) -> PlanResult<Vec<expr::Expr>> {
    let interval = spark_interval_metadata_for_expression(&expr, schema)?;
    let mut arguments = vec![expr];
    if let Some(interval) = interval {
        // Physical expression serialization does not preserve intermediate field metadata.
        arguments.push(lit(interval.to_json()?));
    }
    Ok(arguments)
}

/// Returns true if the cast from `from` to `to` involves a Struct
/// (possibly nested in a List/LargeList/FixedSizeList/Map) whose field names
/// don't share enough overlap for DataFusion's struct cast validator.
fn needs_struct_field_rename(from: &DataType, to: &DataType) -> bool {
    match (from, to) {
        (DataType::Struct(a), DataType::Struct(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .any(|(fa, fb)| fa.name() != fb.name())
        }
        (DataType::List(a), DataType::List(b))
        | (DataType::LargeList(a), DataType::LargeList(b)) => {
            needs_struct_field_rename(a.data_type(), b.data_type())
        }
        (DataType::FixedSizeList(a, sa), DataType::FixedSizeList(b, sb)) if sa == sb => {
            needs_struct_field_rename(a.data_type(), b.data_type())
        }
        (DataType::Map(a, _), DataType::Map(b, _)) => {
            needs_struct_field_rename(a.data_type(), b.data_type())
        }
        _ => false,
    }
}

/// Build a target type that has the names from `to` but the data types from
/// `from`. The result is what `SparkStructRename` produces; the subsequent
/// regular CAST then handles any leaf-type conversion.
fn build_rename_target_type(from: &DataType, to: &DataType) -> DataType {
    match (from, to) {
        (DataType::Struct(src_fields), DataType::Struct(tgt_fields))
            if src_fields.len() == tgt_fields.len() =>
        {
            let fields: Fields = src_fields
                .iter()
                .zip(tgt_fields.iter())
                .map(|(src, tgt)| {
                    Arc::new(
                        Field::new(
                            tgt.name(),
                            build_rename_target_type(src.data_type(), tgt.data_type()),
                            src.is_nullable(),
                        )
                        .with_metadata(src.metadata().clone()),
                    )
                })
                .collect();
            DataType::Struct(fields)
        }
        (DataType::List(src), DataType::List(tgt)) => DataType::List(Arc::new(
            Field::new(
                tgt.name(),
                build_rename_target_type(src.data_type(), tgt.data_type()),
                src.is_nullable(),
            )
            .with_metadata(src.metadata().clone()),
        )),
        (DataType::LargeList(src), DataType::LargeList(tgt)) => DataType::LargeList(Arc::new(
            Field::new(
                tgt.name(),
                build_rename_target_type(src.data_type(), tgt.data_type()),
                src.is_nullable(),
            )
            .with_metadata(src.metadata().clone()),
        )),
        (DataType::FixedSizeList(src, sa), DataType::FixedSizeList(tgt, _)) => {
            DataType::FixedSizeList(
                Arc::new(
                    Field::new(
                        tgt.name(),
                        build_rename_target_type(src.data_type(), tgt.data_type()),
                        src.is_nullable(),
                    )
                    .with_metadata(src.metadata().clone()),
                ),
                *sa,
            )
        }
        (DataType::Map(src, sorted), DataType::Map(tgt, _)) => DataType::Map(
            Arc::new(
                Field::new(
                    tgt.name(),
                    build_rename_target_type(src.data_type(), tgt.data_type()),
                    src.is_nullable(),
                )
                .with_metadata(src.metadata().clone()),
            ),
            *sorted,
        ),
        // Leaves: keep the source data type unchanged.
        _ => from.clone(),
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

/// Spark 4.2 Cast.forceNullable (Cast.scala:427-448), in match order.
/// This is a schema rule, independent of ANSI runtime error handling.
/// Spark's `Cast.canCast`/`canAnsiCast` only allow a `TimeType` to interact with a
/// narrow set of types: TIME<->TIME (precision change), TIME<->STRING, TIME->INTEGRAL,
/// and TIME->DECIMAL (never the reverse: a number or decimal cannot cast TO time).
/// Every other pairing -- TIMESTAMP, TIMESTAMP_NTZ, DATE, BOOLEAN, FLOAT/DOUBLE,
/// intervals -- is rejected at analysis time with `DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION`.
/// Arrow's own cast kernel is more permissive (or Sail may implement some of these
/// conversions manually), so this needs an explicit rejection, not just relying on
/// downstream casts to fail.
fn spark_rejects_time_cast(from: &DataType, to: &DataType) -> bool {
    let is_time = |t: &DataType| matches!(t, DataType::Time32(_) | DataType::Time64(_));
    if !is_time(from) && !is_time(to) {
        return false;
    }
    if from == &DataType::Null {
        return false;
    }
    match (from, to) {
        (a, b) if is_time(a) && is_time(b) => false,
        (a, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) if is_time(a) => false,
        (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View, b) if is_time(b) => false,
        (a, to) if is_time(a) && to.is_integer() => false,
        (
            a,
            DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _),
        ) if is_time(a) => false,
        _ => true,
    }
}

fn spark_cast_force_nullable(from: &DataType, to: &DataType, from_variant: bool) -> bool {
    if from == &DataType::Null || from == to {
        return false;
    }
    if from_variant {
        return true;
    }
    if from.is_string() {
        return !(to.is_string() || to.is_binary());
    }
    if to.is_string() {
        return false;
    }
    match (from, to) {
        (DataType::Timestamp(_, Some(_)), DataType::Int8 | DataType::Int16 | DataType::Int32)
        | (DataType::Time32(_) | DataType::Time64(_), DataType::Int8 | DataType::Int16)
        | (DataType::Float32 | DataType::Float64, DataType::Timestamp(_, Some(_))) => true,
        (DataType::Timestamp(_, Some(_)), DataType::Date32 | DataType::Date64) => false,
        (_, DataType::Date32 | DataType::Date64) => true,
        (DataType::Date32 | DataType::Date64, DataType::Timestamp(_, Some(_))) => false,
        (DataType::Date32 | DataType::Date64, _)
        | (_, DataType::Interval(IntervalUnit::MonthDayNano)) => true,
        (
            _,
            DataType::Decimal32(p, s)
            | DataType::Decimal64(p, s)
            | DataType::Decimal128(p, s)
            | DataType::Decimal256(p, s),
        ) => !spark_cast_decimal_is_safe(from, *p, *s),
        (
            DataType::Float32
            | DataType::Float64
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _),
            to,
        ) if to.is_integer() => true,
        _ => false,
    }
}

/// Cast.canNullSafeCastToDecimal and DecimalType.isWiderThan.
fn spark_cast_decimal_is_safe(from: &DataType, precision: u8, scale: i8) -> bool {
    let integral_digits = i16::from(precision) - i16::from(scale);
    match from {
        DataType::Decimal32(p, s)
        | DataType::Decimal64(p, s)
        | DataType::Decimal128(p, s)
        | DataType::Decimal256(p, s) => {
            let source_digits = i16::from(*p) - i16::from(*s);
            (integral_digits >= source_digits && scale >= *s) || integral_digits > source_digits
        }
        DataType::Boolean => integral_digits >= 1 && scale >= 0,
        DataType::Int8 => integral_digits >= 3 && scale >= 0,
        DataType::Int16 => integral_digits >= 5 && scale >= 0,
        DataType::Int32 => integral_digits >= 10 && scale >= 0,
        DataType::Int64 => integral_digits >= 20 && scale >= 0,
        _ => false,
    }
}
