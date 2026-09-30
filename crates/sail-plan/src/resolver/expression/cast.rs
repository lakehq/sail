use std::ops::{Div, Mul};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, IntervalUnit, TimeUnit};
use datafusion::functions::expr_fn::lpad;
use datafusion::functions::math::expr_fn::isnan;
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
use sail_function::scalar::datetime::spark_interval::{
    SparkCalendarInterval, SparkDayTimeInterval, SparkDayTimeIntervalFromInt64,
    SparkYearMonthInterval, SparkYearMonthIntervalFromInt64, YearMonthIntervalMonths,
};
use sail_function::scalar::datetime::spark_string_to_time::SparkStringToTime;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::misc::raise_error::RaiseError;
use sail_function::scalar::spark_cast_string_to_int32::SparkCastStringToInt32;
use sail_function::scalar::spark_integral_to_binary::SparkIntegralToBinary;
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
        // Spark's TIME has 7 declared precisions (0-6), all physically stored as
        // `Time64(Microsecond)` (see `spec::DataType::Time32`/`Time64`), so the
        // declared precision must be threaded through as Field metadata the same
        // way the day-time interval field range is above -- but only when it is
        // not 6, the "natural" precision a plain TIME literal already has with
        // no metadata at all. Attaching it for 6 too would make every plain TIME
        // literal (which never goes through this metadata-attaching CAST path)
        // inconsistent with an explicit `CAST(... AS TIME)` of the same natural
        // precision, which DataFusion's VALUES-list field-metadata check rejects
        // outright when they're mixed in the same list.
        let time_precision = match &cast_to_type {
            spec::DataType::Time32 { precision, .. } | spec::DataType::Time64 { precision, .. }
                if *precision != 6 =>
            {
                Some(*precision)
            }
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
                DataType::Time32(from_unit) | DataType::Time64(from_unit),
                to @ (DataType::Time32(_) | DataType::Time64(_)),
                _,
            ) => truncate_time_to_precision(expr, from_unit, &to, time_precision)?,
            (
                DataType::Time32(unit) | DataType::Time64(unit),
                to @ (DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)),
                is_try,
            ) => {
                // Cast.scala: TimeType -> Decimal reads the raw value as seconds
                // (Decimal.apply(t, 14, 9)): the fractional seconds must be exact,
                // so build it through a string -- decimal division's inferred result
                // scale drops digits, and float division rounds in binary.
                let raw = cast(expr, DataType::Int64);
                let raw_is_not_null = raw.clone().is_not_null();
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
                    let fraction_str =
                        lpad(vec![cast(fraction, DataType::Utf8), lit(digits), lit("0")]);
                    concat(vec![
                        cast(whole.clone(), DataType::Utf8),
                        lit("."),
                        fraction_str,
                    ])
                };
                let exact = cast(exact, DataType::Decimal128(38, digits.min(38) as i8));
                if is_try || !self.config.ansi_mode {
                    try_cast(exact, to)
                } else {
                    // Spark's NUMERIC_VALUE_OUT_OF_RANGE, unlike CAST_OVERFLOW, is
                    // decimal-precision-specific. `changePrecision` (Decimal.scala:387-476)
                    // rounds the exact value to the target scale FIRST (`ROUND_HALF_UP`),
                    // THEN checks the rounded value's precision -- so, like the sibling
                    // TIMESTAMP->Decimal arm above, checking `whole` (computed BEFORE that
                    // rounding) against a bound would miss a rounding carry that overflows
                    // the target (e.g. `9.999999 -> DECIMAL(3,2)` rounds to `10.00`). Using
                    // `try_cast`'s own rounded rescale result -- NULL only on overflow, since
                    // TIME is never negative and the only NULL source here is a NULL input --
                    // reproduces Spark's round-then-check order exactly.
                    let casted = try_cast(exact, to.clone());
                    let overflow = raw_is_not_null.and(casted.clone().is_null());
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
                    .otherwise(casted)?
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
            // Spark's legacy (non-ANSI) numeric casts (`castToByte`/`castToShort`/`castToInt`'s
            // `case x: NumericType =>` branch, Cast.scala) reuse Java's/Scala's saturating
            // `.toInt`/`.toLong` float-to-integral conversion: NaN becomes `0`, and anything
            // too large or too small saturates to the target's own extreme instead of erroring
            // or returning NULL. TINYINT/SMALLINT/INT all share Int32::MAX/MIN as the
            // saturation point (`.toInt` runs first; TINYINT/SMALLINT then narrow that further
            // via the same two's-complement truncation as `wrap_narrow_integer` below); BIGINT
            // saturates directly via `.toLong` (Int64::MAX/MIN), never through Int32.
            (DataType::Float32 | DataType::Float64, to, is_try)
                if !is_try && !self.config.ansi_mode && to.is_integer() =>
            {
                let saturated = saturating_double_to_i64(expr, !matches!(to, DataType::Int64))?;
                if matches!(to, DataType::Int8 | DataType::Int16) {
                    wrap_narrow_integer(saturated, &to)?
                } else {
                    cast(saturated, to.clone())
                }
            }
            (
                DataType::Float32 | DataType::Float64,
                DataType::Timestamp(time_unit, Some(_)),
                true,
            ) => {
                // TRY_CAST always analyzes/executes against `doubleToTimestampAnsi`
                // (Cast.scala:763,770; `canTryCast` delegates to `canAnsiCast`), which
                // THROWS on NaN/Infinite/overflow -- caught by TRY_CAST's generic
                // exception-to-NULL wrapper. `try_cast` alone already returns NULL for
                // all three (NaN, +-Infinity, and genuine out-of-range-for-Int64), so no
                // explicit NaN guard is needed here.
                let multiplier = time_unit_to_multiplier(&time_unit);
                try_cast(expr.mul(lit(multiplier)), cast_to_type)
            }
            (
                DataType::Float32 | DataType::Float64,
                DataType::Timestamp(time_unit, Some(_)),
                false,
            ) if !self.config.ansi_mode => {
                // Spark's `doubleToTimestamp` (DateTimeUtils.scala:794-796) is `if
                // (d.isNaN || d.isInfinite) null else (d * MICROS_PER_SECOND).toLong` --
                // NaN AND Infinite become NULL (unlike the INTEGER-target rule, where
                // `saturating_double_to_i64` maps NaN to `0` and saturates infinity to
                // MAX/MIN), but any other finite overflow saturates via the same
                // Java/Scala `.toLong` narrowing that helper already reproduces.
                let multiplier = time_unit_to_multiplier(&time_unit);
                let is_null = isnan(expr.clone())
                    .or(expr.clone().eq(lit(f64::INFINITY)))
                    .or(expr.clone().eq(lit(f64::NEG_INFINITY)));
                let saturated = saturating_double_to_i64(expr.mul(lit(multiplier)), false)?;
                datafusion_expr::when(is_null, lit(ScalarValue::try_from(&cast_to_type)?))
                    .otherwise(cast(saturated, cast_to_type.clone()))?
            }
            (
                DataType::Float32 | DataType::Float64,
                DataType::Timestamp(time_unit, Some(_)),
                false,
            ) if self.config.ansi_mode => {
                // `doubleToTimestampAnsi` (DateTimeUtils.scala:74-80) throws
                // `CAST_INVALID_INPUT` on NaN/Infinite (reported against the ORIGINAL
                // value, source DOUBLE, target TIMESTAMP); otherwise it multiplies by
                // `MICROS_PER_SECOND` and hands that off to `DoubleExactNumeric.toLong`
                // (numerics.scala:168-174), which throws `CAST_OVERFLOW` if the
                // MULTIPLIED value doesn't fit `Long` -- reported as DOUBLE -> BIGINT
                // (the intermediate Long conversion, not the TIMESTAMP target), with
                // the already-multiplied value in the message. FloatType takes the
                // same path via `.toDouble` first (Cast.scala:766-770), so both source
                // types report "DOUBLE" (matching `toSQLType(DoubleType)`), never
                // "FLOAT". Verified against the Spark 4.2 JVM: `CAST('NaN' AS DOUBLE) AS
                // TIMESTAMP` raises `[CAST_INVALID_INPUT] ... "DOUBLE" ... "TIMESTAMP" ...
                // malformed ...`; `CAST('1e20' AS DOUBLE) AS TIMESTAMP` raises
                // `[CAST_OVERFLOW] The value 1.0E26D of the type "DOUBLE" cannot be cast
                // to "BIGINT" due to an overflow ...` (1e20 * 1e6 = 1e26).
                let multiplier = time_unit_to_multiplier(&time_unit);
                let double_expr = cast(expr, DataType::Float64);
                let scaled = double_expr.clone().mul(lit(multiplier as f64));
                let is_invalid = isnan(double_expr.clone())
                    .or(double_expr.clone().eq(lit(f64::INFINITY)))
                    .or(double_expr.clone().eq(lit(f64::NEG_INFINITY)));
                let overflow = scaled
                    .clone()
                    .lt(lit(i64::MIN as f64))
                    .or(scaled.clone().gt(lit(i64::MAX as f64)));
                let invalid_value_string =
                    ScalarUDF::from(SparkToUtf8View::new()).call(vec![double_expr.clone()]);
                let invalid_message = concat(vec![
                    lit("[CAST_INVALID_INPUT] The value "),
                    invalid_value_string,
                    lit(
                        " of the type \"DOUBLE\" cannot be cast to \"TIMESTAMP\" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead.",
                    ),
                ]);
                let overflow_value_string =
                    ScalarUDF::from(SparkToUtf8View::new()).call(vec![scaled.clone()]);
                let overflow_message = concat(vec![
                    lit("[CAST_OVERFLOW] The value "),
                    overflow_value_string,
                    lit(
                        " of the type \"DOUBLE\" cannot be cast to \"BIGINT\" due to an overflow. Use `try_cast` to tolerate overflow and return NULL instead.",
                    ),
                ]);
                datafusion_expr::when(
                    is_invalid,
                    ScalarUDF::from(RaiseError::new()).call(vec![invalid_message]),
                )
                .when(
                    overflow,
                    ScalarUDF::from(RaiseError::new()).call(vec![overflow_message]),
                )
                // `otherwise` is evaluated over the WHOLE batch, not just the rows that
                // select it, so it must itself be non-throwing -- a plain `cast` here
                // would still error out on the very overflowing rows the guards above
                // exist to catch. `try_cast` never throws; the guards above are what
                // actually raise, per row, via the WHEN branches selecting over it.
                .otherwise(try_cast(scaled, cast_to_type.clone()))?
            }
            // Spark's `castToTimestamp`/`castToBoolean` for BOOLEAN (Cast.scala:743-744,725)
            // treat the boolean as the raw microsecond value itself (0 or 1), not as a count
            // of seconds like the general NumericType rule below -- and only under the
            // legacy/non-ANSI rule (`canAnsiCast` has no BooleanType <-> TimestampType case at
            // all, only `canCast` does, Cast.scala:237,243).
            (DataType::Boolean, to @ DataType::Timestamp(_, Some(_)), is_try) => {
                // `canAnsiCast` has no BooleanType <-> TimestampType case at all (only
                // `canCast`, the legacy rule, does) -- TRY_CAST always analyzes against
                // `canAnsiCast`, so it must reject this pair even with ANSI off.
                if is_legacy_only_pair(is_try, self.config.ansi_mode) {
                    return Err(PlanError::invalid(format!("cannot cast boolean to {to}")));
                }
                cast(cast(expr, DataType::Int64), to.clone())
            }
            (from @ DataType::Timestamp(_, Some(_)), DataType::Boolean, is_try) => {
                if is_legacy_only_pair(is_try, self.config.ansi_mode) {
                    return Err(PlanError::invalid(format!("cannot cast {from} to boolean")));
                }
                cast(expr, DataType::Int64).not_eq(lit(0_i64))
            }
            // Spark's `castToDecimal` for TIMESTAMP (Cast.scala:1119-1123) can overflow the
            // target precision; under ANSI this throws (a plain, non-try `cast` below,
            // matching Spark's CAST_OVERFLOW), but under non-ANSI Spark returns NULL instead
            // of propagating the overflow as an error. This applies regardless of ANSI to
            // the exact-decimal CONSTRUCTION itself (Cast.scala has no `ansiEnabled` branch
            // for this pair at all) -- only the overflow-handling differs by ANSI.
            (
                from,
                to @ (DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)),
                is_try,
            ) if matches!(from, DataType::Timestamp(_, Some(_))) => {
                let DataType::Timestamp(time_unit, _) = from else {
                    unreachable!("guarded above")
                };
                // `Decimal.apply(t, 19, 6)` (Cast.scala:1119-1121) treats the raw
                // microsecond `Long` as an EXACT unscaled decimal, never floating point --
                // build it the same way the TIME->Decimal arm above does (through a
                // string), rather than `1.0 / multiplier * t` in Float64, which loses
                // precision once `t` exceeds 2^53 (~year 2255).
                let multiplier = time_unit_to_multiplier(&time_unit);
                let digits: i32 = match time_unit {
                    TimeUnit::Second => 0,
                    TimeUnit::Millisecond => 3,
                    TimeUnit::Microsecond => 6,
                    TimeUnit::Nanosecond => 9,
                };
                // Divide/modulo directly on `raw` (truncating division never overflows,
                // unlike negating `raw` itself first, which panics/wraps for the one value
                // `raw` can actually be that has no positive i64 counterpart: `i64::MIN`,
                // reachable via `saturating_seconds_to_micros`'s own saturation above).
                // `whole`/`fraction` are then negated separately, each already far smaller
                // in magnitude than `raw` itself, so that negation is always safe.
                let raw = cast(expr, DataType::Int64);
                let raw_is_not_null = raw.clone().is_not_null();
                let is_negative = raw.clone().lt(lit(0_i64));
                let sign =
                    datafusion_expr::when(is_negative.clone(), lit("-")).otherwise(lit(""))?;
                let whole = raw.clone().div(lit(multiplier));
                let whole_abs =
                    datafusion_expr::when(is_negative.clone(), lit(0_i64) - whole.clone())
                        .otherwise(whole)?;
                let exact = if digits == 0 {
                    concat(vec![sign, cast(whole_abs.clone(), DataType::Utf8)])
                } else {
                    let fraction = raw % lit(multiplier);
                    let fraction_abs =
                        datafusion_expr::when(is_negative, lit(0_i64) - fraction.clone())
                            .otherwise(fraction)?;
                    let fraction_str = lpad(vec![
                        cast(fraction_abs, DataType::Utf8),
                        lit(digits),
                        lit("0"),
                    ]);
                    concat(vec![
                        sign,
                        cast(whole_abs.clone(), DataType::Utf8),
                        lit("."),
                        fraction_str,
                    ])
                };
                let exact = cast(exact, DataType::Decimal128(38, digits.min(38) as i8));
                if is_try || !self.config.ansi_mode {
                    try_cast(exact, to.clone())
                } else {
                    // Spark's decimal-target overflow is always `NUMERIC_VALUE_OUT_OF_RANGE`
                    // (`QueryExecutionErrors.cannotChangeDecimalPrecisionError`,
                    // `changePrecision`'s overflow branch), regardless of source type --
                    // the same class the TIME->Decimal arm above already raises explicitly.
                    // `changePrecision` (Decimal.scala:387-476) rounds the exact value to the
                    // target scale FIRST (`ROUND_HALF_UP`), THEN checks the rounded value's
                    // precision -- so a pre-check against `whole_abs` (computed BEFORE that
                    // rounding) misses the case where rounding carries a digit past the
                    // target's capacity (e.g. `9.999999 -> DECIMAL(3,2)` rounds to `10.00`,
                    // which overflows even though the truncated whole part, `9`, does not).
                    // `try_cast` already performs that exact rounded rescale and returns NULL
                    // on overflow, so comparing its result against non-null input reproduces
                    // Spark's round-then-check order precisely, with no separate bound to
                    // get right (or to overflow computing, as a prior version of this guard
                    // did for `precision - scale >= 19`).
                    let casted = try_cast(exact, to.clone());
                    let overflow = raw_is_not_null.and(casted.clone().is_null());
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
                    .otherwise(casted)?
                }
            }
            // Spark's `longToTimestamp` (Cast.scala:745-752,799) is `SECONDS.toMicros(t)`,
            // one of Java's `TimeUnit` conversions -- these saturate to `Long.MAX_VALUE`/
            // `MIN_VALUE` on overflow instead of throwing or silently wrapping, and they do
            // so unconditionally: for both CAST and TRY_CAST, under ANSI on or off (there is
            // no `ansiEnabled` branch in `castToTimestamp`'s integral arms at all). Spark has
            // no unsigned integer types at all, so this is `is_signed_integer()`, not the
            // broader `is_integer()` (which also matches UInt8/16/32/64) -- a UInt64 value
            // >= 2^63 falls through to the general numeric arm below instead, unaffected by
            // this saturating rule Spark never defined for it.
            (from, DataType::Timestamp(time_unit, Some(_)), _) if from.is_signed_integer() => {
                let multiplier = time_unit_to_multiplier(&time_unit);
                saturating_seconds_to_micros(
                    cast(expr, DataType::Int64),
                    multiplier,
                    &cast_to_type,
                )?
            }
            // Spark's `canCast`/`canAnsiCast` allow NumericType -> TimestampType (with a
            // timezone) but never NumericType -> TimestampNTZType -- only STRING, DATE, and
            // TIMESTAMP itself may become TIMESTAMP_NTZ (Cast.scala:112-114). The `Some(_)`
            // here (vs. the bare `_` this used to be) is what excludes TIMESTAMP_NTZ. Integer
            // sources take the saturating arm above instead; what reaches here is Decimal, or
            // Float/Double under a plain CAST with ANSI on (the NaN-guarded arm above only
            // covers TRY_CAST or ANSI off).
            (from, DataType::Timestamp(time_unit, Some(_)), _) if from.is_numeric() => {
                let multiplier = time_unit_to_multiplier(&time_unit);
                cast(expr.mul(lit(multiplier)), cast_to_type)
            }
            // Spark's day-time interval cast rule is `(IntegralType | DecimalType,
            // AnsiIntervalType) => true` (Cast.scala:265) -- FloatType/DoubleType are
            // deliberately excluded, unlike the broader NumericType rule for TIMESTAMP above.
            (from, DataType::Duration(time_unit), _) if from.is_integer() || from.is_decimal() => {
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
            (DataType::Duration(TimeUnit::Microsecond), to, is_try)
                if to.is_integer() || to.is_decimal() =>
            {
                // Spark's `dayTimeIntervalToLong`/`dayTimeIntervalToDecimal` both divide by
                // the interval's end field (Cast.scala:1141-1144), NOT a fixed unit -- e.g.
                // a plain `INTERVAL DAY` becomes `5.00`, not `432000.00` seconds. Keep integer
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
            // `Some(_)` (not the bare `_` this used to be) excludes TIMESTAMP_NTZ: Spark has
            // `(TimestampType, _: NumericType) => true` but no equivalent rule for
            // TimestampNTZType at all (Cast.scala:135,272) -- see the TIMESTAMP_NTZ ->
            // NumericType rejection arm above, which this would otherwise pre-empt. Duration
            // (day-time interval) is handled by its own dedicated arm above -- Spark's
            // day-time interval cast rule (`AnsiIntervalType -> IntegralType | DecimalType`,
            // Cast.scala:266) excludes FloatType/DoubleType, unlike TimestampType's broader
            // NumericType rule, so it cannot share this arm.
            (DataType::Timestamp(time_unit, Some(_)), to, _) if to.is_numeric() => cast(
                lit(1.0)
                    .div(lit(time_unit_to_multiplier(&time_unit)))
                    .mul(cast(expr, DataType::Int64)),
                to,
            ),
            // See the comment above: FloatType/DoubleType are excluded from Spark's day-time
            // interval cast rule, but Arrow's own numeric cast kernel would otherwise accept
            // this natively.
            (DataType::Duration(_), to @ (DataType::Float32 | DataType::Float64), _) => {
                return Err(PlanError::invalid(format!(
                    "cannot cast interval day to {to}"
                )));
            }
            (DataType::Interval(IntervalUnit::YearMonth), to, is_try)
                if to.is_integer() || to.is_decimal() =>
            {
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
                DataType::Time32(_) | DataType::Time64(_),
                is_try,
            ) => {
                // Spark's `Cast.castToTime` parses a STRING directly to microsecond
                // resolution and does NOT truncate the value to the target's declared
                // precision -- only an explicit TIME -> TIME(n) cast truncates the
                // value (see `truncate_time_to_precision` below). The declared
                // precision is still attached separately as Field metadata further
                // down via `time_precision`.
                ScalarUDF::new_from_impl(SparkStringToTime::new(is_try || !self.config.ansi_mode))
                    .call(vec![expr])
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
            (DataType::Date32 | DataType::Date64, to, is_try)
                if to.is_numeric() || matches!(to, DataType::Boolean) =>
            {
                // Spark has no DATE -> NumericType/BooleanType rule under `canAnsiCast`
                // at all (only the legacy `canCast` does) -- TRY_CAST analyzes against
                // `canAnsiCast`, so it must reject this pair even with ANSI off.
                if is_legacy_only_pair(is_try, self.config.ansi_mode) {
                    return Err(PlanError::invalid(format!("cannot cast date to {to}")));
                }
                lit(ScalarValue::try_from(&to)?)
            }
            // Spark has no TIMESTAMP_NTZ -> NumericType rule at all (Cast.scala only lists
            // STRING/DATE/TIMESTAMP -> TIMESTAMP_NTZ, never the reverse into a number); unlike
            // the DATE -> numeric case above, this is invalid for every ANSI setting, not just
            // ANSI on. Arrow's own cast kernel treats Timestamp(_, None) as a plain integer of
            // its unit and would otherwise silently allow this.
            (DataType::Timestamp(_, None), to, _) if to.is_numeric() => {
                return Err(PlanError::invalid(format!(
                    "cannot cast timestamp_ntz to {to}"
                )));
            }
            // Same rule, the other direction: no NumericType -> TIMESTAMP_NTZ either (Spark
            // only allows NumericType -> the tz-aware TIMESTAMP, Cast.scala:110,244). Arrow's
            // own cast kernel treats Timestamp(_, None) as a plain integer of its unit and
            // would otherwise silently allow this too.
            (from, DataType::Timestamp(_, None), _) if from.is_numeric() => {
                return Err(PlanError::invalid(format!(
                    "cannot cast {from} to timestamp_ntz"
                )));
            }
            // Arrow's own cast kernel supports BOOLEAN <-> every other numeric type
            // natively, but not <-> Decimal; Spark's `castToDecimal`/`castToBoolean`
            // (Cast.scala:1116-1118,725-726) treat it the same as any other numeric type
            // (true/false <-> 1/0, and any nonzero decimal is truthy), so route through a
            // plain Int8 for the BOOLEAN side rather than leaving this unsupported.
            (
                DataType::Boolean,
                DataType::Decimal32(precision, scale)
                | DataType::Decimal64(precision, scale)
                | DataType::Decimal128(precision, scale)
                | DataType::Decimal256(precision, scale),
                is_try,
            ) => {
                let as_int8 = cast(expr, DataType::Int8);
                // Reuse the same non-ANSI/TRY_CAST overflow-to-NULL rule the general
                // NumericType -> Decimal arm below applies (`castToDecimal`'s
                // `toPrecision` returns NULL on overflow outside ANSI) -- this arm
                // would otherwise shadow it and hard-error via a plain `cast` instead.
                if is_try
                    || (!self.config.ansi_mode
                        && decimal_cast_can_overflow(&DataType::Int8, precision, scale))
                {
                    try_cast(as_int8, cast_to_type)
                } else {
                    cast(as_int8, cast_to_type)
                }
            }
            (
                DataType::Decimal32(..)
                | DataType::Decimal64(..)
                | DataType::Decimal128(..)
                | DataType::Decimal256(..),
                DataType::Boolean,
                _,
            ) => {
                // A decimal is zero iff its underlying integer is zero, regardless of
                // scale, so comparing directly to the `0` literal (which DataFusion
                // coerces to match) is exact -- unlike casting through Int8 first, which
                // would truncate/overflow for a value outside Int8's range.
                expr.not_eq(lit(0_i64))
            }
            // Spark's day-time interval cast rule (`IntegralType | DecimalType ->
            // AnsiIntervalType`, Cast.scala:265) excludes FloatType/DoubleType, but Arrow's own
            // numeric-to-duration cast kernel accepts them, so this needs an explicit
            // rejection -- the arms above only *narrow* which numeric types build a value via
            // the multiply UDF, they do not reject what falls through.
            (from @ (DataType::Float32 | DataType::Float64), to @ DataType::Duration(_), _) => {
                return Err(PlanError::invalid(format!("cannot cast {from} to {to}")));
            }
            // Spark has no NumericType -> DateType rule at all (only STRING/TIMESTAMP/
            // TIMESTAMP_NTZ -> DATE, Cast.scala:127-130). Arrow's own cast kernel natively
            // supports Int32/Int64 -> Date32 (interpreting the number as days since the
            // epoch) but not Int8/Int16, which is why only the narrower integer widths were
            // already failing (with an unrelated Arrow-native message) before this arm.
            (from, to @ (DataType::Date32 | DataType::Date64), _) if from.is_numeric() => {
                return Err(PlanError::invalid(format!("cannot cast {from} to {to}")));
            }
            // Spark's `canCast`/`canAnsiCast` only ever allow `ArrayType -> ArrayType`
            // (element-wise) or `NullType -> anything`; a scalar can never become an ARRAY.
            // Arrow's own cast kernel disagrees: it wraps ANY source into a length-1 list by
            // casting each value to the list's element type (`cast_values_to_list` in
            // arrow-cast), so e.g. `CAST(some_timestamp AS ARRAY<INT>)` silently succeeds or
            // fails with an unrelated "Can't cast value ... to type Int32" depending on
            // whether the timestamp's raw micros happen to fit in an Int32 -- never Spark's
            // clean, value-independent rejection.
            (
                from,
                to @ (DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(_, _)
                | DataType::ListView(_)
                | DataType::LargeListView(_)),
                _,
            ) if !matches!(
                from,
                DataType::Null
                    | DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::FixedSizeList(_, _)
                    | DataType::ListView(_)
                    | DataType::LargeListView(_)
            ) =>
            {
                return Err(PlanError::invalid(format!("cannot cast {from} to {to}")));
            }
            (
                from @ (DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64),
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
                is_try,
            ) => {
                // Spark's `canAnsiCast` does not allow integral -> BINARY at all (only
                // `canCast`, the legacy/non-ANSI rule, does); under ANSI a plain CAST is
                // rejected with a "turning off ANSI would allow it" hint, and TRY_CAST
                // (which always analyzes against `canAnsiCast`) rejects it too, even with
                // ANSI off. `NumberConverter.toBinary` (Cast.scala:688-694) then produces
                // fixed-width BIG-ENDIAN bytes, unlike Arrow's own numeric-to-binary
                // cast kernel, which uses native (little-endian) byte order.
                if is_legacy_only_pair(is_try, self.config.ansi_mode) {
                    return Err(PlanError::invalid(format!("cannot cast {from} to binary")));
                }
                ScalarUDF::new_from_impl(SparkIntegralToBinary::new()).call(vec![expr])
            }
            // Spark's non-ANSI integral cast (`Cast.scala` integral `case`s under
            // `castToByte`/`castToShort`/`castToInt`) reuses Java's narrowing primitive
            // conversion: keep the target's low-order bits, sign-extended (`v.toByte`/
            // `.toShort`/`.toInt` in Scala) -- e.g. `CAST(-2147483648 AS SMALLINT)` wraps to
            // `0`, it does not overflow. Under ANSI this instead throws CAST_OVERFLOW (handled
            // by the plain `cast` in the generic fallback below), so this only applies when
            // ANSI is off; Arrow/DataFusion's own cast kernel errors on the overflow either way.
            (
                _from @ (DataType::Int16 | DataType::Int32 | DataType::Int64),
                to @ DataType::Int8,
                is_try,
            )
            | (_from @ (DataType::Int32 | DataType::Int64), to @ DataType::Int16, is_try)
            | (_from @ DataType::Int64, to @ DataType::Int32, is_try)
                if !is_try && !self.config.ansi_mode =>
            {
                wrap_narrow_integer(expr, &to)?
            }
            // Spark's non-ANSI string parsing for a primitive (`castToBooleanCode`/
            // `castToIntegralType`/`castToDecimal`/`castToFloat`/`castToDouble` for
            // `StringType` in Cast.scala) returns NULL for an empty string rather than
            // throwing. The `otherwise` branch uses `try_cast`, not `cast`: constant folding
            // may still visit it (e.g. to evaluate a `WHEN` condition that turns out false),
            // and a plain `cast('', Int8)` throws even when unselected, same reasoning as the
            // `RaiseError` guards elsewhere in this file. `resolve_values_nan_types`
            // (sail-plan's VALUES resolver) is taught to see through both this wrapper and
            // `TryCast` so a `CAST('NaN' AS ...)` literal is still detected. `STRING -> INT`
            // is excluded: it already has its own dedicated lenient parser above.
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                to @ (DataType::Int8
                | DataType::Int16
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal32(..)
                | DataType::Decimal64(..)
                | DataType::Decimal128(..)
                | DataType::Decimal256(..)
                | DataType::Boolean),
                false,
            ) if !self.config.ansi_mode => {
                datafusion_expr::when(expr.clone().eq(lit("")), lit(ScalarValue::try_from(&to)?))
                    .otherwise(try_cast(expr, to))?
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
                DataType::Decimal32(precision, scale)
                | DataType::Decimal64(precision, scale)
                | DataType::Decimal128(precision, scale)
                | DataType::Decimal256(precision, scale),
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
        // Interval field-range metadata and TIME precision metadata are
        // mutually exclusive (a CAST target is never both), so at most one of
        // these is ever `Some`.
        let extra_metadata = spark_interval_metadata
            .map(|metadata| (spec::SAIL_SPARK_INTERVAL_METADATA_KEY, metadata))
            .or_else(|| {
                time_precision.map(|precision| {
                    (
                        spec::SAIL_SPARK_TIME_PRECISION_METADATA_KEY,
                        precision.to_string(),
                    )
                })
            });
        Ok(match extra_metadata {
            Some((key, metadata)) => {
                // Nested expressions consume the Expr without its NamedExpr metadata.
                // Keep the target qualifier on the cast field as well as the projection.
                let field = expr.to_field(schema)?.1;
                let mut field_metadata = field.metadata().clone();
                field_metadata.insert(key.to_string(), metadata.clone());
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
                NamedExpr::new(name, expr).with_metadata(vec![(key.to_string(), metadata)])
            }
            None => NamedExpr::new(name, expr),
        })
    }
}

/// Truncates a TIME value toward zero to `target_precision`'s exact declared
/// digit count (Spark's `DateTimeUtils.truncateTimeToPrecision`), not merely to
/// whichever Arrow `TimeUnit` the target physically uses -- e.g. TIME(6) ->
/// TIME(1) must drop to whole tenths of a second, even though both are backed
/// by `Time64(Microsecond)`/`Time32(Millisecond)`-class storage today. Works in
/// nanoseconds so any (from, to) `TimeUnit` pair is exact.
fn truncate_time_to_precision(
    expr: expr::Expr,
    from_unit: TimeUnit,
    to: &DataType,
    target_precision: Option<u8>,
) -> PlanResult<expr::Expr> {
    let to_unit = match to {
        DataType::Time32(unit) | DataType::Time64(unit) => *unit,
        _ => {
            return Err(PlanError::internal(format!(
                "expected TIME data type, got {to}"
            )));
        }
    };
    let target_precision = target_precision.unwrap_or(match to {
        DataType::Time32(TimeUnit::Second) => 0,
        DataType::Time32(_) => 3,
        DataType::Time64(TimeUnit::Nanosecond) => 9,
        DataType::Time64(_) => 6,
        _ => unreachable!("guarded above"),
    });
    // The common case today: every declared-precision TIME is physically
    // `Time64(Microsecond)` (see `spec::DataType::Time32`/`Time64`), so `from`
    // and `to` already agree and the nanosecond round-trip below buys nothing
    // -- it is exactly `micros / granularity * granularity` either way. Kept as
    // a fast path rather than replacing the general version, which still
    // handles a `from`/`to` pair with genuinely different units (e.g. a plain
    // Arrow/Parquet `Time32` column with no declared-precision metadata).
    if from_unit == TimeUnit::Microsecond && to_unit == TimeUnit::Microsecond {
        let granularity = 10_i64.pow(u32::from(6 - target_precision.min(6)));
        let value = cast(expr, DataType::Int64)
            .div(lit(granularity))
            .mul(lit(granularity));
        return Ok(cast(value, to.clone()));
    }
    let nanos_per_from_unit = 1_000_000_000_i64 / time_unit_to_multiplier(&from_unit);
    let raw_nanos = cast(expr, DataType::Int64).mul(lit(nanos_per_from_unit));
    let granularity = 10_i64.pow(u32::from(9 - target_precision.min(9)));
    let truncated_nanos = raw_nanos.div(lit(granularity)).mul(lit(granularity));
    let nanos_per_to_unit = 1_000_000_000_i64 / time_unit_to_multiplier(&to_unit);
    let value_in_to_unit = truncated_nanos.div(lit(nanos_per_to_unit));
    Ok(match to {
        DataType::Time32(_) => cast(cast(value_in_to_unit, DataType::Int32), to.clone()),
        _ => cast(value_in_to_unit, to.clone()),
    })
}

/// Java's `TimeUnit.SECONDS.toMicros`-style saturating multiply, reused by Spark's
/// `longToTimestamp` (Cast.scala:799) to convert an integral seconds count to microseconds:
/// `TimeUnit`'s conversions saturate to `Long.MAX_VALUE`/`MIN_VALUE` on overflow instead of
/// wrapping or throwing (`java.util.concurrent.TimeUnit`'s internal `x(d, m, over)` helper:
/// `d > over -> MAX`, `d < -over -> MIN`, else `d * m`, where `over = MAX / m`). `wide` must
/// already be `Int64`; the result is cast to `to` (the target `Timestamp`) afterward.
fn saturating_seconds_to_micros(
    wide: expr::Expr,
    multiplier: i64,
    to: &DataType,
) -> PlanResult<expr::Expr> {
    let over = i64::MAX / multiplier;
    let saturated = datafusion_expr::when(wide.clone().gt(lit(over)), lit(i64::MAX))
        .when(wide.clone().lt(lit(-over)), lit(i64::MIN))
        .otherwise(wide.mul(lit(multiplier)))?;
    Ok(cast(saturated, to.clone()))
}

/// Java's/Scala's saturating `double`/`float` -> `int`/`long` conversion (`.toInt`/`.toLong`):
/// NaN becomes `0`, and a value outside the target width saturates to its extreme instead of
/// wrapping, erroring, or becoming NULL. Always returns an `Int64` expression; the caller casts
/// (or, for a width narrower than the `thirty_two_bit` target itself, further truncates via
/// [`wrap_narrow_integer`]) down to the real target afterward.
fn saturating_double_to_i64(expr: expr::Expr, thirty_two_bit: bool) -> PlanResult<expr::Expr> {
    let (max_value, min_value): (i64, i64) = if thirty_two_bit {
        (i64::from(i32::MAX), i64::from(i32::MIN))
    } else {
        (i64::MAX, i64::MIN)
    };
    let safe_type = if thirty_two_bit {
        DataType::Int32
    } else {
        DataType::Int64
    };
    // `try_cast` is NULL exactly when the value is NaN or genuinely out of range for
    // `safe_type` -- reusing it here avoids re-deriving Java's exact floating-point boundary
    // comparisons (which safe/try-cast already implements correctly) by hand.
    let safe = cast(try_cast(expr.clone(), safe_type), DataType::Int64);
    let saturated = datafusion_expr::when(expr.clone().gt(lit(0.0_f64)), lit(max_value))
        .otherwise(lit(min_value))?;
    // A NULL input must stay NULL, not fall into the "out of range" branch below --
    // `try_cast` also returns NULL for a genuine NULL input, so `safe.is_null()` alone
    // cannot tell a NULL apart from overflow.
    Ok(
        datafusion_expr::when(expr.clone().is_null(), lit(ScalarValue::Int64(None)))
            .when(isnan(expr.clone()), lit(0_i64))
            .when(safe.clone().is_null(), saturated)
            .otherwise(safe)?,
    )
}

/// Truncates to `to`'s low-order bits, sign-extended -- Java/Scala's narrowing primitive
/// conversion (`.toByte`/`.toShort`/`.toInt`), which Spark's non-ANSI integral casts reuse
/// (`Cast.scala`'s plain `castToByte`/`castToShort`/`castToInt` for an `IntegralType` source).
/// An in-range value passes through unchanged; this only changes the result for a value that
/// would otherwise overflow the target.
fn wrap_narrow_integer(expr: expr::Expr, to: &DataType) -> PlanResult<expr::Expr> {
    let shift: i64 = match to {
        DataType::Int8 => 64 - 8,
        DataType::Int16 => 64 - 16,
        DataType::Int32 => 64 - 32,
        _ => {
            return Err(PlanError::internal(format!(
                "expected a narrower integer type, got {to}"
            )));
        }
    };
    // Shifting the target width's low-order bits up to the Int64 sign bit and back down
    // (an arithmetic, sign-extending shift for a signed type) keeps only those low-order
    // bits, sign-extended -- the same two's-complement truncation as the modulus-based
    // arithmetic this replaces, without the division/modulo it required.
    let wide = cast(expr, DataType::Int64);
    let wrapped = (wide << lit(shift)) >> lit(shift);
    Ok(cast(wrapped, to.clone()))
}

/// True for a cast pair that `canAnsiCast` never allows (only the legacy, non-ANSI `canCast`
/// does): `TRY_CAST` always analyzes against `canAnsiCast`, so it must reject the pair even
/// with ANSI off, and a plain `CAST` under ANSI is rejected the same way. Shared by the
/// legacy-only pairs below (Boolean<->Timestamp with tz, Date->numeric/boolean,
/// Integral->Binary), each of which still raises its own message on `true`.
fn is_legacy_only_pair(is_try: bool, ansi_mode: bool) -> bool {
    is_try || ansi_mode
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
