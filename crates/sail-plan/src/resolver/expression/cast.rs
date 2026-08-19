use std::ops::{Div, Mul};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, IntervalUnit, TimeUnit};
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::{ExprSchemable, ScalarUDF, cast, expr, lit, try_cast, when};
use sail_common::spec;
use sail_common::utils::datetime::time_unit_to_multiplier;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_common_datafusion::variant::is_variant_storage_field;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_interval::{
    SparkCalendarInterval, SparkDayTimeInterval, SparkYearMonthInterval,
};
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_cast_string_to_int32::SparkCastStringToInt32;
use sail_function::scalar::spark_struct_rename::SparkStructRename;
use sail_function::scalar::spark_to_string::{SparkToLargeUtf8, SparkToUtf8, SparkToUtf8View};
use sail_function::scalar::variant::spark_cast_to_variant::SparkCastToVariant;
use sail_function::scalar::variant::spark_variant_get::SparkVariantGet;
use sail_function::scalar::variant::spark_variant_to_json::SparkVariantToJsonUdf;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
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
            let expr = ScalarUDF::new_from_impl(SparkCastToVariant::new()).call(vec![expr]);
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
        let cast_to_type = self.resolve_data_type(&cast_to_type, state)?;
        let NamedExpr { expr, name, .. } =
            self.resolve_named_expression(expr, schema, state).await?;
        let expr_field = expr.to_field(schema)?.1;
        let expr_type = expr_field.data_type().clone();
        let expr_is_variant = is_variant_storage_field(expr_field.as_ref());
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
        );
        // Spark: `Cast.nullable = child.nullable || Cast.forceNullable(from, to)`
        // (Cast.scala:656). A nullable child already propagates through DataFusion's cast,
        // so forcing is only needed when the child is non-nullable.
        let force_nullable =
            !expr_field.is_nullable() && spark_cast_force_nullable(&expr_type, &cast_to_type);
        let expr = match (expr_type, cast_to_type.clone(), is_try) {
            (_, DataType::Utf8, _) if expr_is_variant => cast(
                ScalarUDF::new_from_impl(SparkVariantToJsonUdf::new()).call(vec![expr]),
                DataType::Utf8,
            ),
            (_, DataType::LargeUtf8, _) if expr_is_variant => cast(
                ScalarUDF::new_from_impl(SparkVariantToJsonUdf::new()).call(vec![expr]),
                DataType::LargeUtf8,
            ),
            (_, DataType::Utf8View, _) if expr_is_variant => {
                ScalarUDF::new_from_impl(SparkVariantToJsonUdf::new()).call(vec![expr])
            }
            (_, to, is_try) if expr_is_variant => {
                let service = self.ctx.extension::<PlanService>()?;
                let data_type_string = service.plan_formatter().data_type_to_simple_string(&to)?;
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
            (from, DataType::Timestamp(time_unit, _) | DataType::Duration(time_unit), _)
                if from.is_numeric() =>
            {
                let multiplier = match (day_time_interval_field, &cast_to_type) {
                    (Some(field), DataType::Duration(_)) => day_time_field_to_microseconds(field),
                    _ => time_unit_to_multiplier(&time_unit),
                };
                with_spark_cast_nullability(
                    cast(expr.mul(lit(multiplier)), cast_to_type),
                    force_nullable,
                )?
            }
            (DataType::Timestamp(time_unit, _) | DataType::Duration(time_unit), to, _)
                if to.is_numeric() =>
            {
                with_spark_cast_nullability(
                    cast(
                        lit(1.0)
                            .div(lit(time_unit_to_multiplier(&time_unit)))
                            .mul(cast(expr, DataType::Int64)),
                        to,
                    ),
                    force_nullable,
                )?
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
                tz,
                self.config.ansi_mode,
                is_try,
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
            (_, to, true) => try_cast(expr, to),
            (_, to, _) => with_spark_cast_nullability(cast(expr, to), force_nullable)?,
        };
        Ok(NamedExpr::new(name, expr))
    }
}

/// Force a plain cast to be planned as nullable by wrapping it in a CASE without ELSE, which
/// DataFusion always derives as nullable (same trick as `array_repeat` in
/// `function/scalar/array.rs`). The schema seen by Spark Connect clients comes from the
/// resolved plan, where the wrapper is visible; on the execution path, DataFusion's
/// `SimplifyExpressions` collapses `CASE WHEN true THEN x END` back to `x`
/// (`expr_simplifier.rs`), so plans and runtime behavior are unchanged.
///
/// This relies on DataFusion NOT folding `WHEN true` at resolution time; the
/// "CASE WHEN true wrapping a CAST" scenario in `conversion/cast_nullability.feature` and the
/// `optimized_plan_collapses_nullability_wrapper` test below guard that coupling.
fn with_spark_cast_nullability(
    cast_expr: expr::Expr,
    force_nullable: bool,
) -> PlanResult<expr::Expr> {
    if force_nullable {
        Ok(when(lit(true), cast_expr).end()?)
    } else {
        Ok(cast_expr)
    }
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

/// The decimal type Spark uses to represent each numeric type, from `DecimalType.forType` (DecimalType.scala:133) plus `BooleanDecimal` (:124).
fn decimal_for_type(data_type: &DataType) -> Option<(u8, i8)> {
    match data_type {
        DataType::Boolean => Some((1, 0)),
        DataType::Int8 | DataType::UInt8 => Some((3, 0)),
        DataType::Int16 | DataType::UInt16 => Some((5, 0)),
        DataType::Int32 | DataType::UInt32 => Some((10, 0)),
        DataType::Int64 | DataType::UInt64 => Some((20, 0)),
        DataType::Float16 | DataType::Float32 => Some((14, 7)),
        DataType::Float64 => Some((30, 15)),
        DataType::Decimal32(p, s) | DataType::Decimal64(p, s) => Some((*p, *s)),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => Some((*p, *s)),
        _ => None,
    }
}

fn as_decimal(data_type: &DataType) -> Option<(u8, i8)> {
    match data_type {
        DataType::Decimal32(p, s)
        | DataType::Decimal64(p, s)
        | DataType::Decimal128(p, s)
        | DataType::Decimal256(p, s) => Some((*p, *s)),
        _ => None,
    }
}

fn is_string(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_binary(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    )
}

fn is_date(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Date32 | DataType::Date64)
}

fn is_time(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Time32(_) | DataType::Time64(_))
}

/// Spark's `TimestampType` (local time zone). `Timestamp(_, None)` is `TimestampNTZType`,
/// which matches NONE of the timestamp-specific `forceNullable` cases and falls through to
/// the generic date arms instead.
fn is_timestamp_ltz(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(_, Some(_)))
}

/// Spark's `IntegralType`. Arrow's unsigned types have no Spark counterpart but coerce to the
/// signed ones, so they are treated the same.
fn is_integral(data_type: &DataType) -> bool {
    data_type.is_integer()
}

/// Spark's `FractionalType`: `FloatType`, `DoubleType` and `DecimalType`.
fn is_fractional(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    ) || as_decimal(data_type).is_some()
}

/// `DecimalType.isWiderThan` (DecimalType.scala:72): whether `other` fits in `(precision,
/// scale)` without losing precision or range. A float/double `other` is neither a decimal nor
/// an integral, so it falls through to `false` — which is why casting FLOAT or DOUBLE to
/// DECIMAL is never null-safe in Spark.
fn decimal_is_wider_than(precision: u8, scale: i8, other: &DataType) -> bool {
    let (op, os) = match as_decimal(other) {
        Some(d) => d,
        None if is_integral(other) => match decimal_for_type(other) {
            Some(d) => d,
            None => return false,
        },
        None => return false,
    };
    i32::from(precision) - i32::from(scale) >= i32::from(op) - i32::from(os)
        && i32::from(scale) >= i32::from(os)
}

/// `Cast.canNullSafeCastToDecimal` (Cast.scala:438).
fn can_null_safe_cast_to_decimal(from: &DataType, precision: u8, scale: i8) -> bool {
    if matches!(from, DataType::Boolean) {
        return decimal_is_wider_than(precision, scale, &DataType::Decimal128(1, 0));
    }
    if let Some((fp, fs)) = as_decimal(from) {
        // Spark tries the `NumericType if to.isWiderThan(from)` arm first; the decimal arm
        // then catches truncation/precision loss with the strict integer-digits check.
        return decimal_is_wider_than(precision, scale, from)
            || i32::from(precision) - i32::from(scale) > i32::from(fp) - i32::from(fs);
    }
    if from.is_numeric() {
        return decimal_is_wider_than(precision, scale, from);
    }
    false // overflow
}

/// Whether casting a **non-null** value of `from` to `to` may still produce NULL, mirroring
/// Spark's `Cast.forceNullable` (Cast.scala:452-473). Spark's rule is
/// `Cast.nullable = child.nullable || Cast.forceNullable(from, to)` (Cast.scala:656); the
/// caller handles the child's own nullability, this only answers whether the cast itself can
/// introduce a null.
///
/// Precondition: variant never reaches this function — a variant TARGET is intercepted before
/// `resolve_data_type` and variant SOURCES are matched by the `expr_is_variant` arms, all of
/// which resolve to UDFs that already report nullable (`forceNullable`'s
/// `case (VariantType, _) => true`).
fn spark_cast_force_nullable(from: &DataType, to: &DataType) -> bool {
    // `case (NullType, _) => false` and `case (_, _) if from == to => false`
    if matches!(from, DataType::Null) || from == to {
        return false;
    }
    // `case (_: StringType, BinaryType | _: StringType) => false`
    // `case (_: StringType, _) => true`
    if is_string(from) {
        return !(is_binary(to) || is_string(to));
    }
    // `case (_, _: StringType) => false`
    if is_string(to) {
        return false;
    }
    // `case (TimestampType, ByteType | ShortType | IntegerType) => true`
    if is_timestamp_ltz(from) && matches!(to, DataType::Int8 | DataType::Int16 | DataType::Int32) {
        return true;
    }
    // `case (_: TimeType, ByteType | ShortType) => true`
    if is_time(from) && matches!(to, DataType::Int8 | DataType::Int16) {
        return true;
    }
    // `case (FloatType | DoubleType, TimestampType) => true`
    if matches!(
        from,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    ) && is_timestamp_ltz(to)
    {
        return true;
    }
    // `case (TimestampType, DateType) => false` / `case (_, DateType) => true`
    if is_date(to) {
        return !is_timestamp_ltz(from);
    }
    // `case (DateType, TimestampType) => false` / `case (DateType, _) => true`
    if is_date(from) {
        return !is_timestamp_ltz(to);
    }
    // `case (_, CalendarIntervalType) => true`
    if matches!(to, DataType::Interval(IntervalUnit::MonthDayNano)) {
        return true;
    }
    // `case (_, to: DecimalType) if !canNullSafeCastToDecimal(from, to) => true`
    if let Some((p, s)) = as_decimal(to)
        && !can_null_safe_cast_to_decimal(from, p, s)
    {
        return true;
    }
    // `case (_: FractionalType, _: IntegralType) => true` — NaN, infinity
    if is_fractional(from) && is_integral(to) {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::TimeUnit;
    use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
    use datafusion_common::DFSchema;
    use datafusion_expr::col;

    use super::*;

    /// Guards the coupling `with_spark_cast_nullability` relies on: DataFusion's
    /// `SimplifyExpressions` must collapse the `CASE WHEN true THEN cast END` wrapper back to
    /// the bare cast on the execution path. If a DataFusion upgrade stops folding it, plans
    /// (and snapshots) would silently grow CASE wrappers and per-row evaluation cost — this
    /// test fails loudly instead.
    #[test]
    fn optimized_plan_collapses_nullability_wrapper() -> PlanResult<()> {
        let schema = Arc::new(DFSchema::try_from(arrow::datatypes::Schema::new(vec![
            Field::new("s", DataType::Utf8, false),
        ]))?);
        let wrapped = with_spark_cast_nullability(cast(col("s"), DataType::Int32), true)?;
        assert!(
            matches!(wrapped, expr::Expr::Case(_)),
            "wrapper must be a CASE without ELSE"
        );
        let context = SimplifyContext::builder()
            .with_schema(Arc::clone(&schema))
            .build();
        let simplified = ExprSimplifier::new(context).simplify(wrapped)?;
        assert_eq!(
            simplified,
            cast(col("s"), DataType::Int32),
            "SimplifyExpressions no longer collapses CASE WHEN true THEN x END"
        );
        Ok(())
    }

    fn ts() -> DataType {
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    }

    fn ntz() -> DataType {
        DataType::Timestamp(TimeUnit::Microsecond, None)
    }

    /// Truth table captured from a real Spark JVM (2026-07-29): for a **non-null** input, the
    /// `nullable` flag of `CAST(<literal> AS <type>)` is exactly `Cast.forceNullable(from,
    /// to)`. Rows were generated by measurement, not hand-written, so this checks the
    /// translation against Spark rather than against a reading of it. The full 19x19 matrix
    /// lives in `conversion/cast_nullability.feature`.
    #[test]
    fn matches_sparks_force_nullable() {
        let cases: &[(DataType, DataType, bool)] = &[
            (DataType::Utf8, DataType::Utf8, false),
            (DataType::Utf8, DataType::Binary, false),
            (DataType::Utf8, DataType::Int32, true),
            (DataType::Utf8, DataType::Int64, true),
            (DataType::Utf8, DataType::Float32, true),
            (DataType::Utf8, DataType::Float64, true),
            (DataType::Utf8, DataType::Decimal128(10, 2), true),
            (DataType::Utf8, DataType::Decimal128(5, 4), true),
            (DataType::Utf8, DataType::Boolean, true),
            (DataType::Utf8, DataType::Date32, true),
            (DataType::Utf8, ts(), true),
            (DataType::Utf8, ntz(), true),
            (
                DataType::Utf8,
                DataType::Interval(IntervalUnit::YearMonth),
                true,
            ),
            (
                DataType::Utf8,
                DataType::Duration(TimeUnit::Microsecond),
                true,
            ),
            (DataType::Int32, DataType::Utf8, false),
            (DataType::Int32, DataType::Int32, false),
            (DataType::Int32, DataType::Int64, false),
            (DataType::Int32, DataType::Float32, false),
            (DataType::Int32, DataType::Float64, false),
            (DataType::Int32, DataType::Decimal128(10, 2), true),
            (DataType::Int32, DataType::Decimal128(5, 4), true),
            (DataType::Int32, DataType::Boolean, false),
            (DataType::Int32, ts(), false),
            (DataType::Int8, DataType::Decimal128(10, 2), false),
            (DataType::Int16, DataType::Decimal128(10, 2), false),
            (DataType::Int64, DataType::Utf8, false),
            (DataType::Int64, DataType::Int32, false),
            (DataType::Int64, DataType::Float64, false),
            (DataType::Int64, DataType::Decimal128(10, 2), true),
            (DataType::Int64, DataType::Boolean, false),
            (DataType::Int64, ts(), false),
            (DataType::Float64, DataType::Utf8, false),
            (DataType::Float64, DataType::Int32, true),
            (DataType::Float64, DataType::Int64, true),
            (DataType::Float64, DataType::Float64, false),
            (DataType::Float64, DataType::Float32, false),
            (DataType::Float64, DataType::Decimal128(10, 2), true),
            (DataType::Float64, DataType::Decimal128(5, 4), true),
            (DataType::Float64, DataType::Boolean, false),
            (DataType::Float64, ts(), true),
            (DataType::Float32, DataType::Utf8, false),
            (DataType::Float32, DataType::Int32, true),
            (DataType::Float32, DataType::Int64, true),
            (DataType::Float32, DataType::Float64, false),
            (DataType::Float32, DataType::Decimal128(10, 2), true),
            (DataType::Float32, DataType::Decimal128(5, 4), true),
            (DataType::Float32, DataType::Boolean, false),
            (DataType::Float32, ts(), true),
            (DataType::Decimal128(10, 2), DataType::Utf8, false),
            (DataType::Decimal128(10, 2), DataType::Int32, true),
            (DataType::Decimal128(10, 2), DataType::Int64, true),
            (DataType::Decimal128(10, 2), DataType::Float64, false),
            (DataType::Decimal128(10, 2), DataType::Float32, false),
            (
                DataType::Decimal128(10, 2),
                DataType::Decimal128(10, 2),
                false,
            ),
            (
                DataType::Decimal128(10, 2),
                DataType::Decimal128(5, 4),
                true,
            ),
            // isWiderThan: (12-4) >= (10-2) && 4 >= 2 -> null-safe despite the strict
            // integer-digits check failing (8 > 8 is false)
            (
                DataType::Decimal128(10, 2),
                DataType::Decimal128(12, 4),
                false,
            ),
            (DataType::Decimal128(10, 2), DataType::Boolean, false),
            (DataType::Decimal128(10, 2), ts(), false),
            (
                DataType::Decimal128(5, 4),
                DataType::Decimal128(10, 2),
                false,
            ),
            (DataType::Decimal128(5, 4), DataType::Int32, true),
            (DataType::Boolean, DataType::Utf8, false),
            (DataType::Boolean, DataType::Int32, false),
            (DataType::Boolean, DataType::Float64, false),
            (DataType::Boolean, DataType::Decimal128(10, 2), false),
            (DataType::Boolean, DataType::Decimal128(5, 4), false),
            (DataType::Date32, DataType::Utf8, false),
            (DataType::Date32, DataType::Date32, false),
            (DataType::Date32, ts(), false),
            (DataType::Date32, ntz(), true),
            (ts(), DataType::Utf8, false),
            (ts(), DataType::Int8, true),
            (ts(), DataType::Int16, true),
            (ts(), DataType::Int32, true),
            (ts(), DataType::Int64, false),
            (ts(), DataType::Float32, false),
            (ts(), DataType::Float64, false),
            (ts(), DataType::Decimal128(10, 2), true),
            (ts(), DataType::Decimal128(5, 4), true),
            (ts(), DataType::Date32, false),
            (ts(), ts(), false),
            (ts(), ntz(), false),
            (ntz(), DataType::Utf8, false),
            (ntz(), DataType::Date32, true),
            (ntz(), ts(), false),
            (ntz(), ntz(), false),
            (
                DataType::Interval(IntervalUnit::YearMonth),
                DataType::Int32,
                false,
            ),
            (
                DataType::Interval(IntervalUnit::YearMonth),
                DataType::Decimal128(10, 2),
                true,
            ),
            (
                DataType::Duration(TimeUnit::Microsecond),
                DataType::Int64,
                false,
            ),
            (
                DataType::Duration(TimeUnit::Microsecond),
                DataType::Decimal128(10, 2),
                true,
            ),
            // `case (_, CalendarIntervalType) => true` (Cast.scala:468); not reachable from
            // SQL in the measured matrix, kept faithful to the source
            (
                DataType::Int32,
                DataType::Interval(IntervalUnit::MonthDayNano),
                true,
            ),
            (DataType::Binary, DataType::Utf8, false),
            (DataType::Binary, DataType::Binary, false),
        ];
        let mismatches = cases
            .iter()
            .filter(|(from, to, expected)| spark_cast_force_nullable(from, to) != *expected)
            .map(|(from, to, expected)| format!("{from} -> {to}: spark={expected}"))
            .collect::<Vec<_>>();
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    }
}
