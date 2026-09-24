use std::ops::{Div, Mul};
use std::sync::Arc;

use arrow::datatypes::{DataType, IntervalUnit, TimeUnit, i256};
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
    SparkCalendarInterval, SparkDayTimeInterval, SparkYearMonthInterval, YearMonthIntervalMonths,
};
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::misc::spark_udt_storage::SparkUdtStorage;
use sail_function::scalar::spark_cast_string_to_int32::SparkCastStringToInt32;
use sail_function::scalar::spark_struct_rename::SparkStructRename;
use sail_function::scalar::spark_to_string::{SparkToLargeUtf8, SparkToUtf8, SparkToUtf8View};
use sail_function::scalar::variant::spark_cast_to_variant::SparkCastToVariant;
use sail_function::scalar::variant::spark_variant_get::SparkVariantGet;
use sail_function::scalar::variant::spark_variant_to_json::SparkVariantToJsonUdf;

use crate::coercion::{build_rename_target_type, needs_struct_field_rename};
use crate::error::{PlanError, PlanResult};
use crate::function::common::is_spark_udt_field;
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
        // A cast yields its target type, never the UDT it is applied to. DataFusion copies the
        // source field's metadata through a cast, UDT marker included, so the UDT is read as its
        // storage first, or a column projected from the cast would still be a UDT.
        let expr = if is_spark_udt_field(&expr_field) {
            ScalarUDF::from(SparkUdtStorage::new()).call(vec![expr])
        } else {
            expr
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
            ) => ScalarUDF::new_from_impl(SparkDate::new(is_try || !self.config.ansi_mode))
                .call(vec![expr]),
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
            // TODO: Spark has no cast from a numeric to DATE in either mode (`Cast.scala:223-255` and
            //  `:92-122`), and takes an INTEGRAL to BINARY only in the non-ANSI `canCast`
            //  (`Cast.scala:234`), never through `TRY_CAST`. Sail casts the underlying integer and
            //  answers. That is an ACCEPT-more gap, and it is left open on purpose: refusing it broke
            //  existing users of the cast -- the ClickBench fixture reads `EventDate` with
            //  `cast("int").cast("date")`. `cast.feature` pins both directions.
            // `castToBoolean` is `value != 0` for every numeric (`Cast.scala:840-847`), and
            // `canAnsiCast` admits the whole family (`Cast.scala:105`). Arrow has no DECIMAL to
            // BOOLEAN kernel, so the comparison is spelled out here rather than refused.
            (DataType::Decimal128(precision, scale), DataType::Boolean, _) => {
                expr.not_eq(lit(ScalarValue::Decimal128(Some(0), precision, scale)))
            }
            (DataType::Decimal256(precision, scale), DataType::Boolean, _) => expr.not_eq(lit(
                ScalarValue::Decimal256(Some(i256::ZERO), precision, scale),
            )),
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
            (_, to, _) => cast(expr, to),
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
