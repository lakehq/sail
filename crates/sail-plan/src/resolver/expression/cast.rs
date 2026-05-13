use std::collections::HashMap;
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
use crate::resolver::expression::NamedExpr;
use crate::resolver::function::{
    user_defined_type_metadata, UDT_JVM_CLASS_METADATA_KEY, UDT_PYTHON_CLASS_METADATA_KEY,
    UDT_SERIALIZED_PYTHON_CLASS_METADATA_KEY,
};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

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
        let NamedExpr { expr, name, .. } =
            self.resolve_named_expression(expr, schema, state).await?;
        let output_metadata = if let spec::DataType::UserDefined {
            jvm_class,
            python_class,
            serialized_python_class,
            ..
        } = &cast_to_type
        {
            let (_, field) = expr.to_field(schema)?;
            let metadata = field.metadata();
            let is_source_udt = [
                UDT_JVM_CLASS_METADATA_KEY,
                UDT_PYTHON_CLASS_METADATA_KEY,
                UDT_SERIALIZED_PYTHON_CLASS_METADATA_KEY,
            ]
            .into_iter()
            .any(|key| metadata.contains_key(key));
            let is_same_udt = udt_metadata_matches(metadata, UDT_JVM_CLASS_METADATA_KEY, jvm_class)
                && udt_metadata_matches(metadata, UDT_PYTHON_CLASS_METADATA_KEY, python_class)
                && udt_metadata_matches(
                    metadata,
                    UDT_SERIALIZED_PYTHON_CLASS_METADATA_KEY,
                    serialized_python_class,
                );
            if is_source_udt && !is_same_udt {
                let source_udt = udt_metadata_display_name(metadata);
                let target_udt = udt_display_name(
                    jvm_class.as_deref(),
                    python_class.as_deref(),
                    serialized_python_class.as_deref(),
                );
                return Err(PlanError::AnalysisError(format!(
                    "cannot cast between different user-defined data types: source {source_udt}, target {target_udt}"
                )));
            }
            user_defined_type_metadata(&cast_to_type)
        } else {
            vec![]
        };
        let cast_to_type = self.resolve_data_type(&cast_to_type, state)?;
        let expr_type = expr.get_type(schema)?;
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
        Ok(NamedExpr::new(name, expr).with_metadata(output_metadata))
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

fn udt_metadata_matches(
    metadata: &HashMap<String, String>,
    key: &str,
    expected: &Option<String>,
) -> bool {
    match (metadata.get(key), expected.as_ref()) {
        (Some(actual), Some(expected)) => actual == expected,
        (None, None) => true,
        _ => false,
    }
}

fn udt_metadata_display_name(metadata: &HashMap<String, String>) -> &str {
    udt_display_name(
        metadata.get(UDT_JVM_CLASS_METADATA_KEY).map(String::as_str),
        metadata
            .get(UDT_PYTHON_CLASS_METADATA_KEY)
            .map(String::as_str),
        metadata
            .get(UDT_SERIALIZED_PYTHON_CLASS_METADATA_KEY)
            .map(String::as_str),
    )
}

fn udt_display_name<'a>(
    jvm_class: Option<&'a str>,
    python_class: Option<&'a str>,
    serialized_python_class: Option<&'a str>,
) -> &'a str {
    python_class
        .or(jvm_class)
        .or(serialized_python_class)
        .unwrap_or("<unknown>")
}
