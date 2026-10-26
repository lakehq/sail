use std::sync::Arc;

use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use datafusion_common::ScalarValue;
use datafusion_expr::{Operator, ScalarUDF};
use sail_common::spec;
use sail_common::utils::datetime::time_unit_to_multiplier;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;
use sail_common_datafusion::variant::is_variant_storage_field;
use sail_function::scalar::datetime::convert_tz::ConvertTz;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_interval::{
    SparkCalendarInterval, SparkDayTimeInterval, SparkYearMonthInterval, YearMonthIntervalMonths,
};
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_cast_string_to_int32::SparkCastStringToInt32;
use sail_function::scalar::spark_struct_rename::SparkStructRename;
use sail_function::scalar::spark_to_string::{SparkToLargeUtf8, SparkToUtf8, SparkToUtf8View};
use sail_function::scalar::variant::spark_cast_to_variant::SparkCastToVariant;
use sail_function::scalar::variant::spark_variant_get::SparkVariantGet;
use sail_function::scalar::variant::spark_variant_to_json::SparkVariantToJsonUdf;

use crate::analyzer::deferred::Deferred;
use crate::analyzer::model::*;
use crate::analyzer::typing::cast::{
    build_rename_target_type, decimal_cast_can_overflow, needs_struct_field_rename,
};
use crate::error::{PlanError, PlanResult};
use crate::resolver::{NamedExpression, PlanResolver};

impl PlanResolver<'_> {
    pub(in crate::resolver) fn explicit_cast(
        &mut self,
        inner: NamedExpression,
        target: spec::DataType,
        is_try: bool,
    ) -> PlanResult<NamedExpression> {
        let source_field = inner
            .expression
            .result_type
            .resolved("cast input")?
            .to_field("");
        if let spec::DataType::UserDefined {
            jvm_class,
            python_class,
            ..
        } = &target
            && let Some(metadata) = source_field
                .metadata()
                .get(spec::SAIL_SPARK_UDT_METADATA_KEY)
        {
            let source: spec::SparkUdtMetadata = serde_json::from_str(metadata)
                .map_err(|e| PlanError::internal(format!("invalid UDT metadata: {e}")))?;
            let same = match (&source.jvm_class, jvm_class) {
                (Some(a), Some(b)) => a == b,
                (None, None) => {
                    source.python_class.is_some() && source.python_class == *python_class
                }
                _ => false,
            };
            if same {
                return Ok(inner);
            }
            if source.jvm_class.is_none() || jvm_class.is_none() {
                return Err(PlanError::analysis(
                    "cannot cast between incompatible user-defined types",
                ));
            }
        }
        let rename = needs_cast_name(&inner.expression)?;
        let source_type = source_field.data_type().clone();
        let target_field = self.types().resolve_field(&spec::Field {
            name: String::new(),
            data_type: target.clone(),
            nullable: source_field.is_nullable() || is_try,
            metadata: vec![],
        })?;
        let target_type = target_field.data_type().clone();
        let name = if rename {
            let target_name = if matches!(target, spec::DataType::Variant) {
                "VARIANT".into()
            } else {
                self.context
                    .session
                    .extension::<PlanService>()?
                    .plan_formatter()
                    .data_type_to_simple_string(&target_type)?
                    .to_ascii_uppercase()
            };
            format!(
                "{}CAST({} AS {target_name})",
                if is_try { "TRY_" } else { "" },
                inner.name
            )
        } else {
            inner.name
        };
        let interval_field = match &target {
            spec::DataType::Interval {
                interval_unit: spec::IntervalUnit::DayTime,
                start_field,
                end_field,
            } => end_field.or(*start_field),
            _ => None,
        };
        let variant = is_variant_storage_field(&source_field);
        let override_string = matches!(
            source_type,
            DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Duration(_)
                | DataType::Interval(_)
                | DataType::Timestamp(..)
                | DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(..)
                | DataType::ListView(_)
                | DataType::LargeListView(_)
                | DataType::Struct(_)
                | DataType::Map(..)
        );
        let expression = inner.expression;
        let mut expression = if matches!(target, spec::DataType::Variant) {
            self.context.function_context().scalar_udf(
                Arc::new(ScalarUDF::from(SparkCastToVariant::new())),
                vec![expression],
            )?
        } else {
            match (source_type, target_type.clone()) {
                (
                    DataType::Timestamp(_, None),
                    DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)),
                ) => {
                    if crate::analyzer::typing::temporal::is_spark_compatible_arrow_fixed_offset(
                        &timezone,
                    ) {
                        self.cast_mode(expression, target_type, is_try)?
                    } else {
                        let ntz = self
                            .context
                            .function_context()
                            .cast(expression, DataType::Timestamp(TimeUnit::Microsecond, None))?;
                        let timezone = self
                            .context
                            .function_context()
                            .literal(ScalarValue::Utf8(Some(timezone.to_string())));
                        let utc = self
                            .context
                            .function_context()
                            .literal(ScalarValue::Utf8(Some("UTC".into())));
                        let value = self.context.function_context().scalar_udf(
                            Arc::new(ScalarUDF::from(ConvertTz::new(false))),
                            vec![timezone, utc, ntz],
                        )?;
                        let value = self
                            .context
                            .function_context()
                            .cast(value, DataType::Int64)?;
                        self.context.function_context().cast(value, target_type)?
                    }
                }
                (_, to) if variant && to.is_string() => {
                    let value = self.context.function_context().scalar_udf(
                        Arc::new(ScalarUDF::from(SparkVariantToJsonUdf::new())),
                        vec![expression],
                    )?;
                    if to == DataType::Utf8View {
                        value
                    } else {
                        self.context.function_context().cast(value, to)?
                    }
                }
                (_, to) if variant => {
                    let target = self
                        .context
                        .session
                        .extension::<PlanService>()?
                        .plan_formatter()
                        .data_type_to_simple_string(&to)?;
                    let root = self
                        .context
                        .function_context()
                        .literal(ScalarValue::Utf8(Some("$".into())));
                    let target = self
                        .context
                        .function_context()
                        .literal(ScalarValue::Utf8(Some(target)));
                    self.context.function_context().scalar_udf(
                        Arc::new(ScalarUDF::from(SparkVariantGet::new(is_try))),
                        vec![expression, root, target],
                    )?
                }
                (from, DataType::Int32)
                    if from.is_string() && !is_try && !self.context.config.ansi_mode =>
                {
                    self.context.function_context().scalar_udf(
                        Arc::new(ScalarUDF::from(SparkCastStringToInt32::new())),
                        vec![expression],
                    )?
                }
                (from, DataType::Timestamp(unit, _) | DataType::Duration(unit))
                    if from.is_numeric() =>
                {
                    let multiplier = match (interval_field, &target_type) {
                        (Some(field), DataType::Duration(_)) => {
                            crate::analyzer::typing::cast::day_time_field_to_microseconds(field)
                        }
                        _ => time_unit_to_multiplier(&unit),
                    };
                    let multiplier = self
                        .context
                        .function_context()
                        .literal(ScalarValue::Int64(Some(multiplier)));
                    let value = self.context.function_context().binary(
                        expression,
                        Operator::Multiply,
                        multiplier,
                    )?;
                    self.context.function_context().cast(value, target_type)?
                }
                (DataType::Timestamp(unit, _) | DataType::Duration(unit), to)
                    if to.is_numeric() =>
                {
                    let one = self
                        .context
                        .function_context()
                        .literal(ScalarValue::Float64(Some(1.0)));
                    let multiplier = self
                        .context
                        .function_context()
                        .literal(ScalarValue::Int64(Some(time_unit_to_multiplier(&unit))));
                    let scale = self.context.function_context().binary(
                        one,
                        Operator::Divide,
                        multiplier,
                    )?;
                    let value = self
                        .context
                        .function_context()
                        .cast(expression, DataType::Int64)?;
                    let value =
                        self.context
                            .function_context()
                            .binary(scale, Operator::Multiply, value)?;
                    self.context.function_context().cast(value, to)?
                }
                (DataType::Interval(IntervalUnit::YearMonth), to) if to.is_integer() => {
                    let interval = source_field
                        .metadata()
                        .get(spec::SAIL_SPARK_INTERVAL_METADATA_KEY)
                        .map(|v| spec::SparkIntervalMetadata::from_json(v))
                        .transpose()?;
                    let mut value = self.context.function_context().scalar_udf(
                        Arc::new(ScalarUDF::from(YearMonthIntervalMonths::new())),
                        vec![expression],
                    )?;
                    if matches!(
                        interval,
                        Some(spec::SparkIntervalMetadata::YearMonth {
                            end_field: spec::YearMonthIntervalField::Year,
                            ..
                        })
                    ) {
                        let months = self
                            .context
                            .function_context()
                            .literal(ScalarValue::Int32(Some(12)));
                        value = self.context.function_context().binary(
                            value,
                            Operator::Divide,
                            months,
                        )?;
                    }
                    self.cast_mode(value, to, is_try)?
                }
                (from, DataType::Interval(IntervalUnit::YearMonth)) if from.is_string() => {
                    self.context.function_context().scalar_udf(
                        Arc::new(ScalarUDF::from(SparkYearMonthInterval::new())),
                        vec![expression],
                    )?
                }
                (from, DataType::Duration(TimeUnit::Microsecond)) if from.is_string() => {
                    self.context.function_context().scalar_udf(
                        Arc::new(ScalarUDF::from(SparkDayTimeInterval::new())),
                        vec![expression],
                    )?
                }
                (from, DataType::Interval(IntervalUnit::MonthDayNano)) if from.is_string() => {
                    self.context.function_context().scalar_udf(
                        Arc::new(ScalarUDF::from(SparkCalendarInterval::new())),
                        vec![expression],
                    )?
                }
                (from, DataType::Date32) if from.is_string() => {
                    let function = Arc::new(ScalarUDF::from(SparkDate::new(
                        is_try || !self.context.config.ansi_mode,
                    )));
                    self.context
                        .function_context()
                        .scalar_udf(function, vec![expression])?
                }
                (from, DataType::Timestamp(TimeUnit::Microsecond, timezone))
                    if from.is_string() =>
                {
                    let function = ScalarUDF::from(SparkTimestamp::try_new(
                        timezone,
                        self.context.config.ansi_mode,
                        is_try,
                    )?);
                    self.context
                        .function_context()
                        .scalar_udf(Arc::new(function), vec![expression])?
                }
                (_, to) if to.is_string() && override_string => {
                    let mut arguments = vec![expression];
                    if let Some(interval) = source_field
                        .metadata()
                        .get(spec::SAIL_SPARK_INTERVAL_METADATA_KEY)
                    {
                        arguments.push(
                            self.context
                                .function_context()
                                .literal(ScalarValue::Utf8(Some(interval.clone()))),
                        );
                    }
                    let function = match to {
                        DataType::LargeUtf8 => ScalarUDF::from(SparkToLargeUtf8::new()),
                        DataType::Utf8View => ScalarUDF::from(SparkToUtf8View::new()),
                        _ => ScalarUDF::from(SparkToUtf8::new()),
                    };
                    self.context
                        .function_context()
                        .scalar_udf(Arc::new(function), arguments)?
                }
                (DataType::Date32 | DataType::Date64, to)
                    if to.is_numeric() || to == DataType::Boolean =>
                {
                    if !is_try && self.context.config.ansi_mode {
                        return Err(PlanError::invalid(format!("cannot cast date to {to}")));
                    }
                    self.context
                        .function_context()
                        .literal(ScalarValue::try_from(&to)?)
                }
                (from, to) if needs_struct_field_rename(&from, &to) => {
                    let target = build_rename_target_type(&from, &to);
                    let value = self.context.function_context().scalar_udf(
                        Arc::new(ScalarUDF::from(SparkStructRename::new(target.clone()))),
                        vec![expression],
                    )?;
                    if target == to {
                        value
                    } else {
                        self.cast_mode(value, to, is_try)?
                    }
                }
                (
                    from,
                    DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale),
                ) if from.is_numeric()
                    && !self.context.config.ansi_mode
                    && (is_try || decimal_cast_can_overflow(&from, precision, scale)) =>
                {
                    self.context
                        .function_context()
                        .try_cast(expression, target_type)?
                }
                (_, to) => self.cast_mode(expression, to, is_try)?,
            }
        };
        if !target_field.metadata().is_empty() {
            let mut result_type = expression.result_type.resolved("cast result")?.clone();
            result_type.metadata.extend(target_field.metadata().clone());
            expression = Expr {
                id: self.context.ids.expression(),
                result_type: Deferred::Resolved(result_type),
                kind: Deferred::Resolved(ExprKind::WithMetadata(Box::new(expression))),
            };
        }
        Ok(NamedExpression { name, expression })
    }

    fn cast_mode(&mut self, value: Expr, target: DataType, safe: bool) -> PlanResult<Expr> {
        if safe {
            self.context.function_context().try_cast(value, target)
        } else {
            self.context.function_context().cast(value, target)
        }
    }
}

fn needs_cast_name(expression: &Expr) -> PlanResult<bool> {
    if matches!(&expression.kind, Deferred::Unresolved(request) if matches!(request.as_ref(), spec::Expr::DefaultColumnValue))
    {
        return Err(PlanError::invalid(
            "DEFAULT must be a standalone assignment value",
        ));
    }
    Ok(match expression.kind.resolved("cast name")? {
        ExprKind::WithMetadata(_) | ExprKind::Attribute(_) | ExprKind::OuterAttribute { .. } => {
            false
        }
        ExprKind::Cast { expression, .. } => needs_cast_name(expression)?,
        _ => true,
    })
}
