use std::sync::Arc;

use arrow_schema::{DataType, TimeUnit};
use datafusion_common::ScalarValue;
use datafusion_expr::{HigherOrderUDF, ScalarUDF};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_common_datafusion::variant::is_variant_storage_field;
use sail_function::scalar::datetime::convert_tz::{ConvertTz, ConvertTzLazy};
use sail_function::scalar::datetime::spark_make_timestamp_ntz::SparkMakeTimestampNtz;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;

use super::Resolver;
use crate::analysis::ir::*;
use crate::config::DefaultTimestampType;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    pub(super) fn timezone_function(
        &mut self,
        name: &str,
        values: &mut Vec<LogicalExpression>,
    ) -> PlanResult<Option<LogicalExpression>> {
        if !matches!(
            name,
            "convert_timezone"
                | "from_utc_timestamp"
                | "to_utc_timestamp"
                | "make_timestamp"
                | "make_timestamp_ltz"
                | "make_timestamp_ntz"
                | "try_make_timestamp"
                | "try_make_timestamp_ltz"
                | "try_make_timestamp_ntz"
        ) {
            return Ok(None);
        }
        let mut values = std::mem::take(values);
        let session_tz = self.context.config.session_timezone.clone();
        let result = match name {
            "convert_timezone" => {
                if values.len() == 2 {
                    values.insert(
                        0,
                        self.literal(ScalarValue::Utf8(Some(session_tz.to_string()))),
                    );
                }
                let (from, to, timestamp) = values.three()?;
                for value in [&from, &to] {
                    let field = value.result_type.resolved("time zone")?.to_field("");
                    if !datafusion::arrow::compute::can_cast_types(
                        field.data_type(),
                        &DataType::Utf8,
                    ) && !is_variant_storage_field(field.as_ref())
                    {
                        return Err(PlanError::invalid(format!(
                            "convert_timezone time zone arguments must be castable to string, got {}",
                            field.data_type()
                        )));
                    }
                }
                let (timestamp, _) = self.ntz_timestamp(timestamp, false)?;
                let args = vec![from, to, timestamp];
                let mut eager = false;
                for arg in &args {
                    eager |= arg.any(&|node| match node {
                        ExpressionNode::ScalarFunction { function, .. } => function.inner().is::<sail_python_udf::udf::pyspark_udf::PySparkUDF>() || function.inner().is::<sail_function::scalar::variant::spark_cast_to_variant::SparkCastToVariant>(),
                        _ => false,
                    })?;
                }
                let function = ConvertTz::new(true).with_null_short_circuit();
                if eager {
                    self.scalar_udf(Arc::new(ScalarUDF::from(function)), args)?
                } else {
                    self.lazy_arguments(
                        Arc::new(HigherOrderUDF::new_from_impl(ConvertTzLazy::new(function))),
                        args,
                    )?
                }
            }
            "from_utc_timestamp" | "to_utc_timestamp" => {
                let (timestamp, zone) = values.two()?;
                let (timestamp, unit) = self.ntz_timestamp(timestamp, true)?;
                let utc = self.literal(ScalarValue::Utf8(Some("UTC".into())));
                let args = if name == "from_utc_timestamp" {
                    vec![utc, zone, timestamp]
                } else {
                    vec![zone, utc, timestamp]
                };
                let result =
                    self.scalar_udf(Arc::new(ScalarUDF::from(ConvertTz::new(false))), args)?;
                let result = self.cast(result, DataType::Timestamp(unit, Some("UTC".into())))?;
                self.cast(result, DataType::Timestamp(unit, Some(session_tz)))?
            }
            _ => {
                let safe = name.starts_with("try_");
                let default = matches!(name, "make_timestamp" | "try_make_timestamp");
                let ntz = name.ends_with("_ntz")
                    || (default
                        && matches!(
                            self.context.config.default_timestamp_type,
                            DefaultTimestampType::TimestampNtz
                        ));
                if default && values.len() == 1 {
                    values.push(self.literal(ScalarValue::Time64Microsecond(Some(0))));
                }
                let zone = if matches!(values.len(), 3 | 7) && (!ntz || default) {
                    values.pop()
                } else {
                    None
                };
                if !matches!(values.len(), 2 | 6) {
                    return Err(PlanError::invalid(format!(
                        "{name} requires {} arguments",
                        if ntz { "2 or 6" } else { "2, 3, 6 or 7" }
                    )));
                }
                let mut result = self.scalar_udf(
                    Arc::new(ScalarUDF::from(SparkMakeTimestampNtz::new(safe))),
                    values,
                )?;
                if !ntz {
                    if let Some(zone) = zone {
                        let target = self.literal(ScalarValue::Utf8(Some(session_tz.to_string())));
                        result = self.scalar_udf(
                            Arc::new(ScalarUDF::from(ConvertTz::new(true))),
                            vec![zone, target, result],
                        )?;
                    }
                    result = self.cast(
                        result,
                        DataType::Timestamp(TimeUnit::Microsecond, Some(session_tz)),
                    )?;
                }
                result
            }
        };
        Ok(Some(result))
    }

    fn ntz_timestamp(
        &mut self,
        value: LogicalExpression,
        relative_to_utc: bool,
    ) -> PlanResult<(LogicalExpression, TimeUnit)> {
        let kind = value
            .result_type
            .resolved("timestamp input")?
            .data_type
            .clone();
        let unit = match kind {
            DataType::Timestamp(unit, _) => unit,
            _ => TimeUnit::Microsecond,
        };
        let timezone = relative_to_utc.then(|| self.context.config.session_timezone.clone());
        let result = match kind {
            DataType::Timestamp(_, Some(_)) if !relative_to_utc => {
                self.scalar("to_local_time", vec![value])?
            }
            DataType::Timestamp(_, Some(_)) => value,
            DataType::Timestamp(_, None) | DataType::Date32 | DataType::Date64 => {
                self.cast(value, DataType::Timestamp(unit, timezone))?
            }
            DataType::Null if !relative_to_utc => {
                self.cast(value, DataType::Timestamp(unit, None))?
            }
            kind if kind.is_string() => self.scalar_udf(
                Arc::new(ScalarUDF::from(SparkTimestamp::try_new(
                    timezone,
                    self.context.config.ansi_mode,
                    false,
                )?)),
                vec![value],
            )?,
            other => {
                return Err(PlanError::invalid(format!(
                    "invalid {}NTZ timestamp type: {other:?}",
                    if relative_to_utc { "UTC " } else { "" }
                )));
            }
        };
        let result = if relative_to_utc {
            self.cast(result, DataType::Timestamp(unit, None))?
        } else {
            result
        };
        Ok((result, unit))
    }
}
