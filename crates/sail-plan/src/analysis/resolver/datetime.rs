use std::sync::Arc;

use arrow_schema::{DataType, TimeUnit};
use datafusion_common::ScalarValue;
use datafusion_expr::{Operator, ScalarUDF};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_date_format::SparkDateFormat;
use sail_function::scalar::datetime::spark_date_part::SparkDatePart;
use sail_function::scalar::datetime::spark_date_trunc::SparkDateTrunc;
use sail_function::scalar::datetime::spark_next_day::SparkNextDay;
use sail_function::scalar::datetime::spark_time::SparkTime;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::datetime::spark_unix_timestamp::SparkUnixTimestamp;
use sail_function::scalar::datetime::timestamp_now::TimestampNow;

use super::Resolver;
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::*;
use crate::config::DefaultTimestampType;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    pub(super) fn datetime_function(
        &mut self,
        name: &str,
        values: &mut Vec<LogicalExpression>,
    ) -> PlanResult<Option<LogicalExpression>> {
        let part = match name {
            "years" => Some("YEAR"),
            "day" | "dayofmonth" => Some("DAY"),
            "dayofweek" | "weekday" => Some("DOW"),
            "dayofyear" => Some("DOY"),
            "hour" => Some("HOUR"),
            "minute" => Some("MINUTE"),
            "month" => Some("MONTH"),
            "quarter" => Some("QUARTER"),
            "second" => Some("SECOND"),
            _ => None,
        };
        if let Some(part) = part {
            let value = std::mem::take(values).one()?;
            let mut result = self.integer_part(value, part)?;
            if matches!(name, "dayofweek" | "weekday") {
                let one = self.literal(ScalarValue::Int32(Some(1)));
                result = self.binary(
                    result,
                    if name == "weekday" {
                        Operator::Minus
                    } else {
                        Operator::Plus
                    },
                    one,
                )?;
            }
            return Ok(Some(result));
        }
        if !matches!(
            name,
            "curdate"
                | "window_time"
                | "current_timezone"
                | "current_timestamp"
                | "now"
                | "localtimestamp"
                | "date_format"
                | "date_part"
                | "datepart"
                | "extract"
                | "dayname"
                | "monthname"
                | "weekofyear"
                | "date_trunc"
                | "trunc"
                | "date_from_unix_date"
                | "unix_date"
                | "unix_micros"
                | "unix_millis"
                | "unix_seconds"
                | "timestamp_micros"
                | "timestamp_millis"
                | "timestamp_seconds"
                | "next_day"
                | "to_time"
                | "try_to_time"
                | "to_date"
                | "to_timestamp"
                | "to_timestamp_ntz"
                | "to_timestamp_ltz"
                | "try_to_timestamp"
                | "unix_timestamp"
                | "to_unix_timestamp"
                | "from_unixtime"
        ) {
            return Ok(None);
        }
        let mut values = std::mem::take(values);
        let timezone = self.context.config.session_timezone.clone();
        let ansi = self.context.config.ansi_mode;
        let result = match name {
            "window_time" => self.window_time(values)?,
            "curdate" => {
                values.zero()?;
                self.scalar("current_date", vec![])?
            }
            "current_timezone" => {
                values.zero()?;
                self.literal(ScalarValue::Utf8(Some(timezone.to_string())))
            }
            "current_timestamp" | "now" | "localtimestamp" => {
                values.zero()?;
                let result = self.scalar_udf(
                    Arc::new(ScalarUDF::from(TimestampNow::new(
                        timezone,
                        TimeUnit::Microsecond,
                    ))),
                    vec![],
                )?;
                if name == "localtimestamp" {
                    self.scalar("to_local_time", vec![result])?
                } else {
                    result
                }
            }
            "date_format" => {
                crate::analysis::typing::coercion::validate_arity(name, values.len(), 2)?;
                self.scalar_udf(
                    Arc::new(ScalarUDF::from(SparkDateFormat::new(timezone))),
                    values,
                )?
            }
            "date_part" | "datepart" | "extract" => {
                let (part, date) = values.two()?;
                self.scalar_udf(
                    Arc::new(ScalarUDF::from(SparkDatePart::new())),
                    vec![part, date],
                )?
            }
            "dayname" | "monthname" | "weekofyear" => {
                let value = values.one()?;
                let pattern = self.literal(ScalarValue::Utf8(Some(
                    match name {
                        "dayname" => "%a",
                        "monthname" => "%b",
                        _ => "%V",
                    }
                    .into(),
                )));
                let result = self.scalar("to_char", vec![value, pattern])?;
                if name == "weekofyear" {
                    self.cast(result, DataType::Int32)?
                } else {
                    result
                }
            }
            "trunc" | "date_trunc" => {
                let (first, second) = values.two()?;
                let (value, part) = if name == "trunc" {
                    (first, second)
                } else {
                    (second, first)
                };
                let part = self.trunc_part(part)?;
                let result = if name == "trunc" {
                    self.scalar("date_trunc", vec![part, value])?
                } else {
                    self.scalar_udf(
                        Arc::new(ScalarUDF::from(SparkDateTrunc::new())),
                        vec![part, value],
                    )?
                };
                let target = if name == "trunc" {
                    DataType::Date32
                } else {
                    match &result.result_type.resolved("date_trunc result")?.data_type {
                        DataType::Timestamp(_, timezone) => {
                            DataType::Timestamp(TimeUnit::Microsecond, timezone.clone())
                        }
                        other => {
                            return Err(PlanError::internal(format!(
                                "date_trunc expected a timestamp result, got {other:?}"
                            )));
                        }
                    }
                };
                self.cast(result, target)?
            }
            "date_from_unix_date" => self.cast(values.one()?, DataType::Date32)?,
            "unix_date" => {
                let value = self.cast(values.one()?, DataType::Date32)?;
                self.cast(value, DataType::Int32)?
            }
            "unix_micros" | "unix_millis" | "unix_seconds" => {
                let unit = match name {
                    "unix_micros" => TimeUnit::Microsecond,
                    "unix_millis" => TimeUnit::Millisecond,
                    _ => TimeUnit::Second,
                };
                let value = self.cast(values.one()?, DataType::Timestamp(unit, Some(timezone)))?;
                self.cast(value, DataType::Int64)?
            }
            "timestamp_micros" | "timestamp_millis" | "timestamp_seconds" => {
                let mut value = values.one()?;
                if name != "timestamp_micros" {
                    value = self.cast(value, DataType::Int64)?;
                    let multiplier =
                        self.literal(ScalarValue::Int64(Some(if name == "timestamp_millis" {
                            1000
                        } else {
                            1_000_000
                        })));
                    value = self.binary(value, Operator::Multiply, multiplier)?;
                }
                self.cast(
                    value,
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                )?
            }
            "next_day" => {
                self.scalar_udf(Arc::new(ScalarUDF::from(SparkNextDay::new(ansi))), values)?
            }
            "to_time" | "try_to_time" => {
                if !matches!(values.len(), 1 | 2) {
                    return Err(PlanError::invalid(format!(
                        "{name} requires 1 or 2 arguments"
                    )));
                }
                self.scalar_udf(
                    Arc::new(ScalarUDF::from(SparkTime::new(name == "try_to_time"))),
                    values,
                )?
            }
            "to_date" => self.resolve_to_date(values)?,
            "to_timestamp" | "to_timestamp_ntz" | "to_timestamp_ltz" | "try_to_timestamp" => {
                self.resolve_to_timestamp(name, values)?
            }
            "unix_timestamp" | "to_unix_timestamp" => {
                if (values.is_empty() && name == "to_unix_timestamp") || values.len() > 2 {
                    return Err(PlanError::invalid(format!(
                        "{name} requires {} arguments",
                        if name == "unix_timestamp" {
                            "0, 1, or 2"
                        } else {
                            "1 or 2"
                        }
                    )));
                }
                if values.is_empty() {
                    let value = self.scalar_udf(
                        Arc::new(ScalarUDF::from(TimestampNow::new(
                            timezone,
                            TimeUnit::Second,
                        ))),
                        vec![],
                    )?;
                    self.cast(value, DataType::Int64)?
                } else {
                    let mut nullable = false;
                    if values.len() == 2
                        && matches!(
                            values[0]
                                .result_type
                                .resolved("unix_timestamp input")?
                                .data_type,
                            DataType::Date32
                                | DataType::Date64
                                | DataType::Timestamp(..)
                                | DataType::Null
                        )
                    {
                        let format = values
                            .pop()
                            .ok_or_else(|| PlanError::internal("missing format"))?;
                        let field = format.result_type.resolved("unix_timestamp format")?;
                        if !(field.data_type.is_string() || field.data_type.is_null()) {
                            return Err(PlanError::invalid(format!(
                                "unix_timestamp format argument must be a string, got {}",
                                field.data_type
                            )));
                        }
                        nullable = !ansi
                            || values[0]
                                .result_type
                                .resolved("unix_timestamp value")?
                                .nullable
                            || field.nullable;
                    }
                    let value = self.scalar_udf(
                        Arc::new(ScalarUDF::from(SparkUnixTimestamp::new(timezone, ansi))),
                        values,
                    )?;
                    self.declare_nullable(value, nullable)?
                }
            }
            "from_unixtime" => {
                if values.len() == 1 {
                    values
                        .push(self.literal(ScalarValue::Utf8(Some("yyyy-MM-dd HH:mm:ss".into()))));
                }
                let (value, format) = values.two()?;
                let value = self.cast(
                    value,
                    DataType::Timestamp(TimeUnit::Second, Some(timezone.clone())),
                )?;
                self.scalar_udf(
                    Arc::new(ScalarUDF::from(SparkDateFormat::new(timezone))),
                    vec![value, format],
                )?
            }
            _ => return Err(PlanError::internal("unknown datetime function")),
        };
        Ok(Some(result))
    }

    pub(super) fn integer_part(
        &mut self,
        value: LogicalExpression,
        part: &str,
    ) -> PlanResult<LogicalExpression> {
        let part = self.literal(ScalarValue::Utf8(Some(part.to_uppercase())));
        let result = self.scalar("date_part", vec![part, value])?;
        self.cast(result, DataType::Int32)
    }

    fn trunc_part(&mut self, part: LogicalExpression) -> PlanResult<LogicalExpression> {
        let mut branches = vec![];
        for (aliases, name) in [
            (vec!["mon", "mm"], "month"),
            (vec!["yy", "yyyy"], "year"),
            (vec!["dd"], "day"),
        ] {
            let mut condition = None;
            for alias in aliases {
                let value = self.copy_expression(&part)?;
                let alias = self.literal(ScalarValue::Utf8(Some(alias.into())));
                let test = self.pattern(value, alias, false, true, None)?;
                condition = Some(match condition {
                    Some(previous) => self.binary(previous, Operator::Or, test)?,
                    None => test,
                });
            }
            let condition = condition.ok_or_else(|| PlanError::internal("empty trunc aliases"))?;
            let value = self.literal(ScalarValue::Utf8(Some(name.into())));
            branches.push((condition, value));
        }
        self.case(branches, Some(part))
    }

    pub(super) fn declare_nullable(
        &mut self,
        value: LogicalExpression,
        nullable: bool,
    ) -> PlanResult<LogicalExpression> {
        if nullable && !value.result_type.resolved("nullable result")?.nullable {
            let null = self.literal(ScalarValue::try_from(
                &value.result_type.resolved("nullable result")?.data_type,
            )?);
            let yes = self.literal(ScalarValue::Boolean(Some(true)));
            self.case(vec![(yes, value)], Some(null))
        } else {
            Ok(value)
        }
    }

    fn resolve_to_date(&mut self, values: Vec<LogicalExpression>) -> PlanResult<LogicalExpression> {
        let null_on_error = !self.context.config.ansi_mode;
        if values.len() == 1 {
            let value = values.one()?;
            if value
                .result_type
                .resolved("to_date input")?
                .data_type
                .is_string()
            {
                return self.scalar_udf(
                    Arc::new(ScalarUDF::from(SparkDate::new(null_on_error))),
                    vec![value],
                );
            }
            return self.cast(value, DataType::Date32);
        }
        let (value, format) = values.two()?;
        let field = format.result_type.resolved("date format")?;
        if field.data_type.is_nested() {
            return Err(PlanError::invalid(format!(
                "to_date format argument must be a string, got {}",
                field.data_type
            )));
        }
        let format = if field.data_type.is_string() {
            format
        } else {
            self.cast(format, DataType::Utf8)?
        };
        let source = value.result_type.resolved("to_date input")?;
        let kind = source.data_type.clone();
        if matches!(
            kind,
            DataType::Timestamp(..) | DataType::Date32 | DataType::Date64
        ) {
            let nullable = null_on_error
                || source.nullable
                || format.result_type.resolved("date format")?.nullable;
            let value = if matches!(kind, DataType::Timestamp(..)) {
                self.scalar("to_local_time", vec![value])?
            } else {
                value
            };
            let value = self.cast(value, DataType::Date32)?;
            return self.declare_nullable(value, nullable);
        }
        let value = if kind.is_string() || kind.is_nested() {
            value
        } else {
            self.cast(value, DataType::Utf8)?
        };
        self.scalar_udf(
            Arc::new(ScalarUDF::from(SparkDate::new(null_on_error))),
            vec![value, format],
        )
    }

    fn resolve_to_timestamp(
        &mut self,
        name: &str,
        values: Vec<LogicalExpression>,
    ) -> PlanResult<LogicalExpression> {
        let ntz = name == "to_timestamp_ntz"
            || (name != "to_timestamp_ltz"
                && matches!(
                    self.context.config.default_timestamp_type,
                    DefaultTimestampType::TimestampNtz
                ));
        let timezone = if ntz {
            None
        } else {
            Some(self.context.config.session_timezone.clone())
        };
        let target = DataType::Timestamp(TimeUnit::Microsecond, timezone.clone());
        let safe = name == "try_to_timestamp";
        let ansi = self.context.config.ansi_mode;
        if values.len() == 1 {
            let value = values.one()?;
            let kind = &value.result_type.resolved("timestamp input")?.data_type;
            if kind.is_string() {
                return self.scalar_udf(
                    Arc::new(ScalarUDF::from(SparkTimestamp::try_new(
                        timezone, ansi, safe,
                    )?)),
                    vec![value],
                );
            }
            let value = if matches!(kind, DataType::Timestamp(_, Some(_))) {
                self.scalar("to_local_time", vec![value])?
            } else {
                value
            };
            return if safe {
                self.try_cast(value, target)
            } else {
                self.cast(value, target)
            };
        }
        if values.len() != 2 {
            return Err(PlanError::invalid(format!(
                "{name} requires 1 or 2 arguments"
            )));
        }
        if values.iter().any(
            |v| matches!(&v.node, Deferred::Resolved(ExpressionNode::Literal(v)) if v.is_null()),
        ) {
            return Ok(self.literal(ScalarValue::TimestampMicrosecond(None, timezone)));
        }
        let value = self.scalar_udf(
            Arc::new(ScalarUDF::from(SparkTimestamp::try_new(
                timezone, ansi, safe,
            )?)),
            values,
        )?;
        self.cast(value, target)
    }
}
