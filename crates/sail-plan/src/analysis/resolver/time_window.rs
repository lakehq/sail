use std::sync::Arc;

use arrow_schema::{DataType, TimeUnit};
use datafusion_common::ScalarValue;
use datafusion_expr::{Operator, ScalarUDF};
use sail_common::utils::datetime::MICROS_PER_DAY;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_window_buckets::SparkWindowBuckets;
use sail_sql_analyzer::literal::interval::IntervalValue;
use sail_sql_analyzer::parser::parse_interval;

use super::Resolver;
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::*;
use crate::error::{PlanError, PlanResult};

fn window_interval_micros(expr: &LogicalExpression) -> PlanResult<i64> {
    let Deferred::Resolved(ExpressionNode::Literal(value)) = &expr.node else {
        return Err(PlanError::invalid(
            "window durations and start time must be literal strings, intervals, or integers",
        ));
    };
    if let Some(s) = value.try_as_str().flatten() {
        return match parse_interval(s)
            .map_err(|e| PlanError::invalid(format!("invalid window interval {s:?}: {e}")))?
        {
            IntervalValue::Microsecond { microseconds, .. } => Ok(microseconds),
            _ => Err(PlanError::invalid(format!(
                "window interval must not contain months or years: {s:?}"
            ))),
        };
    }
    match value {
        // Spark interprets integer literals as microseconds.
        ScalarValue::Int32(Some(v)) => Ok(*v as i64),
        ScalarValue::Int64(Some(v)) => Ok(*v),
        ScalarValue::DurationMicrosecond(Some(v)) => Ok(*v),
        ScalarValue::DurationMillisecond(Some(v)) => Ok(*v * 1_000),
        ScalarValue::DurationSecond(Some(v)) => Ok(*v * 1_000_000),
        ScalarValue::DurationNanosecond(Some(v)) => Ok(*v / 1_000),
        ScalarValue::IntervalDayTime(Some(v)) => {
            Ok(v.days as i64 * MICROS_PER_DAY + v.milliseconds as i64 * 1_000)
        }
        ScalarValue::IntervalMonthDayNano(Some(v)) if v.months == 0 => {
            Ok(v.days as i64 * MICROS_PER_DAY + v.nanoseconds / 1_000)
        }
        _ => Err(PlanError::invalid(
            "window durations and start time must be literal strings, day-time intervals, or integers",
        )),
    }
}

/// The parsed durations (in microseconds) of a Spark `window` call.
#[derive(Debug, Clone, Copy)]
struct WindowSpec {
    window_duration: i64,
    slide_duration: i64,
    start_time: i64,
}

/// Bound on `ceil(windowDuration / slideDuration)`
const MAX_OVERLAPPING_WINDOWS: i64 = 1_000_000;

/// Parses and validates the `window` durations from the full argument list
/// (`args[0]` is the time column; `args[1..]` are window/slide/start).
fn parse_window_spec(args: &[LogicalExpression]) -> PlanResult<WindowSpec> {
    if !(2..=4).contains(&args.len()) {
        return Err(PlanError::invalid(format!(
            "window requires 2 to 4 arguments, got {}",
            args.len()
        )));
    }
    let window_duration = window_interval_micros(&args[1])?;
    let slide_duration = match args.get(2) {
        Some(arg) => window_interval_micros(arg)?,
        None => window_duration,
    };
    let start_time = match args.get(3) {
        Some(arg) => window_interval_micros(arg)?,
        None => 0,
    };
    if window_duration <= 0 {
        return Err(PlanError::invalid(
            "window: the window duration must be greater than 0",
        ));
    }
    if slide_duration <= 0 {
        return Err(PlanError::invalid(
            "window: the slide duration must be greater than 0",
        ));
    }
    if slide_duration > window_duration {
        return Err(PlanError::invalid(
            "window: the slide duration must be less than or equal to the window duration",
        ));
    }
    if start_time >= slide_duration || start_time <= -slide_duration {
        return Err(PlanError::invalid(format!(
            "The `abs(start_time)`({start_time}L) must be < the `slide_duration`({slide_duration}L)."
        )));
    }
    let overlapping = (window_duration + slide_duration - 1) / slide_duration;
    if overlapping > MAX_OVERLAPPING_WINDOWS {
        return Err(PlanError::invalid(format!(
            "window: ceil(windowDuration / slideDuration) = {overlapping} exceeds the limit of {MAX_OVERLAPPING_WINDOWS}"
        )));
    }
    Ok(WindowSpec {
        window_duration,
        slide_duration,
        start_time,
    })
}

/// The `window` struct field type: a microsecond timestamp (non-timestamp inputs
/// are cast, matching Spark's cast of the time column to `TimestampType`).
fn window_field_type(
    time_type: &DataType,
    session_tz: &std::sync::Arc<str>,
) -> PlanResult<DataType> {
    Ok(match time_type {
        DataType::Timestamp(_, tz) => DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
        // Spark casts dates and strings to `TimestampType` (session time zone), so a
        // date becomes midnight in the session time zone, not a naive timestamp.
        DataType::Date32
        | DataType::Date64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View => {
            DataType::Timestamp(TimeUnit::Microsecond, Some(session_tz.clone()))
        }
        other => {
            return Err(PlanError::invalid(format!(
                "window requires a timestamp time column, got {other:?}"
            )));
        }
    })
}

impl Resolver<'_, '_> {
    pub(super) fn window_buckets(
        &mut self,
        values: Vec<LogicalExpression>,
    ) -> PlanResult<LogicalExpression> {
        let spec = parse_window_spec(&values)?;
        let time = values
            .into_iter()
            .next()
            .ok_or_else(|| PlanError::internal("window missing time column"))?;
        let target = window_field_type(
            &time.result_type.resolved("window time")?.data_type,
            &self.context.config.session_timezone,
        )?;
        let time = self.cast(time, target)?;
        self.scalar_udf(
            Arc::new(ScalarUDF::from(SparkWindowBuckets::new(
                spec.window_duration,
                spec.slide_duration,
                spec.start_time,
            ))),
            vec![time],
        )
    }

    pub(super) fn window_time(
        &mut self,
        values: Vec<LogicalExpression>,
    ) -> PlanResult<LogicalExpression> {
        let value = values.one()?;
        let kind = &value
            .result_type
            .resolved("window_time argument")?
            .data_type;
        let target = match kind {
            DataType::Struct(fields)
                if fields.len() == 2
                    && fields[0].name() == "start"
                    && fields[1].name() == "end"
                    && fields.iter().all(|f| {
                        matches!(f.data_type(), DataType::Timestamp(TimeUnit::Microsecond, _))
                    }) =>
            {
                fields[1].data_type().clone()
            }
            _ => {
                return Err(PlanError::invalid(format!(
                    "window_time requires a window column (struct with start and end timestamps), got {kind:?}"
                )));
            }
        };
        let key = self.literal(ScalarValue::Utf8(Some("end".into())));
        let end = self.scalar("get_field", vec![value, key])?;
        let end = self.cast(end, DataType::Int64)?;
        let one = self.literal(ScalarValue::Int64(Some(1)));
        let result = self.binary(end, Operator::Minus, one)?;
        self.cast(result, target)
    }
}
