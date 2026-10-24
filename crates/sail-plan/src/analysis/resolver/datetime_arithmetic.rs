use std::sync::Arc;

use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use datafusion_common::ScalarValue;
use datafusion_expr::{Operator, ScalarUDF};
use datafusion_spark::function::datetime::make_dt_interval::SparkMakeDtInterval;
use datafusion_spark::function::datetime::make_interval::SparkMakeInterval;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_last_day::SparkLastDay;

use super::Resolver;
use crate::analysis::deferred::Deferred;
use crate::analysis::model::*;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    pub(super) fn datetime_arithmetic_function(
        &mut self,
        name: &str,
        values: &mut Vec<ExprNode>,
    ) -> PlanResult<Option<ExprNode>> {
        if !matches!(
            name,
            "add_years"
                | "add_months"
                | "add_days"
                | "date_add"
                | "dateadd"
                | "date_sub"
                | "timestampadd"
                | "timestamp_add"
                | "date_diff"
                | "datediff"
                | "timestampdiff"
                | "timestamp_diff"
                | "make_date"
                | "months_between"
        ) {
            return Ok(None);
        }
        let values = std::mem::take(values);
        let result = match name {
            "add_years" | "add_months" | "add_days" | "date_sub" => {
                let (date, quantity) = values.two()?;
                self.date_interval(
                    date,
                    quantity,
                    match name {
                        "add_years" => "years",
                        "add_months" => "months",
                        _ => "days",
                    },
                    if name == "date_sub" {
                        Operator::Minus
                    } else {
                        Operator::Plus
                    },
                )?
            }
            "date_add" | "dateadd" if values.len() == 2 => {
                let (date, quantity) = values.two()?;
                self.date_interval(date, quantity, "days", Operator::Plus)?
            }
            "date_add" | "dateadd" | "timestampadd" | "timestamp_add" => {
                let (unit, quantity, timestamp) = values.three()?;
                let unit = self.datetime_unit(&unit)?;
                let interval = self.timestampadd_interval(name, &unit, quantity)?;
                let timestamp = self.cast(
                    timestamp,
                    DataType::Timestamp(
                        TimeUnit::Microsecond,
                        Some(self.context.config.session_timezone.clone()),
                    ),
                )?;
                self.binary(timestamp, Operator::Plus, interval)?
            }
            "make_date" => {
                crate::analysis::typing::coercion::validate_arity(name, values.len(), 3)?;
                if values.iter().any(|v| {
                    matches!(
                        v.kind,
                        Deferred::Resolved(ExprKind::Literal(ScalarValue::Null))
                    )
                }) {
                    self.literal(ScalarValue::Null)
                } else {
                    self.scalar("make_date", values)?
                }
            }
            "months_between" => self.months_between(values)?,
            _ => self.datetime_difference(values)?,
        };
        Ok(Some(result))
    }

    fn datetime_unit(&self, value: &ExprNode) -> PlanResult<String> {
        match value.kind.resolved("datetime unit")? {
            ExprKind::Literal(value) => value
                .try_as_str()
                .flatten()
                .map(|s| s.trim().to_uppercase())
                .ok_or_else(|| {
                    PlanError::invalid("datetime unit must be a string literal or keyword")
                }),
            _ => Err(PlanError::invalid(
                "datetime unit must be a string literal or keyword",
            )),
        }
    }

    fn date_interval(
        &mut self,
        date: ExprNode,
        quantity: ExprNode,
        unit: &str,
        operator: Operator,
    ) -> PlanResult<ExprNode> {
        let suffix = self.literal(ScalarValue::Utf8(Some(format!(" {unit}"))));
        let interval = self.binary(quantity, Operator::StringConcat, suffix)?;
        let interval = self.cast(
            interval,
            DataType::Interval(if unit == "days" {
                IntervalUnit::DayTime
            } else {
                IntervalUnit::YearMonth
            }),
        )?;
        let date = self.cast(date, DataType::Date32)?;
        self.binary(date, operator, interval)
    }

    fn timestampadd_interval(
        &mut self,
        name: &str,
        unit: &str,
        quantity: ExprNode,
    ) -> PlanResult<ExprNode> {
        let (index, factor, calendar) = match unit {
            "YEAR" => (0, 1.0, true),
            "QUARTER" => (1, 3.0, true),
            "MONTH" => (1, 1.0, true),
            "WEEK" => (0, 7.0, false),
            "DAY" | "DAYOFYEAR" => (0, 1.0, false),
            "HOUR" => (1, 1.0, false),
            "MINUTE" => (2, 1.0, false),
            "SECOND" => (3, 1.0, false),
            "MILLISECOND" => (3, 0.001, false),
            "MICROSECOND" => (3, 0.000001, false),
            _ => {
                return Err(PlanError::invalid(format!(
                    "{name} does not support interval unit type '{unit}'"
                )));
            }
        };
        let quantity = if index == 3 {
            let quantity = self.cast(quantity, DataType::Float64)?;
            let scale = self.literal(ScalarValue::Float64(Some(1.0 / factor)));
            self.binary(quantity, Operator::Divide, scale)?
        } else if factor != 1.0 {
            let scale = self.literal(ScalarValue::Int32(Some(factor as i32)));
            let quantity = self.binary(quantity, Operator::Multiply, scale)?;
            self.cast(quantity, DataType::Int32)?
        } else {
            self.cast(quantity, DataType::Int32)?
        };
        let mut args = (0..if calendar { index + 1 } else { 4 })
            .map(|i| {
                self.literal(if i == 3 {
                    ScalarValue::Float64(Some(0.0))
                } else {
                    ScalarValue::Int32(Some(0))
                })
            })
            .collect::<Vec<_>>();
        args[index] = quantity;
        let function = if calendar {
            ScalarUDF::from(SparkMakeInterval::new())
        } else {
            ScalarUDF::from(SparkMakeDtInterval::new())
        };
        self.scalar_udf(Arc::new(function), args)
    }

    fn datetime_difference(&mut self, values: Vec<ExprNode>) -> PlanResult<ExprNode> {
        if values.len() == 2 {
            let (left, right) = values.two()?;
            return self.date_days_difference(left, right);
        }
        let (unit, start, end) = values.three()?;
        let unit = self.datetime_unit(&unit)?;
        match unit.as_str() {
            "DAY" => self.date_days_difference(end, start),
            "MONTH" | "QUARTER" | "YEAR" => {
                let months = self.timestamp_month_diff(start, end)?;
                let divisor = self.literal(ScalarValue::Int64(Some(match unit.as_str() {
                    "YEAR" => 12,
                    "QUARTER" => 3,
                    _ => 1,
                })));
                self.binary(months, Operator::Divide, divisor)
            }
            "HOUR" | "MINUTE" | "SECOND" | "WEEK" => {
                let start = self.timestamp_micros(start)?;
                let end = self.timestamp_micros(end)?;
                let diff = self.binary(end, Operator::Minus, start)?;
                let divisor = self.literal(ScalarValue::Int64(Some(match unit.as_str() {
                    "WEEK" => 604_800_000_000,
                    "HOUR" => 3_600_000_000,
                    "MINUTE" => 60_000_000,
                    _ => 1_000_000,
                })));
                self.binary(diff, Operator::Divide, divisor)
            }
            _ => Err(PlanError::unsupported(format!("datediff unit: {unit}"))),
        }
    }

    fn date_days_difference(&mut self, left: ExprNode, right: ExprNode) -> PlanResult<ExprNode> {
        let left = self.cast(left, DataType::Date32)?;
        let right = self.cast(right, DataType::Date32)?;
        let left = self.cast(left, DataType::Int64)?;
        let right = self.cast(right, DataType::Int64)?;
        self.binary(left, Operator::Minus, right)
    }

    fn timestamp_micros(&mut self, value: ExprNode) -> PlanResult<ExprNode> {
        let value = self.cast(value, DataType::Timestamp(TimeUnit::Microsecond, None))?;
        self.cast(value, DataType::Int64)
    }

    fn timestamp_months(&mut self, value: ExprNode) -> PlanResult<ExprNode> {
        let year_value = self.copy_expression(&value)?;
        let year = self.integer_part(year_value, "YEAR")?;
        let month = self.integer_part(value, "MONTH")?;
        let twelve = self.literal(ScalarValue::Int32(Some(12)));
        let year = self.binary(year, Operator::Multiply, twelve)?;
        self.binary(year, Operator::Plus, month)
    }

    fn timestamp_time_micros(&mut self, value: ExprNode) -> PlanResult<ExprNode> {
        let copy = self.copy_expression(&value)?;
        let day = self.literal(ScalarValue::Utf8(Some("DAY".into())));
        let midnight = self.scalar("date_trunc", vec![day, value])?;
        let midnight = self.timestamp_micros(midnight)?;
        let value = self.timestamp_micros(copy)?;
        self.binary(value, Operator::Minus, midnight)
    }

    fn timestamp_day_time_before(
        &mut self,
        left: ExprNode,
        right: ExprNode,
    ) -> PlanResult<ExprNode> {
        let l = self.copy_expression(&left)?;
        let r = self.copy_expression(&right)?;
        let l = self.integer_part(l, "DAY")?;
        let r = self.integer_part(r, "DAY")?;
        let lc = self.copy_expression(&l)?;
        let rc = self.copy_expression(&r)?;
        let before = self.binary(l, Operator::Lt, r)?;
        let same = self.binary(lc, Operator::Eq, rc)?;
        let left = self.timestamp_time_micros(left)?;
        let right = self.timestamp_time_micros(right)?;
        let time_before = self.binary(left, Operator::Lt, right)?;
        let same_before = self.binary(same, Operator::And, time_before)?;
        self.binary(before, Operator::Or, same_before)
    }

    fn timestamp_month_diff(&mut self, start: ExprNode, end: ExprNode) -> PlanResult<ExprNode> {
        let start = self.cast(start, DataType::Timestamp(TimeUnit::Microsecond, None))?;
        let end = self.cast(end, DataType::Timestamp(TimeUnit::Microsecond, None))?;
        let s = self.copy_expression(&start)?;
        let e = self.copy_expression(&end)?;
        let start_months = self.timestamp_months(s)?;
        let end_months = self.timestamp_months(e)?;
        let months = self.binary(end_months, Operator::Minus, start_months)?;
        let months = self.cast(months, DataType::Int64)?;
        let mut branches = vec![];
        for positive in [true, false] {
            let m = self.copy_expression(&months)?;
            let zero = self.literal(ScalarValue::Int64(Some(0)));
            let condition =
                self.binary(m, if positive { Operator::Gt } else { Operator::Lt }, zero)?;
            let s = self.copy_expression(&start)?;
            let e = self.copy_expression(&end)?;
            let before = if positive {
                self.timestamp_day_time_before(e, s)?
            } else {
                self.timestamp_day_time_before(s, e)?
            };
            let condition = self.binary(condition, Operator::And, before)?;
            let m = self.copy_expression(&months)?;
            let one = self.literal(ScalarValue::Int64(Some(1)));
            let value = self.binary(
                m,
                if positive {
                    Operator::Minus
                } else {
                    Operator::Plus
                },
                one,
            )?;
            branches.push((condition, value));
        }
        let yes = self.literal(ScalarValue::Boolean(Some(true)));
        branches.push((yes, months));
        self.case(branches, None)
    }

    fn months_between(&mut self, mut args: Vec<ExprNode>) -> PlanResult<ExprNode> {
        let round = if args.len() == 3 {
            args.pop()
                .ok_or_else(|| PlanError::internal("missing round flag"))?
        } else {
            self.literal(ScalarValue::Boolean(Some(true)))
        };
        let (left, right) = args.two()?;
        let mut dates = vec![];
        for value in [left, right] {
            let value = if matches!(
                value.result_type.resolved("months_between date")?.data_type,
                DataType::Timestamp(..)
            ) {
                value
            } else {
                self.cast(value, DataType::Timestamp(TimeUnit::Microsecond, None))?
            };
            dates.push(value);
        }
        let (left, right) = dates.two()?;
        let l = self.copy_expression(&left)?;
        let r = self.copy_expression(&right)?;
        let lm = self.timestamp_months(l)?;
        let rm = self.timestamp_months(r)?;
        let months = self.binary(lm, Operator::Minus, rm)?;
        let months = self.cast(months, DataType::Float64)?;
        let l = self.copy_expression(&left)?;
        let r = self.copy_expression(&right)?;
        let ld = self.integer_part(l, "DAY")?;
        let rd = self.integer_part(r, "DAY")?;
        let lc = self.copy_expression(&ld)?;
        let rc = self.copy_expression(&rd)?;
        let same_day = self.binary(lc, Operator::Eq, rc)?;
        let mut last_days = vec![];
        let mut seconds = vec![];
        for date in [left, right] {
            let copy = self.copy_expression(&date)?;
            let day = self.cast(copy, DataType::Date32)?;
            let copy = self.copy_expression(&day)?;
            let last =
                self.scalar_udf(Arc::new(ScalarUDF::from(SparkLastDay::new())), vec![copy])?;
            last_days.push(self.binary(last, Operator::Eq, day)?);
            let micros = self.timestamp_time_micros(date)?;
            let divisor = self.literal(ScalarValue::Int64(Some(1_000_000)));
            seconds.push(self.binary(micros, Operator::Divide, divisor)?);
        }
        let (ll, rl) = last_days.two()?;
        let both_last = self.binary(ll, Operator::And, rl)?;
        let condition = self.binary(same_day, Operator::Or, both_last)?;
        let days_diff = self.binary(ld, Operator::Minus, rd)?;
        let seconds_per_day = self.literal(ScalarValue::Int64(Some(86400)));
        let diff = self.binary(days_diff, Operator::Multiply, seconds_per_day)?;
        let (ls, rs) = seconds.two()?;
        let diff = self.binary(diff, Operator::Plus, ls)?;
        let diff = self.binary(diff, Operator::Minus, rs)?;
        let divisor = self.literal(ScalarValue::Float64(Some(2678400.0)));
        let fraction = self.binary(diff, Operator::Divide, divisor)?;
        let copy = self.copy_expression(&months)?;
        let fractional = self.binary(copy, Operator::Plus, fraction)?;
        let yes = self.literal(ScalarValue::Boolean(Some(true)));
        let result = self.case(vec![(condition, months), (yes, fractional)], None)?;
        let copy = self.copy_expression(&result)?;
        let precision = self.literal(ScalarValue::Int32(Some(8)));
        let rounded = self.scalar("round", vec![copy, precision])?;
        let yes = self.literal(ScalarValue::Boolean(Some(true)));
        self.case(vec![(round, rounded), (yes, result)], None)
    }
}
