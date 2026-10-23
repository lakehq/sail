use std::sync::Arc;

use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use datafusion_common::ScalarValue;
use datafusion_expr::{Operator, ScalarUDF};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::negate_duration::NegateDuration;
use sail_function::scalar::datetime::spark_interval::SparkDayTimeIntervalToCalendarInterval;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::math::spark_bin::SparkBin;
use sail_function::scalar::math::spark_ceil_floor::{SparkCeil, SparkFloor};
use sail_function::scalar::math::spark_div::SparkDiv;
use sail_function::scalar::math::spark_negative::SparkNegative;
use sail_function::scalar::math::spark_pmod::SparkPmod;
use sail_function::scalar::math::spark_sqrt::SparkSqrt;
use sail_function::scalar::misc::raise_error::RaiseError;
use sail_function::scalar::spark_to_string::{SparkToLargeUtf8, SparkToUtf8, SparkToUtf8View};

use super::Resolver;
use crate::analysis::deferred::Deferred;
use crate::analysis::ir::*;
use crate::error::{PlanError, PlanResult};

impl Resolver<'_, '_> {
    pub(super) fn math_function(
        &mut self,
        name: &str,
        values: &mut Vec<LogicalExpression>,
    ) -> PlanResult<Option<LogicalExpression>> {
        if !matches!(
            name,
            "+" | "-"
                | "*"
                | "/"
                | "%"
                | "mod"
                | "div"
                | "negative"
                | "positive"
                | "acos"
                | "acosh"
                | "asin"
                | "asinh"
                | "atan"
                | "atan2"
                | "atanh"
                | "cbrt"
                | "cos"
                | "cosh"
                | "cot"
                | "degrees"
                | "exp"
                | "radians"
                | "sin"
                | "sinh"
                | "tan"
                | "tanh"
                | "ln"
                | "log"
                | "log10"
                | "log1p"
                | "log2"
                | "sqrt"
                | "csc"
                | "sec"
                | "ceil"
                | "ceiling"
                | "floor"
                | "bin"
                | "pmod"
                | "pow"
                | "power"
                | "hypot"
                | "rint"
                | "round"
                | "e"
        ) {
            return Ok(None);
        }
        let values = std::mem::take(values);
        let ansi = self.context.config.ansi_mode;
        let result = match name {
            "e" => {
                values.zero()?;
                self.literal(ScalarValue::Float64(Some(std::f64::consts::E)))
            }
            "positive" | "+" if values.len() == 1 => values.one()?,
            "negative" | "-" if values.len() == 1 => self.negate(values.one()?)?,
            "+" | "-" | "*" | "/" | "%" | "mod" | "div" => {
                let (left, right) = values.two()?;
                self.arithmetic(name, left, right)?
            }
            "bin" => self.scalar_udf(Arc::new(ScalarUDF::from(SparkBin::new(ansi))), values)?,
            "pmod" => self.scalar_udf(Arc::new(ScalarUDF::from(SparkPmod::new(ansi))), values)?,
            "ceil" | "ceiling" | "floor" => {
                let mut values = values;
                if values.len() == 1
                    && matches!(
                        values[0].result_type.resolved("rounding input")?.data_type,
                        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                    )
                {
                    let value = values.one()?;
                    return Ok(Some(self.cast(value, DataType::Int64)?));
                }
                if values.len() == 2 {
                    let scale = values
                        .pop()
                        .ok_or_else(|| PlanError::internal("missing scale"))?;
                    let value = crate::analysis::lowering::evaluate_constant(&scale)?;
                    if value.data_type() != DataType::Int32 {
                        return Err(PlanError::invalid("Target scale must be Integer literal"));
                    }
                    values.push(self.literal(value.cast_to(&DataType::Int32)?));
                }
                let function = if name == "floor" {
                    ScalarUDF::from(SparkFloor::new(ansi))
                } else {
                    ScalarUDF::from(SparkCeil::new(ansi))
                };
                self.scalar_udf(Arc::new(function), values)?
            }
            "pow" | "power" => {
                let result = self.scalar("power", values)?;
                self.cast(result, DataType::Float64)?
            }
            "hypot" => {
                let (a, b) = values.two()?;
                let a_copy = self.copy_expression(&a)?;
                let b_copy = self.copy_expression(&b)?;
                let a = self.binary(a, Operator::Multiply, a_copy)?;
                let b = self.binary(b, Operator::Multiply, b_copy)?;
                let value = self.binary(a, Operator::Plus, b)?;
                let result = self.scalar("sqrt", vec![value])?;
                self.cast(result, DataType::Float64)?
            }
            "rint" => {
                let result = self.scalar("round", vec![values.one()?])?;
                self.cast(result, DataType::Float64)?
            }
            "round" => {
                let mut values = values;
                if let Some(first) = values.first()
                    && first
                        .result_type
                        .resolved("round value")?
                        .data_type
                        .is_string()
                {
                    let scale = match values.get(1) {
                        Some(scale) => self.copy_expression(scale)?,
                        None => self.literal(ScalarValue::Int32(Some(0))),
                    };
                    let condition = self.is_null(scale);
                    let null = self.literal(ScalarValue::Utf8(None));
                    let value = values.remove(0);
                    let value = self.case(vec![(condition, null)], Some(value))?;
                    let value = self.string_to_double(value)?;
                    let value = self.try_cast(value, DataType::Float64)?;
                    values.insert(0, value);
                }
                self.scalar("round", values)?
            }
            _ => {
                let expected = if matches!(name, "atan2" | "log") {
                    2
                } else {
                    1
                };
                sail_type_system::coercion::validate_arity(name, values.len(), expected)?;
                let mut values = values
                    .into_iter()
                    .map(|value| self.cast(value, DataType::Float64))
                    .collect::<PlanResult<Vec<_>>>()?;
                match name {
                    "sqrt" => {
                        self.scalar_udf(Arc::new(ScalarUDF::from(SparkSqrt::new())), values)?
                    }
                    "csc" | "sec" => {
                        let denominator =
                            self.scalar(if name == "csc" { "sin" } else { "cos" }, values)?;
                        let one = self.literal(ScalarValue::Float64(Some(1.0)));
                        self.binary(one, Operator::Divide, denominator)?
                    }
                    "ln" | "log" | "log10" | "log1p" | "log2" => {
                        let mut value = values
                            .pop()
                            .ok_or_else(|| PlanError::internal("missing logarithm argument"))?;
                        if name == "log1p" {
                            let one = self.literal(ScalarValue::Float64(Some(1.0)));
                            value = self.binary(value, Operator::Plus, one)?;
                        }
                        let copy = self.copy_expression(&value)?;
                        let zero = self.literal(ScalarValue::Float64(Some(0.0)));
                        let positive = self.binary(copy, Operator::Gt, zero)?;
                        values.push(self.case(vec![(positive, value)], None)?);
                        self.scalar(if name == "log1p" { "ln" } else { name }, values)?
                    }
                    _ => self.scalar(name, values)?,
                }
            }
        };
        Ok(Some(result))
    }

    fn string_to_double(&mut self, value: LogicalExpression) -> PlanResult<LogicalExpression> {
        if self.context.config.ansi_mode {
            self.cast(value, DataType::Float64)
        } else {
            self.try_cast(value, DataType::Float64)
        }
    }

    fn negate(&mut self, value: LogicalExpression) -> PlanResult<LogicalExpression> {
        let data_type = value
            .result_type
            .resolved("negation input")?
            .data_type
            .clone();
        if matches!(data_type, DataType::Duration(_)) {
            return self.scalar_udf(
                Arc::new(ScalarUDF::from(NegateDuration::new())),
                vec![value],
            );
        }
        if data_type.is_string() {
            let value = self.string_to_double(value)?;
            return self.scalar_udf(
                Arc::new(ScalarUDF::from(SparkNegative::new(
                    self.context.config.ansi_mode,
                ))),
                vec![value],
            );
        }
        if data_type.is_floating() {
            return Ok(LogicalExpression {
                id: self.context.ids.expression(),
                result_type: value.result_type.clone(),
                node: Deferred::Resolved(ExpressionNode::Negative(Box::new(value))),
            });
        }
        let folded = if let Deferred::Resolved(ExpressionNode::Literal(value)) = &value.node {
            match value {
                ScalarValue::Int8(Some(v)) => v.checked_neg().map(|v| ScalarValue::Int8(Some(v))),
                ScalarValue::Int16(Some(v)) => v.checked_neg().map(|v| ScalarValue::Int16(Some(v))),
                ScalarValue::Int32(Some(v)) => v.checked_neg().map(|v| ScalarValue::Int32(Some(v))),
                ScalarValue::Int64(Some(v)) => v.checked_neg().map(|v| ScalarValue::Int64(Some(v))),
                _ => None,
            }
        } else {
            None
        };
        if let Some(value) = folded {
            return Ok(self.literal(value));
        }
        self.scalar_udf(
            Arc::new(ScalarUDF::from(SparkNegative::new(
                self.context.config.ansi_mode,
            ))),
            vec![value],
        )
    }

    fn arithmetic(
        &mut self,
        name: &str,
        mut left: LogicalExpression,
        mut right: LogicalExpression,
    ) -> PlanResult<LogicalExpression> {
        let left_type = left
            .result_type
            .resolved("arithmetic left")?
            .data_type
            .clone();
        let right_type = right
            .result_type
            .resolved("arithmetic right")?
            .data_type
            .clone();
        let duration = DataType::Duration(TimeUnit::Microsecond);
        let operator = match name {
            "+" => Operator::Plus,
            "-" => Operator::Minus,
            "*" => Operator::Multiply,
            "/" | "div" => Operator::Divide,
            _ => Operator::Modulo,
        };
        if name == "+"
            && ((left_type.is_string() && right_type == duration)
                || (right_type.is_string() && left_type == duration))
        {
            let string_type = if left_type.is_string() {
                left_type
            } else {
                std::mem::swap(&mut left, &mut right);
                right_type
            };
            let timestamp = ScalarUDF::from(SparkTimestamp::try_new(
                Some(self.context.config.session_timezone.clone()),
                self.context.config.ansi_mode,
                false,
            )?);
            let left = self.scalar_udf(Arc::new(timestamp), vec![left])?;
            let right = self.scalar_udf(
                Arc::new(ScalarUDF::from(
                    SparkDayTimeIntervalToCalendarInterval::new(),
                )),
                vec![right],
            )?;
            let value = self.binary(left, operator, right)?;
            let function = match string_type {
                DataType::LargeUtf8 => ScalarUDF::from(SparkToLargeUtf8::new()),
                DataType::Utf8View => ScalarUDF::from(SparkToUtf8View::new()),
                _ => ScalarUDF::from(SparkToUtf8::new()),
            };
            return self.scalar_udf(Arc::new(function), vec![value]);
        }
        if matches!(name, "+" | "-") {
            if left_type == DataType::Date32 && right_type == duration {
                right = self.cast(right, DataType::Interval(IntervalUnit::MonthDayNano))?;
            } else if name == "+" && right_type == DataType::Date32 && left_type == duration {
                left = self.cast(left, DataType::Interval(IntervalUnit::MonthDayNano))?;
            } else if left_type == DataType::Date32 && right_type.is_numeric() {
                let left = self.cast(left, DataType::Int32)?;
                let value = self.binary(left, operator, right)?;
                return self.cast(value, DataType::Date32);
            } else if name == "+" && right_type == DataType::Date32 && left_type.is_numeric() {
                let right = self.cast(right, DataType::Int32)?;
                let value = self.binary(left, operator, right)?;
                return self.cast(value, DataType::Date32);
            }
        }
        if matches!(name, "/" | "div" | "%" | "mod") {
            if is_zero_literal(&right) {
                if !self.context.config.ansi_mode {
                    return Ok(self.literal(ScalarValue::Null));
                }
                return Err(PlanError::ArrowError(if matches!(name, "%" | "mod") {
                    arrow::error::ArrowError::ArithmeticOverflow("Remainder by zero".into())
                } else {
                    arrow::error::ArrowError::DivideByZero
                }));
            }
            right = self.safe_divisor(
                right,
                &right_type,
                if matches!(name, "%" | "mod") {
                    "Remainder by zero"
                } else {
                    "Division by zero"
                },
            )?;
        }
        let value = if name == "div" {
            if matches!(
                (&left_type, &right_type),
                (DataType::Duration(_), DataType::Duration(_))
            ) {
                let left = self.cast(left, DataType::Int64)?;
                let right = self.cast(right, DataType::Int64)?;
                self.binary(left, operator, right)?
            } else {
                self.scalar_udf(
                    Arc::new(ScalarUDF::from(SparkDiv::new())),
                    vec![left, right],
                )?
            }
        } else if matches!(name, "*" | "/")
            && (left_type == duration || (name == "*" && right_type == duration))
        {
            if left_type == duration {
                left = self.cast(left, DataType::Int64)?;
            }
            if right_type == duration {
                right = self.cast(right, DataType::Int64)?;
            }
            let value = self.binary(left, operator, right)?;
            return self.cast(value, duration);
        } else if name == "/"
            && !matches!(
                left_type,
                DataType::Decimal128(..)
                    | DataType::Decimal256(..)
                    | DataType::Interval(IntervalUnit::YearMonth | IntervalUnit::DayTime)
            )
            && !matches!(
                right_type,
                DataType::Decimal128(..) | DataType::Decimal256(..)
            )
        {
            let left = self.cast(left, DataType::Float64)?;
            let right = self.cast(right, DataType::Float64)?;
            self.binary(left, operator, right)?
        } else {
            self.binary(left, operator, right)?
        };
        if name == "div" {
            self.cast(value, DataType::Int64)
        } else {
            Ok(value)
        }
    }

    fn safe_divisor(
        &mut self,
        value: LogicalExpression,
        data_type: &DataType,
        message: &str,
    ) -> PlanResult<LogicalExpression> {
        if matches!(data_type, DataType::Interval(_) | DataType::Duration(_)) {
            return Ok(value);
        }
        let zero = self.literal(ScalarValue::Int32(Some(0)));
        let copy = self.copy_expression(&value)?;
        let mut condition = self.binary(copy, Operator::Eq, zero)?;
        if matches!(
            data_type,
            DataType::Float16 | DataType::Float32 | DataType::Float64
        ) {
            let negative_zero = self.literal(ScalarValue::Float64(Some(-0.0)).cast_to(data_type)?);
            let copy = self.copy_expression(&value)?;
            let negative = self.binary(copy, Operator::Eq, negative_zero)?;
            condition = self.binary(condition, Operator::Or, negative)?;
        }
        let invalid = if self.context.config.ansi_mode {
            let message = self.literal(ScalarValue::Utf8(Some(message.into())));
            self.scalar_udf(Arc::new(ScalarUDF::from(RaiseError::new())), vec![message])?
        } else {
            self.literal(ScalarValue::Null)
        };
        self.case(vec![(condition, invalid)], Some(value))
    }
}

fn is_zero_literal(expression: &LogicalExpression) -> bool {
    match &expression.node {
        Deferred::Resolved(ExpressionNode::Literal(value)) => {
            value.data_type().is_numeric()
                && !value.is_null()
                && value
                    .cast_to(&DataType::Float64)
                    .is_ok_and(|v| matches!(v, ScalarValue::Float64(Some(value)) if value == 0.0))
        }
        Deferred::Resolved(
            ExpressionNode::Cast { expression, .. } | ExpressionNode::WithMetadata(expression),
        ) => is_zero_literal(expression),
        _ => false,
    }
}
