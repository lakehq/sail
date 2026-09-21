//! Arithmetic where a Spark ANSI interval is an operand or the result: the field range of the
//! interval it produces, and the operand pairs Spark refuses (`BinaryArithmeticWithDatetimeResolver`).
//!
//! Each entry point returns `None` when the operands are not its business, so the caller keeps
//! its existing rewrite for every other pair.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit};
use datafusion::functions::expr_fn;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{Expr, ExprSchemable, ScalarUDF, cast, lit, try_cast, when};
use sail_common::spec::IntervalFieldType;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::interval::IntervalRange;
use sail_common_datafusion::session::plan::PlanService;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::misc::raise_error::RaiseError;

use crate::error::{PlanError, PlanResult};
use crate::function::common::FunctionContextInput;
use crate::function::interval::with_interval_range;

const MICROS_PER_DAY: i64 = 86_400_000_000;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Operator {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
}

impl Operator {
    fn symbol(self) -> &'static str {
        match self {
            Operator::Plus => "+",
            Operator::Minus => "-",
            Operator::Multiply => "*",
            Operator::Divide => "/",
            Operator::Modulo => "%",
        }
    }
}

/// The class of an arithmetic operand, as the datetime resolver sees it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Operand {
    DayTime(IntervalRange),
    YearMonth(IntervalRange),
    Calendar,
    Date,
    Timestamp,
    Time,
    Numeric,
    Str,
    Null,
    Other,
}

fn operand(field: &Field) -> Operand {
    let data_type = field.data_type();
    match data_type {
        DataType::Duration(TimeUnit::Microsecond) | DataType::Interval(IntervalUnit::DayTime) => {
            Operand::DayTime(IntervalRange::from_field(field).unwrap_or(IntervalRange::DAY_TIME))
        }
        DataType::Interval(IntervalUnit::YearMonth) => Operand::YearMonth(
            IntervalRange::from_field(field).unwrap_or(IntervalRange::YEAR_MONTH),
        ),
        DataType::Interval(IntervalUnit::MonthDayNano) => Operand::Calendar,
        DataType::Date32 | DataType::Date64 => Operand::Date,
        DataType::Timestamp(_, _) => Operand::Timestamp,
        DataType::Time32(_) | DataType::Time64(_) => Operand::Time,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Operand::Str,
        DataType::Null => Operand::Null,
        x if x.is_numeric() => Operand::Numeric,
        _ => Operand::Other,
    }
}

/// Whether Spark refuses the operand pair at analysis. Only pairs with an interval operand, and a
/// timestamp beside a number, are judged; everything else is left to the existing rules.
/// The day-time table is the one measured on the Spark 4.2 JVM, which ANSI mode does not change.
fn refuses(op: Operator, left: Operand, right: Operand) -> bool {
    use Operand::*;
    let datetime_partner =
        |x: Operand| matches!(x, DayTime(_) | Date | Timestamp | Time | Str | Null);
    let scale_partner = |x: Operand| matches!(x, Numeric | Str | Null);
    match (left, right) {
        (DayTime(_), _) | (_, DayTime(_)) => match op {
            Operator::Plus => {
                let partner = if matches!(left, DayTime(_)) {
                    right
                } else {
                    left
                };
                !datetime_partner(partner)
            }
            Operator::Minus => match (left, right) {
                (DayTime(_), r) => !matches!(r, DayTime(_) | Null),
                (l, _) => !datetime_partner(l),
            },
            Operator::Multiply => match (left, right) {
                (DayTime(_), DayTime(_)) => true,
                (DayTime(_), x) | (x, DayTime(_)) => !scale_partner(x),
                _ => false,
            },
            Operator::Divide => !(matches!(left, DayTime(_)) && scale_partner(right)),
            Operator::Modulo => true,
        },
        (YearMonth(_) | Calendar, x) | (x, YearMonth(_) | Calendar) => x == Other,
        (Timestamp, Numeric) | (Numeric, Timestamp) => {
            matches!(op, Operator::Plus | Operator::Minus)
        }
        _ => false,
    }
}

fn type_name(field: &Field, ctx: &FunctionContextInput) -> PlanResult<String> {
    if let Some(range) = IntervalRange::from_field(field) {
        return Ok(range.type_name().to_uppercase());
    }
    let service = ctx.session_context.extension::<PlanService>()?;
    Ok(service
        .plan_formatter()
        .data_type_to_simple_string(field.data_type())?
        .to_uppercase())
}

/// Refuses the pair the way Spark does, naming the operands by their Spark types.
pub(super) fn check_operands(
    op: Operator,
    left: &Expr,
    right: &Expr,
    ctx: &FunctionContextInput,
) -> PlanResult<()> {
    let (_, l) = left.to_field(ctx.schema)?;
    let (_, r) = right.to_field(ctx.schema)?;
    if refuses(op, operand(&l), operand(&r)) {
        return Err(PlanError::analysis(format!(
            "cannot resolve arithmetic '{}' with operand types {} and {}",
            op.symbol(),
            type_name(&l, ctx)?,
            type_name(&r, ctx)?
        )));
    }
    Ok(())
}

/// A day count as a DAY interval (`SubtractDates`, `datetimeExpressions.scala:3605-3628`).
fn subtract_dates(left: Expr, right: Expr, ctx: &FunctionContextInput) -> PlanResult<Expr> {
    let days = cast(left, DataType::Int64) - cast(right, DataType::Int64);
    let micros = cast(
        days * lit(MICROS_PER_DAY),
        DataType::Duration(TimeUnit::Microsecond),
    );
    with_interval_range(
        micros,
        IntervalRange::single(IntervalFieldType::Day),
        ctx.schema,
    )
}

/// `SubtractTimes` is `DayTimeIntervalType(HOUR, SECOND)` (`timeExpressions.scala:626`).
fn subtract_times(left: Expr, right: Expr, ctx: &FunctionContextInput) -> PlanResult<Expr> {
    let difference = cast(left - right, DataType::Duration(TimeUnit::Microsecond));
    with_interval_range(
        difference,
        IntervalRange::new(IntervalFieldType::Hour, IntervalFieldType::Second),
        ctx.schema,
    )
}

/// A string read as a date beside a date (`BinaryArithmeticWithDatetimeResolver`): always on the
/// left, and only with ANSI on on the right.
fn string_as_date(expr: Expr, ansi_mode: bool) -> Expr {
    ScalarUDF::from(SparkDate::new(!ansi_mode)).call(vec![expr])
}

const INTERVAL_DIVIDED_BY_ZERO: &str = "[INTERVAL_DIVIDED_BY_ZERO] Division by zero. Use \
     `try_divide` to tolerate divisor being 0 and return NULL instead.";

/// A day-time interval scaled by a number (`MultiplyDTInterval`/`DivideDTInterval`,
/// `intervalExpressions.scala:640-760`): the microseconds are scaled exactly and rounded HALF_UP,
/// away from zero on a tie, and a zero divisor is an error in both ANSI modes. An integral number
/// scales exactly, a DECIMAL through exact decimal arithmetic, and anything else -- a fraction, a
/// STRING or NULL -- as a DOUBLE, which a malformed STRING reaches as NULL with ANSI off.
fn scale_day_time(
    interval: Expr,
    number: Expr,
    number_type: &DataType,
    divide: bool,
    ctx: &FunctionContextInput,
) -> PlanResult<Expr> {
    let micros = cast(interval, DataType::Int64);
    let (number, zero) = if number_type.is_integer() {
        (cast(number, DataType::Int64), lit(0_i64))
    } else if let DataType::Decimal128(_, _) | DataType::Decimal256(_, _) = number_type {
        (number, lit(0_i64))
    } else {
        let number = match number_type {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                if !ctx.plan_config.ansi_mode =>
            {
                try_cast(number, DataType::Float64)
            }
            _ => cast(number, DataType::Float64),
        };
        (number, lit(0.0_f64))
    };
    let is_zero = number.clone().eq(zero.clone());
    // A zero divisor raises when the row is evaluated, not when the query is planned: Spark answers
    // `if(false, i / 0, NULL)`. The division itself sees a NULL, so constant folding cannot fail
    // on it before the CASE below decides.
    let number = if divide {
        expr_fn::nullif(number, zero)
    } else {
        number
    };
    let scaled = match (number_type, divide) {
        (t, false) if t.is_integer() => micros * number,
        (t, true) if t.is_integer() => divide_half_up(micros, number)?,
        (DataType::Decimal128(_, _) | DataType::Decimal256(_, _), divide) => {
            let micros = cast(micros, DataType::Decimal128(38, 0));
            let exact = if divide {
                micros / number
            } else {
                micros * number
            };
            expr_fn::round(vec![exact, lit(0_i64)])
        }
        (_, divide) => {
            let micros = cast(micros, DataType::Float64);
            let exact = if divide {
                micros / number
            } else {
                micros * number
            };
            expr_fn::round(vec![exact])
        }
    };
    let scaled = cast(scaled, DataType::Int64);
    let scaled = if divide {
        let raise = ScalarUDF::from(RaiseError::new()).call(vec![lit(INTERVAL_DIVIDED_BY_ZERO)]);
        when(is_zero, raise).otherwise(scaled)?
    } else {
        scaled
    };
    with_interval_range(
        cast(scaled, DataType::Duration(TimeUnit::Microsecond)),
        IntervalRange::DAY_TIME,
        ctx.schema,
    )
}

/// `LongMath.divide(a, b, HALF_UP)`: the remainder, not a float quotient, decides the tie.
fn divide_half_up(a: Expr, b: Expr) -> PlanResult<Expr> {
    let quotient = a.clone() / b.clone();
    let remainder = a.clone() % b.clone();
    let abs_r = expr_fn::abs(remainder);
    let abs_b = expr_fn::abs(b.clone());
    let away = when(a.lt(lit(0_i64)).eq(b.lt(lit(0_i64))), lit(1_i64)).otherwise(lit(-1_i64))?;
    // `|r| >= |b| - |r|` is `2|r| >= |b|` without the overflow of doubling.
    let round = when(abs_r.clone().gt_eq(abs_b - abs_r), away).otherwise(lit(0_i64))?;
    Ok(quotient + round)
}

/// The rewrite for a pair that involves a DATE or TIME difference or a day-time interval, or
/// `None` to leave the pair to the caller.
pub(super) fn rewrite(
    op: Operator,
    left: Expr,
    right: Expr,
    ctx: &FunctionContextInput,
) -> PlanResult<Result<Expr, (Expr, Expr)>> {
    use Operand::*;
    let ansi_mode = ctx.plan_config.ansi_mode;
    let (_, lf) = left.to_field(ctx.schema)?;
    let (_, rf) = right.to_field(ctx.schema)?;
    let (l, r) = (operand(&lf), operand(&rf));
    let expr = match (op, l, r) {
        (Operator::Minus, Date, Date) => subtract_dates(left, right, ctx)?,
        (Operator::Minus, Date, Null) => subtract_dates(left, cast(right, DataType::Date32), ctx)?,
        (Operator::Minus, Null, Date) => subtract_dates(cast(left, DataType::Date32), right, ctx)?,
        (Operator::Minus, Str, Date) => {
            subtract_dates(string_as_date(left, ansi_mode), right, ctx)?
        }
        (Operator::Minus, Date, Str) if ansi_mode => {
            subtract_dates(left, string_as_date(right, ansi_mode), ctx)?
        }
        (Operator::Minus, Time, Time) => subtract_times(left, right, ctx)?,
        (Operator::Minus, Time, Null) => {
            let time = lf.data_type().clone();
            subtract_times(left, cast(right, time), ctx)?
        }
        (Operator::Minus, Null, Time) => {
            let time = rf.data_type().clone();
            subtract_times(cast(left, time), right, ctx)?
        }
        (Operator::Minus, Str, Time) if ansi_mode => {
            let time = rf.data_type().clone();
            subtract_times(cast(left, time), right, ctx)?
        }
        (Operator::Minus, Time, Str) if ansi_mode => {
            let time = lf.data_type().clone();
            subtract_times(left, cast(right, time), ctx)?
        }
        (Operator::Plus | Operator::Minus, DayTime(a), DayTime(b)) => {
            let expr = if op == Operator::Plus {
                left + right
            } else {
                left - right
            };
            with_interval_range(expr, a.widen(b), ctx.schema)?
        }
        (Operator::Plus | Operator::Minus, YearMonth(a), YearMonth(b)) => {
            let expr = if op == Operator::Plus {
                left + right
            } else {
                left - right
            };
            with_interval_range(expr, a.widen(b), ctx.schema)?
        }
        // An untyped NULL beside an interval takes the interval's type.
        (Operator::Plus | Operator::Minus, DayTime(range) | YearMonth(range), Null) => {
            let right = cast(right, lf.data_type().clone());
            let expr = if op == Operator::Plus {
                left + right
            } else {
                left - right
            };
            with_interval_range(expr, range, ctx.schema)?
        }
        (Operator::Plus | Operator::Minus, Null, DayTime(range) | YearMonth(range)) => {
            let left = cast(left, rf.data_type().clone());
            let expr = if op == Operator::Plus {
                left + right
            } else {
                left - right
            };
            with_interval_range(expr, range, ctx.schema)?
        }
        // A date moves by a day-time interval as a DATE only when the interval is DAY to DAY;
        // any finer interval makes it a TIMESTAMP (`BinaryArithmeticWithDatetimeResolver.scala:68-69`).
        (Operator::Plus | Operator::Minus, Date, DayTime(range))
            if range != IntervalRange::single(IntervalFieldType::Day) =>
        {
            let timestamp = date_as_timestamp(left, ctx);
            if op == Operator::Plus {
                timestamp + right
            } else {
                timestamp - right
            }
        }
        (Operator::Plus, DayTime(range), Date)
            if range != IntervalRange::single(IntervalFieldType::Day) =>
        {
            date_as_timestamp(right, ctx) + left
        }
        (Operator::Plus, DayTime(_), Timestamp) => right + left,
        (Operator::Multiply, DayTime(_), Numeric | Str | Null) => {
            scale_day_time(left, right, rf.data_type(), false, ctx)?
        }
        (Operator::Multiply, Numeric | Str | Null, DayTime(_)) => {
            scale_day_time(right, left, lf.data_type(), false, ctx)?
        }
        (Operator::Divide, DayTime(_), Numeric | Str | Null) => {
            scale_day_time(left, right, rf.data_type(), true, ctx)?
        }
        _ => return Ok(Err((left, right))),
    };
    Ok(Ok(expr))
}

fn date_as_timestamp(date: Expr, ctx: &FunctionContextInput) -> Expr {
    cast(
        date,
        DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::clone(&ctx.plan_config.session_timezone)),
        ),
    )
}

/// The result of combining two intervals of one family takes the range that covers both; any
/// other result is returned as it is.
pub(super) fn widen_interval_result(
    result: Expr,
    arguments: &[Expr],
    ctx: &FunctionContextInput,
) -> PlanResult<Expr> {
    let [left, right] = arguments else {
        return Ok(result);
    };
    let (_, lf) = left.to_field(ctx.schema)?;
    let (_, rf) = right.to_field(ctx.schema)?;
    match (operand(&lf), operand(&rf)) {
        (Operand::DayTime(a), Operand::DayTime(b))
        | (Operand::YearMonth(a), Operand::YearMonth(b)) => {
            with_interval_range(result, a.widen(b), ctx.schema)
        }
        _ => Ok(result),
    }
}

/// Unary minus of an interval keeps its range.
/// Written as a subtraction from zero, whose nullability is the operand's, where a negating
/// function would be nullable whatever its input.
pub(super) fn negate_interval(arg: &Expr, schema: &DFSchema) -> PlanResult<Option<Expr>> {
    let (_, field) = arg.to_field(schema)?;
    let negated = match (field.data_type(), IntervalRange::from_field(&field)) {
        (DataType::Duration(TimeUnit::Microsecond), Some(range)) => {
            let micros = lit(0_i64) - cast(arg.clone(), DataType::Int64);
            (
                cast(micros, DataType::Duration(TimeUnit::Microsecond)),
                range,
            )
        }
        (DataType::Interval(IntervalUnit::YearMonth), Some(range)) => (
            lit(ScalarValue::IntervalYearMonth(Some(0))) - arg.clone(),
            range,
        ),
        _ => return Ok(None),
    };
    Ok(Some(with_interval_range(negated.0, negated.1, schema)?))
}
