use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, IntervalUnit, TimeUnit};
use datafusion::functions::expr_fn;
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::{
    BinaryExpr, Expr, ExprSchemable, Operator, ScalarUDF, WindowFunctionDefinition, cast, expr,
    lit, try_cast, when,
};
use datafusion_spark::function::math::expr_fn as math_fn;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::error::generic_exec_err;
use sail_function::scalar::datetime::negate_duration::NegateDuration;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_interval::SparkDayTimeIntervalToCalendarInterval;
use sail_function::scalar::datetime::spark_interval_scale::{
    SparkDivideCalendarInterval, SparkDivideDtInterval, SparkDivideYmInterval,
    SparkMultiplyCalendarInterval, SparkMultiplyDtInterval, SparkMultiplyYmInterval,
};
use sail_function::scalar::datetime::spark_time_add_interval::SparkTimeAddDtInterval;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::math::rand_poisson::RandPoisson;
use sail_function::scalar::math::randn::Randn;
use sail_function::scalar::math::random::Random;
use sail_function::scalar::math::spark_abs::SparkAbs;
use sail_function::scalar::math::spark_bin::SparkBin;
use sail_function::scalar::math::spark_bround::SparkBRound;
use sail_function::scalar::math::spark_ceil_floor::{SparkCeil, SparkFloor};
use sail_function::scalar::math::spark_conv::SparkConv;
use sail_function::scalar::math::spark_div::SparkIntervalDiv;
use sail_function::scalar::math::spark_negative::SparkNegative;
use sail_function::scalar::math::spark_pmod::SparkPmod;
use sail_function::scalar::math::spark_signum::SparkSignum;
use sail_function::scalar::math::spark_sqrt::SparkSqrt;
use sail_function::scalar::math::spark_try_add::SparkTryAdd;
use sail_function::scalar::math::spark_try_div::SparkTryDiv;
use sail_function::scalar::math::spark_try_mod::SparkTryMod;
use sail_function::scalar::math::spark_try_mult::SparkTryMult;
use sail_function::scalar::math::spark_try_subtract::SparkTrySubtract;
use sail_function::scalar::math::spark_unhex::SparkUnHex;
use sail_function::scalar::math::spark_uniform::SparkUniform;
use sail_function::scalar::misc::raise_error::RaiseError;
use sail_function::scalar::spark_to_string::{SparkToLargeUtf8, SparkToUtf8, SparkToUtf8View};

use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    FunctionContextInput, ScalarFunction, ScalarFunctionInput, is_spark_udt_field,
    spark_field_type_name, spark_type_name,
};

/// A string shifted by an interval is read as a TIMESTAMP, shifted, and written back as a string:
/// `Cast(TimestampAddInterval(l, r), l.dataType)` for `+`, and the same with the interval negated
/// for `-` (`BinaryArithmeticWithDatetimeResolver.scala:92-93,137-138`). That holds for the
/// day-time interval and for the legacy calendar one alike; only `-` requires the string on the
/// left, since Spark has no `interval - string` arm.
fn shift_string_by_interval(
    string: Expr,
    interval: Expr,
    string_type: DataType,
    interval_type: &DataType,
    subtract: bool,
    session_timezone: Arc<str>,
    ansi_mode: bool,
) -> PlanResult<Expr> {
    let timestamp = ScalarUDF::from(SparkTimestamp::try_new(
        Some(session_timezone),
        ansi_mode,
        false,
    )?)
    .call(vec![string]);
    let calendar_interval = match interval_type {
        DataType::Interval(IntervalUnit::MonthDayNano) => interval,
        _ => ScalarUDF::from(SparkDayTimeIntervalToCalendarInterval::new()).call(vec![interval]),
    };
    let shifted = if subtract {
        timestamp - calendar_interval
    } else {
        timestamp + calendar_interval
    };
    match string_type {
        DataType::Utf8 => Ok(ScalarUDF::from(SparkToUtf8::new()).call(vec![shifted])),
        DataType::LargeUtf8 => Ok(ScalarUDF::from(SparkToLargeUtf8::new()).call(vec![shifted])),
        DataType::Utf8View => Ok(ScalarUDF::from(SparkToUtf8View::new()).call(vec![shifted])),
        data_type => Err(PlanError::internal(format!(
            "expected string type for interval arithmetic, got {data_type}"
        ))),
    }
}

/// Arguments:
///   - left: A numeric, STRING, DATE, TIMESTAMP, or INTERVAL expression.
///   - right: If left is a numeric right must be numeric expression, or an INTERVAL otherwise.
///
/// Returns:
///   - If left is a numeric, the common maximum type of the arguments.
///   - If one expression is a STRING and the other is a day-time interval, the result is a STRING.
///   - If left is a DATE and right is a day-time interval the result is a TIMESTAMP.
///   - If both expressions are interval they must be of the same class.
///   - Otherwise, the result type matches left.
///
/// Most of the above conditions are handled by DataFusion. Spark-specific coercion differences are
/// rewritten here before constructing the DataFusion expression. For DataFusion's rules, see:
///   https://github.com/apache/datafusion/blob/a28f2834c6969a0c0eb26165031f8baa1e1156a5/datafusion/expr-common/src/type_coercion/binary.rs#L194
fn spark_plus(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    if arguments.len() < 2 {
        // The arity-1 `+` used to hand its operand back untouched, so it answered every type Spark
        // refuses -- a DATE, a BOOLEAN, an ARRAY -- and kept a STRING as a string where Spark gives
        // a DOUBLE.
        let arg = arguments.one()?;
        if let Some(error) = rejects_unary_operand("+", &arg, function_context.schema) {
            return Err(error);
        }
        Ok(match arg.get_type(function_context.schema) {
            Ok(DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => {
                if function_context.plan_config.ansi_mode {
                    cast(arg, DataType::Float64)
                } else {
                    try_cast(arg, DataType::Float64)
                }
            }
            Ok(DataType::Null) => cast(arg, DataType::Float64),
            _ => arg,
        })
    } else {
        let (left, right) = arguments.two()?;
        if let Some(error) = rejects_time_operand_when_disabled(
            &left,
            &right,
            function_context.schema,
            function_context.plan_config.time_type_enabled,
        ) {
            return Err(error);
        }
        if let Some(error) = rejects_udt_operand("+", &left, &right, function_context.schema) {
            return Err(error);
        }
        if let Some(error) =
            rejects_binary_string_operand("+", &left, &right, function_context.schema)
        {
            return Err(error);
        }
        if let Some(error) =
            rejects_date_difference_operand(spark_plus, &left, &right, &function_context)
        {
            return Err(error);
        }
        let (left, right) = promote_string_operands(
            left,
            right,
            function_context.schema,
            function_context.plan_config.ansi_mode,
        );
        let (left, right) = cast_untyped_null_beside_datetime(
            left,
            right,
            function_context.schema,
            NullPartner::DayTimeInterval(Arc::clone(
                &function_context.plan_config.session_timezone,
            )),
        );
        let (left_type, right_type) = (
            left.get_type(function_context.schema),
            right.get_type(function_context.schema),
        );
        if let (Ok(left_type), Ok(right_type)) = (&left_type, &right_type)
            && rejects_add(
                left_type,
                right_type,
                function_context.plan_config.ansi_mode,
            )
        {
            return Err(arithmetic_operand_error("+", left_type, right_type));
        }
        Ok(match (left_type, right_type) {
            (
                Ok(string_type @ (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)),
                Ok(DataType::Duration(TimeUnit::Microsecond)),
            ) => shift_string_by_interval(
                left,
                right,
                string_type,
                &DataType::Duration(TimeUnit::Microsecond),
                false,
                Arc::clone(&function_context.plan_config.session_timezone),
                function_context.plan_config.ansi_mode,
            )?,
            (
                Ok(DataType::Duration(TimeUnit::Microsecond)),
                Ok(string_type @ (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)),
            ) => shift_string_by_interval(
                right,
                left,
                string_type,
                &DataType::Duration(TimeUnit::Microsecond),
                false,
                Arc::clone(&function_context.plan_config.session_timezone),
                function_context.plan_config.ansi_mode,
            )?,
            (
                Ok(string_type @ (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)),
                Ok(interval_type @ DataType::Interval(IntervalUnit::MonthDayNano)),
            ) => shift_string_by_interval(
                left,
                right,
                string_type,
                &interval_type,
                false,
                Arc::clone(&function_context.plan_config.session_timezone),
                function_context.plan_config.ansi_mode,
            )?,
            (
                Ok(interval_type @ DataType::Interval(IntervalUnit::MonthDayNano)),
                Ok(string_type @ (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)),
            ) => shift_string_by_interval(
                right,
                left,
                string_type,
                &interval_type,
                false,
                Arc::clone(&function_context.plan_config.session_timezone),
                function_context.plan_config.ansi_mode,
            )?,
            (Ok(DataType::Date32), Ok(DataType::Duration(TimeUnit::Microsecond))) => {
                left + cast(right, DataType::Interval(IntervalUnit::MonthDayNano))
            }
            (Ok(DataType::Duration(TimeUnit::Microsecond)), Ok(DataType::Date32)) => {
                cast(left, DataType::Interval(IntervalUnit::MonthDayNano)) + right
            }
            // A Spark day-time interval reaches `+` as Arrow `Duration`, but DataFusion's
            // `time +- interval` rule matches only `Interval(_)`. Spell it the way that rule
            // expects, so the `(Time, IntervalDt)` pair `rejects_add` accepts really resolves
            // (`TimeAddInterval`, `BinaryArithmeticWithDatetimeResolver.scala:87,90`) instead of
            // dying one layer down. The `TIME - TIME` arm in `spark_minus` produces exactly such
            // a `Duration`, so without this the two halves contradict each other.
            (Ok(DataType::Time32(_) | DataType::Time64(_)), Ok(DataType::Duration(_))) => {
                // A UDF, not DataFusion's `time + interval`: that `BinaryExpr` panics in interval
                // bound propagation when the TIME's bounds are known (a CTE column).
                ScalarUDF::from(SparkTimeAddDtInterval::new()).call(vec![left, right])
            }
            (Ok(DataType::Duration(_)), Ok(DataType::Time32(_) | DataType::Time64(_))) => {
                ScalarUDF::from(SparkTimeAddDtInterval::new()).call(vec![right, left])
            }
            (Ok(left_type), Ok(DataType::Date32)) if left_type.is_numeric() => {
                cast(left + cast(right, DataType::Int32), DataType::Date32)
            }
            (Ok(DataType::Date32), Ok(right_type)) if right_type.is_numeric() => {
                cast(cast(left, DataType::Int32) + right, DataType::Date32)
            }
            // TODO: In case getting the type fails, we don't want to fail the query.
            //  Future work is needed here, ideally we create something like `Operator::SparkPlus`.
            (Ok(_), Ok(_)) | (Err(_), _) | (_, Err(_)) => left + right,
        })
    }
}

/// Arguments:
///   - left: A numeric, DATE, TIMESTAMP, or INTERVAL expression.
///   - right: The accepted type depends on the type of expr:
///     - If left is a numeric right must be numeric expression.
///     - If left is a year-month or day-time interval, right must be the same class.
///     - Otherwise right must be a DATE or TIMESTAMP.
///
/// Returns:
///   - If left is a numeric, the result is common maximum type of the arguments.
///   - If left is a DATE and right is a day-time interval the result is a TIMESTAMP.
///   - If left is a TIMESTAMP and right is an interval the result is a TIMESTAMP.
///   - If left and right are DATEs the result is an INTERVAL DAYS.
///   - If left or right are TIMESTAMP the result is an INTERVAL DAY TO SECOND.
///   - If both expressions are interval they must be of the same class.
///   - Otherwise, the result type matches left.
///
/// All of the above conditions should be handled by the DataFusion.
/// If there is a discrepancy in parity, check the link below and adjust Sail's logic accordingly:
///   https://github.com/apache/datafusion/blob/a28f2834c6969a0c0eb26165031f8baa1e1156a5/datafusion/expr-common/src/type_coercion/binary.rs#L194
fn spark_minus(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    if arguments.len() < 2 {
        let arg = arguments.one()?;
        // Same accept set as the unary `+`. DataFusion's `negative` already refused these, but with
        // its own `Failed to coerce arguments` message and an Arrow `Debug` dump in it.
        if let Some(error) = rejects_unary_operand("-", &arg, function_context.schema) {
            return Err(error);
        }
        Ok(spark_unary_negate(
            arg,
            function_context.plan_config.ansi_mode,
            function_context.schema,
        ))
    } else {
        let (left, right) = arguments.two()?;
        if let Some(error) = rejects_time_operand_when_disabled(
            &left,
            &right,
            function_context.schema,
            function_context.plan_config.time_type_enabled,
        ) {
            return Err(error);
        }
        if let Some(error) = rejects_udt_operand("-", &left, &right, function_context.schema) {
            return Err(error);
        }
        if let Some(error) =
            rejects_binary_string_operand("-", &left, &right, function_context.schema)
        {
            return Err(error);
        }
        if let Some(error) =
            rejects_date_difference_operand(spark_minus, &left, &right, &function_context)
        {
            return Err(error);
        }
        let (left, right) = promote_string_operands(
            left,
            right,
            function_context.schema,
            function_context.plan_config.ansi_mode,
        );
        let (left, right) = cast_untyped_null_beside_datetime(
            left,
            right,
            function_context.schema,
            NullPartner::OtherOperand,
        );
        let (left, right) = promote_string_beside_datetime(
            left,
            right,
            function_context.schema,
            &function_context.plan_config.session_timezone,
            function_context.plan_config.ansi_mode,
        )?;
        let (left_type, right_type) = (
            left.get_type(function_context.schema),
            right.get_type(function_context.schema),
        );
        if let (Ok(left_type), Ok(right_type)) = (&left_type, &right_type)
            && rejects_subtract(
                left_type,
                right_type,
                function_context.plan_config.ansi_mode,
            )
        {
            return Err(arithmetic_operand_error("-", left_type, right_type));
        }
        Ok(match (left_type, right_type) {
            (
                Ok(string_type @ (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)),
                Ok(
                    interval_type @ (DataType::Duration(TimeUnit::Microsecond)
                    | DataType::Interval(IntervalUnit::MonthDayNano)),
                ),
            ) => shift_string_by_interval(
                left,
                right,
                string_type,
                &interval_type,
                true,
                Arc::clone(&function_context.plan_config.session_timezone),
                function_context.plan_config.ansi_mode,
            )?,
            // `SubtractTimes` returns a day-time interval (`timeExpressions.scala:632`), but
            // DataFusion coerces a `Time64` pair to `Interval(MonthDayNano)` -- the CALENDAR
            // interval, which combines with nothing day-time. Cast to `Duration` to restore the
            // class Spark gives it.
            // TODO: `Duration` keeps the value and the day-time family but not Spark's
            // `HOUR TO SECOND` start/end fields; `arithmetic_time_subtraction.feature` pins it.
            (
                Ok(DataType::Time32(_) | DataType::Time64(_)),
                Ok(DataType::Time32(_) | DataType::Time64(_)),
            ) => cast(left - right, DataType::Duration(TimeUnit::Microsecond)),
            // The `-` half of the arms above: `TimeAddInterval` with a negated interval
            // (`BinaryArithmeticWithDatetimeResolver.scala:133-134`). `interval - time` is absent
            // on purpose: Spark has no such arm and `rejects_subtract` rejects the pair first.
            (Ok(DataType::Time32(_) | DataType::Time64(_)), Ok(DataType::Duration(_))) => {
                ScalarUDF::from(SparkTimeAddDtInterval::new()).call(vec![
                    left,
                    ScalarUDF::from(NegateDuration::new()).call(vec![right]),
                ])
            }
            (Ok(DataType::Date32), Ok(DataType::Duration(TimeUnit::Microsecond))) => {
                left - cast(right, DataType::Interval(IntervalUnit::MonthDayNano))
            }
            // `SubtractTimestamps` takes the pair whenever EITHER side is a timestamp, and that
            // arm comes before the `SubtractDates` one
            // (`BinaryArithmeticWithDatetimeResolver.scala:139-142`), so the DATE is read as a
            // timestamp. Casting it explicitly is what makes the answer right: Spark reads a DATE
            // as midnight in the SESSION time zone, and DataFusion's own coercion reads it as
            // midnight UTC -- a wrong value under any other zone -- and yields
            // `Duration(Nanosecond)`, which has no Spark type at all.
            // A zoned column may carry a zone of its own (a Parquet file written elsewhere), so
            // the DATE is read in the session zone, not in the column's.
            (Ok(DataType::Date32), Ok(DataType::Timestamp(unit, zone))) => cast(
                cast(
                    left,
                    DataType::Timestamp(
                        unit,
                        zone.map(|_| Arc::clone(&function_context.plan_config.session_timezone)),
                    ),
                ) - right,
                DataType::Duration(TimeUnit::Microsecond),
            ),
            (Ok(DataType::Timestamp(unit, zone)), Ok(DataType::Date32)) => cast(
                left - cast(
                    right,
                    DataType::Timestamp(
                        unit,
                        zone.map(|_| Arc::clone(&function_context.plan_config.session_timezone)),
                    ),
                ),
                DataType::Duration(TimeUnit::Microsecond),
            ),
            // TODO: `SubtractDates` returns `DayTimeIntervalType(DAY)` (`datetimeExpressions.scala:3616`).
            //  Sail's only day-time spelling is `Duration`, which carries no field range, and its
            //  consumers read a `Duration` by seconds: `CAST(date - date AS INT)` answered 1209600
            //  where Spark casts by the end field (`IntervalUtils.scala:921-928`) and answers 14, and
            //  `hash`, `to_json`, `try_sum` and `try_avg` refused it. Until the interval keeps its
            //  fields (PR #2350), the difference stays the day count, typed INT -- the offset
            //  `DateAdd` takes, so `DATE + (date - date)` still resolves.
            (Ok(DataType::Date32), Ok(DataType::Date32)) => {
                mark_date_difference(cast(left, DataType::Int32) - cast(right, DataType::Int32))
            }
            (Ok(DataType::Date32), Ok(right_type)) if right_type.is_numeric() => {
                cast(cast(left, DataType::Int32) - right, DataType::Date32)
            }
            // TODO: In case getting the type fails, we don't want to fail the query.
            //  Future work is needed here, ideally we create something like `Operator::SparkMinus`.
            (Ok(_), Ok(_)) | (Err(_), _) | (_, Err(_)) => left - right,
        })
    }
}

/// Arguments:
///   - left: A numeric or INTERVAL expression.
///   - right: A numeric expression or INTERVAL expression.
///
/// You may not specify an INTERVAL for both arguments.
///
/// Returns:
///   - If both left and right are DECIMAL, the result is DECIMAL.
///   - If left or right is an INTERVAL, the result is of the same type.
///   - If both left and right are integral numeric types, the result is the larger of the two types.
///   - In all other cases the result is a DOUBLE.
///
/// All of the above conditions should be handled by the DataFusion.
/// If there is a discrepancy in parity, check the link below and adjust Sail's logic accordingly:
///   https://github.com/apache/datafusion/blob/a28f2834c6969a0c0eb26165031f8baa1e1156a5/datafusion/expr-common/src/type_coercion/binary.rs#L194
fn spark_multiply(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;

    let (left, right) = arguments.two()?;
    if let Some(error) = rejects_udt_operand("*", &left, &right, function_context.schema) {
        return Err(error);
    }
    if let Some(error) = rejects_binary_string_operand("*", &left, &right, function_context.schema)
    {
        return Err(error);
    }
    if let Some(error) =
        rejects_date_difference_operand(spark_multiply, &left, &right, &function_context)
    {
        return Err(error);
    }
    let (left, right) = promote_string_operands(
        left,
        right,
        function_context.schema,
        function_context.plan_config.ansi_mode,
    );
    let (left_type, right_type) = (
        left.get_type(function_context.schema),
        right.get_type(function_context.schema),
    );
    if let (Ok(left_type), Ok(right_type)) = (&left_type, &right_type)
        && rejects_multiply(
            left_type,
            right_type,
            function_context.plan_config.ansi_mode,
        )
    {
        return Err(arithmetic_operand_error("*", left_type, right_type));
    }
    let ansi_mode = function_context.plan_config.ansi_mode;
    let is_interval = |data_type: &Result<DataType, _>| {
        matches!(
            data_type,
            Ok(
                DataType::Interval(IntervalUnit::YearMonth | IntervalUnit::MonthDayNano)
                    | DataType::Duration(TimeUnit::Microsecond)
            )
        )
    };
    let (left, right) = match (&left_type, &right_type) {
        (left_interval, Ok(number_type)) if is_interval(left_interval) => {
            (left, interval_scale_number(right, number_type, ansi_mode))
        }
        (Ok(number_type), right_interval) if is_interval(right_interval) => {
            (interval_scale_number(left, number_type, ansi_mode), right)
        }
        _ => (left, right),
    };
    Ok(match (left_type, right_type) {
        // `MultiplyYMInterval` (`BinaryArithmeticWithDatetimeResolver.scala:154-155`), either
        // operand order. It scales the MONTHS and rounds HALF_UP, and DataFusion has no coercion
        // at all for `Interval(YearMonth)` against a number, so without this the pair is refused.
        (Ok(DataType::Interval(IntervalUnit::YearMonth)), Ok(_)) => {
            ScalarUDF::from(SparkMultiplyYmInterval::new()).call(vec![left, right])
        }
        (Ok(_), Ok(DataType::Interval(IntervalUnit::YearMonth))) => {
            ScalarUDF::from(SparkMultiplyYmInterval::new()).call(vec![right, left])
        }
        // `MultiplyInterval` (`:150-151`), the LEGACY calendar interval, either operand order.
        // It reads the ANSI flag, unlike the two ANSI-interval pairs.
        (Ok(DataType::Interval(IntervalUnit::MonthDayNano)), Ok(_)) => ScalarUDF::from(
            SparkMultiplyCalendarInterval::new(function_context.plan_config.ansi_mode),
        )
        .call(vec![left, right]),
        (Ok(_), Ok(DataType::Interval(IntervalUnit::MonthDayNano))) => ScalarUDF::from(
            SparkMultiplyCalendarInterval::new(function_context.plan_config.ansi_mode),
        )
        .call(vec![right, left]),
        // `MultiplyDTInterval` (`:156-157`). Sail spells a day-time interval as `Duration`, and
        // scaling its micros through DataFusion truncated the product instead of rounding it
        // HALF_UP -- `INTERVAL '0.000001' SECOND * 0.5` came back as zero where Spark answers one
        // microsecond -- so it goes through the same UDF as the year-month one.
        (Ok(DataType::Duration(TimeUnit::Microsecond)), Ok(_)) => {
            ScalarUDF::from(SparkMultiplyDtInterval::new()).call(vec![left, right])
        }
        (Ok(_), Ok(DataType::Duration(TimeUnit::Microsecond))) => {
            ScalarUDF::from(SparkMultiplyDtInterval::new()).call(vec![right, left])
        }
        // TODO: In case getting the type fails, we don't want to fail the query.
        //  Future work is needed here, ideally we create something like `Operator::SparkMultiply`.
        (Ok(_), Ok(_)) | (Err(_), _) | (_, Err(_)) => left * right,
    })
}

/// Returns a guarded divisor expression that handles division by zero at runtime.
///
/// In non-ANSI mode: returns `nullif(divisor, 0)` — evaluates to NULL when divisor is zero.
/// In ANSI mode: returns `CASE WHEN divisor = 0 THEN raise_error(msg) ELSE divisor END`.
///
/// This wraps the divisor itself (not the entire division expression) to avoid
/// duplicating complex divisor expressions (e.g., window functions) in the plan.
fn make_safe_divisor(
    divisor: Expr,
    divisor_type: &DataType,
    ansi_mode: bool,
    error_message: &str,
) -> Expr {
    // Skip wrapping for Interval/Duration types (cannot be compared to lit(0)).
    if matches!(divisor_type, DataType::Interval(_) | DataType::Duration(_)) {
        return divisor;
    }

    if ansi_mode {
        let zero_check = divisor.clone().eq(lit(0));
        let raise = Expr::ScalarFunction(expr::ScalarFunction {
            func: Arc::new(ScalarUDF::from(RaiseError::new())),
            args: vec![lit(error_message)],
        });
        Expr::Case(expr::Case {
            expr: None,
            when_then_expr: vec![(Box::new(zero_check), Box::new(raise))],
            else_expr: Some(Box::new(divisor)),
        })
    } else {
        expr_fn::nullif(divisor, lit(0))
    }
}

/// Arguments:
///   - dividend: A numeric or INTERVAL expression.
///   - divisor: A numeric expression.
///
/// Returns:
///   - If both dividend and divisor are DECIMAL, the result is DECIMAL.
///   - If dividend is a year-month interval, the result is an INTERVAL YEAR TO MONTH.
///   - If dividend is a day-time interval, the result is an INTERVAL DAY TO SECOND.
///   - In all other cases, a DOUBLE.
///
/// All of the above conditions should be handled by the DataFusion.
/// If there is a discrepancy in parity, check the link below and adjust Sail's logic accordingly:
///   https://github.com/apache/datafusion/blob/a28f2834c6969a0c0eb26165031f8baa1e1156a5/datafusion/expr-common/src/type_coercion/binary.rs#L194
fn spark_divide(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;

    let (dividend, divisor) = arguments.two()?;
    if let Some(error) = rejects_udt_operand("/", &dividend, &divisor, function_context.schema) {
        return Err(error);
    }
    if let Some(error) =
        rejects_binary_string_operand("/", &dividend, &divisor, function_context.schema)
    {
        return Err(error);
    }
    if let Some(error) =
        rejects_date_difference_operand(spark_divide, &dividend, &divisor, &function_context)
    {
        return Err(error);
    }
    // `/` is a `BinaryArithmetic` too, so the string promotion applies. Its generic branch below
    // already cast a string to DOUBLE, which hid the gap for most pairs -- but not beside a
    // DECIMAL (refused, or typed DECIMAL), not for a malformed string with ANSI off (raised where
    // Spark gives NULL), and not for `'2.5' / 2` with ANSI on, which Spark REFUSES because the
    // string goes to BIGINT and Sail answered `1.25`.
    let (dividend, divisor) = promote_string_operands(
        dividend,
        divisor,
        function_context.schema,
        function_context.plan_config.ansi_mode,
    );

    let ansi_mode = function_context.plan_config.ansi_mode;
    let dividend_type = dividend.get_type(function_context.schema);
    let divisor_type = divisor.get_type(function_context.schema);
    // `Divide.inputType = TypeCollection(DoubleType, DecimalType)` (`arithmetic.scala:812`).
    // Left to DataFusion a non-numeric operand is reinterpreted as its raw integer and yields a
    // meaningless number. A string pair with no numeric operand to anchor the cast is rejected
    // under ANSI on only; under ANSI off both sides coerce to DOUBLE.
    if let (Ok(dividend_type), Ok(divisor_type)) = (&dividend_type, &divisor_type)
        && (rejects_as_divide_dividend(dividend_type)
            || rejects_as_divide_divisor(divisor_type)
            || rejects_unanchored_string_pair(
                operand_role(dividend_type),
                operand_role(divisor_type),
                ansi_mode,
            ))
    {
        return Err(arithmetic_operand_error("/", dividend_type, divisor_type));
    }
    let divisor = match (&dividend_type, &divisor_type) {
        (
            Ok(
                DataType::Interval(IntervalUnit::YearMonth | IntervalUnit::MonthDayNano)
                | DataType::Duration(TimeUnit::Microsecond),
            ),
            Ok(divisor_type),
        ) => interval_scale_number(divisor, divisor_type, ansi_mode),
        _ => divisor,
    };
    // `DivideYMInterval` (`BinaryArithmeticWithDatetimeResolver.scala:167`) scales the MONTHS and
    // rounds HALF_UP. It goes before the zero-divisor short-circuit below on purpose: the interval
    // divisions do not read the ANSI flag (`IntervalDivide`), so `INTERVAL '1' MONTH / 0` raises
    // in BOTH modes, where a numeric `/` returns NULL with ANSI off.
    if let Ok(DataType::Interval(IntervalUnit::YearMonth)) = &dividend_type {
        return Ok(ScalarUDF::from(SparkDivideYmInterval::new()).call(vec![dividend, divisor]));
    }
    // `DivideDTInterval` (`:169`), same story: the generic path below reached DataFusion as
    // `Duration / <number>`, which refuses a DECIMAL divisor outright, truncates instead of
    // rounding HALF_UP, and returns NULL for a zero divisor with ANSI off.
    if let Ok(DataType::Duration(TimeUnit::Microsecond)) = &dividend_type {
        return Ok(ScalarUDF::from(SparkDivideDtInterval::new()).call(vec![dividend, divisor]));
    }
    // `DivideInterval` (`:166`). This one DOES read the ANSI flag: with it off a zero divisor -- a
    // negative zero included -- gives NULL, which the UDF produces itself. Returning here also keeps
    // the result typed as an interval, where the shared short-circuit below would return an untyped
    // NULL.
    if let Ok(DataType::Interval(IntervalUnit::MonthDayNano)) = &dividend_type {
        return Ok(ScalarUDF::from(SparkDivideCalendarInterval::new(ansi_mode))
            .call(vec![dividend, divisor]));
    }

    // NOT short-circuited at plan time: Spark raises the division by zero only when the division is
    // EVALUATED (`DivModLike.eval`), so `if(false, 1 / 0, NULL)` answers there and refusing the
    // literal zero at analysis refused a query Spark accepts. The runtime guard below raises for the
    // rows that reach it, which is where Spark raises too.

    // Apply runtime zero-divisor guard to the divisor before building the division expression.
    let effective_divisor_type = divisor_type.as_ref().cloned().unwrap_or(DataType::Int32);
    let divisor = make_safe_divisor(
        divisor,
        &effective_divisor_type,
        ansi_mode,
        "Division by zero",
    );

    let div_expr = match (&dividend_type, &divisor_type) {
        // TODO: Casting DataType::Interval(_) to DataType::Int64 is not supported yet.
        //  Seems to be a bug in DataFusion.
        // TODO: Cast the precision and scale that matches the Spark's behavior after the division.
        //  See `test_divide` in python/pysail/tests/spark/test_math.py
        (Ok(DataType::Decimal128(_, _)), Ok(_))
        | (Ok(_), Ok(DataType::Decimal128(_, _)))
        | (Ok(DataType::Decimal256(_, _)), Ok(_))
        | (Ok(_), Ok(DataType::Decimal256(_, _)))
        | (Ok(DataType::Interval(IntervalUnit::YearMonth)), Ok(_))
        | (Ok(DataType::Interval(IntervalUnit::DayTime)), Ok(_)) => dividend / divisor,
        (Ok(DataType::Duration(TimeUnit::Microsecond)), Ok(_)) => {
            // Match duration because we cast Spark's DayTime interval to Duration.
            cast(
                cast(dividend, DataType::Int64) / divisor,
                DataType::Duration(TimeUnit::Microsecond),
            )
        }
        (Ok(_), Ok(_)) => cast(dividend, DataType::Float64) / cast(divisor, DataType::Float64),
        // TODO: In case getting the type fails, we don't want to fail the query.
        //  Future work is needed here, ideally we create something like `Operator::SparkDivide`.
        (Err(_), _) | (_, Err(_)) => dividend / divisor,
    };

    Ok(div_expr)
}

/// Returns the integral part of the division of dividend by divisor.
///
/// Arguments:
///   - dividend: An expression that evaluates to a numeric or interval.
///   - divisor: A matching interval type if dividend is an interval, a numeric otherwise.
///
/// Returns:
///   A BIGINT
///
fn spark_div(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;

    let (dividend, divisor) = arguments.two()?;

    // NOT short-circuited at plan time: Spark raises the division by zero only when the division is
    // EVALUATED (`DivModLike.eval`), so `if(false, 1 / 0, NULL)` answers there and refusing the
    // literal zero at analysis refused a query Spark accepts. The runtime guard below raises for the
    // rows that reach it, which is where Spark raises too.

    let ansi_mode = function_context.plan_config.ansi_mode;
    let dividend_type = dividend.get_type(function_context.schema);
    let divisor_type = divisor.get_type(function_context.schema);

    // Apply runtime zero-divisor guard to the divisor before building the division expression.
    let effective_divisor_type = divisor_type.as_ref().cloned().unwrap_or(DataType::Int32);
    let divisor = make_safe_divisor(
        divisor,
        &effective_divisor_type,
        ansi_mode,
        "Division by zero",
    );

    let div_expr = match (&dividend_type, &divisor_type) {
        // TODO: Casting DataType::Interval(_) to DataType::Int64 is not supported yet.
        //  Seems to be a bug in DataFusion.
        (Ok(DataType::Duration(_)), Ok(DataType::Duration(_))) => {
            // Match duration because we cast Spark's DayTime interval to Duration.
            cast(dividend, DataType::Int64) / cast(divisor, DataType::Int64)
        }
        // Handle Interval / Interval division using custom UDF
        (Ok(DataType::Interval(_)), Ok(DataType::Interval(_))) => {
            let interval_div = Arc::new(ScalarUDF::from(SparkIntervalDiv::new()));
            Expr::ScalarFunction(expr::ScalarFunction {
                func: interval_div,
                args: vec![dividend, divisor],
            })
        }
        // `IntegralDivide.inputType` is `LongType` (`arithmetic.scala:890-893`): integers are widened
        // to BIGINT BEFORE dividing, so `-2147483648 DIV -1` is 2147483648 where an INT division
        // overflows -- and `-2147483648` is an INT literal now that the sign is folded into it.
        (Ok(left), Ok(right)) if left.is_integer() && right.is_integer() => {
            cast(dividend, DataType::Int64) / cast(divisor, DataType::Int64)
        }
        // TODO: In case getting the type fails, we don't want to fail the query.
        //  Future work is needed here, ideally we create something like `Operator::SparkDivide`.
        (Ok(_), Ok(_)) | (Err(_), _) | (_, Err(_)) => dividend / divisor,
    };

    Ok(cast(div_expr, DataType::Int64))
}

fn power(base: Expr, exponent: Expr) -> Expr {
    cast(expr_fn::power(base, exponent), DataType::Float64)
}

fn hypot(expr1: Expr, expr2: Expr) -> Expr {
    let sum_squared = expr1.clone() * expr1 + expr2.clone() * expr2;
    cast(expr_fn::sqrt(sum_squared), DataType::Float64)
}

fn rint(expr: Expr) -> Expr {
    cast(expr_fn::round(vec![expr]), DataType::Float64)
}

fn positive_or_null(expr: Expr) -> Expr {
    Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![(Box::new(expr.clone().gt(lit(0_f64))), Box::new(expr))],
        else_expr: None,
    })
}

#[inline]
fn eulers_constant() -> Expr {
    lit(std::f64::consts::E)
}

fn ceil_floor(input: ScalarFunctionInput, name: &str) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let ansi_mode = function_context.plan_config.ansi_mode;
    // DataFusion bug: `ReturnTypeArgs.scalar_arguments` is None if scalar argument is nested
    let arguments = if arguments.len() == 2 {
        let (arg, target_scale) = arguments.two()?;
        let target_scale = match target_scale {
            Expr::Literal(_, _) => Ok(target_scale),
            Expr::Negative(negative) => {
                if let Expr::Literal(scalar, metadata) = *negative {
                    match scalar {
                        ScalarValue::Int8(v) => Ok(Expr::Literal(
                            ScalarValue::Int32(v.map(|v| -v as i32)),
                            metadata,
                        )),
                        ScalarValue::Int16(v) => Ok(Expr::Literal(
                            ScalarValue::Int32(v.map(|v| -v as i32)),
                            metadata,
                        )),
                        ScalarValue::Int32(v) => {
                            Ok(Expr::Literal(ScalarValue::Int32(v.map(|v| -v)), metadata))
                        }
                        ScalarValue::Int64(v) => Ok(Expr::Literal(
                            ScalarValue::Int32(v.map(|v| -(v as i32))),
                            metadata,
                        )),
                        ScalarValue::UInt8(v) => Ok(Expr::Literal(
                            ScalarValue::Int32(v.map(|v| -(v as i32))),
                            metadata,
                        )),
                        ScalarValue::UInt16(v) => Ok(Expr::Literal(
                            ScalarValue::Int32(v.map(|v| -(v as i32))),
                            metadata,
                        )),
                        ScalarValue::UInt32(v) => Ok(Expr::Literal(
                            ScalarValue::Int32(v.map(|v| -(v as i32))),
                            metadata,
                        )),
                        ScalarValue::UInt64(v) => Ok(Expr::Literal(
                            ScalarValue::Int32(v.map(|v| -(v as i32))),
                            metadata,
                        )),
                        other => Err(generic_exec_err(
                            "ceil",
                            format!("Target scale must be Integer literal, got {other}").as_str(),
                        )),
                    }
                } else {
                    Err(generic_exec_err(
                        "ceil",
                        format!("Target scale must be Integer literal, got {negative}").as_str(),
                    ))
                }
            }
            _ => Err(generic_exec_err(
                "ceil",
                format!("Target scale must be Integer literal, got {target_scale}").as_str(),
            )),
        }?;
        vec![arg, target_scale]
    } else {
        arguments
    };
    let func = if matches!(name.to_lowercase().trim(), "ceil") {
        Arc::new(ScalarUDF::from(SparkCeil::new(ansi_mode)))
    } else {
        Arc::new(ScalarUDF::from(SparkFloor::new(ansi_mode)))
    };
    Ok(Expr::ScalarFunction(expr::ScalarFunction {
        func,
        args: arguments,
    }))
}

fn ln(expr: Expr) -> Expr {
    expr_fn::ln(positive_or_null(expr))
}

fn log(base: Expr, num: Expr) -> Expr {
    expr_fn::log(base, positive_or_null(num))
}

fn log10(expr: Expr) -> Expr {
    expr_fn::log10(positive_or_null(expr))
}

fn log1p(expr: Expr) -> Expr {
    expr_fn::ln(positive_or_null(expr + lit(1.0_f64)))
}

fn log2(expr: Expr) -> Expr {
    expr_fn::log2(positive_or_null(expr))
}

fn double(func: impl Fn(Expr) -> Expr) -> impl Fn(Expr) -> Expr {
    move |arg: Expr| func(cast(arg, DataType::Float64))
}

fn double2(func: impl Fn(Expr, Expr) -> Expr) -> impl Fn(Expr, Expr) -> Expr {
    move |arg1: Expr, arg2| func(cast(arg1, DataType::Float64), cast(arg2, DataType::Float64))
}

fn spark_sqrt(arg: Expr) -> Expr {
    ScalarUDF::from(SparkSqrt::new()).call(vec![cast(arg, DataType::Float64)])
}

/// Modulo operation with division-by-zero handling.
///
/// In ANSI mode: raises error for integral/decimal modulo by zero.
/// In non-ANSI mode: returns NULL for modulo by zero.
/// Float/double modulo by zero returns NaN (IEEE 754).
fn spark_modulo(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;

    let (dividend, divisor) = arguments.two()?;
    if let Some(error) = rejects_udt_operand("%", &dividend, &divisor, function_context.schema) {
        return Err(error);
    }
    if let Some(error) =
        rejects_binary_string_operand("%", &dividend, &divisor, function_context.schema)
    {
        return Err(error);
    }
    if let Some(error) =
        rejects_date_difference_operand(spark_modulo, &dividend, &divisor, &function_context)
    {
        return Err(error);
    }
    let (dividend, divisor) = promote_string_operands(
        dividend,
        divisor,
        function_context.schema,
        function_context.plan_config.ansi_mode,
    );

    let ansi_mode = function_context.plan_config.ansi_mode;
    let divisor_type = divisor.get_type(function_context.schema);
    // Spark's `%` rejects a non-numeric operand at analysis, and an unanchored string pair
    // only under ANSI on. Left to DataFusion, the rejected pairs reinterpret an operand as a
    // raw integer and compute a meaningless remainder.
    if let (Ok(dividend_type), Ok(divisor_type)) =
        (dividend.get_type(function_context.schema), &divisor_type)
        && (rejects_unanchored_string_pair(
            operand_role(&dividend_type),
            operand_role(divisor_type),
            ansi_mode,
        ) || rejects_as_divide_divisor(&dividend_type)
            || rejects_as_divide_divisor(divisor_type))
    {
        return Err(arithmetic_operand_error("%", &dividend_type, divisor_type));
    }
    // NOT short-circuited at plan time: Spark raises the division by zero only when the division is
    // EVALUATED (`DivModLike.eval`), so `if(false, 1 / 0, NULL)` answers there and refusing the
    // literal zero at analysis refused a query Spark accepts. The runtime guard below raises for the
    // rows that reach it, which is where Spark raises too.

    // Apply runtime zero-divisor guard to the divisor before building the modulo expression.
    let effective_divisor_type = divisor_type.unwrap_or(DataType::Int32);
    let divisor = make_safe_divisor(
        divisor,
        &effective_divisor_type,
        ansi_mode,
        "Remainder by zero",
    );

    Ok(Expr::BinaryExpr(BinaryExpr {
        left: Box::new(dividend),
        op: Operator::Modulo,
        right: Box::new(divisor),
    }))
}

fn spark_abs(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    // `Abs` takes NUMERIC or an ANSI interval (`arithmetic.scala:158`) and no coercion rule casts a
    // UDT, so Spark refuses `abs(udt)` itself, not only an arithmetic around it. Its storage is a
    // numeric, so without this Sail computed on it.
    if let Some(argument) = arguments.first()
        && let Some(field) = operand_udt_field(argument, function_context.schema)
    {
        return Err(PlanError::analysis(format!(
            "cannot resolve 'abs' with operand type {}",
            spark_field_type_name(&field)
        )));
    }
    // `NumericAndAnsiInterval` leaves out the LEGACY calendar interval that `UnaryMinus` accepts
    // (`arithmetic.scala:158` against `:54`), so `abs(make_interval(...))` is refused where
    // `abs(INTERVAL '1' DAY)` answers.
    if let Some(argument) = arguments.first()
        && matches!(
            argument.get_type(function_context.schema),
            Ok(DataType::Interval(IntervalUnit::MonthDayNano))
        )
    {
        return Err(PlanError::analysis(
            "cannot resolve 'abs' with operand type INTERVAL".to_string(),
        ));
    }
    let udf = ScalarUDF::from(SparkAbs::new(function_context.plan_config.ansi_mode));
    Ok(udf.call(arguments))
}

fn spark_bin(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ansi_mode = input.function_context.plan_config.ansi_mode;
    let udf = ScalarUDF::from(SparkBin::new(ansi_mode));
    Ok(udf.call(input.arguments))
}

fn spark_pmod(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ansi_mode = input.function_context.plan_config.ansi_mode;
    let udf = ScalarUDF::from(SparkPmod::new(ansi_mode));
    Ok(udf.call(input.arguments))
}

/// Negate a numeric literal at planning time so a constant operand stays a
/// literal (some functions, e.g. `ceil`/`floor` target scale, require a literal
/// argument and run before the optimizer would fold a `SparkNegative` call).
/// Returns `None` when the value is not a foldable numeric literal or the
/// negation overflows (e.g. `-INT_MIN`), leaving such cases to the runtime UDF.
fn negate_literal(arg: &Expr) -> Option<Expr> {
    let Expr::Literal(value, _) = arg else {
        return None;
    };
    let negated = match value {
        ScalarValue::Int8(Some(v)) => ScalarValue::Int8(Some(v.checked_neg()?)),
        ScalarValue::Int16(Some(v)) => ScalarValue::Int16(Some(v.checked_neg()?)),
        ScalarValue::Int32(Some(v)) => ScalarValue::Int32(Some(v.checked_neg()?)),
        ScalarValue::Int64(Some(v)) => ScalarValue::Int64(Some(v.checked_neg()?)),
        ScalarValue::Float32(Some(v)) => ScalarValue::Float32(Some(-v)),
        ScalarValue::Float64(Some(v)) => ScalarValue::Float64(Some(-v)),
        _ => return None,
    };
    Some(lit(negated))
}

fn string_to_double(arg: Expr, ansi_mode: bool) -> Expr {
    if ansi_mode {
        cast(arg, DataType::Float64)
    } else {
        try_cast(arg, DataType::Float64)
    }
}

// TODO: Spark rounds a DOUBLE via `BigDecimal(d).setScale(scale, HALF_UP)` on the shortest
//  decimal representation, while DataFusion computes `(x * 10^scale).round() / 10^scale`,
//  so inexact binary ties differ (e.g. `round('1.005', 2)` is 1.01 in Spark but 1.0 in Sail).
fn spark_round(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        mut arguments,
        function_context,
    } = input;
    let scale = arguments.get(1).cloned().unwrap_or_else(|| lit(0));
    // TODO: A SQL parameter marker (`:p` or `?`) is resolved as an untyped placeholder whose
    //  value is bound after planning, so a STRING parameter is not cast here and `round(:p)`
    //  still fails to plan, while Spark binds parameters before analysis.
    // TODO: Resolve CASE branch coercion before checking the argument type here. Mixed
    //  CASE expressions can expose the first branch's type instead of their final type,
    //  and their shared coercion does not yet follow Spark's ANSI rules.
    if let Some(value) = arguments.first_mut()
        && matches!(
            value.get_type(function_context.schema),
            Ok(DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
        )
    {
        // Guard the string before casting so a NULL scale skips malformed literals too.
        // TODO: Shared expression resolution raises literal division-by-zero errors before
        //  this guard; defer those errors so a NULL scale can skip the entire value.
        let guarded =
            when(scale.is_null(), lit(ScalarValue::Utf8(None))).otherwise(value.clone())?;
        // Preserve Spark's nullable result; the inner ANSI cast still propagates errors.
        *value = try_cast(
            string_to_double(guarded, function_context.plan_config.ansi_mode),
            DataType::Float64,
        );
    }
    Ok(expr_fn::round(arguments))
}

/// Spark unary minus / `negative(x)`. Duration negation goes through
/// `NegateDuration`; everything else uses `SparkNegative`, which honors the ANSI
/// overflow semantics with `ansi_mode` baked at planning time.
fn spark_unary_negate(arg: Expr, ansi_mode: bool, schema: &DFSchemaRef) -> Expr {
    match arg.get_type(schema) {
        // DataFusion's `Negative` doesn't support Duration types, so route those
        // to the dedicated UDF.
        Ok(DataType::Duration(_)) => ScalarUDF::from(NegateDuration::new()).call(vec![arg]),
        // Spark's unary minus coerces strings to DOUBLE before negating. The
        // cast honors ANSI mode: an invalid string is NULL under ANSI off and
        // errors under ANSI on. (Without this, the `SparkNegative` signature
        // would coerce the string to an interval instead.)
        Ok(DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => {
            ScalarUDF::from(SparkNegative::new(ansi_mode))
                .call(vec![string_to_double(arg, ansi_mode)])
        }
        // Floating-point negation never overflows and is identical in both ANSI
        // modes, so use the native (vectorized, foldable) operator.
        Ok(DataType::Float16 | DataType::Float32 | DataType::Float64) => {
            Expr::Negative(Box::new(arg))
        }
        // A negated numeric literal folds to a literal so constant-arg functions
        // (e.g. `ceil`/`floor` target scale) still see a constant; overflow
        // (`-INT_MIN`) can't fold and falls through to the runtime UDF.
        _ => match negate_literal(&arg) {
            Some(folded) => folded,
            None => ScalarUDF::from(SparkNegative::new(ansi_mode)).call(vec![arg]),
        },
    }
}

/// `positive(x)` is `UnaryPositive` (`FunctionRegistry.scala:470`), the expression the unary `+`
/// parses to, so it takes the unary `+` path: the same guard and the same string promotion.
fn spark_positive(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arg = arguments.one()?;
    spark_plus(ScalarFunctionInput {
        arguments: vec![arg],
        function_context,
    })
}

/// `negative(x)` is `UnaryMinus` (`FunctionRegistry.scala:467`), and PySpark's `-col` calls it, so it
/// takes the unary `-` path: the same guard and the same negation.
fn spark_negative(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arg = arguments.one()?;
    spark_minus(ScalarFunctionInput {
        arguments: vec![arg],
        function_context,
    })
}

pub(super) fn list_built_in_math_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("%", F::custom(spark_modulo)),
        ("*", F::custom(spark_multiply)),
        ("+", F::custom(spark_plus)),
        ("-", F::custom(spark_minus)),
        ("/", F::custom(spark_divide)),
        ("abs", F::custom(spark_abs)),
        ("acos", F::unary(double(expr_fn::acos))),
        ("acosh", F::unary(double(expr_fn::acosh))),
        ("asin", F::unary(double(expr_fn::asin))),
        ("asinh", F::unary(double(expr_fn::asinh))),
        ("atan", F::unary(double(expr_fn::atan))),
        ("atan2", F::binary(double2(expr_fn::atan2))),
        ("atanh", F::unary(double(expr_fn::atanh))),
        ("bin", F::custom(spark_bin)),
        ("bround", F::udf(SparkBRound::new())),
        ("cbrt", F::unary(double(expr_fn::cbrt))),
        ("ceil", F::custom(|arg| ceil_floor(arg, "ceil"))),
        ("ceiling", F::custom(|arg| ceil_floor(arg, "ceil"))),
        ("conv", F::udf(SparkConv::new())),
        ("cos", F::unary(double(expr_fn::cos))),
        ("cosh", F::unary(double(expr_fn::cosh))),
        ("cot", F::unary(double(expr_fn::cot))),
        ("csc", F::unary(double(|arg| lit(1.0) / expr_fn::sin(arg)))),
        ("degrees", F::unary(double(expr_fn::degrees))),
        ("div", F::custom(spark_div)),
        ("e", F::nullary(eulers_constant)),
        ("exp", F::unary(double(expr_fn::exp))),
        ("expm1", F::unary(math_fn::expm1)),
        ("factorial", F::unary(expr_fn::factorial)),
        ("floor", F::custom(|arg| ceil_floor(arg, "floor"))),
        ("greatest", F::var_arg(expr_fn::greatest)),
        ("hex", F::unary(math_fn::hex)),
        ("hypot", F::binary(hypot)),
        ("least", F::var_arg(expr_fn::least)),
        ("ln", F::unary(double(ln))),
        ("log", F::binary(double2(log))),
        ("log10", F::unary(double(log10))),
        ("log1p", F::unary(double(log1p))),
        ("log2", F::unary(double(log2))),
        ("mod", F::custom(spark_modulo)),
        ("negative", F::custom(spark_negative)),
        ("pi", F::nullary(expr_fn::pi)),
        ("pmod", F::custom(spark_pmod)),
        ("positive", F::custom(spark_positive)),
        ("pow", F::binary(power)),
        ("power", F::binary(power)),
        ("radians", F::unary(double(expr_fn::radians))),
        ("rand", F::udf(Random::new())),
        ("random_poisson", F::udf(RandPoisson::new())),
        ("randn", F::udf(Randn::new())),
        ("random", F::udf(Random::new())),
        ("rint", F::unary(rint)),
        ("round", F::custom(spark_round)),
        ("sec", F::unary(double(|arg| lit(1.0) / expr_fn::cos(arg)))),
        ("sign", F::udf(SparkSignum::new())),
        ("signum", F::udf(SparkSignum::new())),
        ("sin", F::unary(double(expr_fn::sin))),
        ("sinh", F::unary(double(expr_fn::sinh))),
        ("sqrt", F::unary(spark_sqrt)),
        ("tan", F::unary(double(expr_fn::tan))),
        ("tanh", F::unary(double(expr_fn::tanh))),
        ("try_add", F::udf(SparkTryAdd::new())),
        ("try_divide", F::udf(SparkTryDiv::new())),
        ("try_multiply", F::udf(SparkTryMult::new())),
        ("try_mod", F::udf(SparkTryMod::new())),
        ("try_subtract", F::udf(SparkTrySubtract::new())),
        ("unhex", F::udf(SparkUnHex::new())),
        ("uniform", F::udf(SparkUniform::new())),
        ("width_bucket", F::quaternary(math_fn::width_bucket)),
    ]
}

/// The Spark arithmetic operand class of a type. `+`, `-` and `*` decide accept/reject by these
/// classes, so the plan-time guards below are written against them. The per-operator accept sets
/// were validated cell-by-cell against Spark JVM 4.2.0 when this table was built, and both sides
/// are asserted in the suite: `math/arithmetic_operand_rejection.feature` pins every pair Spark
/// rejects, and `math/arithmetic_operand_resolution.feature` pins every pair both engines
/// resolve — so narrowing a guard too far turns a row of the latter red. The latter asserts
/// resolution only, never the result type: that is the coercion contract, not this one.
/// `Unsupported` is a type Spark never accepts in arithmetic that would otherwise compute a
/// garbage value: boolean and every binary width, including the fixed-size one a Parquet
/// FIXED_LEN_BYTE_ARRAY produces. `Other` is any type outside the validated matrix (dictionary,
/// run-end-encoded, union -- struct, list, map and time have their own roles): the guards leave
/// those to DataFusion rather than hard-reject a pair whose behavior was never measured.
#[derive(PartialEq, Eq, Clone, Copy)]
enum OperandRole {
    Numeric,
    Str,
    UntypedNull,
    Date,
    Timestamp,
    Time,
    IntervalDt,
    IntervalCalendar,
    IntervalYm,
    Unsupported,
    Other,
}

fn operand_role(data_type: &DataType) -> OperandRole {
    use OperandRole::*;
    if data_type.is_numeric() {
        return Numeric;
    }
    if data_type.is_string() {
        return Str;
    }
    match data_type {
        DataType::Null => UntypedNull,
        DataType::Date32 | DataType::Date64 => Date,
        DataType::Timestamp(_, _) => Timestamp,
        // Spark's TIME (`spark.sql.timeType.enabled`) is an `AnyTimeType`, not a numeric, so
        // `Add`/`Subtract` (`inputType = NumericAndInterval`, `arithmetic.scala:417,508`) and
        // `Multiply` (`inputType = NumericType`, `:591`) reject every pair the datetime resolver
        // does not rewrite -- an interval survives `*` only via that rewrite, never via
        // `inputType`. Classified rather than left to the `Other` role,
        // which defers to DataFusion and lets `date + time` resolve to a TIMESTAMP_NTZ that
        // Spark rejects at analysis.
        DataType::Time32(_) | DataType::Time64(_) => Time,
        DataType::Interval(IntervalUnit::YearMonth) => IntervalYm,
        // Arrow has two spellings of Spark's day-time interval: `Duration`, which the resolver
        // produces (`resolver/data_type.rs:145`, chosen for microsecond precision), and
        // `Interval(DayTime)`, which Sail maps to Spark's `DayTimeInterval` on the way out
        // (`data_type_arrow.rs:200`) and renders as `interval day to second`
        // (`formatter.rs:81`). Both must share the role, or the guard would judge one of Spark's
        // day-time intervals by the calendar rules.
        DataType::Duration(_) | DataType::Interval(IntervalUnit::DayTime) => IntervalDt,
        // Spark's legacy CalendarInterval (`make_interval`), which Arrow stores as
        // `Interval(MonthDayNano)`. It is NOT an `AnsiIntervalType`: Spark pairs it with a date,
        // timestamp, string or another calendar interval, and rejects it against a day-time
        // interval or a TIME, so it cannot share the day-time role.
        DataType::Interval(_) => IntervalCalendar,
        // Spark's Parquet reader maps an unannotated FIXED_LEN_BYTE_ARRAY to `BinaryType`
        // (`ParquetSchemaConverter.scala`), so a fixed-size binary reaches arithmetic as a
        // plain BINARY and Spark rejects it like the other widths.
        DataType::Boolean
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => Unsupported,
        // Containers (VARIANT included — Sail stores it as a struct). Spark rejects them for
        // every arithmetic operator against every operand: measured across all 2080 cells of the
        // cartesian product, it accepts none. They are `Unsupported` rather than `Other` so the
        // additive and multiplicative guards reject at plan time and name the Spark type, as `/`
        // and `%` already do, instead of deferring to DataFusion — which multiplies a list by a
        // duration and leaks `List(non-null Int32)` into the message.
        DataType::Struct(_)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Map(_, _) => Unsupported,
        _ => Other,
    }
}

/// The verdict every `+`/`-`/`*` guard shares before its per-operator rules: an `Unsupported`
/// operand (boolean, binary) is always rejected, and an `Other` operand (a type outside the
/// validated matrix — dictionary, run-end-encoded, union) is deferred to
/// DataFusion rather than hard-rejected.
/// `None` means neither applies, so the caller runs its own accept/reject logic.
fn framing_verdict(a: OperandRole, b: OperandRole) -> Option<bool> {
    use OperandRole::*;
    if a == Unsupported || b == Unsupported {
        return Some(true);
    }
    if a == Other || b == Other {
        return Some(false);
    }
    None
}

/// A string paired with another string or an untyped `NULL`, with no numeric operand to anchor the
/// implicit cast. Spark accepts such a pair only under ANSI off (both coerce to DOUBLE); under ANSI
/// on it stays a string arithmetic and fails analysis. `NULL` paired with `NULL` is not included.
fn unanchored_string_pair(a: OperandRole, b: OperandRole) -> bool {
    use OperandRole::*;
    matches!((a, b), (Str, Str) | (Str, UntypedNull) | (UntypedNull, Str))
}

/// [`unanchored_string_pair`] as Spark rejects it: only under ANSI on.
fn rejects_unanchored_string_pair(a: OperandRole, b: OperandRole, ansi_mode: bool) -> bool {
    ansi_mode && unanchored_string_pair(a, b)
}

/// The numeric offset Spark accepts for a `DATE` in `+`/`-`: `DateAdd`/`DateSub` take an
/// `INT` (`IntegerType | ShortType | ByteType`), so only integrals that fit losslessly in an
/// `INT` qualify. A `BIGINT`, `FLOAT`, `DOUBLE` or `DECIMAL` offset is rejected at analysis
/// (Sail would otherwise silently truncate it). Spark has no unsigned types, but Arrow-native
/// sources can, so `UInt8`/`UInt16` (both within `INT` range) are accepted too.
fn is_date_offset_numeric(data_type: &DataType) -> bool {
    // `DateAdd`/`DateSub` take `TypeCollection(IntegerType, ShortType, ByteType)`
    // (`datetimeExpressions.scala:331,371`) and are `ExpectsInputTypes`, not
    // `ImplicitCastInputTypes`, so a BIGINT offset is refused -- and so is `UInt32`, which Spark's
    // Parquet reader widens to BIGINT. This used to accept both, because several functions were
    // typed BIGINT here where Spark types them INT (`datediff`, `date_diff`, `date - date`,
    // `regexp_count`, `regexp_instr`), and narrowing would have refused `DATE + regexp_count(...)`,
    // which Spark answers. They all carry Spark's type now, so this is `DateAdd`'s own accept set;
    // `arithmetic_derived_operand.feature` is the guard against one of them drifting back.
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::UInt8 | DataType::UInt16
    )
}

/// Spark promotes a STRING operand of `+`, `-`, `*` and `%` to a number before the operator ever
/// sees it, and the two ANSI modes do it DIFFERENTLY -- the same `'2' + 1` is a DOUBLE with ANSI off
/// and a BIGINT with it on:
///
/// * ANSI off, `StringPromotionTypeCoercion.scala`: a string beside anything but an interval is cast
///   to DOUBLE, alone. Two strings both go, and so does a string beside an untyped `NULL`.
/// * ANSI on, `AnsiStringPromotionTypeCoercion.findWiderTypeForString`: BOTH operands are cast to
///   BIGINT when the partner is integral and to DOUBLE when it is fractional or decimal. A string
///   beside another string or a `NULL` is not promoted, which is why the guards still reject it.
///
/// The cast follows the mode too: ANSI off reads a malformed string as NULL (`try_cast`), ANSI on
/// raises. Without this Sail handed DataFusion a `Utf8` operand it cannot coerce, and refused every
/// one of these pairs -- queries Spark answers.
fn promote_string_operands(
    left: Expr,
    right: Expr,
    schema: &DFSchemaRef,
    ansi_mode: bool,
) -> (Expr, Expr) {
    use OperandRole::*;
    let (Ok(left_type), Ok(right_type)) = (left.get_type(schema), right.get_type(schema)) else {
        return (left, right);
    };
    let (left_role, right_role) = (operand_role(&left_type), operand_role(&right_type));
    if !ansi_mode {
        let promotes = |partner: OperandRole| matches!(partner, Numeric | Str | UntypedNull);
        let left = if left_role == Str && promotes(right_role) {
            try_cast(left, DataType::Float64)
        } else {
            left
        };
        let right = if right_role == Str && promotes(left_role) {
            try_cast(right, DataType::Float64)
        } else {
            right
        };
        return (left, right);
    }
    let target = match (left_role, right_role) {
        (Str, Numeric) => Some(&right_type),
        (Numeric, Str) => Some(&left_type),
        _ => None,
    };
    // Spark reads an unsigned 64-bit Parquet column as DECIMAL(20,0), not an integral type, so the
    // pair goes to DOUBLE.
    match target {
        Some(numeric) if numeric.is_integer() && numeric != &DataType::UInt64 => {
            (cast(left, DataType::Int64), cast(right, DataType::Int64))
        }
        Some(_) => (
            cast(left, DataType::Float64),
            cast(right, DataType::Float64),
        ),
        None => (left, right),
    }
}

/// A string subtracted with a datetime is read AS that datetime -- but only in the cases Spark
/// resolves, which are not symmetric across ANSI modes:
///
/// * ANSI on, `AnsiStringPromotionTypeCoercion.findWiderTypeForString` (`(StringType, AtomicType)
///   => the atomic type`): the string becomes the DATE, TIMESTAMP, TIMESTAMP_NTZ or TIME beside it,
///   in either operand order, and the pair is subtracted as two of those.
/// * ANSI off: only `string - date` survives, through `SubtractDates`, whose implicit cast reads the
///   string as a DATE (`BinaryArithmeticWithDatetimeResolver.scala:142`). `date - string` becomes a
///   `DateSub` that wants an INT, and a timestamp or TIME pair is rejected -- measured on the JVM.
fn promote_string_beside_datetime(
    left: Expr,
    right: Expr,
    schema: &DFSchemaRef,
    session_timezone: &Arc<str>,
    ansi_mode: bool,
) -> PlanResult<(Expr, Expr)> {
    use OperandRole::*;
    let (Ok(left_type), Ok(right_type)) = (left.get_type(schema), right.get_type(schema)) else {
        return Ok((left, right));
    };
    let as_datetime = |string: Expr, datetime: &DataType| -> PlanResult<Expr> {
        Ok(match datetime {
            DataType::Date32 => ScalarUDF::from(SparkDate::new(!ansi_mode)).call(vec![string]),
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                Arc::new(ScalarUDF::from(SparkTimestamp::try_new(
                    tz.as_ref().map(|_| Arc::clone(session_timezone)),
                    ansi_mode,
                    false,
                )?))
                .call(vec![string])
            }
            other => cast(string, other.clone()),
        })
    };
    let (left_role, right_role) = (operand_role(&left_type), operand_role(&right_type));
    match (left_role, right_role) {
        (Str, Date) => Ok((as_datetime(left, &right_type)?, right)),
        (Str, Timestamp | Time) if ansi_mode => Ok((as_datetime(left, &right_type)?, right)),
        (Date | Timestamp | Time, Str) if ansi_mode => Ok((left, as_datetime(right, &left_type)?)),
        _ => Ok((left, right)),
    }
}

/// `TimeAddInterval` and `SubtractTimes` are `TimeExpression`s, which refuse the TIME type when
/// `spark.sql.timeType.enabled` is off (`timeExpressions.scala:41-47`). A TIME literal is not gated
/// on its own, so the arithmetic that would reach those expressions is.
fn rejects_time_operand_when_disabled(
    left: &Expr,
    right: &Expr,
    schema: &DFSchemaRef,
    time_type_enabled: bool,
) -> Option<PlanError> {
    let is_time = |e: &Expr| {
        matches!(
            e.get_type(schema),
            Ok(DataType::Time32(_) | DataType::Time64(_))
        )
    };
    (!time_type_enabled && (is_time(left) || is_time(right))).then(|| {
        PlanError::analysis("[UNSUPPORTED_TIME_TYPE] The data type TIME is not supported.")
    })
}

/// The number an interval is scaled by. `MultiplyYMInterval`, `MultiplyDTInterval` and their
/// divisions take a `NumericType` (`intervalExpressions.scala:605,658,745,828`), and
/// `MultiplyInterval`/`DivideInterval` a `DoubleType` (`:181`), all through implicit casts, so a
/// STRING arrives as `Cast(s, DoubleType)`, which reads a malformed string as NULL with ANSI off.
/// The scaling UDFs coerce a string with DataFusion's strict cast, which raises instead.
fn interval_scale_number(number: Expr, number_type: &DataType, ansi_mode: bool) -> Expr {
    if !ansi_mode
        && matches!(
            number_type,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        )
    {
        try_cast(number, DataType::Float64)
    } else {
        number
    }
}

/// Whether an operand is `substr`/`substring`/`left`/`overlay` over a BINARY. Spark keeps that result
/// a BINARY (`stringExpressions.scala:1000-1010,2301-2313,2408`), which is not an arithmetic operand,
/// so the pair is refused at analysis.
///
/// TODO: Sail reads that input as a STRING, because most of its string functions do not take a
///   BINARY yet and a BINARY result broke them downstream (`trim(substr(b, 2))`). The operand is
///   recognised by shape meanwhile, so a column a subquery projects from it is still a STRING.
///
/// Only the casts Sail inserts are looked through: `substr`/`substring` read their input through one
/// cast and return through another (`string.rs`), while `left` and `overlay` add none. A cast the user
/// writes around the input or the result is a STRING (`stringExpressions.scala:2309`), an operand.
fn is_binary_string_function(expr: &Expr, schema: &DFSchemaRef) -> bool {
    fn peel_alias(expr: &Expr) -> &Expr {
        match expr {
            Expr::Alias(alias) => peel_alias(&alias.expr),
            _ => expr,
        }
    }
    let is_binary = |expr: &Expr| {
        matches!(
            expr.get_type(schema),
            Ok(DataType::Binary | DataType::LargeBinary | DataType::BinaryView)
        )
    };
    match peel_alias(expr) {
        Expr::Cast(output) => match output.expr.as_ref() {
            Expr::ScalarFunction(function)
                if matches!(function.func.name(), "substr" | "substring") =>
            {
                matches!(function.args.first(), Some(Expr::Cast(input)) if is_binary(&input.expr))
            }
            _ => false,
        },
        Expr::ScalarFunction(function) if matches!(function.func.name(), "left" | "overlay") => {
            function.args.first().is_some_and(is_binary)
        }
        _ => false,
    }
}

fn rejects_binary_string_operand(
    op: &str,
    left: &Expr,
    right: &Expr,
    schema: &DFSchemaRef,
) -> Option<PlanError> {
    let name = |expr: &Expr| {
        if is_binary_string_function(expr, schema) {
            "BINARY".to_string()
        } else {
            expr.get_type(schema)
                .map_or_else(|_| "UNKNOWN".to_string(), |t| spark_type_name(&t))
        }
    };
    (is_binary_string_function(left, schema) || is_binary_string_function(right, schema)).then(
        || {
            PlanError::analysis(format!(
                "cannot resolve arithmetic '{op}' with operand types {} and {}",
                name(left),
                name(right)
            ))
        },
    )
}

/// What a bare `NULL` becomes when it sits next to a datetime.
enum NullPartner {
    /// `Add` casts it to a day-time interval, whatever the datetime is. A DATE partner is promoted
    /// to a timestamp in the session time zone along with it, for the reason given below.
    DayTimeInterval(Arc<str>),
    /// `Subtract` casts it to the other operand's own type.
    OtherOperand,
}

/// Spark never leaves a bare `NULL` as `NullType` beside a datetime, and the cast it inserts
/// decides the whole arm that follows: `Add` casts the NULL side to a day-time interval
/// (`BinaryArithmeticWithDatetimeResolver.scala:88,91`), so `DATE + NULL` is a date plus an
/// interval and yields a TIMESTAMP (`:69`), while `Subtract` casts it to the other operand's own
/// type (`:119,121`), so `DATE - NULL` is `date - date` and yields an `INTERVAL DAY` (`:142`).
/// Without this Sail hands DataFusion a `Null` operand it cannot coerce, and refuses twelve pairs
/// Spark answers. The same rule covers an interval partner, which Sail already handles.
fn cast_untyped_null_beside_datetime(
    left: Expr,
    right: Expr,
    schema: &DFSchemaRef,
    partner: NullPartner,
) -> (Expr, Expr) {
    let is_datetime = |data_type: &DataType| {
        matches!(
            data_type,
            DataType::Date32
                | DataType::Date64
                | DataType::Timestamp(_, _)
                | DataType::Time32(_)
                | DataType::Time64(_)
        )
    };
    let (Ok(left_type), Ok(right_type)) = (left.get_type(schema), right.get_type(schema)) else {
        return (left, right);
    };
    // Spark's `+` splits on the interval's declared fields: a DAY-to-DAY interval keeps the DATE
    // (`:68`) and anything wider promotes it to a TIMESTAMP (`:69`). Sail cannot tell the two
    // apart -- `Duration(Microsecond)` is the only day-time spelling it has -- but here it does
    // not have to: the interval is one the resolver itself inserted, and it is always
    // `DayTimeIntervalType.DEFAULT`, DAY TO SECOND. So the wide branch is the certain one, and
    // the DATE is promoted with it. A user-written `DATE + INTERVAL '2' DAY` is untouched.
    let datetime = |expr: Expr, data_type: &DataType| match (&partner, data_type) {
        (NullPartner::DayTimeInterval(timezone), DataType::Date32 | DataType::Date64) => cast(
            expr,
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::clone(timezone))),
        ),
        _ => expr,
    };
    let null = |expr: Expr, data_type: &DataType| match &partner {
        NullPartner::DayTimeInterval(_) => cast(expr, DataType::Duration(TimeUnit::Microsecond)),
        NullPartner::OtherOperand => cast(expr, data_type.clone()),
    };
    match (&left_type, &right_type) {
        (DataType::Null, data_type) if is_datetime(data_type) => {
            (null(left, data_type), datetime(right, data_type))
        }
        (data_type, DataType::Null) if is_datetime(data_type) => {
            (datetime(left, data_type), null(right, data_type))
        }
        _ => (left, right),
    }
}

/// Whether Spark rejects this operand pair for `*` (`Multiply`) at analysis. `*` accepts only
/// numeric×numeric and interval×numeric (either order, with string→numeric coercion); a
/// datetime, boolean, binary, or interval×interval pair is rejected. Left to DataFusion, several
/// of these reinterpret an operand as raw integer and compute a meaningless product.
fn rejects_multiply(left: &DataType, right: &DataType, ansi_mode: bool) -> bool {
    use OperandRole::*;
    let (a, b) = (operand_role(left), operand_role(right));
    if let Some(verdict) = framing_verdict(a, b) {
        return verdict;
    }
    if matches!(a, Date | Timestamp | Time) || matches!(b, Date | Timestamp | Time) {
        return true;
    }
    let is_interval = |r: OperandRole| matches!(r, IntervalDt | IntervalYm | IntervalCalendar);
    if is_interval(a) && is_interval(b) {
        return true;
    }
    // Exactly one operand is an interval. `framing_verdict` already removed `Unsupported` and
    // `Other`, the datetime roles were rejected above, and the both-interval case was handled, so
    // the peer can only be `Numeric`, `Str` or `UntypedNull` -- every one of which Spark accepts,
    // because the resolver rewrites `interval * number` to `Multiply*Interval`
    // (`BinaryArithmeticWithDatetimeResolver.scala:149-154`). Nothing left to reject.
    if is_interval(a) || is_interval(b) {
        return false;
    }
    rejects_unanchored_string_pair(a, b, ansi_mode)
}

/// Whether Spark rejects this operand pair for `+` (`Add`) at analysis. `Add` is commutative, so
/// the accept rule is symmetric: numeric×numeric, date/timestamp ± interval, date + INT offset,
/// same-class interval±interval, and the datetime/string forms Spark's datetime resolver allows.
fn rejects_add(left: &DataType, right: &DataType, ansi_mode: bool) -> bool {
    use OperandRole::*;
    let (a, b) = (operand_role(left), operand_role(right));
    if let Some(verdict) = framing_verdict(a, b) {
        return verdict;
    }
    let numlike = |r: OperandRole| matches!(r, Numeric | Str | UntypedNull);
    if numlike(a) && numlike(b) {
        return rejects_unanchored_string_pair(a, b, ansi_mode);
    }
    // A DATE takes an interval, an untyped NULL, or an INT-width numeric offset (`DateAdd`, whose
    // `days` input is `IntegerType | ShortType | ByteType`); a wider integral, a string, another
    // date or a timestamp is rejected.
    if a == Date || b == Date {
        let (other, other_type) = if a == Date { (b, right) } else { (a, left) };
        return match other {
            UntypedNull | IntervalDt | IntervalYm | IntervalCalendar => false,
            Numeric => !is_date_offset_numeric(other_type),
            _ => true,
        };
    }
    // A TIMESTAMP takes only an interval or an untyped NULL (no numeric offset).
    if a == Timestamp || b == Timestamp {
        let other = if a == Timestamp { b } else { a };
        return !matches!(
            other,
            IntervalDt | IntervalYm | UntypedNull | IntervalCalendar
        );
    }
    // A TIME takes only a day-time interval (`TimeAddInterval`, whose `inputTypes` are
    // `(AnyTimeType, DayTimeIntervalType)`) or an untyped NULL, which the datetime resolver casts
    // to the default day-time interval (`TimeType` is a `DatetimeType`, so it reaches that arm).
    // Another TIME, a year-month interval, a numeric or a string leaves `Add` with its own
    // `inputType` (`NumericAndInterval`), which rejects them.
    if a == Time || b == Time {
        let other = if a == Time { b } else { a };
        return !matches!(other, IntervalDt | UntypedNull);
    }
    // Both operands are intervals (date/timestamp handled above): same class only, except a
    // day-time interval also accepts a string or untyped NULL peer (Spark's day-time resolver).
    match (a, b) {
        (IntervalDt, IntervalDt)
        | (IntervalYm, IntervalYm)
        | (IntervalCalendar, IntervalCalendar) => false,
        (
            IntervalDt | IntervalYm | IntervalCalendar,
            IntervalDt | IntervalYm | IntervalCalendar,
        ) => true,
        _ => {
            let (interval, other) = if matches!(a, IntervalDt | IntervalYm | IntervalCalendar) {
                (a, b)
            } else {
                (b, a)
            };
            match other {
                UntypedNull => false,
                Str => !matches!(interval, IntervalDt | IntervalCalendar),
                _ => true,
            }
        }
    }
}

/// Whether Spark rejects this operand pair for `-` (`Subtract`) at analysis. Unlike `+`,
/// subtraction is NOT commutative — `date - date` yields an interval but `numeric - date` never
/// resolves, `str - date` is accepted while `date - str` needs ANSI — so the accept set is an
/// ordered table validated cell-by-cell against Spark 4.2.0. Left to DataFusion, the rejected
/// pairs reinterpret an operand as raw integer and compute a meaningless difference.
fn rejects_subtract(left: &DataType, right: &DataType, ansi_mode: bool) -> bool {
    use OperandRole::*;
    let (a, b) = (operand_role(left), operand_role(right));
    if let Some(verdict) = framing_verdict(a, b) {
        return verdict;
    }
    // `date - <INT offset>` (`DateSub`); `<numeric> - date` never resolves and falls through.
    if a == Date && b == Numeric {
        return !is_date_offset_numeric(right);
    }
    let accepted = matches!(
        (a, b),
        (Date, Date)
            | (Date, IntervalDt)
            | (Date, IntervalYm)
            | (Date, Timestamp)
            | (Date, UntypedNull)
            | (IntervalDt, IntervalDt)
            | (IntervalDt, UntypedNull)
            | (IntervalYm, IntervalYm)
            | (IntervalYm, UntypedNull)
            | (Numeric, Numeric)
            | (Numeric, Str)
            | (Numeric, UntypedNull)
            | (Str, Date)
            | (Str, IntervalDt)
            | (Str, Numeric)
            | (Timestamp, Date)
            | (Timestamp, IntervalDt)
            | (Timestamp, IntervalYm)
            | (Timestamp, Timestamp)
            | (Timestamp, UntypedNull)
            | (UntypedNull, Date)
            | (UntypedNull, IntervalDt)
            | (UntypedNull, IntervalYm)
            | (UntypedNull, Numeric)
            | (UntypedNull, Timestamp)
            | (UntypedNull, UntypedNull)
            // `TimeAddInterval` with a negated interval, `SubtractTimes` (`interval hour to
            // second`), and the untyped-NULL arms of the datetime resolver. `interval - time` is
            // absent on purpose: subtraction is not commutative and Spark has no such arm.
            | (Time, IntervalDt)
            | (Time, Time)
            // The calendar interval mirrors the day-time pairs except against a TIME, which
            // `TimeAddInterval` (`(AnyTimeType, DayTimeIntervalType)`) does not accept.
            | (Date, IntervalCalendar)
            | (IntervalCalendar, IntervalCalendar)
            | (IntervalCalendar, UntypedNull)
            | (Str, IntervalCalendar)
            | (Timestamp, IntervalCalendar)
            | (UntypedNull, IntervalCalendar)
            | (Time, UntypedNull)
            | (UntypedNull, Time)
    ) || (!ansi_mode && unanchored_string_pair(a, b))
        || (ansi_mode
            && matches!(
                (a, b),
                (Date, Str) | (Str, Timestamp) | (Timestamp, Str) | (Str, Time) | (Time, Str)
            ));
    !accepted
}

/// A `/` dividend Spark rejects: not numeric and not an interval it could scale
/// (booleans, dates, times, timestamps and binary are reinterpreted as raw integers by
/// DataFusion, producing a meaningless quotient — or, for `time`, an unsupported-kernel
/// error at execution where Spark rejects at analysis).
fn rejects_as_divide_dividend(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
            // Container types (and VARIANT, which is stored as a struct). Spark rejects them
            // at ANALYSIS with DATATYPE_MISMATCH for every arithmetic operator, while `/`
            // otherwise lets them fall through to a `Float64` cast that fails in the EXECUTOR
            // — a runtime error where Spark has an analysis one, so a never-evaluated row
            // changes the outcome. `Dictionary` and `RunEndEncoded` are deliberately absent:
            // they wrap a value type that may well be numeric.
            | DataType::Struct(_)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _)
    )
}

/// A `/` divisor Spark rejects: everything a dividend rejects, plus intervals/durations —
/// Spark has no "number / interval", so DataFusion dividing by the interval's raw nanos is
/// a silent wrong value.
fn rejects_as_divide_divisor(data_type: &DataType) -> bool {
    rejects_as_divide_dividend(data_type)
        || matches!(data_type, DataType::Interval(_) | DataType::Duration(_))
}

/// Spark rejects a UDT operand for every arithmetic operator: a UDT is none of the input types
/// the five operators accept (`Expression.scala:840-857`), whatever it is stored as. Sail keeps
/// UDT identity in the field metadata rather than in the `DataType`, so this is the one operand
/// check [`operand_role`] cannot make -- it would judge the storage type underneath instead.
fn rejects_udt_operand(
    op: &str,
    left: &Expr,
    right: &Expr,
    schema: &DFSchemaRef,
) -> Option<PlanError> {
    let (left_udt, right_udt) = (
        operand_udt_field(left, schema),
        operand_udt_field(right, schema),
    );
    if left_udt.is_none() && right_udt.is_none() {
        return None;
    }
    let name = |expr: &Expr, udt: Option<FieldRef>| {
        udt.or_else(|| expr.to_field(schema).ok().map(|(_, field)| field))
            .map_or_else(
                || "UNKNOWN".to_string(),
                |field| spark_field_type_name(&field),
            )
    };
    Some(PlanError::analysis(format!(
        "cannot resolve arithmetic '{op}' with operand types {} and {}",
        name(left, left_udt),
        name(right, right_udt)
    )))
}

/// The UDT field an arithmetic operand evaluates to. Only a column (or a struct field of one)
/// carries the UDT metadata on its own field; the expressions that return one of their inputs
/// unchanged -- `coalesce`/`nvl`, `nvl2`, `nullif`, `CASE`/`if`, and an array or map element
/// access -- build their result field without it, so they are looked through to the value they
/// return.
fn operand_udt_field(expr: &Expr, schema: &DFSchemaRef) -> Option<FieldRef> {
    // A cast yields its target type, never a UDT. DataFusion copies the source field's metadata
    // onto the cast's output field, so without this stop `CAST(udt AS STRING)` would still read
    // as a UDT and a query Spark resolves would be rejected.
    // An alias yields its child's type (`namedExpressions.scala:170`), but its field inherits the
    // child's metadata, so a cast under it is looked through before that metadata is read.
    match expr {
        Expr::Cast(_) | Expr::TryCast(_) => return None,
        Expr::Alias(alias) => return operand_udt_field(&alias.expr, schema),
        _ => {}
    }
    if let Ok((_, field)) = expr.to_field(schema)
        && is_spark_udt_field(&field)
    {
        return Some(field);
    }
    match expr {
        Expr::Case(case) => case
            .when_then_expr
            .iter()
            .map(|(_, then)| then.as_ref())
            .chain(case.else_expr.as_deref())
            .find_map(|branch| operand_udt_field(branch, schema)),
        // An aggregate that returns one of the values it aggregates keeps their type, as a plain
        // aggregate or over a window.
        Expr::AggregateFunction(function) if returns_its_input(function.func.name()) => function
            .params
            .args
            .first()
            .and_then(|arg| operand_udt_field(arg, schema)),
        Expr::WindowFunction(function) => match &function.fun {
            WindowFunctionDefinition::AggregateUDF(udf) if returns_its_input(udf.name()) => {
                function
                    .params
                    .args
                    .first()
                    .and_then(|arg| operand_udt_field(arg, schema))
            }
            _ => None,
        },
        Expr::ScalarFunction(function) => match function.func.name() {
            "coalesce" | "nvl" | "greatest" | "least" => function
                .args
                .iter()
                .find_map(|arg| operand_udt_field(arg, schema)),
            // `nvl2(x, y, z)` returns `y` or `z`.
            "nvl2" => function
                .args
                .iter()
                .skip(1)
                .find_map(|arg| operand_udt_field(arg, schema)),
            // `nullif(a, b)` returns `a` or NULL.
            "nullif" => function
                .args
                .first()
                .and_then(|arg| operand_udt_field(arg, schema)),
            // `abs(a)` keeps the type of `a`.
            "spark_abs" => function
                .args
                .first()
                .and_then(|arg| operand_udt_field(arg, schema)),
            // `named_struct('x', a).x` is `a`.
            "get_field" => struct_field_udt_field(&function.args, schema),
            // `explode(arr)` yields the array's elements, and `explode(map)` is not a UDT operand.
            "array_element" | "explode" | "explode_outer" | "array_min" | "array_max" => function
                .args
                .first()
                .and_then(|collection| collection_element_udt_field(collection, schema)),
            _ => None,
        },
        _ => None,
    }
}

/// The UDT element field of the arrays an array holds, for `flatten`.
fn nested_collection_element_udt_field(
    collection: &Expr,
    schema: &DFSchemaRef,
) -> Option<FieldRef> {
    match collection {
        Expr::Cast(cast) if casts_only_nullability(&cast.expr, cast.field.data_type(), schema) => {
            nested_collection_element_udt_field(&cast.expr, schema)
        }
        Expr::Case(case) => case_branches(case)
            .find_map(|branch| nested_collection_element_udt_field(branch, schema)),
        Expr::ScalarFunction(function)
            if matches!(function.func.name(), "array" | "make_array" | "spark_array") =>
        {
            function
                .args
                .iter()
                .find_map(|array| collection_element_udt_field(array, schema))
        }
        // The values of a map, or the value a map index pulls out, when they are collections.
        Expr::ScalarFunction(function)
            if matches!(function.func.name(), "map_values" | "map_extract") =>
        {
            function
                .args
                .first()
                .and_then(|map| map_value_collection_element_udt_field(map, schema))
        }
        _ => match collection.get_type(schema).ok()? {
            DataType::List(outer) | DataType::LargeList(outer) => match outer.data_type() {
                DataType::List(inner) | DataType::LargeList(inner) => {
                    Some(Arc::clone(inner)).filter(|field| is_spark_udt_field(field))
                }
                _ => None,
            },
            _ => None,
        },
    }
}

/// The UDT value field of a map, looking through `map(...)`, which the resolver builds with
/// `map_from_arrays` over arrays built in place.
fn map_value_udt_field(map: &Expr, schema: &DFSchemaRef) -> Option<FieldRef> {
    match map {
        Expr::Cast(cast) if casts_only_nullability(&cast.expr, cast.field.data_type(), schema) => {
            map_value_udt_field(&cast.expr, schema)
        }
        // `map_concat` is a `CASE` that yields NULL or the concatenated map.
        Expr::Case(case) => {
            case_branches(case).find_map(|branch| map_value_udt_field(branch, schema))
        }
        Expr::ScalarFunction(function) if function.func.name() == "map_from_arrays" => function
            .args
            .get(1)
            .and_then(|values| collection_element_udt_field(values, schema)),
        _ => match map.get_type(schema).ok()? {
            DataType::Map(entries, _) => match entries.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => {
                    Some(Arc::clone(&fields[1])).filter(|field| is_spark_udt_field(field))
                }
                _ => None,
            },
            _ => None,
        },
    }
}

/// The UDT key field of a map, looking through `map(...)` the way `map_value_udt_field` does.
fn map_key_udt_field(map: &Expr, schema: &DFSchemaRef) -> Option<FieldRef> {
    match map {
        Expr::Cast(cast) if casts_only_nullability(&cast.expr, cast.field.data_type(), schema) => {
            map_key_udt_field(&cast.expr, schema)
        }
        Expr::Case(case) => {
            case_branches(case).find_map(|branch| map_key_udt_field(branch, schema))
        }
        Expr::ScalarFunction(function) if function.func.name() == "map_from_arrays" => function
            .args
            .first()
            .and_then(|keys| collection_element_udt_field(keys, schema)),
        _ => match map.get_type(schema).ok()? {
            DataType::Map(entries, _) => match entries.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => {
                    Some(Arc::clone(&fields[0])).filter(|field| is_spark_udt_field(field))
                }
                _ => None,
            },
            _ => None,
        },
    }
}

/// The UDT element field of the collections a map holds as values.
fn map_value_collection_element_udt_field(map: &Expr, schema: &DFSchemaRef) -> Option<FieldRef> {
    match map {
        Expr::Cast(cast) if casts_only_nullability(&cast.expr, cast.field.data_type(), schema) => {
            map_value_collection_element_udt_field(&cast.expr, schema)
        }
        Expr::Case(case) => case_branches(case)
            .find_map(|branch| map_value_collection_element_udt_field(branch, schema)),
        Expr::ScalarFunction(function) if function.func.name() == "map_from_arrays" => function
            .args
            .get(1)
            .and_then(|values| nested_collection_element_udt_field(values, schema)),
        _ => match map.get_type(schema).ok()? {
            DataType::Map(entries, _) => match entries.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => match fields[1].data_type() {
                    DataType::List(element) | DataType::LargeList(element) => {
                        Some(Arc::clone(element)).filter(|field| is_spark_udt_field(field))
                    }
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        },
    }
}

/// The value `array_repeat` repeats, under the `CASE WHEN true THEN CAST(...) END` the resolver
/// builds around it. The cast keeps the value's own type, so it is looked through; a user cast of a
/// UDT to its storage type is refused by Spark at the cast (`Cast.canCast`), so refusing it here
/// refuses nothing Spark accepts.
fn peel_repeated_value<'a>(value: &'a Expr, schema: &DFSchemaRef) -> &'a Expr {
    match value {
        Expr::Case(case)
            if case.expr.is_none()
                && case.else_expr.is_none()
                && matches!(
                    case.when_then_expr.as_slice(),
                    [(when, _)] if matches!(when.as_ref(), Expr::Literal(ScalarValue::Boolean(Some(true)), _))
                ) =>
        {
            case.when_then_expr
                .first()
                .map_or(value, |(_, then)| peel_repeated_value(then, schema))
        }
        Expr::Cast(cast) if casts_only_nullability(&cast.expr, cast.field.data_type(), schema) => {
            peel_repeated_value(&cast.expr, schema)
        }
        _ => value,
    }
}

/// The branches a `CASE` can yield.
fn case_branches(case: &datafusion_expr::expr::Case) -> impl Iterator<Item = &Expr> {
    case.when_then_expr
        .iter()
        .map(|(_, then)| then.as_ref())
        .chain(case.else_expr.as_deref())
}

/// Whether casting `expr` to `target` changes nothing but the nullability of nested fields.
fn casts_only_nullability(expr: &Expr, target: &DataType, schema: &DFSchemaRef) -> bool {
    expr.get_type(schema)
        .is_ok_and(|source| same_type_ignoring_nullability(&source, target))
}

fn same_type_ignoring_nullability(left: &DataType, right: &DataType) -> bool {
    match (left, right) {
        (DataType::List(l), DataType::List(r))
        | (DataType::LargeList(l), DataType::LargeList(r))
        | (DataType::Map(l, _), DataType::Map(r, _)) => {
            same_type_ignoring_nullability(l.data_type(), r.data_type())
        }
        (DataType::Struct(l), DataType::Struct(r)) => {
            l.len() == r.len()
                && l.iter().zip(r.iter()).all(|(l, r)| {
                    l.name() == r.name()
                        && same_type_ignoring_nullability(l.data_type(), r.data_type())
                })
        }
        _ => left == right,
    }
}

/// Whether an aggregate returns one of the values it aggregates, so its type is theirs
/// (`Min`, `Max`, `First`, `Last`, `MaxMinBy` and `Mode` all declare `dataType = child.dataType`).
fn returns_its_input(name: &str) -> bool {
    matches!(
        name,
        "min" | "max" | "first_value" | "last_value" | "any_value" | "max_by" | "min_by" | "mode"
    )
}

/// The UDT field `get_field` reads out of a struct built in place with `named_struct`, whose own
/// field does not carry the UDT metadata of the value it was built from.
fn struct_field_udt_field(args: &[Expr], schema: &DFSchemaRef) -> Option<FieldRef> {
    let [base, Expr::Literal(ScalarValue::Utf8(Some(name)), _)] = args else {
        return None;
    };
    named_struct_field_udt_field(base, name, schema)
}

/// The UDT field `name` of a struct built in place with `named_struct`, reached directly, through
/// the `CASE` of an array index, or as an element of an array built in place.
fn named_struct_field_udt_field(base: &Expr, name: &str, schema: &DFSchemaRef) -> Option<FieldRef> {
    match base {
        Expr::ScalarFunction(function) if function.func.name() == "named_struct" => {
            function.args.chunks(2).find_map(|pair| match pair {
                [Expr::Literal(ScalarValue::Utf8(Some(key)), _), value] if key == name => {
                    operand_udt_field(value, schema)
                }
                _ => None,
            })
        }
        Expr::Case(case) => case_branches(case)
            .find_map(|branch| named_struct_field_udt_field(branch, name, schema)),
        Expr::Cast(cast) if casts_only_nullability(&cast.expr, cast.field.data_type(), schema) => {
            named_struct_field_udt_field(&cast.expr, name, schema)
        }
        Expr::ScalarFunction(function) if function.func.name() == "array_element" => function
            .args
            .first()
            .and_then(|array| element_struct_field_udt_field(array, name, schema)),
        _ => None,
    }
}

fn element_struct_field_udt_field(
    array: &Expr,
    name: &str,
    schema: &DFSchemaRef,
) -> Option<FieldRef> {
    match array {
        Expr::ScalarFunction(function)
            if matches!(function.func.name(), "array" | "make_array" | "spark_array") =>
        {
            function
                .args
                .iter()
                .find_map(|element| named_struct_field_udt_field(element, name, schema))
        }
        Expr::Cast(cast) if casts_only_nullability(&cast.expr, cast.field.data_type(), schema) => {
            element_struct_field_udt_field(&cast.expr, name, schema)
        }
        _ => None,
    }
}

/// The UDT element field of an array, or of the value list `map_extract` pulls out of a map. The
/// resolver builds both element fields through `resolve_field`, so they keep the UDT metadata; an
/// array built in place with `array(...)` does not, so its elements are looked at instead.
fn collection_element_udt_field(collection: &Expr, schema: &DFSchemaRef) -> Option<FieldRef> {
    match collection {
        // The resolver wraps some array functions in a cast that only changes nullability, which
        // drops the element metadata. A user cast of a UDT array never gets here: Spark refuses to
        // cast a UDT to its storage type (`Cast.canCast`).
        Expr::Cast(cast) if casts_only_nullability(&cast.expr, cast.field.data_type(), schema) => {
            return collection_element_udt_field(&cast.expr, schema);
        }
        // `flatten` is guarded by a `CASE` that yields it or NULL.
        Expr::Case(case) => {
            return case_branches(case)
                .find_map(|branch| collection_element_udt_field(branch, schema));
        }
        // The functions that return a subset or a reordering of their input array's elements.
        Expr::ScalarFunction(function)
            if matches!(
                function.func.name(),
                "array_slice" | "spark_reverse" | "array_sort" | "array_distinct"
            ) =>
        {
            return function
                .args
                .first()
                .and_then(|array| collection_element_udt_field(array, schema));
        }
        Expr::ScalarFunction(function)
            if matches!(function.func.name(), "spark_concat" | "array_concat") =>
        {
            return function
                .args
                .iter()
                .find_map(|array| collection_element_udt_field(array, schema));
        }
        Expr::ScalarFunction(function) if function.func.name() == "map_keys" => {
            return function
                .args
                .first()
                .and_then(|map| map_key_udt_field(map, schema));
        }
        // `array_repeat(a, n)` repeats `a`, which the resolver wraps in `CASE WHEN true THEN
        // CAST(a AS <its own type>) END`.
        Expr::ScalarFunction(function) if function.func.name() == "array_repeat" => {
            return function
                .args
                .first()
                .and_then(|value| operand_udt_field(peel_repeated_value(value, schema), schema));
        }
        // `arr[i]` whose elements are themselves collections yields their elements.
        Expr::ScalarFunction(function) if function.func.name() == "array_element" => {
            return function
                .args
                .first()
                .and_then(|outer| nested_collection_element_udt_field(outer, schema));
        }
        // `flatten(arr)` yields the elements of the arrays `arr` holds.
        Expr::ScalarFunction(function) if function.func.name() == "flatten" => {
            return function
                .args
                .first()
                .and_then(|array| nested_collection_element_udt_field(array, schema));
        }
        Expr::ScalarFunction(function) if function.func.name() == "map_values" => {
            return function
                .args
                .first()
                .and_then(|map| map_value_udt_field(map, schema));
        }
        Expr::HigherOrderFunction(function) if function.func.name() == "filter" => {
            return function
                .args
                .first()
                .and_then(|array| collection_element_udt_field(array, schema));
        }
        // `collect_list(a)` is an array of `a`, wrapped in `coalesce(..., [])` by the resolver.
        Expr::AggregateFunction(function) if function.func.name() == "array_agg" => {
            return function
                .params
                .args
                .first()
                .and_then(|arg| operand_udt_field(arg, schema));
        }
        Expr::ScalarFunction(function) if function.func.name() == "coalesce" => {
            return function
                .args
                .iter()
                .find_map(|arg| collection_element_udt_field(arg, schema));
        }
        // `transform(arr, x -> x)` keeps the elements when the lambda returns its own parameter;
        // the other higher-order functions keep the array's element field and need no help.
        Expr::HigherOrderFunction(function)
            if matches!(function.func.name(), "transform" | "array_transform") =>
        {
            if let [array, Expr::Lambda(lambda)] = function.args.as_slice()
                && let Expr::LambdaVariable(variable) = lambda.body.as_ref()
                && lambda.params.first() == Some(&variable.name)
            {
                return collection_element_udt_field(array, schema);
            }
        }
        _ => {}
    }
    if let Expr::ScalarFunction(function) = collection
        && matches!(function.func.name(), "array" | "make_array" | "spark_array")
    {
        return function
            .args
            .iter()
            .find_map(|arg| operand_udt_field(arg, schema));
    }
    if let Expr::ScalarFunction(function) = collection
        && function.func.name() == "map_extract"
    {
        return function
            .args
            .first()
            .and_then(|map| map_value_udt_field(map, schema));
    }
    match collection.get_type(schema).ok()? {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => Some(field).filter(|field| is_spark_udt_field(field)),
        _ => None,
    }
}

/// Spark's unary `+` and `-` take `NumericAndInterval` (`arithmetic.scala:54,124`), so a DATE,
/// TIMESTAMP, TIME, BOOLEAN, BINARY, container or UDT is refused at analysis. A STRING is not: string
/// promotion casts it to DOUBLE first (`AnsiStringPromotionTypeCoercion`:
/// `UnaryPositive(Cast(e, DoubleType))`), and an untyped NULL becomes a DOUBLE too.
fn rejects_unary_operand(op: &str, arg: &Expr, schema: &DFSchemaRef) -> Option<PlanError> {
    use OperandRole::*;
    let udt = operand_udt_field(arg, schema);
    let field = udt
        .clone()
        .or_else(|| arg.to_field(schema).ok().map(|(_, field)| field));
    let refused = udt.is_some()
        || is_binary_string_function(arg, schema)
        || field.as_ref().is_some_and(|field| {
            matches!(
                operand_role(field.data_type()),
                Unsupported | Date | Timestamp | Time
            )
        });
    refused.then(|| {
        let name = field.map_or_else(
            || "UNKNOWN".to_string(),
            |field| spark_field_type_name(&field),
        );
        PlanError::analysis(format!(
            "cannot resolve arithmetic unary '{op}' with operand type {name}"
        ))
    })
}

/// The plan-time rejection Spark raises at analysis for an arithmetic operand pair it cannot
/// resolve. The `cannot resolve` substring it shares with Spark's `Cannot resolve …` text is what
/// the `.feature` reject scenarios assert.
///
/// TODO: Spark picks the `DATATYPE_MISMATCH` subclass per expression -- `BINARY_OP_DIFF_TYPES`,
/// `BINARY_OP_WRONG_TYPE` or `UNEXPECTED_INPUT_TYPE` -- always with SQLSTATE `42K09`, plus the
/// rewritten expression text and query context. Emit them once Sail has structured analysis
/// errors; `arithmetic_error_metadata.feature` pins the gap.
/// Sail-internal field metadata that marks the INT day count `date - date` is typed with as what
/// Spark types it: `DayTimeIntervalType(DAY)` (`datetimeExpressions.scala:3616`). The `SAIL::`
/// prefix keeps it off the wire (`sail-spark-connect/src/schema.rs`).
const SAIL_DATE_DIFFERENCE_METADATA_KEY: &str = "SAIL::spark::date_difference";

/// Wraps the day count in a cast to its own type whose target field carries the marker. Only the
/// target field of this very cast is trusted: DataFusion hands the source metadata on through a
/// type-only cast, so a user's `CAST(d1 - d2 AS INT)` inherits it, and Spark accepts that INT.
fn mark_date_difference(day_count: Expr) -> Expr {
    let field = Field::new("", DataType::Int32, true).with_metadata(
        [(
            SAIL_DATE_DIFFERENCE_METADATA_KEY.to_string(),
            "true".to_string(),
        )]
        .into(),
    );
    Expr::Cast(expr::Cast::new_from_field(
        Box::new(day_count),
        Arc::new(field),
    ))
}

fn is_date_difference(expr: &Expr) -> bool {
    match expr {
        Expr::Alias(alias) => is_date_difference(&alias.expr),
        Expr::Cast(cast) => cast
            .field
            .metadata()
            .contains_key(SAIL_DATE_DIFFERENCE_METADATA_KEY),
        _ => false,
    }
}

/// Refuses an arithmetic whose operand is a date difference when Spark, which types it as an
/// INTERVAL DAY, refuses it. The operator is resolved again with a day-time interval (a typed NULL
/// `Duration`, Sail's spelling of one) in place of the difference, and only a refusal by the
/// arithmetic guards counts: that one is Spark's verdict for an interval operand. It never accepts
/// on that basis, so what the INT resolves today and Spark answers is left alone.
// TODO: remove once `date - date` is typed as an interval that keeps its field range
//  (PR #2350); then the operator sees Spark's type directly.
fn rejects_date_difference_operand(
    operator: fn(ScalarFunctionInput) -> PlanResult<Expr>,
    left: &Expr,
    right: &Expr,
    function_context: &FunctionContextInput,
) -> Option<PlanError> {
    let (left_is, right_is) = (is_date_difference(left), is_date_difference(right));
    if !left_is && !right_is {
        return None;
    }
    let interval = || Expr::Literal(ScalarValue::DurationMicrosecond(None), None);
    let arguments = vec![
        if left_is { interval() } else { left.clone() },
        if right_is { interval() } else { right.clone() },
    ];
    let input = ScalarFunctionInput {
        arguments,
        function_context: FunctionContextInput {
            argument_display_names: function_context.argument_display_names,
            plan_config: function_context.plan_config,
            session_context: function_context.session_context,
            schema: function_context.schema,
        },
    };
    match operator(input) {
        Err(PlanError::AnalysisError(message))
            if message.starts_with("cannot resolve arithmetic") =>
        {
            Some(PlanError::AnalysisError(message))
        }
        _ => None,
    }
}

fn arithmetic_operand_error(op: &str, left: &DataType, right: &DataType) -> PlanError {
    PlanError::analysis(format!(
        "cannot resolve arithmetic '{op}' with operand types {} and {}",
        spark_type_name(left),
        spark_type_name(right)
    ))
}
