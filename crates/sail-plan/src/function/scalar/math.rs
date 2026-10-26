use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit, i256};
use datafusion::arrow::error::ArrowError;
use datafusion::functions::expr_fn;
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::{
    BinaryExpr, Expr, ExprSchemable, Operator, ScalarUDF, cast, expr, lit, try_cast, when,
};
use datafusion_spark::function::math::expr_fn as math_fn;
use half::f16;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::error::generic_exec_err;
use sail_function::scalar::datetime::negate_duration::NegateDuration;
use sail_function::scalar::datetime::spark_interval::SparkDayTimeIntervalToCalendarInterval;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::math::rand_poisson::RandPoisson;
use sail_function::scalar::math::randn::Randn;
use sail_function::scalar::math::random::Random;
use sail_function::scalar::math::spark_abs::SparkAbs;
use sail_function::scalar::math::spark_bin::SparkBin;
use sail_function::scalar::math::spark_bround::SparkBRound;
use sail_function::scalar::math::spark_ceil_floor::{SparkCeil, SparkFloor};
use sail_function::scalar::math::spark_conv::SparkConv;
use sail_function::scalar::math::spark_div::{
    DIVIDE_BY_ZERO_MESSAGE, SparkIntegerDiv, SparkIntervalDiv,
};
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
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn add_day_time_interval_to_string(
    string: Expr,
    interval: Expr,
    string_type: DataType,
    session_timezone: Arc<str>,
    ansi_mode: bool,
) -> PlanResult<Expr> {
    let timestamp = ScalarUDF::from(SparkTimestamp::try_new(
        Some(session_timezone),
        ansi_mode,
        false,
    )?)
    .call(vec![string]);
    let calendar_interval =
        ScalarUDF::from(SparkDayTimeIntervalToCalendarInterval::new()).call(vec![interval]);
    let shifted = timestamp + calendar_interval;
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
        Ok(arguments.one()?)
    } else {
        let (left, right) = arguments.two()?;
        let (left_type, right_type) = (
            left.get_type(function_context.schema),
            right.get_type(function_context.schema),
        );
        Ok(match (left_type, right_type) {
            (
                Ok(string_type @ (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)),
                Ok(DataType::Duration(TimeUnit::Microsecond)),
            ) => add_day_time_interval_to_string(
                left,
                right,
                string_type,
                Arc::clone(&function_context.plan_config.session_timezone),
                function_context.plan_config.ansi_mode,
            )?,
            (
                Ok(DataType::Duration(TimeUnit::Microsecond)),
                Ok(string_type @ (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)),
            ) => add_day_time_interval_to_string(
                right,
                left,
                string_type,
                Arc::clone(&function_context.plan_config.session_timezone),
                function_context.plan_config.ansi_mode,
            )?,
            (Ok(DataType::Date32), Ok(DataType::Duration(TimeUnit::Microsecond))) => {
                left + cast(right, DataType::Interval(IntervalUnit::MonthDayNano))
            }
            (Ok(DataType::Duration(TimeUnit::Microsecond)), Ok(DataType::Date32)) => {
                cast(left, DataType::Interval(IntervalUnit::MonthDayNano)) + right
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
        Ok(spark_unary_negate(
            arg,
            function_context.plan_config.ansi_mode,
            function_context.schema,
        ))
    } else {
        let (left, right) = arguments.two()?;
        let (left_type, right_type) = (
            left.get_type(function_context.schema),
            right.get_type(function_context.schema),
        );
        Ok(match (left_type, right_type) {
            (Ok(DataType::Date32), Ok(DataType::Duration(TimeUnit::Microsecond))) => {
                left - cast(right, DataType::Interval(IntervalUnit::MonthDayNano))
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
    let (left_type, right_type) = (
        left.get_type(function_context.schema),
        right.get_type(function_context.schema),
    );
    Ok(match (left_type, right_type) {
        // TODO: Casting DataType::Interval(_) to DataType::Int64 is not supported yet.
        //  Seems to be a bug in DataFusion.
        (Ok(DataType::Duration(TimeUnit::Microsecond)), Ok(_)) => {
            // Match duration because we cast Spark's DayTime interval to Duration.
            cast(
                cast(left, DataType::Int64) * right,
                DataType::Duration(TimeUnit::Microsecond),
            )
        }
        (Ok(_), Ok(DataType::Duration(TimeUnit::Microsecond))) => {
            // Match duration because we cast Spark's DayTime interval to Duration.
            cast(
                left * cast(right, DataType::Int64),
                DataType::Duration(TimeUnit::Microsecond),
            )
        }
        // TODO: In case getting the type fails, we don't want to fail the query.
        //  Future work is needed here, ideally we create something like `Operator::SparkMultiply`.
        (Ok(_), Ok(_)) | (Err(_), _) | (_, Err(_)) => left * right,
    })
}

/// Check if an expression represents a zero literal value.
/// Handles both direct literals and CAST expressions wrapping literals.
fn is_zero_literal(expr: &Expr) -> bool {
    // Helper to check if a ScalarValue is zero
    fn is_scalar_zero(scalar: &ScalarValue) -> bool {
        match scalar {
            ScalarValue::Int8(Some(0))
            | ScalarValue::Int16(Some(0))
            | ScalarValue::Int32(Some(0))
            | ScalarValue::Int64(Some(0))
            | ScalarValue::UInt8(Some(0))
            | ScalarValue::UInt16(Some(0))
            | ScalarValue::UInt32(Some(0))
            | ScalarValue::UInt64(Some(0))
            | ScalarValue::Decimal128(Some(0), _, _) => true,
            ScalarValue::Float32(Some(v)) if *v == 0.0 => true,
            ScalarValue::Float64(Some(v)) if *v == 0.0 => true,
            ScalarValue::Float16(Some(f)) if *f == f16::from_f32(0.0) => true,
            ScalarValue::Decimal256(Some(v), _, _) if *v == i256::ZERO => true,
            _ => false,
        }
    }

    match expr {
        // Direct literal
        Expr::Literal(scalar, _) => is_scalar_zero(scalar),
        // CAST(literal AS type) - unwrap the cast and check the inner literal
        Expr::Cast(cast_expr) => {
            if let Expr::Literal(scalar, _) = cast_expr.expr.as_ref() {
                is_scalar_zero(scalar)
            } else {
                false
            }
        }
        // TryCast is similar to Cast
        Expr::TryCast(try_cast_expr) => {
            if let Expr::Literal(scalar, _) = try_cast_expr.expr.as_ref() {
                is_scalar_zero(scalar)
            } else {
                false
            }
        }
        _ => false,
    }
}

/// `Decimal128` only: the kernel behind `div` divides at 256-bit from a 128-bit pair, so a
/// `Decimal256` operand keeps the previous path rather than silently taking a narrower one.
fn is_spark_decimal(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Decimal128(_, _))
}

/// The four types `IntegralDivide.inputType` accepts (arithmetic.scala:890), checked on the pair.
fn accepts_div_input(data_type: &DataType) -> bool {
    data_type.is_integer()
        || matches!(
            data_type,
            DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::Duration(_)
                | DataType::Interval(IntervalUnit::YearMonth)
        )
}

/// The types Spark's coercion rules LEAVE BEHIND for a rejected `div` pair.
///
/// `BinaryOperator.checkInputDataTypes` (Expression.scala:842-856) runs after the coercion batch,
/// so both the subclass it picks and the names it prints are post-coercion: the types the user
/// wrote can appear nowhere in the message. Only the names are needed here — an accepted pair is
/// coerced for real further down — so this rebuilds the three rules that can fire before the
/// check and returns the resulting pair.
fn coerced_type_names_for_rejection(
    left: &DataType,
    right: &DataType,
    ansi_mode: bool,
) -> (DataType, DataType) {
    let is_string =
        |t: &DataType| matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View);
    let is_interval = |t: &DataType| matches!(t, DataType::Interval(_) | DataType::Duration(_));
    let is_float =
        |t: &DataType| matches!(t, DataType::Float16 | DataType::Float32 | DataType::Float64);
    // Spark's `FractionalType` covers every DECIMAL width, not just the one the kernel takes.
    let is_decimal = |t: &DataType| {
        matches!(
            t,
            DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
        )
    };
    // Spark's `AtomicType`: everything but the containers, and `NullType` is matched earlier.
    let is_atomic = |t: &DataType| {
        !is_interval(t)
            && !matches!(
                t,
                DataType::Null
                    | DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::ListView(_)
                    | DataType::LargeListView(_)
                    | DataType::FixedSizeList(_, _)
                    | DataType::Struct(_)
                    | DataType::Map(_, _)
                    | DataType::Union(_, _)
                    | DataType::Dictionary(_, _)
                    | DataType::RunEndEncoded(_, _)
            )
    };

    // `IntegralDivisionTypeCoercion.scala:34-37` widens a narrower integral to LONG whatever the
    // peer is, so an INT is reported as BIGINT even beside a DATE.
    let widen = |t: &DataType| match t {
        DataType::Int8 | DataType::Int16 | DataType::Int32 => DataType::Int64,
        other => other.clone(),
    };
    let (mut left, mut right) = (widen(left), widen(right));

    if ansi_mode {
        // `findWiderTypeForString` (AnsiStringPromotionTypeCoercion.scala:92-109) sends BOTH
        // sides to one type, or neither moves. The arm order is load-bearing: a DECIMAL is
        // `FractionalType`, so it goes to DOUBLE rather than staying DECIMAL.
        let wider = |string: &DataType, peer: &DataType| match peer {
            _ if peer.is_integer() => Some(DataType::Int64),
            _ if is_float(peer) || is_decimal(peer) => Some(DataType::Float64),
            DataType::Null => Some(string.clone()),
            _ if is_interval(peer) => None,
            _ if is_string(peer) => Some(string.clone()),
            _ if is_atomic(peer) => Some(peer.clone()),
            _ => None,
        };
        let promoted = match (is_string(&left), is_string(&right)) {
            (true, _) => wider(&left, &right),
            (_, true) => wider(&right, &left),
            (false, false) => None,
        };
        if let Some(promoted) = promoted {
            (left, right) = (promoted.clone(), promoted);
        }
    } else {
        // `StringPromotionTypeCoercion.scala:48-54` sends a string to DOUBLE on its own, one
        // side at a time, and skips it only when the peer is an interval.
        if is_string(&left) && !is_interval(&right) {
            left = DataType::Float64;
        }
        if is_string(&right) && !is_interval(&left) {
            right = DataType::Float64;
        }
    }

    // `DecimalPrecisionTypeCoercion.scala:182-185` sends the DECIMAL to DOUBLE and leaves the
    // float side alone, so a FLOAT peer is still reported as FLOAT.
    if is_decimal(&left) && is_float(&right) {
        left = DataType::Float64;
    } else if is_float(&left) && is_decimal(&right) {
        right = DataType::Float64;
    }
    (left, right)
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

    // Plan-time check for literal zero divisors (fast path, better error UX).
    if is_zero_literal(&divisor) {
        if function_context.plan_config.ansi_mode {
            return Err(PlanError::ArrowError(ArrowError::DivideByZero));
        } else {
            return Ok(Expr::Literal(ScalarValue::Null, None));
        }
    }

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

    // Spark's `wrongNumArgsError` (QueryCompilationErrors.scala:1009-1016); checked here because
    // `ItemTaker` would leak a Rust `Debug` rendering of the argument list instead.
    if arguments.len() != 2 {
        return Err(PlanError::analysis(format!(
            "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `div` requires 2 parameters but the actual \
             number is {}. Please, refer to \
             'https://spark.apache.org/docs/latest/sql-ref-functions.html' for a fix. \
             SQLSTATE: 42605",
            arguments.len()
        )));
    }
    let (dividend, divisor) = arguments.two()?;

    let ansi_mode = function_context.plan_config.ansi_mode;
    let dividend_type = dividend.get_type(function_context.schema);
    let divisor_type = divisor.get_type(function_context.schema);
    // Spark checks the types after ANSI-dependent coercion. Keep this rejection after coercion
    // too, so an unsupported pair cannot be hidden by a zero-divisor guard.
    // `BinaryOperator.checkInputDataTypes` (Expression.scala:842-856) tests `!sameType` BEFORE
    // `!inputType.acceptsType`, so a mismatched pair is DIFF_TYPES even when neither type is
    // accepted. Spark's parent template is `Cannot resolve <sqlExpr> due to data type mismatch:`
    // (error-conditions.json), so the clause carries the operand SQL — which the builder DOES
    // have, as `argument_display_names` (the same source the resolver uses to name the output
    // column, resolver/expression/function.rs:291).
    let reject = |left: Option<&DataType>, right: Option<&DataType>| {
        // Built here rather than above: `function_to_string` checks the arity of the DISPLAY
        // NAMES, which the arity guard above does not, so computing it eagerly would let a
        // caller that supplies no names fail a VALID `div` with a leaked `Debug` rendering.
        let formatter = function_context
            .session_context
            .extension::<PlanService>()?;
        let formatter = formatter.plan_formatter();
        let sql_expr = formatter.function_to_string(
            "div",
            function_context
                .argument_display_names
                .iter()
                .map(String::as_str)
                .collect(),
            false,
        )?;
        let coerced = match (left, right) {
            (Some(l), Some(r)) => Some(coerced_type_names_for_rejection(l, r, ansi_mode)),
            _ => None,
        };
        let (left, right) = match coerced.as_ref() {
            Some((l, r)) => (Some(l), Some(r)),
            None => (left, right),
        };
        let name = |t: Option<&DataType>| {
            t.and_then(|t| formatter.data_type_to_simple_string(t).ok())
                .map_or_else(
                    || "\"UNKNOWN\"".to_string(),
                    |t| format!("\"{}\"", t.to_uppercase()),
                )
        };
        // `sameType` ignores the storage width of a string, so Spark sees `Utf8` and `Utf8View`
        // as one STRING type where `==` would not. Defensive: SQL always resolves a string to
        // `Utf8`, so only a Connect literal can reach the other arms and no test covers them.
        let same_type = |l: &DataType, r: &DataType| {
            let normalize = |t: &DataType| match t {
                DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
                other => other.clone(),
            };
            normalize(l) == normalize(r)
        };
        let message = match (left, right) {
            (Some(l), Some(r)) if same_type(l, r) => format!(
                "[DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE] Cannot resolve \"{sql_expr}\" due to \
                 data type mismatch: the binary operator requires the input type (\"BIGINT\" or \
                 \"DECIMAL\" or \"INTERVAL YEAR TO MONTH\" or \"INTERVAL DAY TO SECOND\"), not \
                 {}. SQLSTATE: 42K09",
                name(left)
            ),
            _ => format!(
                "[DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES] Cannot resolve \"{sql_expr}\" due to \
                 data type mismatch: the left and right operands of the binary operator have \
                 incompatible types ({} and {}). SQLSTATE: 42K09",
                name(left),
                name(right)
            ),
        };
        Err(PlanError::analysis(message))
    };
    // Legacy promotion sends a STRING to DOUBLE, which `div` rejects; ANSI promotion widens
    // it to LONG, but only when the other side is integral (`findWiderTypeForString`).
    let dividend_is_string = matches!(
        dividend_type.as_ref().ok(),
        Some(DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
    );
    let divisor_is_string = matches!(
        divisor_type.as_ref().ok(),
        Some(DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
    );
    let dividend_is_integral = dividend_type.as_ref().is_ok_and(DataType::is_integer);
    let divisor_is_integral = divisor_type.as_ref().is_ok_and(DataType::is_integer);
    let (dividend, divisor, dividend_type, divisor_type) =
        match (dividend_is_string, divisor_is_string) {
            (false, false) => (dividend, divisor, dividend_type, divisor_type),
            (true, false) if ansi_mode && divisor_is_integral => (
                cast(dividend, DataType::Int64),
                divisor,
                Ok(DataType::Int64),
                divisor_type,
            ),
            (false, true) if ansi_mode && dividend_is_integral => (
                dividend,
                cast(divisor, DataType::Int64),
                dividend_type,
                Ok(DataType::Int64),
            ),
            // An unresolvable type is tolerated here for the same reason the `accepted` gate
            // below tolerates it: a failed `get_type` must not turn into a rejected query.
            _ if dividend_type.is_err() || divisor_type.is_err() => {
                (dividend, divisor, dividend_type, divisor_type)
            }
            _ => return reject(dividend_type.as_ref().ok(), divisor_type.as_ref().ok()),
        };

    // Spark rewrites an untyped NULL to the expected concrete type rather than tolerating it
    // (TypeCoercionHelper.scala:570-578); with both sides NULL it falls back to LONG. The
    // rewrite is conditional: `TypeCoercionHelper.scala:550` only replaces the children when
    // `inputType` accepts the common type, so beside a REJECTED peer the NULL keeps VOID and
    // that is the name the rejection carries.
    let dividend_is_null = matches!(dividend_type.as_ref().ok(), Some(DataType::Null));
    let divisor_is_null = matches!(divisor_type.as_ref().ok(), Some(DataType::Null));
    let (mut dividend, mut divisor) = (dividend, divisor);
    let (mut dividend_type, mut divisor_type) = (dividend_type, divisor_type);
    let accepted_peer = |t: &datafusion_common::Result<DataType>| {
        t.as_ref().ok().filter(|t| accepts_div_input(t)).cloned()
    };
    let null_target = match (dividend_is_null, divisor_is_null) {
        (true, true) => Some(DataType::Int64),
        (true, false) => accepted_peer(&divisor_type),
        (false, true) => accepted_peer(&dividend_type),
        (false, false) => None,
    };
    if let Some(target) = null_target {
        if dividend_is_null {
            dividend = cast(dividend, target.clone());
            dividend_type = Ok(target.clone());
        }
        if divisor_is_null {
            divisor = cast(divisor, target.clone());
            divisor_type = Ok(target);
        }
    }

    // `IntegralDivide.inputType` (arithmetic.scala:890), checked on the PAIR: per-operand
    // checks let a mixed pair through and it dies inside DataFusion instead of here.
    let numeric = |t: &DataType| {
        t.is_integer() || matches!(t, DataType::Decimal128(_, _) | DataType::Decimal256(_, _))
    };
    let accepted = match (&dividend_type, &divisor_type) {
        (Err(_), _) | (_, Err(_)) => true,
        (Ok(l), Ok(r)) => {
            (numeric(l) && numeric(r))
                || matches!((l, r), (DataType::Duration(a), DataType::Duration(b)) if a == b)
                || matches!(
                    (l, r),
                    (
                        DataType::Interval(IntervalUnit::YearMonth),
                        DataType::Interval(IntervalUnit::YearMonth)
                    )
                )
        }
    };
    if !accepted {
        return reject(dividend_type.as_ref().ok(), divisor_type.as_ref().ok());
    }

    // Each kernel owns the zero policy for its own arm, including "a NULL dividend wins over a
    // zero divisor" (arithmetic.scala:664-684): under ANSI it raises, so the divisor is passed
    // through untouched; without ANSI it needs the divisor already NULLed.
    let kernel_guarded_divisor = |divisor: Expr, divisor_type: &DataType| {
        if ansi_mode {
            divisor
        } else {
            // `make_safe_divisor` reads the message only on the ANSI path.
            make_safe_divisor(divisor, divisor_type, false, "")
        }
    };
    let div_expr = match (&dividend_type, &divisor_type) {
        // TODO: Casting DataType::Interval(_) to DataType::Int64 is not supported yet.
        //  Seems to be a bug in DataFusion.
        (Ok(DataType::Duration(_)), Ok(DataType::Duration(_))) => {
            // Sail resolves Spark's DAY TO SECOND to Duration, and Spark divides it as LONG
            // (`PhysicalLongType.integral`, arithmetic.scala:918-919). `checkDivideOverflow`
            // only fires for a LONG *dividend* (arithmetic.scala:885-888), so an interval
            // dividend wraps at `LONG_MIN / -1` in BOTH ANSI modes instead of raising — hence
            // the kernel with the overflow check disabled rather than a checked division.
            let dividend = cast(dividend, DataType::Int64);
            let divisor = kernel_guarded_divisor(cast(divisor, DataType::Int64), &DataType::Int64);
            ScalarUDF::from(SparkIntegerDiv::new(ansi_mode, false)).call(vec![dividend, divisor])
        }
        // The UDF owns its zero policy. Each arm hands the UDF the ORIGINAL operands, so
        // grouping and aggregate extraction still see the expression the user wrote (#2822).
        (
            Ok(DataType::Interval(IntervalUnit::YearMonth)),
            Ok(DataType::Interval(IntervalUnit::YearMonth)),
        ) => ScalarUDF::from(SparkIntervalDiv::new(ansi_mode)).call(vec![dividend, divisor]),
        // Integer operands: delegate to SparkIntegerDiv, which raises on a zero divisor
        // under ANSI and handles the LONG_MIN / -1 overflow edge. Outside ANSI the zero
        // has to become a NULL before it reaches the kernel.
        (Ok(d), Ok(s)) if d.is_integer() && s.is_integer() => {
            let divisor = kernel_guarded_divisor(divisor, s);
            ScalarUDF::from(SparkIntegerDiv::new(ansi_mode, true)).call(vec![dividend, divisor])
        }
        // DECIMAL on either side. Spark promotes an integral peer with `DecimalType.forType`
        // and then divides on BigDecimal WITHOUT bringing the two decimals to a common type
        // (the analyzed plan keeps both), so the pair is handed to the kernel as it is. Routing
        // this through DataFusion's `/` instead is what made a wide scale overflow, or fail to
        // resolve a result type at all. The kernel owns both zero policies, like the interval
        // one, so no guard is wrapped around the divisor here.
        (Ok(d), Ok(s))
            if (is_spark_decimal(d) && is_spark_decimal(s))
                || (is_spark_decimal(d) && s.is_integer())
                || (d.is_integer() && is_spark_decimal(s)) =>
        {
            // The operands are passed through UNPROMOTED: `SparkIntegerDiv::coerce_types`
            // resolves a mixed pair, because promoting here with `DecimalType.forType` (scale 0)
            // destroys the value whenever the declared type understates it — Sail types
            // `CASE WHEN false THEN 0 ELSE 1.5 END` as INT, and rounding it to a scale-0 decimal
            // answers 4 where Spark answers 3 (measured; #2822 pins this shape).
            // `checkDivideOverflow` is LONG-dividend-only (arithmetic.scala:885-888), so a DECIMAL
            // dividend is never overflow-checked.
            ScalarUDF::from(SparkIntegerDiv::new(ansi_mode, false)).call(vec![dividend, divisor])
        }
        // TODO: In case getting the type fails, we don't want to fail the query.
        //  Future work is needed here, ideally we create something like `Operator::SparkDivide`.
        (Ok(_), Ok(_)) | (Err(_), _) | (_, Err(_)) => {
            let effective_divisor_type = divisor_type.as_ref().cloned().unwrap_or(DataType::Int32);
            let divisor = make_safe_divisor(
                divisor,
                &effective_divisor_type,
                ansi_mode,
                DIVIDE_BY_ZERO_MESSAGE,
            );
            dividend / divisor
        }
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

fn positive(expr: Expr) -> Expr {
    expr
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

    // Plan-time check for literal zero divisors.
    if is_zero_literal(&divisor) {
        if function_context.plan_config.ansi_mode {
            return Err(PlanError::ArrowError(ArrowError::ArithmeticOverflow(
                "Remainder by zero".to_string(),
            )));
        } else {
            return Ok(Expr::Literal(ScalarValue::Null, None));
        }
    }

    let ansi_mode = function_context.plan_config.ansi_mode;
    let divisor_type = divisor.get_type(function_context.schema);

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
    let ansi_mode = input.function_context.plan_config.ansi_mode;
    let udf = ScalarUDF::from(SparkAbs::new(ansi_mode));
    Ok(udf.call(input.arguments))
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

fn spark_negative(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arg = arguments.one()?;
    Ok(spark_unary_negate(
        arg,
        function_context.plan_config.ansi_mode,
        function_context.schema,
    ))
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
        ("positive", F::unary(positive)),
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
