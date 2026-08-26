use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION, DataType, IntervalUnit, TimeUnit,
};
use datafusion::functions::expr_fn;
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::{
    BinaryExpr, Expr, ExprSchemable, Operator, ScalarUDF, cast, expr, lit, try_cast,
};
use datafusion_spark::function::math::expr_fn as math_fn;
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
use sail_function::scalar::math::spark_div::SparkIntervalDiv;
use sail_function::scalar::math::spark_negative::SparkNegative;
use sail_function::scalar::math::spark_pmod::SparkPmod;
use sail_function::scalar::math::spark_signum::SparkSignum;
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
    FunctionContextInput, ScalarFunction, ScalarFunctionInput, spark_string_to_numeric,
    spark_type_name,
};
use crate::function::decimal::{
    spark_decimal_add_diverges, spark_decimal_add_type, spark_decimal_divide_type,
    spark_decimal_multiply_type, spark_decimal_remainder_type,
};

/// The `+` / `-` arm for two operands whose types are both known: apply Spark's operand
/// coercion, build the operator, and re-type a decimal result when Spark's
/// `adjustPrecisionScale` gives something narrower than Arrow's.
///
/// `+` and `-` share one `resultDecimalType` in Spark, so they share this path and differ
/// only in the operator handed to `op`.
fn spark_additive_operands(
    left: Expr,
    right: Expr,
    left_type: &DataType,
    right_type: &DataType,
    function_context: &FunctionContextInput<'_>,
    op: impl FnOnce(Expr, Expr) -> Expr,
) -> Expr {
    let allow_precision_loss = function_context
        .plan_config
        .decimal_operations_allow_precision_loss;
    let (left, right) = coerce_spark_arithmetic_operands(
        left,
        right,
        left_type,
        right_type,
        function_context.plan_config.ansi_mode,
        function_context.plan_config.literal_pick_minimum_precision,
    );
    let (left, right) = coerce_decimal_peer_operand(left, right, function_context.schema);
    let operands = (
        left.get_type(function_context.schema),
        right.get_type(function_context.schema),
    );
    match operands {
        (Ok(DataType::Decimal128(p1, s1)), Ok(DataType::Decimal128(p2, s2)))
            if spark_decimal_add_diverges(p1, s1, p2, s2) =>
        {
            // Widen the operands to Decimal256 BEFORE adding (mirroring the capped multiply
            // path). Spark reduces the result scale via `adjustPrecisionScale`, but the native
            // i128 kernel would add at the un-reduced scale and OVERFLOW on values Spark
            // represents fine (e.g. `decimal(38,10) + decimal(38,2)` = `decimal(38,6)`); the
            // i256 intermediate has the headroom, then the retype rounds and narrows.
            let wide_sum = op(
                cast(left, DataType::Decimal256(DECIMAL256_MAX_PRECISION, s1)),
                cast(right, DataType::Decimal256(DECIMAL256_MAX_PRECISION, s2)),
            );
            spark_decimal_add_retype(
                wide_sum,
                p1,
                s1,
                p2,
                s2,
                allow_precision_loss,
                function_context.plan_config.ansi_mode,
            )
        }
        _ => op(left, right),
    }
}

/// Spark's `TimestampType`, i.e. the session-timezone timestamp `Cast(date, TimestampType)`
/// produces in the datetime rewrite rules.
fn session_timestamp_type(function_context: &FunctionContextInput<'_>) -> DataType {
    DataType::Timestamp(
        TimeUnit::Microsecond,
        Some(Arc::clone(&function_context.plan_config.session_timezone)),
    )
}

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
        let arg = arguments.one()?;
        Ok(spark_unary_plus(
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
        if let (Ok(left_type), Ok(right_type)) = (&left_type, &right_type)
            && rejects_add(
                left_type,
                right_type,
                function_context.plan_config.ansi_mode,
            )
        {
            return Err(arithmetic_operand_error('+', left_type, right_type));
        }
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
            // Spark's `Add` casts an untyped NULL beside a datetime to
            // `DayTimeIntervalType.DEFAULT` — NOT to the peer's type, which is what `Subtract`
            // does instead (BinaryArithmeticWithDatetimeResolver.scala:88-92 vs :119-122). The
            // rewrite then re-runs on the new pair: `(DateType, DayTimeIntervalType)` becomes
            // `TimestampAddInterval(Cast(date, TimestampType), ..)` (:69) and a TIMESTAMP peer
            // keeps its own type (:93-94), so the result is TIMESTAMP either way.
            //
            // The non-NULL `date + interval` case cannot follow this rule: Spark keeps DATE only
            // for a `DayTimeIntervalType(DAY, DAY)` operand (:68) and Arrow erases the interval's
            // declared field range, so the two are indistinguishable. A NULL has no such
            // ambiguity — the default range applies — which is why this pair is fixable and the
            // general one is not.
            (Ok(DataType::Date32), Ok(DataType::Null)) => {
                cast(left, session_timestamp_type(&function_context))
                    + cast(right, DataType::Interval(IntervalUnit::MonthDayNano))
            }
            (Ok(DataType::Null), Ok(DataType::Date32)) => {
                cast(right, session_timestamp_type(&function_context))
                    + cast(left, DataType::Interval(IntervalUnit::MonthDayNano))
            }
            (Ok(DataType::Timestamp(_, _)), Ok(DataType::Null)) => {
                left + cast(right, DataType::Interval(IntervalUnit::MonthDayNano))
            }
            (Ok(DataType::Null), Ok(DataType::Timestamp(_, _))) => {
                right + cast(left, DataType::Interval(IntervalUnit::MonthDayNano))
            }
            (Ok(left_type), Ok(DataType::Date32)) if left_type.is_numeric() => {
                cast(left + cast(right, DataType::Int32), DataType::Date32)
            }
            (Ok(DataType::Date32), Ok(right_type)) if right_type.is_numeric() => {
                cast(cast(left, DataType::Int32) + right, DataType::Date32)
            }
            (Ok(left_type), Ok(right_type)) => spark_additive_operands(
                left,
                right,
                &left_type,
                &right_type,
                &function_context,
                |left, right| left + right,
            ),
            // TODO: In case getting the type fails, we don't want to fail the query.
            //  Future work is needed here, ideally we create something like `Operator::SparkPlus`.
            (Err(_), _) | (_, Err(_)) => left + right,
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
        if let (Ok(left_type), Ok(right_type)) = (&left_type, &right_type)
            && rejects_subtract(
                left_type,
                right_type,
                function_context.plan_config.ansi_mode,
            )
        {
            return Err(arithmetic_operand_error('-', left_type, right_type));
        }
        Ok(match (left_type, right_type) {
            (Ok(DataType::Date32), Ok(DataType::Duration(TimeUnit::Microsecond))) => {
                left - cast(right, DataType::Interval(IntervalUnit::MonthDayNano))
            }
            (Ok(DataType::Date32), Ok(right_type)) if right_type.is_numeric() => {
                cast(cast(left, DataType::Int32) - right, DataType::Date32)
            }
            (Ok(DataType::Date32), Ok(DataType::Timestamp(_, _)))
            | (Ok(DataType::Timestamp(_, _)), Ok(DataType::Date32)) => {
                // DataFusion subtracts DATE and TIMESTAMP to a `Duration(Nanosecond)`, which
                // is not a Spark type; Spark's DATE-TIMESTAMP is a day-time interval, mapped
                // in Sail to `Duration(Microsecond)`.
                cast(left - right, DataType::Duration(TimeUnit::Microsecond))
            }
            (Ok(left_type), Ok(right_type)) => spark_additive_operands(
                left,
                right,
                &left_type,
                &right_type,
                &function_context,
                |left, right| left - right,
            ),
            // TODO: In case getting the type fails, we don't want to fail the query.
            //  Future work is needed here, ideally we create something like `Operator::SparkMinus`.
            (Err(_), _) | (_, Err(_)) => left - right,
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
    if let (Ok(left_type), Ok(right_type)) = (&left_type, &right_type)
        && rejects_multiply(
            left_type,
            right_type,
            function_context.plan_config.ansi_mode,
        )
    {
        return Err(arithmetic_operand_error('*', left_type, right_type));
    }
    Ok(match (left_type, right_type) {
        // TODO: Casting DataType::Interval(_) to DataType::Int64 is not supported yet.
        //  Seems to be a bug in DataFusion.
        (Ok(DataType::Duration(TimeUnit::Microsecond)), Ok(right_type)) => {
            // Match duration because we cast Spark's DayTime interval to Duration.
            // These arms return before `coerce_spark_arithmetic_operands` runs, so the
            // scaling operand is anchored here instead.
            let right = coerce_interval_scale_operand(
                right,
                &right_type,
                function_context.plan_config.ansi_mode,
            );
            cast(
                cast(left, DataType::Int64) * right,
                DataType::Duration(TimeUnit::Microsecond),
            )
        }
        (Ok(left_type), Ok(DataType::Duration(TimeUnit::Microsecond))) => {
            // Match duration because we cast Spark's DayTime interval to Duration.
            let left = coerce_interval_scale_operand(
                left,
                &left_type,
                function_context.plan_config.ansi_mode,
            );
            cast(
                left * cast(right, DataType::Int64),
                DataType::Duration(TimeUnit::Microsecond),
            )
        }
        (Ok(left_type), Ok(right_type)) => {
            let ansi_mode = function_context.plan_config.ansi_mode;
            let (left, right) = coerce_spark_arithmetic_operands(
                left,
                right,
                &left_type,
                &right_type,
                ansi_mode,
                function_context.plan_config.literal_pick_minimum_precision,
            );
            let (left, right) = coerce_decimal_peer_operand(left, right, function_context.schema);
            // Spark caps a decimal product's precision at 38 by REDUCING the scale
            // (adjustPrecisionScale) and HALF_UP-rounding the value; DataFusion keeps
            // the full scale.
            //
            // The gate is "does the exact product need more than 38 digits", NOT "does
            // Spark's type differ from Arrow's" — deliberately wider than the
            // `spark_decimal_add_diverges` gate `+`/`-` use. Once the product is capped,
            // Arrow's native i128 multiply RAISES on the values that no longer fit, while
            // Spark's CheckOverflow yields NULL under ANSI off. Computing in i256 and
            // narrowing with `try_cast` reproduces that, so the path is worth taking even
            // for the shapes where the two result TYPES happen to agree (e.g.
            // `decimal(38,0) * decimal(38,0)`, or the whole `allowPrecisionLoss = false`
            // config, where Spark uses `bounded` — exactly Arrow's rule).
            //
            // Below precision 38 nothing is capped, the product is exact, and it stays on
            // the native kernel.
            match (
                left.get_type(function_context.schema),
                right.get_type(function_context.schema),
            ) {
                (Ok(DataType::Decimal128(p1, s1)), Ok(DataType::Decimal128(p2, s2)))
                    if u16::from(p1) + u16::from(p2) + 1 > u16::from(DECIMAL128_MAX_PRECISION) =>
                {
                    let (result_precision, result_scale) = spark_decimal_multiply_type(
                        p1,
                        s1,
                        p2,
                        s2,
                        function_context
                            .plan_config
                            .decimal_operations_allow_precision_loss,
                    );
                    let product = cast(left, DataType::Decimal256(DECIMAL256_MAX_PRECISION, s1))
                        * cast(right, DataType::Decimal256(DECIMAL256_MAX_PRECISION, s2));
                    // The product already carries scale `s1 + s2`; only round when Spark's
                    // capped scale is strictly smaller (the common `s1 + s2 <= 6` and the
                    // whole `allowPrecisionLoss = false` shapes keep the full scale, where a
                    // `round` to the same scale would be a wasted per-row kernel pass).
                    let rounded = if i32::from(result_scale) == i32::from(s1) + i32::from(s2) {
                        product
                    } else {
                        expr_fn::round(vec![product, lit(i32::from(result_scale))])
                    };
                    narrow_decimal_by_ansi(rounded, result_precision, result_scale, ansi_mode)
                }
                _ => left * right,
            }
        }
        // TODO: In case getting the type fails, we don't want to fail the query.
        //  Future work is needed here, ideally we create something like `Operator::SparkMultiply`.
        (Err(_), _) | (_, Err(_)) => left * right,
    })
}

/// Re-types a decimal `+`/`-` whose exact precision exceeds 38 to Spark's result type.
///
/// Spark caps such a result with `adjustPrecisionScale`, which REDUCES the scale to keep
/// the integer digits (`decimal(38,10) + decimal(38,2)` is `decimal(38,6)`). Arrow caps
/// with a plain `min(_, 38)` that keeps the scale, giving `decimal(38,10)` — a silently
/// wrong type under the default `allowPrecisionLoss = true`.
///
/// The narrowing takes the ANSI gate like the capped multiply / divide paths do: `cast`
/// under ANSI on, `try_cast` under ANSI off. The gate (`spark_decimal_add_diverges`) fires
/// whenever the exact precision exceeds 38. When Spark only reduces the scale, the round drops
/// digits and the value is the same either way; when the capped result also overflows (a
/// scale-unchanged shape like `decimal(38,0) + decimal(38,0)`), `try_cast` yields NULL under
/// ANSI off while `cast` raises under ANSI on — exactly Spark's `CheckOverflow`. Either way
/// `try_cast` makes the output field `nullable = true`, matching Spark's non-ANSI Add/Subtract
/// (`CheckOverflow`, arithmetic.scala) where a plain `cast` would declare `nullable = false`.
fn spark_decimal_add_retype(
    sum: Expr,
    p1: u8,
    s1: i8,
    p2: u8,
    s2: i8,
    allow_precision_loss: bool,
    ansi_mode: bool,
) -> Expr {
    let (result_precision, result_scale) =
        spark_decimal_add_type(p1, s1, p2, s2, allow_precision_loss);
    let rounded = expr_fn::round(vec![sum, lit(i32::from(result_scale))]);
    narrow_decimal_by_ansi(rounded, result_precision, result_scale, ansi_mode)
}

/// Applies Spark's ANSI gate to an optional cast target: a strict `cast` under ANSI on (an
/// out-of-range value raises), a `try_cast` under ANSI off (out-of-range yields NULL and the
/// field is declared nullable, matching Spark's `CheckOverflow` / non-ANSI decimal arithmetic);
/// `None` leaves the expression unchanged. Shared by the capped decimal retypes and the `%`/
/// `pmod` result narrowing so the ANSI→nullability rule lives in one place.
fn ansi_cast_opt(expr: Expr, target: Option<DataType>, ansi_mode: bool) -> Expr {
    match target {
        Some(target) if ansi_mode => cast(expr, target),
        Some(target) => try_cast(expr, target),
        None => expr,
    }
}

/// Narrows a decimal expression to `Decimal128(precision, scale)` taking Spark's ANSI gate,
/// shared by the capped `*`, `/` and `+`/`-` retypes.
fn narrow_decimal_by_ansi(expr: Expr, precision: u8, scale: i8, ansi_mode: bool) -> Expr {
    ansi_cast_opt(
        expr,
        Some(DataType::Decimal128(precision, scale)),
        ansi_mode,
    )
}

/// The plan-time rejection Spark raises at analysis (`DATATYPE_MISMATCH`) for an arithmetic
/// operand pair it cannot resolve. Sail reports its own message (it has no Spark error classes
/// yet); the shared `cannot resolve` prefix is what the `.feature` reject scenarios assert.
fn arithmetic_operand_error(op: char, left: &DataType, right: &DataType) -> PlanError {
    PlanError::invalid(format!(
        "cannot resolve arithmetic '{op}' with operand types {} and {}",
        spark_type_name(left),
        spark_type_name(right)
    ))
}

/// Spark-specific operand coercion for `+ - *` applied at plan-construction time,
/// so the logical plan is valid by construction (rather than relying on a later
/// analyzer rule, which would run after `ExprSchemable::get_type` has already typed
/// the binary op via DataFusion's `BinaryTypeCoercer`).
///
/// Covers the cases where DataFusion's default coercion diverges from Spark:
///   - FLOAT/DOUBLE combined with DECIMAL: Spark promotes both to DOUBLE.
///   - integer LITERAL combined with DECIMAL: Spark narrows the literal to its
///     minimal-precision decimal (so `dec(10,2) * 3` => `decimal(12,2)`).
fn coerce_spark_arithmetic_operands(
    left: Expr,
    right: Expr,
    left_type: &DataType,
    right_type: &DataType,
    ansi_mode: bool,
    pick_minimum_precision: bool,
) -> (Expr, Expr) {
    // STRING operands. DataFusion rejects string arithmetic; Spark coerces
    // (validated vs Spark 4.2.0):
    //   ANSI off -> a string paired with a string, numeric or NULL promotes BOTH to
    //               DOUBLE, and a string that does not parse yields NULL rather than
    //               an error (so the cast must be a `try_cast`).
    //   ANSI on  -> a string paired with an INTEGRAL numeric promotes to BIGINT, and
    //               with a FRACTIONAL numeric (float or decimal) to DOUBLE. The cast
    //               is strict: a malformed string raises. `string + string` and
    //               `string + NULL` are left as-is; Spark rejects both.
    // https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TypeCoercion.scala (PromoteStrings)
    // https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/AnsiTypeCoercion.scala
    // Two untyped NULLs have no peer to take a type from, so Spark falls back to the
    // operator's declared input type: `implicitCast(NullType, target)` returns
    // `target.defaultConcreteType` (TypeCoercion.scala:202), the operator's `inputType` is
    // `TypeCollection.NumericAndInterval` (arithmetic.scala:417), a TypeCollection's default is
    // its head's (AbstractDataType.scala:66), and that head is `NumericType`, whose default is
    // `DoubleType` (AbstractDataType.scala:81-89, :131). So `NULL <op> NULL` is DOUBLE under
    // both ANSI modes, where DataFusion instead settles on BIGINT (and on INT for `%`, which
    // also makes the remainder type depend on the ANSI mode).
    if matches!(left_type, DataType::Null) && matches!(right_type, DataType::Null) {
        return (
            cast(left, DataType::Float64),
            cast(right, DataType::Float64),
        );
    }
    let left_string = left_type.is_string();
    let right_string = right_type.is_string();
    if left_string || right_string {
        // Under ANSI off a NULL operand rides along as DOUBLE; under ANSI Spark
        // rejects `string <op> NULL`, so it must not be coerced here.
        let operand_ok = |is_string: bool, data_type: &DataType| {
            is_string
                || data_type.is_numeric()
                || (!ansi_mode && matches!(data_type, DataType::Null))
        };
        if operand_ok(left_string, left_type) && operand_ok(right_string, right_type) {
            if !ansi_mode {
                return (
                    coerce_string_operand(left, left_type, &DataType::Float64, true),
                    coerce_string_operand(right, right_type, &DataType::Float64, true),
                );
            }
            // ANSI on. `string + string` has no numeric peer to promote to; leave it
            // for DataFusion to reject, as Spark does.
            if !(left_string && right_string) {
                let fractional = left_type.is_floating()
                    || right_type.is_floating()
                    || is_decimal_type(left_type)
                    || is_decimal_type(right_type);
                let target = if fractional {
                    DataType::Float64
                } else {
                    DataType::Int64
                };
                return (
                    coerce_string_operand(left, left_type, &target, false),
                    coerce_string_operand(right, right_type, &target, false),
                );
            }
        }
    }
    // FLOAT/DOUBLE x DECIMAL -> DOUBLE.
    // https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/DecimalPrecision.scala
    if is_float_decimal_pair(left_type, right_type) {
        return (
            cast(left, DataType::Float64),
            cast(right, DataType::Float64),
        );
    }
    // ANSI only: an integral combined with a 32-bit FLOAT promotes both to DOUBLE, not
    // FLOAT — widening the integral into a float would lose precision. Non-ANSI keeps
    // Spark's legacy FLOAT result. `AnsiTypeCoercion`: when the wider of two numerics is
    // FloatType and the other side is integral, the common type is DoubleType (validated
    // vs Spark 4.2.0). `float`/`double`/`decimal`/`string` peers already widen to DOUBLE
    // above and elsewhere, so only the integral×float pair needs this.
    // https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/AnsiTypeCoercion.scala
    if ansi_mode && is_integral_float_pair(left_type, right_type) {
        return (
            cast(left, DataType::Float64),
            cast(right, DataType::Float64),
        );
    }
    // integer literal x DECIMAL -> narrow the literal to its minimal decimal.
    // https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/types/DecimalType.scala
    let left = match spark_decimal_literal_datatype(&left, right_type, pick_minimum_precision) {
        Some(target) => cast(left, target),
        None => left,
    };
    let right = match spark_decimal_literal_datatype(&right, left_type, pick_minimum_precision) {
        Some(target) => cast(right, target),
        None => right,
    };
    (left, right)
}

/// Brings one operand of a string-and-numeric pair to the type Spark promotes them to.
///
/// A string is parsed with the shared Spark rules (shared with `CAST` and the type
/// constructors, so all three agree on trimming and on NULL-vs-raise). The peer is cast
/// to the same target: a binary operator would not need it, since DataFusion widens the
/// pair anyway, but a UDF picks its own common type from the operands it is handed —
/// leaving the peer alone is what typed `pmod('5.5', decimal(10,2))` as `decimal(30,15)`
/// instead of DOUBLE, and made `pmod(NULL, '3')` fail to plan.
fn coerce_string_operand(
    expr: Expr,
    expr_type: &DataType,
    target: &DataType,
    null_on_failure: bool,
) -> Expr {
    if expr_type.is_string() {
        spark_string_to_numeric(expr, target.clone(), null_on_failure)
    } else if expr_type == target {
        expr
    } else {
        cast(expr, target.clone())
    }
}

/// Deliberately narrower than `DataType::is_decimal()`, which also admits `Decimal32`/
/// `Decimal64`: every Spark result-type rule below is written against `Decimal128` (and
/// `Decimal256` as its widened intermediate), so admitting the narrow widths here would route
/// them into retype arms that cannot match them. They keep DataFusion's coercion instead.
/// Sail has no SQL syntax that produces a `Decimal32`/`Decimal64` column — they arrive only
/// from an Arrow or Parquet source — so the gap is not reachable from a BDD scenario and is
/// left as is rather than changed untested.
fn is_decimal_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
    )
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

fn is_interval_like(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Interval(_) | DataType::Duration(_))
}

/// Anchors the numeric operand that scales an interval, which is the one case where a STRING
/// does NOT follow the general arithmetic promotion rule.
///
/// `BinaryArithmeticWithDatetimeResolver` rewrites `interval * x` / `interval / x` by the
/// operands' *data types*, before any coercion, so a STRING still enters as the `num` child of
/// `MultiplyDTInterval`/`DivideDTInterval` (`BinaryArithmeticWithDatetimeResolver.scala:147-163`).
/// Those are `ImplicitCastInputTypes` with `Seq(DayTimeIntervalType, NumericType)`
/// (`intervalExpressions.scala:650-658`, `:819-827`), so the string is cast by
/// `implicitCast(StringType, NumericType)` — and BOTH coercion rules agree on the target:
/// `TypeCoercion.scala:212` and `AnsiTypeCoercion.scala:195-196` return
/// `NumericType.defaultConcreteType`, which is `DoubleType` (`AbstractDataType.scala:131`).
///
/// So this is DOUBLE under both ANSI modes, unlike the general string promotion (where an
/// integral peer gives BIGINT under ANSI on) — the expected type here is the abstract
/// `NumericType` of the interval expression, not the peer's type. The ANSI gate only decides
/// whether a malformed string raises or yields NULL, which is what the shared helper does.
/// Promotes a STRING operand of `DIV` to BIGINT, the first member of `IntegralDivide`'s input
/// collection that a string implicitly casts to (see the note in [`spark_div`]). Only reached
/// under ANSI on, where a malformed string raises rather than yielding NULL — which is what
/// `null_on_failure = !ansi_mode` gives the shared helper.
fn coerce_integral_divide_operand(expr: Expr, data_type: &DataType, ansi_mode: bool) -> Expr {
    if data_type.is_string() {
        spark_string_to_numeric(expr, DataType::Int64, !ansi_mode)
    } else {
        expr
    }
}

fn coerce_interval_scale_operand(expr: Expr, data_type: &DataType, ansi_mode: bool) -> Expr {
    if data_type.is_string() {
        spark_string_to_numeric(expr, DataType::Float64, !ansi_mode)
    } else {
        expr
    }
}

/// The numeric offset Spark accepts for a `DATE` in `+`/`-`: `DateAdd`/`DateSub` take an
/// `INT` (`IntegerType | ShortType | ByteType`), so only integrals that fit losslessly in an
/// `INT` qualify. A `BIGINT`, `FLOAT`, `DOUBLE` or `DECIMAL` offset is rejected at analysis
/// (Sail would otherwise silently truncate it). Spark has no unsigned types, but Arrow-native
/// sources can, so `UInt8`/`UInt16` (both within `INT` range) are accepted too.
fn is_date_offset_numeric(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::UInt8 | DataType::UInt16
    )
}

/// The Spark arithmetic operand class of a type. `+`, `-` and `*` decide accept/reject by these
/// classes, so the plan-time guards below are written against them. The per-operator accept sets
/// are validated cell-by-cell against Spark 4.2.0 in `math/arithmetic_operand_matrix.feature`.
/// `Unsupported` is a type Spark never accepts in arithmetic that would otherwise compute a
/// garbage value (boolean, binary). `Other` is any type outside the validated matrix (struct,
/// list, dictionary, time, …): the guards leave those to DataFusion rather than hard-reject a
/// pair whose behavior was never measured.
#[derive(PartialEq, Eq, Clone, Copy)]
enum OperandRole {
    Numeric,
    Str,
    UntypedNull,
    Date,
    Timestamp,
    IntervalDt,
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
        DataType::Interval(IntervalUnit::YearMonth) => IntervalYm,
        DataType::Interval(_) | DataType::Duration(_) => IntervalDt,
        DataType::Boolean | DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            Unsupported
        }
        _ => Other,
    }
}

/// The verdict every `+`/`-`/`*` guard shares before its per-operator rules: an `Unsupported`
/// operand (boolean, binary) is always rejected, and an `Other` operand (a type outside the
/// validated matrix — struct, list, time, …) is deferred to DataFusion rather than hard-rejected.
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
    if matches!(a, Date | Timestamp) || matches!(b, Date | Timestamp) {
        return true;
    }
    let is_interval = |r: OperandRole| matches!(r, IntervalDt | IntervalYm);
    if is_interval(a) && is_interval(b) {
        return true;
    }
    if is_interval(a) || is_interval(b) {
        let other = if is_interval(a) { b } else { a };
        return !matches!(other, Numeric | Str | UntypedNull);
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
            UntypedNull | IntervalDt | IntervalYm => false,
            Numeric => !is_date_offset_numeric(other_type),
            _ => true,
        };
    }
    // A TIMESTAMP takes only an interval or an untyped NULL (no numeric offset).
    if a == Timestamp || b == Timestamp {
        let other = if a == Timestamp { b } else { a };
        return !matches!(other, IntervalDt | IntervalYm | UntypedNull);
    }
    // Both operands are intervals (date/timestamp handled above): same class only, except a
    // day-time interval also accepts a string or untyped NULL peer (Spark's day-time resolver).
    match (a, b) {
        (IntervalDt, IntervalDt) | (IntervalYm, IntervalYm) => false,
        (IntervalDt, IntervalYm) | (IntervalYm, IntervalDt) => true,
        _ => {
            let (interval, other) = if matches!(a, IntervalDt | IntervalYm) {
                (a, b)
            } else {
                (b, a)
            };
            match other {
                UntypedNull => false,
                Str => interval != IntervalDt,
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
    ) || (!ansi_mode && unanchored_string_pair(a, b))
        || (ansi_mode && matches!((a, b), (Date, Str) | (Str, Timestamp) | (Timestamp, Str)));
    !accepted
}

/// Spark's `DecimalType.forType` for an integer type: the type-based decimal an
/// integer *column* is cast to when combined with a decimal in division
/// (`Int -> Decimal(10,0)`, etc.). Integer *literals* narrow to their minimal
/// decimal instead (see [`spark_decimal_literal_datatype`]).
fn spark_integer_decimal_type(data_type: &DataType) -> Option<DataType> {
    let precision = match data_type {
        DataType::Int8 | DataType::UInt8 => 3,
        DataType::Int16 | DataType::UInt16 => 5,
        DataType::Int32 | DataType::UInt32 => 10,
        DataType::Int64 | DataType::UInt64 => 20,
        _ => return None,
    };
    Some(DataType::Decimal128(precision, 0))
}

/// Casts an integer or NULL operand paired with a decimal to the decimal Spark gives it,
/// so the decimal arithmetic rule sees two decimals.
///
/// An integer *column* takes its type-based decimal (`Int -> Decimal(10,0)`, ...); bare
/// integer literals are narrowed to their minimal decimal separately by
/// [`coerce_spark_arithmetic_operands`]. A NULL takes the peer's decimal type, which is
/// what Spark's implicit cast does — without it `decimal(38,18) * NULL` keeps
/// DataFusion's `decimal(38,36)` instead of Spark's `decimal(38,6)`.
///
/// Used by `+ - * /`: every operator whose decimal result type depends on the widened
/// operand's precision. `/` resolves its own (asymmetric) NULL rule first, so no NULL
/// reaches here from that path.
fn coerce_decimal_peer_operand(left: Expr, right: Expr, schema: &DFSchemaRef) -> (Expr, Expr) {
    fn peer_decimal_type(data_type: &DataType, decimal_type: &DataType) -> Option<DataType> {
        match data_type {
            DataType::Null => Some(decimal_type.clone()),
            _ => spark_integer_decimal_type(data_type),
        }
    }
    match (left.get_type(schema), right.get_type(schema)) {
        (Ok(left_type), Ok(right_type)) if is_decimal_type(&right_type) => {
            match peer_decimal_type(&left_type, &right_type) {
                Some(target) => (cast(left, target), right),
                None => (left, right),
            }
        }
        (Ok(left_type), Ok(right_type)) if is_decimal_type(&left_type) => {
            match peer_decimal_type(&right_type, &left_type) {
                Some(target) => (left, cast(right, target)),
                None => (left, right),
            }
        }
        _ => (left, right),
    }
}

/// Spark's NullType coercion for `/`, which is asymmetric (validated vs Spark 4.2.0).
///
/// `decimal / NULL` coerces the NULL to the dividend's decimal, so the Spark divide
/// rule applies (`decimal(10,2) / NULL` is `decimal(23,13)`), while `NULL / decimal`
/// never reaches the decimal rule and falls back to `Divide`'s default DOUBLE. This
/// only concerns `/`: `+ - *` coerce a NULL operand to the peer's type either way, so
/// they need no special case.
fn coerce_spark_divide_null_operand(
    dividend: Expr,
    divisor: Expr,
    dividend_type: &DataType,
    divisor_type: &DataType,
) -> (Expr, Expr) {
    match (dividend_type, divisor_type) {
        (DataType::Decimal128(_, _) | DataType::Decimal256(_, _), DataType::Null) => {
            let divisor = cast(divisor, dividend_type.clone());
            (dividend, divisor)
        }
        (DataType::Null, DataType::Decimal128(_, _) | DataType::Decimal256(_, _)) => (
            cast(dividend, DataType::Float64),
            cast(divisor, DataType::Float64),
        ),
        _ => (dividend, divisor),
    }
}

/// One operand is a 32-bit `Float` and the other an integral type (either order) — the
/// pair whose ANSI common type is `Double` (a `Float` result would lose precision).
fn is_integral_float_pair(a: &DataType, b: &DataType) -> bool {
    (matches!(a, DataType::Float32) && b.is_integer())
        || (a.is_integer() && matches!(b, DataType::Float32))
}

/// True when one operand is a floating-point type and the other a decimal (either
/// order) — the pair Spark promotes to `DoubleType` in arithmetic.
fn is_float_decimal_pair(a: &DataType, b: &DataType) -> bool {
    (a.is_floating() && is_decimal_type(b)) || (is_decimal_type(a) && b.is_floating())
}

/// When `expr` is an integer literal and `other_type` is a decimal, returns the decimal
/// Spark casts the literal to.
///
/// With `pick_minimum_precision` (the default), that is the minimal decimal holding the
/// literal's *value* (`Decimal(digit_count, 0)`), per `DataTypeUtils.fromLiteral`.
/// DataFusion would instead widen it to the type-based `Decimal(10, 0)`.
///
/// With `spark.sql.legacy.literal.pickMinimumPrecision = false` the narrowing rule does
/// not fire, and the literal falls through to the same type-based decimal any integer
/// *column* gets (`Decimal(10, 0)` for an INT), so `decimal(10,2) * 3` widens from
/// `decimal(12,2)` to `decimal(21,2)`.
/// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/DecimalPrecisionTypeCoercion.scala#L150-L188>
fn spark_decimal_literal_datatype(
    expr: &Expr,
    other_type: &DataType,
    pick_minimum_precision: bool,
) -> Option<DataType> {
    let is_256 = match other_type {
        DataType::Decimal128(_, _) => false,
        DataType::Decimal256(_, _) => true,
        _ => return None,
    };
    let scalar = match expr {
        Expr::Literal(scalar, _) => scalar,
        // A negative integer literal can appear as `Negative(Literal)` in the plan;
        // the digit count is sign-agnostic so only the magnitude matters.
        Expr::Negative(inner) => match inner.as_ref() {
            Expr::Literal(scalar, _) => scalar,
            _ => return None,
        },
        _ => return None,
    };
    let value = scalar_integer_value(scalar)?;
    let precision = if pick_minimum_precision {
        integer_digit_count(value)
    } else {
        match spark_integer_decimal_type(&scalar.data_type())? {
            DataType::Decimal128(precision, _) => precision,
            _ => return None,
        }
    };
    Some(if is_256 {
        DataType::Decimal256(precision, 0)
    } else {
        DataType::Decimal128(precision, 0)
    })
}

/// The integer value of a literal Spark narrows to its minimal decimal.
///
/// `DataTypeUtils.fromLiteral` only narrows Short, Int and Long literals; a Byte
/// literal falls through to `forType(ByteType)` = `Decimal(3, 0)`, so `Int8` is
/// deliberately absent here (`decimal(10,2) * 3Y` is `decimal(14,2)` in Spark, not
/// `decimal(12,2)`). Byte operands are widened by [`coerce_decimal_peer_operand`]
/// like any other integer column.
/// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/types/DataTypeUtils.scala#L253-L257>
fn scalar_integer_value(scalar: &ScalarValue) -> Option<i128> {
    Some(match scalar {
        ScalarValue::Int16(Some(v)) => i128::from(*v),
        ScalarValue::Int32(Some(v)) => i128::from(*v),
        ScalarValue::Int64(Some(v)) => i128::from(*v),
        ScalarValue::UInt16(Some(v)) => i128::from(*v),
        ScalarValue::UInt32(Some(v)) => i128::from(*v),
        ScalarValue::UInt64(Some(v)) => i128::from(*v),
        _ => return None,
    })
}

/// Number of base-10 digits in `value` (sign ignored), minimum 1.
fn integer_digit_count(value: i128) -> u8 {
    // `checked_ilog10` is `None` only for zero, which Spark counts as one digit.
    value.unsigned_abs().checked_ilog10().unwrap_or(0) as u8 + 1
}

/// Returns a guarded divisor expression that handles division by zero at runtime.
///
/// In non-ANSI mode: returns `nullif(divisor, 0)` — evaluates to NULL when divisor is zero.
/// In ANSI mode: returns `CASE WHEN divisor = 0 THEN raise_error(msg) ELSE divisor END`.
///
/// This wraps the divisor itself (not the entire division expression) to avoid
/// duplicating complex divisor expressions (e.g., window functions) in the plan.
///
/// The `nullif` zero takes the divisor's own type. A bare `lit(0)` there makes `nullif`
/// widen the divisor to the common type of the pair, silently changing the operator's
/// result type — `decimal(10,2) % 3` came out as `decimal(10,2)` under ANSI off but
/// `decimal(3,2)` under ANSI on, and Spark's remainder rule does not depend on the ANSI
/// flag. The ANSI branch keeps the untyped zero: there the literal only feeds a
/// comparison (coerced anyway) and the `CASE` takes its type from the else branch, so
/// typing it would only leave a cross-type comparison the optimizer can no longer fold.
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
        // `new_zero` also succeeds for non-numeric types, where its "zero" is a real value
        // (`false`, the epoch date): guarding on it would null out live data instead of a
        // zero divisor. Those operands fail coercion downstream anyway, so keep the
        // untyped literal and let the error come from there.
        let zero = match divisor_type.is_numeric() {
            true => ScalarValue::new_zero(divisor_type).map_or_else(|_| lit(0), lit),
            false => lit(0),
        };
        expr_fn::nullif(divisor, zero)
    }
}

/// The fixed scale increment Arrow's decimal `Op::Div` adds to the dividend's scale
/// (`result_scale = min(s1 + 4, MAX_SCALE)`), following Postgres and MySQL. The decimal
/// division path below rescales the dividend against it to buy the guard digit HALF_UP
/// needs, so a change to this constant upstream silently makes every decimal division
/// one ulp wrong — it is named here so a DataFusion/Arrow bump has to look at it.
/// <https://github.com/apache/arrow-rs/blob/58.3.0/arrow-arith/src/numeric.rs>
const ARROW_DIV_SCALE_INCREMENT: i8 = 4;

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

    let ansi_mode = function_context.plan_config.ansi_mode;
    let allow_precision_loss = function_context
        .plan_config
        .decimal_operations_allow_precision_loss;

    // Under ANSI mode Spark rejects a `/` where a STRING operand has no numeric partner to
    // anchor the implicit cast: `string / string`, `string / NULL` and `NULL / string` all
    // fail analysis (a string paired with a numeric still coerces, and non-ANSI casts both to
    // DOUBLE). Sail's `/` otherwise coerces the string(s) to DOUBLE and computes, where
    // `+`/`-`/`*` already reject the pair — reject it here too so divide matches Spark.
    if ansi_mode
        && let (Ok(dividend_type), Ok(divisor_type)) = (
            dividend.get_type(function_context.schema),
            divisor.get_type(function_context.schema),
        )
        && unanchored_string_pair(operand_role(&dividend_type), operand_role(&divisor_type))
    {
        return Err(arithmetic_operand_error('/', &dividend_type, &divisor_type));
    }

    // DataFusion scales an interval divisor by integers and floats but not by a decimal
    // (`Duration / Decimal128` fails to coerce), while Spark scales an interval by any
    // numeric. Cast a decimal divisor to DOUBLE so the interval division type-checks; the
    // interval result type is unaffected.
    //
    // NOTE: this routes a decimal divisor through Spark's fractional path (double division),
    // whereas Spark's `DivideDTInterval` divides a decimal divisor in `BigDecimal`
    // (`intervalExpressions.scala`), so the scaled micros can differ at a HALF_UP rounding
    // boundary. Exact decimal scaling belongs to the interval-arithmetic follow-up.
    let divisor = match (
        dividend.get_type(function_context.schema),
        divisor.get_type(function_context.schema),
    ) {
        (Ok(dividend_type), Ok(divisor_type))
            if is_interval_like(&dividend_type)
                && matches!(
                    divisor_type,
                    DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
                ) =>
        {
            cast(divisor, DataType::Float64)
        }
        // A STRING divisor scaling an interval is DOUBLE in both ANSI modes — see
        // `coerce_interval_scale_operand`. Anchoring it here rather than leaving it to
        // `coerce_spark_arithmetic_operands` below is what makes the ANSI-on case work: that
        // helper needs a numeric peer to pick the width, and an interval peer is not one.
        (Ok(dividend_type), Ok(divisor_type)) if is_interval_like(&dividend_type) => {
            coerce_interval_scale_operand(divisor, &divisor_type, ansi_mode)
        }
        _ => divisor,
    };

    // Coerce operands the same way `*` does (narrow an integer literal combined with
    // a decimal, promote float×decimal to double) before deriving the division type,
    // because Spark's divide scale depends on the divisor precision.
    let (dividend, divisor) = match (
        dividend.get_type(function_context.schema),
        divisor.get_type(function_context.schema),
    ) {
        (Ok(dividend_type), Ok(divisor_type)) => coerce_spark_arithmetic_operands(
            dividend,
            divisor,
            &dividend_type,
            &divisor_type,
            ansi_mode,
            function_context.plan_config.literal_pick_minimum_precision,
        ),
        _ => (dividend, divisor),
    };

    let (dividend, divisor) = match (
        dividend.get_type(function_context.schema),
        divisor.get_type(function_context.schema),
    ) {
        (Ok(dividend_type), Ok(divisor_type)) => {
            coerce_spark_divide_null_operand(dividend, divisor, &dividend_type, &divisor_type)
        }
        _ => (dividend, divisor),
    };

    let (dividend, divisor) =
        coerce_decimal_peer_operand(dividend, divisor, function_context.schema);

    let dividend_type = dividend.get_type(function_context.schema);
    let divisor_type = divisor.get_type(function_context.schema);

    // Spark's `/` (`inputType = TypeCollection(DoubleType, DecimalType)`) rejects a
    // non-numeric operand at analysis with DATATYPE_MISMATCH; DataFusion would instead
    // reinterpret it (a boolean as 0/1, a timestamp/date as its raw integer, an interval
    // as its raw nanos) and compute a meaningless number. Reject those pairs here so the
    // failure is a plan-time error rather than a silent wrong value. Strings are already
    // coerced to a numeric type upstream, so they never reach here as `Utf8`.
    if let (Ok(dividend_type), Ok(divisor_type)) = (&dividend_type, &divisor_type)
        && (rejects_as_divide_dividend(dividend_type) || rejects_as_divide_divisor(divisor_type))
    {
        return Err(arithmetic_operand_error('/', dividend_type, divisor_type));
    }

    // Apply runtime zero-divisor guard to the divisor before building the division expression.
    let effective_divisor_type = divisor_type.as_ref().cloned().unwrap_or(DataType::Int32);
    let divisor = make_safe_divisor(
        divisor,
        &effective_divisor_type,
        ansi_mode,
        "[DIVIDE_BY_ZERO] Division by zero.",
    );

    let div_expr = match (&dividend_type, &divisor_type) {
        // Spark DECIMAL / DECIMAL: DataFusion (Arrow `div`) uses a smaller scale and
        // truncates, so we compute Spark's `(precision, scale)` and reproduce its
        // HALF_UP value: widen to Decimal256, divide with one guard digit, HALF_UP-round
        // to Spark's scale, then narrow to the target (error on overflow under ANSI,
        // NULL otherwise).
        //
        // Widening to i256 does NOT make the intermediate overflow-proof: Arrow's
        // decimal `div` rescales the numerator by `10^(result_scale - s1 + s2)`, which
        // is `10^(4 + s2)` here, so the intermediate carries
        // `(p1 - s1) + dividend_scale + 4 + s2` digits against i256's ~76. The overflow
        // is value-dependent, not type-dependent — only rows whose rescaled numerator
        // exceeds i256 raise — so this path is always taken rather than gated on the
        // type (rejecting the whole type would send every row to the native divide,
        // which overflows i128 even sooner). The extreme case `decimal(38,38) /
        // decimal(38,38)` is a known gap; see its `@sail-bug` scenario in
        // `arithmetic_coercion.feature`. Emulating Spark exactly needs BigDecimal
        // (`1e38 * 1e39 = 1e77` does not fit i256 either) — the custom PhysicalExpr
        // follow-up.
        // https://github.com/apache/arrow-rs/blob/58.3.0/arrow-arith/src/numeric.rs (Op::Div)
        //
        // Performance: this path adds a HALF_UP `round` pass and, when needed, an i256
        // intermediate. That is the inherent cost of Spark's decimal-division semantics —
        // Spark itself computes it in `BigDecimal`, and no pure-`Expr` alternative is both
        // correct and cheaper. It stays fully vectorized (native Arrow kernels, no UDF
        // dispatch), and only decimal/decimal division pays it; `+ - * %`, integer and float
        // division are unchanged. The i256 widening is GATED: the intermediate stays in the
        // cheaper i128 kernel whenever the rescaled numerator provably fits 38 digits (the
        // common narrow-decimal case), and only widens to i256 when it does not.
        (Ok(DataType::Decimal128(p1, s1)), Ok(DataType::Decimal128(p2, s2))) => {
            let (result_precision, result_scale) =
                spark_decimal_divide_type(*p1, *s1, *p2, *s2, allow_precision_loss);
            // Rescale the dividend so Arrow's quotient carries at least one digit past
            // Spark's scale. One guard digit is exactly enough for HALF_UP over a
            // truncating divide: the digit at `result_scale + 1` survives truncation, so
            // it decides the carry the same way the exact quotient would.
            let dividend_scale = (*s1).max(result_scale - (ARROW_DIV_SCALE_INCREMENT - 1));
            // Arrow's decimal `div` rescales the numerator to
            // `(p1 - s1) + dividend_scale + ARROW_DIV_SCALE_INCREMENT + s2` digits. When that
            // fits Decimal128 the quotient is computed in i128 (2-4x cheaper per row);
            // otherwise it widens to i256. The two are value-identical whenever the
            // intermediate does not overflow, so this is a pure performance gate.
            let intermediate_digits = i32::from(*p1) - i32::from(*s1)
                + i32::from(dividend_scale)
                + i32::from(ARROW_DIV_SCALE_INCREMENT)
                + i32::from(*s2);
            let (dividend_target, divisor_target) =
                if intermediate_digits <= i32::from(DECIMAL128_MAX_PRECISION) {
                    (
                        DataType::Decimal128(DECIMAL128_MAX_PRECISION, dividend_scale),
                        DataType::Decimal128(DECIMAL128_MAX_PRECISION, *s2),
                    )
                } else {
                    (
                        DataType::Decimal256(DECIMAL256_MAX_PRECISION, dividend_scale),
                        DataType::Decimal256(DECIMAL256_MAX_PRECISION, *s2),
                    )
                };
            let quotient = cast(dividend, dividend_target) / cast(divisor, divisor_target);
            let rounded = expr_fn::round(vec![quotient, lit(result_scale as i32)]);
            narrow_decimal_by_ansi(rounded, result_precision, result_scale, ansi_mode)
        }
        // TODO: Casting DataType::Interval(_) to DataType::Int64 is not supported yet.
        //  Seems to be a bug in DataFusion.
        // TODO: Decimal256 operands still use DataFusion's scale (not Spark's);
        //  Decimal256 is Sail-internal only. Integer operands need no arm here: both
        //  literals (narrowed) and columns (type-based decimal) are coerced to
        //  Decimal128 above, so they take the Spark arm.
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

    let ansi_mode = function_context.plan_config.ansi_mode;

    // `IntegralDivide` takes `TypeCollection(LongType, DecimalType, YearMonthIntervalType,
    // DayTimeIntervalType)` and returns `LongType` (arithmetic.scala:890-893), so a STRING
    // operand resolves differently from the other operators, and differently per ANSI mode:
    //   ANSI off -> `PromoteStrings` casts the string to DOUBLE, which is not the peer's
    //               integral type, so Spark rejects with BINARY_OP_DIFF_TYPES.
    //   ANSI on  -> `implicitCast(StringType, TypeCollection(..))` takes the first castable
    //               member, `LongType` (AnsiTypeCoercion.scala:213 falling into :195), so the
    //               string becomes BIGINT and the division proceeds.
    // Two strings have no common integral type and are rejected in both modes.
    let (dividend, divisor) = {
        let (dividend_type, divisor_type) = (
            dividend.get_type(function_context.schema),
            divisor.get_type(function_context.schema),
        );
        match (&dividend_type, &divisor_type) {
            (Ok(left), Ok(right)) if left.is_string() || right.is_string() => {
                if !ansi_mode || (left.is_string() && right.is_string()) {
                    return Err(arithmetic_operand_error('/', left, right));
                }
                (
                    coerce_integral_divide_operand(dividend, left, ansi_mode),
                    coerce_integral_divide_operand(divisor, right, ansi_mode),
                )
            }
            _ => (dividend, divisor),
        }
    };

    let dividend_type = dividend.get_type(function_context.schema);
    let divisor_type = divisor.get_type(function_context.schema);

    // Apply runtime zero-divisor guard to the divisor before building the division expression.
    let effective_divisor_type = divisor_type.as_ref().cloned().unwrap_or(DataType::Int32);
    let divisor = make_safe_divisor(
        divisor,
        &effective_divisor_type,
        ansi_mode,
        "[DIVIDE_BY_ZERO] Division by zero.",
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

/// The operand coercion and result type Spark's remainder rule gives to both `%` and
/// `pmod` — `Pmod` documents itself as following `Remainder`, and the two share one
/// `resultDecimalType`.
///
/// Spark types them from the *original* operand types, which DataFusion cannot do: its
/// coercion unifies both operands to one common type before the result type is computed,
/// so by then the narrow operand's precision is gone. Compute it here, where both are
/// still visible, and let the caller narrow DataFusion's wider result down to it.
///
/// The types are re-derived from the coerced expressions rather than taken from the
/// caller, so a caller that rewrites an operand first (as `pmod` does for a bare NULL)
/// cannot feed a stale type into the rule.
/// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala#L980-L991>
/// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala#L1065-L1071>
fn coerce_spark_remainder_operands(
    left: Expr,
    right: Expr,
    function_context: &FunctionContextInput<'_>,
) -> (Expr, Expr, Option<DataType>) {
    let (Ok(left_type), Ok(right_type)) = (
        left.get_type(function_context.schema),
        right.get_type(function_context.schema),
    ) else {
        return (left, right, None);
    };
    let (left, right) = coerce_spark_arithmetic_operands(
        left,
        right,
        &left_type,
        &right_type,
        function_context.plan_config.ansi_mode,
        function_context.plan_config.literal_pick_minimum_precision,
    );
    // An integer *column* paired with a decimal takes its type-based decimal here too,
    // the way `+ - * /` do it — without this the remainder rule below never sees two
    // decimals and `decimal(3,2) % INT column` keeps DataFusion's `decimal(12,2)`.
    let (left, right) = coerce_decimal_peer_operand(left, right, function_context.schema);
    let remainder_type = match (
        left.get_type(function_context.schema),
        right.get_type(function_context.schema),
    ) {
        (Ok(DataType::Decimal128(p1, s1)), Ok(DataType::Decimal128(p2, s2))) => {
            let (precision, scale) = spark_decimal_remainder_type(
                p1,
                s1,
                p2,
                s2,
                function_context
                    .plan_config
                    .decimal_operations_allow_precision_loss,
            );
            Some(DataType::Decimal128(precision, scale))
        }
        _ => None,
    };
    (left, right, remainder_type)
}

/// Modulo operation with division-by-zero handling.
///
/// Modulo by zero (all numeric types, including float/double) matches Spark's `%`:
/// in ANSI mode it raises an error, in non-ANSI mode it returns NULL — Spark does
/// not fall back to IEEE `NaN` for a zero divisor.
fn spark_modulo(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;

    let (dividend, divisor) = arguments.two()?;
    let ansi_mode = function_context.plan_config.ansi_mode;

    // Spark's `%` (`Remainder`, `inputType = NumericType`) rejects a non-numeric operand on
    // either side at analysis with DATATYPE_MISMATCH; DataFusion would instead reinterpret a
    // boolean/date/timestamp as its raw integer or an interval as its raw nanos and compute a
    // meaningless remainder. Reject those pairs at plan time, matching the sibling `/` (its
    // divisor reject set — numeric-only, intervals included — is exactly `%`'s on both sides).
    // A string paired with a numeric operand is coerced to a numeric type (upstream under ANSI
    // off; via Spark's ANSI string promotion under ANSI on), so it is NOT rejected here. Only a
    // string with no numeric anchor — string×string or string×untyped-NULL — stays non-numeric,
    // which Spark rejects under ANSI on; that is the `rejects_unanchored_string_pair` case below.
    if let (Ok(dividend_type), Ok(divisor_type)) = (
        dividend.get_type(function_context.schema),
        divisor.get_type(function_context.schema),
    ) {
        let string_rejected = rejects_unanchored_string_pair(
            operand_role(&dividend_type),
            operand_role(&divisor_type),
            ansi_mode,
        );
        if string_rejected
            || rejects_as_divide_divisor(&dividend_type)
            || rejects_as_divide_divisor(&divisor_type)
        {
            return Err(arithmetic_operand_error('%', &dividend_type, &divisor_type));
        }
    }

    // Apply Spark operand coercion (e.g. narrow an integer literal combined with a
    // decimal) so the modulo result type matches Spark, before the zero guard.
    let (dividend, divisor, remainder_type) =
        coerce_spark_remainder_operands(dividend, divisor, &function_context);

    let divisor_type = divisor.get_type(function_context.schema);

    // Apply runtime zero-divisor guard to the divisor before building the modulo expression.
    let effective_divisor_type = divisor_type.unwrap_or(DataType::Int32);
    let divisor = make_safe_divisor(
        divisor,
        &effective_divisor_type,
        ansi_mode,
        "[REMAINDER_BY_ZERO] Remainder by zero.",
    );

    let modulo = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(dividend),
        op: Operator::Modulo,
        right: Box::new(divisor),
    });
    // Narrow DataFusion's wider decimal result down to Spark's remainder type, the same
    // way `pmod` does. A remainder is bounded by both operands, so unlike `pmod` this
    // cast cannot overflow; it takes the ANSI gate only to keep the two paths identical.
    Ok(ansi_cast_opt(modulo, remainder_type, ansi_mode))
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
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let ansi_mode = function_context.plan_config.ansi_mode;
    let udf = ScalarUDF::from(SparkPmod::new(ansi_mode));
    // `pmod` is Spark's remainder under another name, so its operands take the same
    // coercion as `%` (float x decimal to double, integer literal narrowed against a
    // decimal).
    if arguments.len() != 2 {
        return Ok(udf.call(arguments));
    }
    let (left, right) = arguments.two()?;
    let (left_type, right_type) = (
        left.get_type(function_context.schema),
        right.get_type(function_context.schema),
    );
    let (Ok(left_type), Ok(right_type)) = (left_type, right_type) else {
        return Ok(udf.call(vec![left, right]));
    };
    // A bare NULL never reaches the remainder rule: `SparkPmod` inherits DataFusion's
    // `Signature::numeric`, whose coercion takes the *first* argument as the seed type
    // without a NULL check, so `pmod(NULL, 3)` fails to plan ("Null and Int32 are not
    // coercible") while `pmod(3, NULL)` happens to work. Spark returns NULL either way,
    // so give the NULL its peer's type up front.
    let (left, right) = match (&left_type, &right_type) {
        (DataType::Null, peer) if peer.is_numeric() => (cast(left, peer.clone()), right),
        (peer, DataType::Null) if peer.is_numeric() => (left, cast(right, peer.clone())),
        _ => (left, right),
    };
    let (left, right, pmod_type) = coerce_spark_remainder_operands(left, right, &function_context);
    // The narrowing can overflow, so it takes the same ANSI gate as `*` and `/`. Unlike
    // `%`, whose result is bounded by the dividend too, `pmod` adds the divisor back
    // (`a % n + n`), so it is only bounded by `|n|` while the remainder type takes
    // `min(p1-s1, p2-s2)`: `pmod(decimal(3,2), decimal(5,0))` is typed `decimal(3,2)`
    // but can reach 99994.00. Spark's CheckOverflow turns that into NULL under ANSI off
    // and raises under ANSI on.
    let call = udf.call(vec![left, right]);
    Ok(ansi_cast_opt(call, pmod_type, ansi_mode))
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

/// Spark unary minus / `negative(x)`. Duration negation goes through
/// `NegateDuration`; everything else uses `SparkNegative`, which honors the ANSI
/// overflow semantics with `ansi_mode` baked at planning time.
fn spark_unary_negate(arg: Expr, ansi_mode: bool, schema: &DFSchemaRef) -> Expr {
    match arg.get_type(schema) {
        // DataFusion's `Negative` doesn't support Duration types, so route those
        // to the dedicated UDF.
        Ok(DataType::Duration(_)) => ScalarUDF::from(NegateDuration::new()).call(vec![arg]),
        // Spark's unary minus coerces strings to DOUBLE before negating, with the
        // same parse the binary operators and `CAST` use: surrounding whitespace is
        // trimmed, and an invalid string is NULL under ANSI off but raises under ANSI
        // on. (Without the cast, the `SparkNegative` signature would coerce the string
        // to an interval instead.)
        Ok(DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => {
            let casted = spark_string_to_numeric(arg, DataType::Float64, !ansi_mode);
            ScalarUDF::from(SparkNegative::new(ansi_mode)).call(vec![casted])
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

/// Spark's unary plus (`UnaryPositive`): a string operand coerces to DOUBLE with the
/// same parse the binary operators, unary minus and `CAST` use — surrounding
/// whitespace is trimmed, and an invalid string is NULL under ANSI off but raises
/// under ANSI on (validated vs Spark 4.2.0: `+'5'` and `positive('5')` are DOUBLE in
/// both ANSI modes). Any other operand passes through unchanged.
fn spark_unary_plus(arg: Expr, ansi_mode: bool, schema: &DFSchemaRef) -> Expr {
    match arg.get_type(schema) {
        Ok(DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => {
            spark_string_to_numeric(arg, DataType::Float64, !ansi_mode)
        }
        _ => arg,
    }
}

/// `positive()` is Spark's `UnaryPositive` under its function name, so it shares
/// [`spark_unary_plus`] with the one-argument `+`.
fn spark_positive(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arg = arguments.one()?;
    Ok(spark_unary_plus(
        arg,
        function_context.plan_config.ansi_mode,
        function_context.schema,
    ))
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
        ("positive", F::custom(spark_positive)),
        ("pow", F::binary(power)),
        ("power", F::binary(power)),
        ("radians", F::unary(double(expr_fn::radians))),
        ("rand", F::udf(Random::new())),
        ("random_poisson", F::udf(RandPoisson::new())),
        ("randn", F::udf(Randn::new())),
        ("random", F::udf(Random::new())),
        ("rint", F::unary(rint)),
        ("round", F::var_arg(expr_fn::round)),
        ("sec", F::unary(double(|arg| lit(1.0) / expr_fn::cos(arg)))),
        ("sign", F::udf(SparkSignum::new())),
        ("signum", F::udf(SparkSignum::new())),
        ("sin", F::unary(double(expr_fn::sin))),
        ("sinh", F::unary(double(expr_fn::sinh))),
        ("sqrt", F::unary(double(expr_fn::sqrt))),
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
