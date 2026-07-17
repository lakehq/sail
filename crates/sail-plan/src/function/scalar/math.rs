use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION, DataType, IntervalUnit, TimeUnit, i256,
};
use datafusion::arrow::error::ArrowError;
use datafusion::functions::expr_fn;
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::{
    BinaryExpr, Expr, ExprSchemable, Operator, ScalarUDF, cast, expr, lit, try_cast,
};
use datafusion_spark::function::math::expr_fn as math_fn;
use half::f16;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::error::generic_exec_err;
use sail_function::scalar::datetime::negate_duration::NegateDuration;
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
use sail_function::scalar::math::spark_round::SparkRound;
use sail_function::scalar::math::spark_signum::SparkSignum;
use sail_function::scalar::math::spark_try_add::SparkTryAdd;
use sail_function::scalar::math::spark_try_div::SparkTryDiv;
use sail_function::scalar::math::spark_try_mod::SparkTryMod;
use sail_function::scalar::math::spark_try_mult::SparkTryMult;
use sail_function::scalar::math::spark_try_subtract::SparkTrySubtract;
use sail_function::scalar::math::spark_unhex::SparkUnHex;
use sail_function::scalar::math::spark_uniform::SparkUniform;
use sail_function::scalar::math::utils::decimal::{
    spark_decimal_add_diverges, spark_decimal_add_type, spark_decimal_divide_type,
    spark_decimal_multiply_type, spark_decimal_remainder_type,
};
use sail_function::scalar::misc::raise_error::RaiseError;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    ScalarFunction, ScalarFunctionInput, is_string_type, spark_string_to_numeric,
};

/// Arguments:
///   - left: A numeric, DATE, TIMESTAMP, or INTERVAL expression.
///   - right: If left is a numeric right must be numeric expression, or an INTERVAL otherwise.
///
/// Returns:
///   - If left is a numeric, the common maximum type of the arguments.
///   - If left is a DATE and right is a day-time interval the result is a TIMESTAMP.
///   - If both expressions are interval they must be of the same class.
///   - Otherwise, the result type matches left.
///
/// All of the above conditions should be handled by the DataFusion.
/// If there is a discrepancy in parity, check the link below and adjust Sail's logic accordingly:
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
            (Ok(left_type), Ok(right_type)) => {
                let (left, right) = coerce_spark_arithmetic_operands(
                    left,
                    right,
                    &left_type,
                    &right_type,
                    function_context.plan_config.ansi_mode,
                    function_context.plan_config.literal_pick_minimum_precision,
                );
                let operands = (
                    left.get_type(function_context.schema),
                    right.get_type(function_context.schema),
                );
                let sum = left + right;
                match operands {
                    (Ok(DataType::Decimal128(p1, s1)), Ok(DataType::Decimal128(p2, s2)))
                        if spark_decimal_add_diverges(
                            p1,
                            s1,
                            p2,
                            s2,
                            function_context
                                .plan_config
                                .decimal_operations_allow_precision_loss,
                        ) =>
                    {
                        spark_decimal_add_retype(
                            sum,
                            p1,
                            s1,
                            p2,
                            s2,
                            function_context
                                .plan_config
                                .decimal_operations_allow_precision_loss,
                        )
                    }
                    _ => sum,
                }
            }
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
        Ok(match (left_type, right_type) {
            (Ok(DataType::Date32), Ok(DataType::Duration(TimeUnit::Microsecond))) => {
                left - cast(right, DataType::Interval(IntervalUnit::MonthDayNano))
            }
            (Ok(DataType::Date32), Ok(right_type)) if right_type.is_numeric() => {
                cast(cast(left, DataType::Int32) - right, DataType::Date32)
            }
            (Ok(left_type), Ok(right_type)) => {
                let (left, right) = coerce_spark_arithmetic_operands(
                    left,
                    right,
                    &left_type,
                    &right_type,
                    function_context.plan_config.ansi_mode,
                    function_context.plan_config.literal_pick_minimum_precision,
                );
                let operands = (
                    left.get_type(function_context.schema),
                    right.get_type(function_context.schema),
                );
                let sum = left - right;
                match operands {
                    (Ok(DataType::Decimal128(p1, s1)), Ok(DataType::Decimal128(p2, s2)))
                        if spark_decimal_add_diverges(
                            p1,
                            s1,
                            p2,
                            s2,
                            function_context
                                .plan_config
                                .decimal_operations_allow_precision_loss,
                        ) =>
                    {
                        spark_decimal_add_retype(
                            sum,
                            p1,
                            s1,
                            p2,
                            s2,
                            function_context
                                .plan_config
                                .decimal_operations_allow_precision_loss,
                        )
                    }
                    _ => sum,
                }
            }
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
            let (left, right) =
                coerce_integer_operand_to_decimal(left, right, function_context.schema);
            // Spark caps a decimal product's precision at 38 by REDUCING the scale
            // (adjustPrecisionScale) and HALF_UP-rounding the value; DataFusion keeps
            // the full scale. Only intervene when the product would exceed precision 38
            // — the common (non-capped) product is exact and stays native.
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
                    let rounded = expr_fn::round(vec![product, lit(i32::from(result_scale))]);
                    let target = DataType::Decimal128(result_precision, result_scale);
                    if ansi_mode {
                        cast(rounded, target)
                    } else {
                        try_cast(rounded, target)
                    }
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
/// This only fixes the TYPE. The sum itself stays on the native kernel, so its overflow
/// behaviour is untouched (that is the custom-`PhysicalExpr` follow-up's job). The final
/// `cast` cannot add an error path: it is only reached when Spark's scale is strictly
/// below the native one (the gate skips the equal case), so the round drops digits and the
/// value fits `(38-s+1)+n <= 38` — it never narrows into an overflow the native sum did
/// not already have.
fn spark_decimal_add_retype(
    sum: Expr,
    p1: u8,
    s1: i8,
    p2: u8,
    s2: i8,
    allow_precision_loss: bool,
) -> Expr {
    let (result_precision, result_scale) =
        spark_decimal_add_type(p1, s1, p2, s2, allow_precision_loss);
    let rounded = expr_fn::round(vec![sum, lit(i32::from(result_scale))]);
    cast(
        rounded,
        DataType::Decimal128(result_precision, result_scale),
    )
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
    // (validated vs Spark 4.1.1):
    //   ANSI off -> a string paired with a string, numeric or NULL promotes BOTH to
    //               DOUBLE, and a string that does not parse yields NULL rather than
    //               an error (so the cast must be a `try_cast`).
    //   ANSI on  -> a string paired with an INTEGRAL numeric promotes to BIGINT, and
    //               with a FRACTIONAL numeric (float or decimal) to DOUBLE. The cast
    //               is strict: a malformed string raises. `string + string` and
    //               `string + NULL` are left as-is; Spark rejects both.
    // https://github.com/apache/spark/blob/v4.1.1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TypeCoercion.scala (PromoteStrings)
    // https://github.com/apache/spark/blob/v4.1.1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/AnsiTypeCoercion.scala
    let left_string = is_string_type(left_type);
    let right_string = is_string_type(right_type);
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
                    coerce_string_operand(left, left_string, &DataType::Float64, true),
                    coerce_string_operand(right, right_string, &DataType::Float64, true),
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
                // Only the string is parsed; DataFusion widens the numeric peer to the
                // same type, which is what makes the result BIGINT/DOUBLE like Spark.
                return (
                    coerce_string_operand(left, left_string, &target, false),
                    coerce_string_operand(right, right_string, &target, false),
                );
            }
        }
    }
    // FLOAT/DOUBLE x DECIMAL -> DOUBLE.
    // https://github.com/apache/spark/blob/v4.1.1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/DecimalPrecision.scala
    if is_float_decimal_pair(left_type, right_type) {
        return (
            cast(left, DataType::Float64),
            cast(right, DataType::Float64),
        );
    }
    // integer literal x DECIMAL -> narrow the literal to its minimal decimal.
    // https://github.com/apache/spark/blob/v4.1.1/sql/catalyst/src/main/scala/org/apache/spark/sql/types/DecimalType.scala
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

/// Applies Spark's string-to-number parse to a string operand, leaving a non-string
/// operand alone. Shared with `CAST` and the type constructors so all three agree on
/// trimming and on NULL-vs-raise.
fn coerce_string_operand(
    expr: Expr,
    is_string: bool,
    target: &DataType,
    null_on_failure: bool,
) -> Expr {
    if is_string {
        spark_string_to_numeric(expr, target.clone(), null_on_failure)
    } else {
        expr
    }
}

fn is_decimal_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
    )
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

/// Casts an integer operand paired with a decimal to its Spark type-based decimal
/// (`Int -> Decimal(10,0)`, ...) so the decimal arithmetic rule applies to integer
/// *columns* — bare integer literals are narrowed separately by
/// [`coerce_spark_arithmetic_operands`]. Used by `/` and `*` (the operators whose
/// decimal result type depends on the widened integer's precision).
fn coerce_integer_operand_to_decimal(
    left: Expr,
    right: Expr,
    schema: &DFSchemaRef,
) -> (Expr, Expr) {
    match (left.get_type(schema), right.get_type(schema)) {
        (Ok(left_type), Ok(right_type)) if is_decimal_type(&right_type) => {
            match spark_integer_decimal_type(&left_type) {
                Some(target) => (cast(left, target), right),
                None => (left, right),
            }
        }
        (Ok(left_type), Ok(right_type)) if is_decimal_type(&left_type) => {
            match spark_integer_decimal_type(&right_type) {
                Some(target) => (left, cast(right, target)),
                None => (left, right),
            }
        }
        _ => (left, right),
    }
}

/// Spark's NullType coercion for `/`, which is asymmetric (validated vs Spark 4.1.1).
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

/// True when one operand is a floating-point type and the other a decimal (either
/// order) — the pair Spark promotes to `DoubleType` in arithmetic.
fn is_float_decimal_pair(a: &DataType, b: &DataType) -> bool {
    fn is_decimal(dt: &DataType) -> bool {
        matches!(dt, DataType::Decimal128(_, _) | DataType::Decimal256(_, _))
    }
    (a.is_floating() && is_decimal(b)) || (is_decimal(a) && b.is_floating())
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
/// <https://github.com/apache/spark/blob/v4.1.1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/DecimalPrecisionTypeCoercion.scala#L150-L188>
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
/// `DecimalType.fromLiteral` only narrows Short, Int and Long literals; a Byte
/// literal falls through to `forType(ByteType)` = `Decimal(3, 0)`, so `Int8` is
/// deliberately absent here (`decimal(10,2) * 3Y` is `decimal(14,2)` in Spark, not
/// `decimal(12,2)`). Byte operands are widened by [`coerce_integer_operand_to_decimal`]
/// like any other integer column.
/// <https://github.com/apache/spark/blob/v4.1.1/sql/catalyst/src/main/scala/org/apache/spark/sql/types/DecimalType.scala>
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
    let mut n = value.unsigned_abs();
    let mut digits = 0u8;
    loop {
        digits += 1;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    digits
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
    let allow_precision_loss = function_context
        .plan_config
        .decimal_operations_allow_precision_loss;

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
        coerce_integer_operand_to_decimal(dividend, divisor, function_context.schema);

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
        // Performance: this path is heavier than the previous native i128 divide (it
        // widens to i256 and adds a HALF_UP `round` pass). That is the inherent cost
        // of Spark's decimal-division semantics — Spark itself computes it in
        // `BigDecimal`, and no pure-`Expr` alternative is both correct and cheaper.
        // It stays fully vectorized (native Arrow kernels, no UDF dispatch), and only
        // decimal/decimal division pays it; `+ - * %`, integer and float division are
        // unchanged. A future optimization could keep the intermediate in i128 when it
        // provably cannot overflow, instead of always widening to i256.
        (Ok(DataType::Decimal128(p1, s1)), Ok(DataType::Decimal128(p2, s2))) => {
            let (result_precision, result_scale) =
                spark_decimal_divide_type(*p1, *s1, *p2, *s2, allow_precision_loss);
            let dividend_scale = (*s1).max(result_scale - 3);
            let quotient = cast(
                dividend,
                DataType::Decimal256(DECIMAL256_MAX_PRECISION, dividend_scale),
            ) / cast(divisor, DataType::Decimal256(DECIMAL256_MAX_PRECISION, *s2));
            let rounded = expr_fn::round(vec![quotient, lit(result_scale as i32)]);
            let target = DataType::Decimal128(result_precision, result_scale);
            if ansi_mode {
                cast(rounded, target)
            } else {
                try_cast(rounded, target)
            }
        }
        // TODO: Casting DataType::Interval(_) to DataType::Int64 is not supported yet.
        //  Seems to be a bug in DataFusion.
        // TODO: DECIMAL / integer-column and Decimal256 operands still use DataFusion's
        //  scale (not Spark's). Integer *literals* are already narrowed above so
        //  DECIMAL / int-literal takes the Spark arm; Decimal256 is Sail-internal only.
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

    // Plan-time check for literal zero divisors.
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

    // Apply Spark operand coercion (e.g. narrow an integer literal combined with a
    // decimal) so the modulo result type matches Spark, before the zero guard.
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

    // Plan-time check for literal zero divisors.
    if is_zero_literal(&divisor) {
        if ansi_mode {
            return Err(PlanError::ArrowError(ArrowError::ArithmeticOverflow(
                "Remainder by zero".to_string(),
            )));
        } else {
            return Ok(Expr::Literal(ScalarValue::Null, None));
        }
    }

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
    let (left, right) = coerce_spark_arithmetic_operands(
        left,
        right,
        &left_type,
        &right_type,
        ansi_mode,
        function_context.plan_config.literal_pick_minimum_precision,
    );
    // Spark types `pmod` by the remainder rule over the *original* operand types. The
    // UDF cannot: `Signature::numeric` unifies both operands to one common type before
    // `return_type` runs, so by then the narrow operand's precision is gone. Compute
    // the type here, where both are still visible, and narrow the UDF's wider result
    // down to it.
    //
    // The narrowing can overflow, so it takes the same ANSI gate as `*` and `/`. Unlike
    // `%`, whose result is bounded by the dividend too, `pmod` adds the divisor back
    // (`a % n + n`), so it is only bounded by `|n|` while the remainder type takes
    // `min(p1-s1, p2-s2)`: `pmod(decimal(3,2), decimal(5,0))` is typed `decimal(3,2)`
    // but can reach 99994.00. Spark's CheckOverflow turns that into NULL under ANSI off
    // and raises under ANSI on.
    let pmod_type = match (
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
    let call = udf.call(vec![left, right]);
    Ok(match pmod_type {
        Some(target) if ansi_mode => cast(call, target),
        Some(target) => try_cast(call, target),
        None => call,
    })
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
        // Spark's unary minus coerces strings to DOUBLE before negating. The
        // cast honors ANSI mode: an invalid string is NULL under ANSI off and
        // errors under ANSI on. (Without this, the `SparkNegative` signature
        // would coerce the string to an interval instead.)
        Ok(DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => {
            let casted = if ansi_mode {
                cast(arg, DataType::Float64)
            } else {
                try_cast(arg, DataType::Float64)
            };
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
        ("round", F::udf(SparkRound::new())),
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
