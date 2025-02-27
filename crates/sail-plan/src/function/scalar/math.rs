use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::{expr, BinaryExpr, ExprSchemable, Operator};

use crate::error::PlanResult;
use crate::extension::function::math::least_greatest;
use crate::extension::function::math::randn::Randn;
use crate::extension::function::math::random::Random;
use crate::extension::function::math::spark_abs::SparkAbs;
use crate::extension::function::math::spark_hex_unhex::{SparkHex, SparkUnHex};
use crate::extension::function::math::spark_signum::SparkSignum;
use crate::function::common::{Function, FunctionInput};
use crate::utils::ItemTaker;

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
fn plus(input: FunctionInput) -> PlanResult<expr::Expr> {
    let FunctionInput {
        arguments, schema, ..
    } = input;
    if arguments.len() < 2 {
        Ok(arguments.one()?)
    } else {
        let (left, right) = arguments.two()?;
        let (left_type, right_type) = (left.get_type(schema), right.get_type(schema));
        match (left_type, right_type) {
            // TODO: In case getting the type fails, we don't want to fail the query.
            //  Future work is needed here, ideally we create something like `Operator::SparkPlus`.
            (Err(_), _) | (_, Err(_)) => Ok(expr::Expr::BinaryExpr(BinaryExpr {
                left: Box::new(left),
                op: Operator::Plus,
                right: Box::new(right),
            })),
            (Ok(left_type), Ok(right_type)) => {
                if (left_type.is_numeric() && matches!(right_type, DataType::Date32))
                    || (right_type.is_numeric() && matches!(left_type, DataType::Date32))
                {
                    let (left, right) = if left_type.is_numeric() {
                        (
                            left,
                            expr::Expr::Cast(expr::Cast {
                                expr: Box::new(right),
                                data_type: DataType::Int32,
                            }),
                        )
                    } else {
                        (
                            expr::Expr::Cast(expr::Cast {
                                expr: Box::new(left),
                                data_type: DataType::Int32,
                            }),
                            right,
                        )
                    };
                    let expr = expr::Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(left),
                        op: Operator::Plus,
                        right: Box::new(right),
                    });
                    Ok(expr::Expr::Cast(expr::Cast {
                        expr: Box::new(expr),
                        data_type: DataType::Date32,
                    }))
                } else {
                    Ok(expr::Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(left),
                        op: Operator::Plus,
                        right: Box::new(right),
                    }))
                }
            }
        }
    }
}

/// Arguments:
///   - left: A numeric, DATE, TIMESTAMP, or INTERVAL expression.
///   - right: The accepted type depends on the type of expr:
///            - If left is a numeric right must be numeric expression.
///            - If left is a year-month or day-time interval, right must be the same class.
///            - Otherwise right must be a DATE or TIMESTAMP.
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
fn minus(input: FunctionInput) -> PlanResult<expr::Expr> {
    let FunctionInput {
        arguments, schema, ..
    } = input;
    if arguments.len() < 2 {
        Ok(expr::Expr::Negative(Box::new(arguments.one()?)))
    } else {
        let (left, right) = arguments.two()?;
        let (left_type, right_type) = (left.get_type(schema), right.get_type(schema));
        match (left_type, right_type) {
            // TODO: In case getting the type fails, we don't want to fail the query.
            //  Future work is needed here, ideally we create something like `Operator::SparkMinus`.
            (Err(_), _) | (_, Err(_)) => Ok(expr::Expr::BinaryExpr(BinaryExpr {
                left: Box::new(left),
                op: Operator::Minus,
                right: Box::new(right),
            })),
            (Ok(left_type), Ok(right_type)) => {
                if matches!(left_type, DataType::Date32) && right_type.is_numeric() {
                    let left = expr::Expr::Cast(expr::Cast {
                        expr: Box::new(left),
                        data_type: DataType::Int32,
                    });
                    let expr = expr::Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(left),
                        op: Operator::Minus,
                        right: Box::new(right),
                    });
                    Ok(expr::Expr::Cast(expr::Cast {
                        expr: Box::new(expr),
                        data_type: DataType::Date32,
                    }))
                } else {
                    Ok(expr::Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(left),
                        op: Operator::Minus,
                        right: Box::new(right),
                    }))
                }
            }
        }
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
fn spark_divide(input: FunctionInput) -> PlanResult<expr::Expr> {
    let FunctionInput {
        arguments, schema, ..
    } = input;

    let (dividend, divisor) = arguments.two()?;
    let (dividend_type, divisor_type) = (dividend.get_type(schema), divisor.get_type(schema));
    match (dividend_type, divisor_type) {
        // TODO: In case getting the type fails, we don't want to fail the query.
        //  Future work is needed here, ideally we create something like `Operator::SparkDivide`.
        (Err(_), _) | (_, Err(_)) => Ok(expr::Expr::BinaryExpr(BinaryExpr {
            left: Box::new(dividend),
            op: Operator::Divide,
            right: Box::new(divisor),
        })),
        (Ok(dividend_type), Ok(divisor_type)) => {
            match (dividend_type, divisor_type) {
                (DataType::Decimal128(_, _), DataType::Decimal128(_, _))
                            | (DataType::Decimal128(_, _), DataType::Decimal256(_, _))
                            | (DataType::Decimal256(_, _), DataType::Decimal128(_, _))
                            | (DataType::Decimal256(_, _), DataType::Decimal256(_, _))
                            | (DataType::Interval(IntervalUnit::YearMonth), _)
                            | (DataType::Interval(IntervalUnit::DayTime), _)
                            // Match duration because we cast Spark's DayTime interval to Duration in `sail-spark-connect`.
                            | (DataType::Duration(TimeUnit::Microsecond), _) => {
                            Ok(expr::Expr::BinaryExpr(BinaryExpr {
                                left: Box::new(dividend),
                                op: Operator::Divide,
                                right: Box::new(divisor),
                            }))
                        }
                        _ => {
                            Ok(expr::Expr::Cast(expr::Cast {
                                expr: Box::new(expr::Expr::BinaryExpr(BinaryExpr {
                                    left: Box::new(dividend),
                                    op: Operator::Divide,
                                    right: Box::new(divisor),
                                })),
                                data_type: DataType::Float64,
                            }))
                        }
            }
        }
    }
}

fn ceil(num: expr::Expr) -> expr::Expr {
    expr::Expr::Cast(expr::Cast {
        expr: Box::new(expr_fn::ceil(num)),
        data_type: DataType::Int64,
    })
}

fn floor(num: expr::Expr) -> expr::Expr {
    expr::Expr::Cast(expr::Cast {
        expr: Box::new(expr_fn::floor(num)),
        data_type: DataType::Int64,
    })
}

fn power(base: expr::Expr, exponent: expr::Expr) -> expr::Expr {
    expr::Expr::Cast(expr::Cast {
        expr: Box::new(expr_fn::power(base, exponent)),
        data_type: DataType::Float64,
    })
}

// FIXME: Implement the UDF for better numerical precision.
fn expm1(input: FunctionInput) -> PlanResult<expr::Expr> {
    let num = input.arguments.one()?;
    let name = input.argument_names.to_vec().one()?;
    minus(FunctionInput {
        arguments: vec![
            expr_fn::exp(num),
            expr::Expr::Literal(ScalarValue::Float64(Some(1.0))),
        ],
        argument_names: &[name, "1.0".to_string()],
        plan_config: input.plan_config,
        session_context: input.session_context,
        schema: input.schema,
    })
}

fn hypot(expr1: expr::Expr, expr2: expr::Expr) -> expr::Expr {
    let expr1 = expr::Expr::BinaryExpr(BinaryExpr {
        left: Box::new(expr1.clone()),
        op: Operator::Multiply,
        right: Box::new(expr1),
    });
    let expr2 = expr::Expr::BinaryExpr(BinaryExpr {
        left: Box::new(expr2.clone()),
        op: Operator::Multiply,
        right: Box::new(expr2),
    });
    let sum = expr::Expr::BinaryExpr(BinaryExpr {
        left: Box::new(expr1),
        op: Operator::Plus,
        right: Box::new(expr2),
    });
    expr::Expr::Cast(expr::Cast {
        expr: Box::new(expr_fn::sqrt(sum)),
        data_type: DataType::Float64,
    })
}

fn log1p(expr: expr::Expr) -> expr::Expr {
    let expr = expr::Expr::BinaryExpr(BinaryExpr {
        left: Box::new(expr),
        op: Operator::Plus,
        right: Box::new(expr::Expr::Literal(ScalarValue::Float64(Some(1.0)))),
    });
    expr_fn::ln(expr)
}

fn positive(expr: expr::Expr) -> expr::Expr {
    expr
}

fn rint(expr: expr::Expr) -> expr::Expr {
    expr::Expr::Cast(expr::Cast {
        expr: Box::new(expr_fn::round(vec![expr])),
        data_type: DataType::Float64,
    })
}

fn spark_ln(expr: expr::Expr) -> expr::Expr {
    expr::Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![(
            Box::new(
                expr.clone()
                    .eq(expr::Expr::Literal(ScalarValue::Int64(Some(0)))),
            ),
            Box::new(expr::Expr::Literal(ScalarValue::Null)),
        )],
        else_expr: Some(Box::new(expr_fn::ln(expr))),
    })
}

pub(super) fn list_built_in_math_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("%", F::binary_op(Operator::Modulo)),
        ("*", F::binary_op(Operator::Multiply)),
        ("+", F::custom(plus)),
        ("-", F::custom(minus)),
        ("/", F::custom(spark_divide)),
        ("abs", F::udf(SparkAbs::new())),
        ("acos", F::unary(expr_fn::acos)),
        ("acosh", F::unary(expr_fn::acosh)),
        ("asin", F::unary(expr_fn::asin)),
        ("asinh", F::unary(expr_fn::asinh)),
        ("atan", F::unary(expr_fn::atan)),
        ("atan2", F::binary(expr_fn::atan2)),
        ("atanh", F::unary(expr_fn::atanh)),
        ("bin", F::unknown("bin")),
        ("bround", F::unknown("bround")),
        ("cbrt", F::unary(expr_fn::cbrt)),
        ("ceil", F::unary(ceil)),
        ("ceiling", F::unary(ceil)),
        ("conv", F::unknown("conv")),
        ("cos", F::unary(expr_fn::cos)),
        ("cosh", F::unary(expr_fn::cosh)),
        ("cot", F::unary(expr_fn::cot)),
        ("csc", F::unknown("csc")),
        ("degrees", F::unary(expr_fn::degrees)),
        ("div", F::custom(spark_divide)),
        ("e", F::unknown("e")),
        ("exp", F::unary(expr_fn::exp)),
        ("expm1", F::custom(expm1)),
        ("factorial", F::unary(expr_fn::factorial)),
        ("floor", F::unary(floor)),
        ("greatest", F::udf(least_greatest::Greatest::new())),
        ("hex", F::udf(SparkHex::new())),
        ("hypot", F::binary(hypot)),
        ("least", F::udf(least_greatest::Least::new())),
        ("ln", F::unary(spark_ln)),
        ("log", F::binary(expr_fn::log)),
        ("log10", F::unary(expr_fn::log10)),
        ("log1p", F::unary(log1p)),
        ("log2", F::unary(expr_fn::log2)),
        ("mod", F::binary_op(Operator::Modulo)),
        ("negative", F::unary(|x| expr::Expr::Negative(Box::new(x)))),
        ("pi", F::nullary(expr_fn::pi)),
        ("pmod", F::unknown("pmod")),
        ("positive", F::unary(positive)),
        ("pow", F::binary(power)),
        ("power", F::binary(power)),
        ("radians", F::unary(expr_fn::radians)),
        ("rand", F::udf(Random::new())),
        ("randn", F::udf(Randn::new())),
        ("random", F::udf(Random::new())),
        ("rint", F::unary(rint)),
        ("round", F::var_arg(expr_fn::round)),
        ("sec", F::unknown("sec")),
        ("shiftleft", F::binary_op(Operator::BitwiseShiftLeft)),
        ("sign", F::udf(SparkSignum::new())),
        ("signum", F::udf(SparkSignum::new())),
        ("sin", F::unary(expr_fn::sin)),
        ("sinh", F::unary(expr_fn::sinh)),
        ("sqrt", F::unary(expr_fn::sqrt)),
        ("tan", F::unary(expr_fn::tan)),
        ("tanh", F::unary(expr_fn::tanh)),
        ("try_add", F::unknown("try_add")),
        ("try_divide", F::unknown("try_divide")),
        ("try_multiply", F::unknown("try_multiply")),
        ("try_subtract", F::unknown("try_subtract")),
        ("unhex", F::udf(SparkUnHex::new())),
        ("width_bucket", F::unknown("width_bucket")),
    ]
}
