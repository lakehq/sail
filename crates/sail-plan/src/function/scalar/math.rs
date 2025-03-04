use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion::arrow::error::ArrowError;
use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::{expr, BinaryExpr, Expr, ExprSchemable, Operator, ScalarUDF};
use half::f16;

use crate::error::{PlanError, PlanResult};
use crate::extension::function::error_utils::generic_exec_err;
use crate::extension::function::math::least_greatest;
use crate::extension::function::math::randn::Randn;
use crate::extension::function::math::random::Random;
use crate::extension::function::math::spark_abs::SparkAbs;
use crate::extension::function::math::spark_bin::SparkBin;
use crate::extension::function::math::spark_ceil_floor::{SparkCeil, SparkFloor};
use crate::extension::function::math::spark_expm1::SparkExpm1;
use crate::extension::function::math::spark_hex_unhex::{SparkHex, SparkUnHex};
use crate::extension::function::math::spark_pmod::SparkPmod;
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
fn spark_plus(input: FunctionInput) -> PlanResult<Expr> {
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
            (Err(_), _) | (_, Err(_)) => Ok(Expr::BinaryExpr(BinaryExpr {
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
                            Expr::Cast(expr::Cast {
                                expr: Box::new(right),
                                data_type: DataType::Int32,
                            }),
                        )
                    } else {
                        (
                            Expr::Cast(expr::Cast {
                                expr: Box::new(left),
                                data_type: DataType::Int32,
                            }),
                            right,
                        )
                    };
                    let expr = Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(left),
                        op: Operator::Plus,
                        right: Box::new(right),
                    });
                    Ok(Expr::Cast(expr::Cast {
                        expr: Box::new(expr),
                        data_type: DataType::Date32,
                    }))
                } else {
                    Ok(Expr::BinaryExpr(BinaryExpr {
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
fn spark_minus(input: FunctionInput) -> PlanResult<Expr> {
    let FunctionInput {
        arguments, schema, ..
    } = input;
    if arguments.len() < 2 {
        Ok(Expr::Negative(Box::new(arguments.one()?)))
    } else {
        let (left, right) = arguments.two()?;
        let (left_type, right_type) = (left.get_type(schema), right.get_type(schema));
        match (left_type, right_type) {
            // TODO: In case getting the type fails, we don't want to fail the query.
            //  Future work is needed here, ideally we create something like `Operator::SparkMinus`.
            (Err(_), _) | (_, Err(_)) => Ok(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(left),
                op: Operator::Minus,
                right: Box::new(right),
            })),
            (Ok(left_type), Ok(right_type)) => {
                if matches!(left_type, DataType::Date32) && right_type.is_numeric() {
                    let left = Expr::Cast(expr::Cast {
                        expr: Box::new(left),
                        data_type: DataType::Int32,
                    });
                    let expr = Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(left),
                        op: Operator::Minus,
                        right: Box::new(right),
                    });
                    Ok(Expr::Cast(expr::Cast {
                        expr: Box::new(expr),
                        data_type: DataType::Date32,
                    }))
                } else {
                    Ok(Expr::BinaryExpr(BinaryExpr {
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
fn spark_multiply(input: FunctionInput) -> PlanResult<Expr> {
    let FunctionInput {
        arguments, schema, ..
    } = input;

    let (left, right) = arguments.two()?;
    let (left_type, right_type) = (left.get_type(schema), right.get_type(schema));
    match (left_type, right_type) {
        // TODO: In case getting the type fails, we don't want to fail the query.
        //  Future work is needed here, ideally we create something like `Operator::SparkMultiply`.
        (Err(_), _) | (_, Err(_)) => Ok(Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op: Operator::Multiply,
            right: Box::new(right),
        })),
        (Ok(left_type), Ok(right_type)) => {
            // TODO: Casting DataType::Interval(_) to DataType::Int64 is not supported yet.
            //  Seems to be a bug in DataFusion.
            match (left_type, right_type) {
                (DataType::Duration(TimeUnit::Microsecond), _) => {
                    // Match duration because we cast Spark's DayTime interval to Duration.
                    Ok(Expr::Cast(expr::Cast {
                        expr: Box::new(Expr::BinaryExpr(BinaryExpr {
                            left: Box::new(Expr::Cast(expr::Cast {
                                expr: Box::new(left),
                                data_type: DataType::Int64,
                            })),
                            op: Operator::Multiply,
                            right: Box::new(right),
                        })),
                        data_type: DataType::Duration(TimeUnit::Microsecond),
                    }))
                }
                (_, DataType::Duration(TimeUnit::Microsecond)) => {
                    // Match duration because we cast Spark's DayTime interval to Duration.
                    Ok(Expr::Cast(expr::Cast {
                        expr: Box::new(Expr::BinaryExpr(BinaryExpr {
                            left: Box::new(left),
                            op: Operator::Multiply,
                            right: Box::new(Expr::Cast(expr::Cast {
                                expr: Box::new(right),
                                data_type: DataType::Int64,
                            })),
                        })),
                        data_type: DataType::Duration(TimeUnit::Microsecond),
                    }))
                }
                _ => Ok(Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(left),
                    op: Operator::Multiply,
                    right: Box::new(right),
                })),
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
fn spark_divide(input: FunctionInput) -> PlanResult<Expr> {
    let FunctionInput {
        arguments,
        schema,
        plan_config,
        ..
    } = input;

    let (dividend, divisor) = arguments.two()?;
    if matches!(
        divisor,
        Expr::Literal(ScalarValue::Int8(Some(0)))
            | Expr::Literal(ScalarValue::Int16(Some(0)))
            | Expr::Literal(ScalarValue::Int32(Some(0)))
            | Expr::Literal(ScalarValue::Int64(Some(0)))
            | Expr::Literal(ScalarValue::UInt8(Some(0)))
            | Expr::Literal(ScalarValue::UInt16(Some(0)))
            | Expr::Literal(ScalarValue::UInt32(Some(0)))
            | Expr::Literal(ScalarValue::UInt64(Some(0)))
            | Expr::Literal(ScalarValue::Float32(Some(0.0)))
            | Expr::Literal(ScalarValue::Float64(Some(0.0)))
    ) || divisor == Expr::Literal(ScalarValue::Float16(Some(f16::from_f32(0.0))))
    {
        // FIXME: Account for array input.
        let ansi_mode = plan_config.ansi_mode;
        return if ansi_mode {
            Err(PlanError::ArrowError(ArrowError::DivideByZero))
        } else {
            Ok(Expr::Literal(ScalarValue::Null))
        };
    }

    let (dividend_type, divisor_type) = (dividend.get_type(schema), divisor.get_type(schema));
    match (dividend_type, divisor_type) {
        // TODO: In case getting the type fails, we don't want to fail the query.
        //  Future work is needed here, ideally we create something like `Operator::SparkDivide`.
        (Err(_), _) | (_, Err(_)) => Ok(Expr::BinaryExpr(BinaryExpr {
            left: Box::new(dividend),
            op: Operator::Divide,
            right: Box::new(divisor),
        })),
        (Ok(dividend_type), Ok(divisor_type)) => {
            // TODO: Casting DataType::Interval(_) to DataType::Int64 is not supported yet.
            //  Seems to be a bug in DataFusion.
            match (dividend_type, divisor_type) {
                (DataType::Decimal128(_, _), _)
                | (_, DataType::Decimal128(_, _))
                | (DataType::Decimal256(_, _), _)
                | (_, DataType::Decimal256(_, _))
                | (DataType::Interval(IntervalUnit::YearMonth), _)
                | (DataType::Interval(IntervalUnit::DayTime), _) => {
                    // TODO: Cast the precision and scale that matches the Spark's behavior after the division.
                    //  See `test_divide` in python/pysail/tests/spark/test_math.py
                    Ok(Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(dividend),
                        op: Operator::Divide,
                        right: Box::new(divisor),
                    }))
                }
                (DataType::Duration(TimeUnit::Microsecond), _) => {
                    // Match duration because we cast Spark's DayTime interval to Duration.
                    Ok(Expr::Cast(expr::Cast {
                        expr: Box::new(Expr::BinaryExpr(BinaryExpr {
                            left: Box::new(Expr::Cast(expr::Cast {
                                expr: Box::new(dividend),
                                data_type: DataType::Int64,
                            })),
                            op: Operator::Divide,
                            right: Box::new(divisor),
                        })),
                        data_type: DataType::Duration(TimeUnit::Microsecond),
                    }))
                }
                _ => Ok(Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(Expr::Cast(expr::Cast {
                        expr: Box::new(dividend),
                        data_type: DataType::Float64,
                    })),
                    op: Operator::Divide,
                    right: Box::new(Expr::Cast(expr::Cast {
                        expr: Box::new(divisor),
                        data_type: DataType::Float64,
                    })),
                })),
            }
        }
    }
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
fn spark_div(input: FunctionInput) -> PlanResult<Expr> {
    let FunctionInput {
        arguments, schema, ..
    } = input;

    let (dividend, divisor) = arguments.two()?;
    let (dividend_type, divisor_type) = (dividend.get_type(schema), divisor.get_type(schema));
    let expr = match (dividend_type, divisor_type) {
        // TODO: In case getting the type fails, we don't want to fail the query.
        //  Future work is needed here, ideally we create something like `Operator::SparkDivide`.
        (Err(_), _) | (_, Err(_)) => Expr::BinaryExpr(BinaryExpr {
            left: Box::new(dividend),
            op: Operator::Divide,
            right: Box::new(divisor),
        }),
        (Ok(dividend_type), Ok(divisor_type)) => match (&dividend_type, &divisor_type) {
            // TODO: Casting DataType::Interval(_) to DataType::Int64 is not supported yet.
            //  Seems to be a bug in DataFusion.
            (DataType::Duration(_), DataType::Duration(_)) => Expr::BinaryExpr(BinaryExpr {
                // Match duration because we cast Spark's DayTime interval to Duration.
                left: Box::new(Expr::Cast(expr::Cast {
                    expr: Box::new(dividend),
                    data_type: DataType::Int64,
                })),
                op: Operator::Divide,
                right: Box::new(Expr::Cast(expr::Cast {
                    expr: Box::new(divisor),
                    data_type: DataType::Int64,
                })),
            }),
            _ => Expr::BinaryExpr(BinaryExpr {
                left: Box::new(dividend),
                op: Operator::Divide,
                right: Box::new(divisor),
            }),
        },
    };
    // We need this final cast because we are doing integer division.
    Ok(Expr::Cast(expr::Cast {
        expr: Box::new(expr),
        data_type: DataType::Int64,
    }))
}

fn power(base: Expr, exponent: Expr) -> Expr {
    Expr::Cast(expr::Cast {
        expr: Box::new(expr_fn::power(base, exponent)),
        data_type: DataType::Float64,
    })
}

fn hypot(expr1: Expr, expr2: Expr) -> Expr {
    let expr1 = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(expr1.clone()),
        op: Operator::Multiply,
        right: Box::new(expr1),
    });
    let expr2 = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(expr2.clone()),
        op: Operator::Multiply,
        right: Box::new(expr2),
    });
    let sum = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(expr1),
        op: Operator::Plus,
        right: Box::new(expr2),
    });
    Expr::Cast(expr::Cast {
        expr: Box::new(expr_fn::sqrt(sum)),
        data_type: DataType::Float64,
    })
}

fn log1p(expr: Expr) -> Expr {
    let expr = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(expr),
        op: Operator::Plus,
        right: Box::new(Expr::Literal(ScalarValue::Float64(Some(1.0)))),
    });
    expr_fn::ln(expr)
}

fn positive(expr: Expr) -> Expr {
    expr
}

fn rint(expr: Expr) -> Expr {
    Expr::Cast(expr::Cast {
        expr: Box::new(expr_fn::round(vec![expr])),
        data_type: DataType::Float64,
    })
}

fn spark_ln(expr: Expr) -> Expr {
    Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![(
            Box::new(expr.clone().eq(Expr::Literal(ScalarValue::Int64(Some(0))))),
            Box::new(Expr::Literal(ScalarValue::Null)),
        )],
        else_expr: Some(Box::new(expr_fn::ln(expr))),
    })
}

#[inline]
fn eulers_constant() -> Expr {
    Expr::Literal(ScalarValue::Float64(Some(std::f64::consts::E)))
}

fn ceil_floor(input: FunctionInput, name: &str) -> PlanResult<Expr> {
    let FunctionInput { arguments, .. } = input;
    // DataFusion bug: `ReturnTypeArgs.scalar_arguments` is None if scalar argument is nested
    let arguments = if arguments.len() == 2 {
        let (arg, target_scale) = arguments.two()?;
        let target_scale = match target_scale {
            Expr::Literal(_) => Ok(target_scale),
            Expr::Negative(negative) => {
                if let Expr::Literal(scalar) = negative.as_ref() {
                    match scalar {
                        ScalarValue::Int8(v) => {
                            Ok(Expr::Literal(ScalarValue::Int32(v.map(|v| -v as i32))))
                        }
                        ScalarValue::Int16(v) => {
                            Ok(Expr::Literal(ScalarValue::Int32(v.map(|v| -v as i32))))
                        }
                        ScalarValue::Int32(v) => {
                            Ok(Expr::Literal(ScalarValue::Int32(v.map(|v| -v))))
                        }
                        ScalarValue::Int64(v) => {
                            Ok(Expr::Literal(ScalarValue::Int32(v.map(|v| -(v as i32)))))
                        }
                        ScalarValue::UInt8(v) => {
                            Ok(Expr::Literal(ScalarValue::Int32(v.map(|v| -(v as i32)))))
                        }
                        ScalarValue::UInt16(v) => {
                            Ok(Expr::Literal(ScalarValue::Int32(v.map(|v| -(v as i32)))))
                        }
                        ScalarValue::UInt32(v) => {
                            Ok(Expr::Literal(ScalarValue::Int32(v.map(|v| -(v as i32)))))
                        }
                        ScalarValue::UInt64(v) => {
                            Ok(Expr::Literal(ScalarValue::Int32(v.map(|v| -(v as i32)))))
                        }
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
        Arc::new(ScalarUDF::from(SparkCeil::new()))
    } else {
        Arc::new(ScalarUDF::from(SparkFloor::new()))
    };
    Ok(Expr::ScalarFunction(expr::ScalarFunction {
        func,
        args: arguments,
    }))
}

pub(super) fn list_built_in_math_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("%", F::binary_op(Operator::Modulo)),
        ("*", F::custom(spark_multiply)),
        ("+", F::custom(spark_plus)),
        ("-", F::custom(spark_minus)),
        ("/", F::custom(spark_divide)),
        ("abs", F::udf(SparkAbs::new())),
        ("acos", F::unary(expr_fn::acos)),
        ("acosh", F::unary(expr_fn::acosh)),
        ("asin", F::unary(expr_fn::asin)),
        ("asinh", F::unary(expr_fn::asinh)),
        ("atan", F::unary(expr_fn::atan)),
        ("atan2", F::binary(expr_fn::atan2)),
        ("atanh", F::unary(expr_fn::atanh)),
        ("bin", F::udf(SparkBin::new())),
        ("bround", F::unknown("bround")),
        ("cbrt", F::unary(expr_fn::cbrt)),
        ("ceil", F::custom(|arg| ceil_floor(arg, "ceil"))),
        ("ceiling", F::custom(|arg| ceil_floor(arg, "ceil"))),
        ("conv", F::unknown("conv")),
        ("cos", F::unary(expr_fn::cos)),
        ("cosh", F::unary(expr_fn::cosh)),
        ("cot", F::unary(expr_fn::cot)),
        ("csc", F::unknown("csc")),
        ("degrees", F::unary(expr_fn::degrees)),
        ("div", F::custom(spark_div)),
        ("e", F::nullary(eulers_constant)),
        ("exp", F::unary(expr_fn::exp)),
        ("expm1", F::udf(SparkExpm1::new())),
        ("factorial", F::unary(expr_fn::factorial)),
        ("floor", F::custom(|arg| ceil_floor(arg, "floor"))),
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
        ("negative", F::unary(|x| Expr::Negative(Box::new(x)))),
        ("pi", F::nullary(expr_fn::pi)),
        ("pmod", F::udf(SparkPmod::new())),
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
