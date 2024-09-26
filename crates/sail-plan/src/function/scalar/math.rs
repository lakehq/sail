use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::{expr, BinaryExpr, Operator};

use crate::config::PlanConfig;
use crate::error::{PlanError, PlanResult};
use crate::extension::function::least_greatest;
use crate::extension::function::randn::Randn;
use crate::extension::function::random::Random;
use crate::function::common::Function;
use crate::utils::ItemTaker;

fn plus(args: Vec<expr::Expr>, _config: Arc<PlanConfig>) -> PlanResult<expr::Expr> {
    if args.len() < 2 {
        Ok(args.one()?)
    } else {
        let (left, right) = args.two()?;
        Ok(expr::Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op: Operator::Plus,
            right: Box::new(right),
        }))
    }
}

fn minus(args: Vec<expr::Expr>, _config: Arc<PlanConfig>) -> PlanResult<expr::Expr> {
    if args.len() < 2 {
        Ok(expr::Expr::Negative(Box::new(args.one()?)))
    } else {
        let (left, right) = args.two()?;
        Ok(expr::Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op: Operator::Minus,
            right: Box::new(right),
        }))
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

fn hex(args: Vec<expr::Expr>, _config: Arc<PlanConfig>) -> PlanResult<expr::Expr> {
    let expr = args.one()?;
    let data_type = match &expr {
        expr::Expr::Literal(l) => Ok(l.data_type()),
        _ => Err(PlanError::invalid("hex requires a literal argument")),
        // FIXME: Create UDF for hex to properly determine datatype
    }?;
    match data_type {
        DataType::Int32 => {
            let expr = expr::Expr::Cast(expr::Cast {
                expr: Box::new(expr),
                data_type: DataType::Int64,
            });
            Ok(expr_fn::to_hex(expr))
        }
        DataType::Int64 => Ok(expr_fn::to_hex(expr)),
        _ => Ok(expr_fn::encode(
            expr,
            expr::Expr::Literal(ScalarValue::Utf8(Some("hex".to_string()))),
        )),
    }
}

// FIXME: Implement the UDF for better numerical precision.
fn expm1(args: Vec<expr::Expr>, config: Arc<PlanConfig>) -> PlanResult<expr::Expr> {
    let num = args.one()?;
    minus(
        vec![
            expr_fn::exp(num),
            expr::Expr::Literal(ScalarValue::Float64(Some(1.0))),
        ],
        config,
    )
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

pub(super) fn list_built_in_math_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("%", F::binary_op(Operator::Modulo)),
        ("*", F::binary_op(Operator::Multiply)),
        ("+", F::custom(plus)),
        ("-", F::custom(minus)),
        ("/", F::binary_op(Operator::Divide)),
        ("abs", F::unary(expr_fn::abs)),
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
        ("div", F::unknown("div")),
        ("e", F::unknown("e")),
        ("exp", F::unary(expr_fn::exp)),
        ("expm1", F::custom(expm1)),
        ("factorial", F::unary(expr_fn::factorial)),
        ("floor", F::unary(floor)),
        ("greatest", F::udf(least_greatest::Greatest::new())),
        ("hex", F::custom(hex)),
        ("hypot", F::binary(hypot)),
        ("least", F::udf(least_greatest::Least::new())),
        ("ln", F::unary(expr_fn::ln)),
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
        ("sign", F::unary(expr_fn::signum)),
        ("signum", F::unary(expr_fn::signum)),
        ("sin", F::unary(expr_fn::sin)),
        ("sinh", F::unary(expr_fn::sinh)),
        ("sqrt", F::unary(expr_fn::sqrt)),
        ("tan", F::unary(expr_fn::tan)),
        ("tanh", F::unary(expr_fn::tanh)),
        ("try_add", F::unknown("try_add")),
        ("try_divide", F::unknown("try_divide")),
        ("try_multiply", F::unknown("try_multiply")),
        ("try_subtract", F::unknown("try_subtract")),
        ("unhex", F::unknown("unhex")),
        ("width_bucket", F::unknown("width_bucket")),
    ]
}
