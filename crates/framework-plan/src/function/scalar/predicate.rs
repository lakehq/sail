use crate::function::common::Function;
use datafusion::functions::expr_fn;
use datafusion_common::plan_err;
use datafusion_expr::{expr, Operator};

fn like(expr: expr::Expr, pattern: expr::Expr) -> expr::Expr {
    expr::Expr::Like(expr::Like {
        negated: false,
        expr: Box::new(expr),
        pattern: Box::new(pattern),
        case_insensitive: false,
        escape_char: None,
    })
}

fn ilike(expr: expr::Expr, pattern: expr::Expr) -> expr::Expr {
    expr::Expr::Like(expr::Like {
        negated: false,
        expr: Box::new(expr),
        pattern: Box::new(pattern),
        case_insensitive: true,
        escape_char: None,
    })
}

fn rlike(expr: expr::Expr, pattern: expr::Expr) -> expr::Expr {
    expr::Expr::SimilarTo(expr::Like {
        negated: false,
        expr: Box::new(expr),
        pattern: Box::new(pattern),
        case_insensitive: false,
        escape_char: None,
    })
}

fn in_list(mut args: Vec<expr::Expr>) -> datafusion_common::Result<expr::Expr> {
    if args.is_empty() {
        return plan_err!("in operator requires at least 1 argument");
    }
    let value = args.pop().unwrap();
    let list = args;
    Ok(expr::Expr::InList(expr::InList {
        expr: Box::new(value),
        list,
        negated: false,
    }))
}

pub(super) fn list_built_in_predicate_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

    vec![
        ("!", F::unknown("!")),
        ("!=", F::binary_op(Operator::NotEq)),
        ("<", F::binary_op(Operator::Lt)),
        ("<=", F::binary_op(Operator::LtEq)),
        ("<=>", F::unknown("null-safe equal")),
        ("=", F::binary_op(Operator::Eq)),
        ("==", F::binary_op(Operator::Eq)),
        (">", F::binary_op(Operator::Gt)),
        (">=", F::binary_op(Operator::GtEq)),
        ("and", F::binary_op(Operator::And)),
        ("ilike", F::binary(ilike)),
        ("in", F::custom(in_list)),
        ("isnan", F::unary(expr_fn::isnan)),
        (
            "isnotnull",
            F::unary(|x| expr::Expr::IsNotNull(Box::new(x))),
        ),
        ("isnull", F::unary(|x| expr::Expr::IsNull(Box::new(x)))),
        ("like", F::binary(like)),
        ("not", F::unary(|x| expr::Expr::Not(Box::new(x)))),
        ("or", F::binary_op(Operator::Or)),
        ("regexp", F::binary(rlike)),
        ("regexp_like", F::binary(rlike)),
        ("rlike", F::binary(rlike)),
    ]
}
