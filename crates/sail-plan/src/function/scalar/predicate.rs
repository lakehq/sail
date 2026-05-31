use std::sync::Arc;

use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::{expr, lit, not, Operator, ScalarUDF};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::predicate::rewrite_like_pattern::RewriteLikePatternFunc;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn extract_escape_char(escape_expr: expr::Expr) -> PlanResult<Option<char>> {
    match escape_expr {
        expr::Expr::Literal(ScalarValue::Utf8(Some(ref s)), _)
        | expr::Expr::Literal(ScalarValue::Utf8View(Some(ref s)), _)
        | expr::Expr::Literal(ScalarValue::LargeUtf8(Some(ref s)), _) => {
            let mut chars = s.chars();
            match (chars.next(), chars.next()) {
                (Some(c), None) => Ok(Some(c)),
                _ => Err(PlanError::invalid(
                    "escape character must be a single character",
                )),
            }
        }
        _ => Err(PlanError::invalid(
            "escape character must be a string literal",
        )),
    }
}

fn build_like_expr(input: ScalarFunctionInput, case_insensitive: bool) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput { arguments, .. } = input;
    let n = arguments.len();
    match n {
        2 => {
            let (value, pattern) = arguments.two()?;
            Ok(expr::Expr::Like(expr::Like {
                negated: false,
                expr: Box::new(value),
                pattern: Box::new(pattern),
                case_insensitive,
                escape_char: None,
            }))
        }
        3 => {
            let (value, pattern, escape) = arguments.three()?;
            let escape_char = extract_escape_char(escape)?;
            // Arrow's LIKE kernel only supports `\` as the escape character.
            // For any other escape, wrap the pattern in a UDF that rewrites
            // it so Arrow sees `\` as the effective escape; then build an
            // `Expr::Like` with `escape_char: Some('\\')` to be explicit
            // about the escape that the rewritten pattern actually uses.
            let (pattern, escape_char) = match escape_char {
                Some(c) if c != '\\' => {
                    let rewritten = expr::Expr::ScalarFunction(expr::ScalarFunction {
                        func: Arc::new(ScalarUDF::from(RewriteLikePatternFunc::new())),
                        args: vec![pattern, lit(c.to_string())],
                    });
                    (rewritten, Some('\\'))
                }
                _ => (pattern, escape_char),
            };
            Ok(expr::Expr::Like(expr::Like {
                negated: false,
                expr: Box::new(value),
                pattern: Box::new(pattern),
                case_insensitive,
                escape_char,
            }))
        }
        _ => Err(PlanError::invalid(format!(
            "like/ilike expects 2 or 3 arguments, got {n}"
        ))),
    }
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

fn is_in_list(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput { arguments, .. } = input;
    let (value, list) = arguments.at_least_one()?;
    Ok(expr::Expr::InList(expr::InList {
        expr: Box::new(value),
        list,
        negated: false,
    }))
}

pub(super) fn list_built_in_predicate_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("!", F::unary(not)),
        ("!=", F::binary_op(Operator::NotEq)),
        ("<", F::binary_op(Operator::Lt)),
        ("<=", F::binary_op(Operator::LtEq)),
        ("<=>", F::binary_op(Operator::IsNotDistinctFrom)),
        ("=", F::binary_op(Operator::Eq)),
        ("==", F::binary_op(Operator::Eq)),
        (">", F::binary_op(Operator::Gt)),
        (">=", F::binary_op(Operator::GtEq)),
        ("and", F::binary_op(Operator::And)),
        ("ilike", F::custom(|input| build_like_expr(input, true))),
        // TODO:
        //  If we want to prevent `IN` as a function in SQL,
        //  we can remove that from the built-in functions,
        //  and instead resolve it to spec::Expr::InList in the proto converter.
        ("in", F::custom(is_in_list)), // Spark passes isin as in
        ("isnan", F::unary(expr_fn::isnan)),
        (
            "isnotnull",
            F::unary(|x| expr::Expr::IsNotNull(Box::new(x))),
        ),
        ("isnull", F::unary(|x| expr::Expr::IsNull(Box::new(x)))),
        ("like", F::custom(|input| build_like_expr(input, false))),
        ("not", F::unary(not)),
        ("or", F::binary_op(Operator::Or)),
        ("regexp", F::binary(rlike)),
        ("regexp_like", F::binary(rlike)),
        ("rlike", F::binary(rlike)),
    ]
}
