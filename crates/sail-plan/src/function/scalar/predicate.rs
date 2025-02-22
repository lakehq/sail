use datafusion::functions::expr_fn;
use datafusion_expr::{expr, not, Operator};

use crate::error::PlanResult;
use crate::function::common::{Function, FunctionInput};
use crate::utils::ItemTaker;

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
    // FIXME: There is no `PhysicalExpr` for `Expr::SimilarTo` in DataFusion.
    //  create_physical_expr in datafusion/planner/src/planner doesn't have a `SimilarTo` match arm.
    //  which leads to the error:
    //      not_impl_err!("Physical plan does not support logical expression {other:?}")
    //  Once the physical expr for `SimilarTo` is implemented, use the code below for performance:
    //     expr::Expr::SimilarTo(expr::Like {
    //         negated: false,
    //         expr: Box::new(expr),
    //         pattern: Box::new(pattern),
    //         case_insensitive: false,
    //         escape_char: None,
    //     })
    expr_fn::regexp_like(expr, pattern, None)
}

fn is_in_list(input: FunctionInput) -> PlanResult<expr::Expr> {
    let FunctionInput { arguments, .. } = input;
    let (value, list) = arguments.at_least_one()?;
    Ok(expr::Expr::InList(expr::InList {
        expr: Box::new(value),
        list,
        negated: false,
    }))
}

pub(super) fn list_built_in_predicate_functions() -> Vec<(&'static str, Function)> {
    use crate::function::common::FunctionBuilder as F;

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
        ("ilike", F::binary(ilike)),
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
        ("like", F::binary(like)),
        ("not", F::unary(not)),
        ("or", F::binary_op(Operator::Or)),
        ("regexp", F::binary(rlike)),
        ("regexp_like", F::binary(rlike)),
        ("rlike", F::binary(rlike)),
    ]
}
