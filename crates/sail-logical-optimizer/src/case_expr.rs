use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::logical_expr::logical_plan::LogicalPlan;
use datafusion::logical_expr::{BinaryExpr, Case, Expr, Operator};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

/// Reconstructs the simple CASE form from flattened equality conditions.
///
/// Sail's SQL analyzer converts `CASE col WHEN val THEN result` into
/// `CASE WHEN col = val THEN result` (with `expr: None`).
/// DataFusion has a built-in `LiteralLookupTable` optimization that uses
/// a HashMap for O(1) per-row evaluation, but it only activates when
/// `expr: Some(...)`. This rule detects the flattened pattern and
/// reconstructs the simple form to enable that optimization.
#[derive(Debug)]
pub struct ReconstructSimpleCaseExpr;

impl OptimizerRule for ReconstructSimpleCaseExpr {
    fn name(&self) -> &str {
        "reconstruct_simple_case_expr"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.map_expressions(|expr| {
            expr.transform_up(|e| match e {
                Expr::Case(case) => Ok(try_reconstruct_simple_case(case)),
                _ => Ok(Transformed::no(e)),
            })
        })
    }
}

pub fn try_reconstruct_simple_case(case: Case) -> Transformed<Expr> {
    let Case {
        expr: case_expr,
        when_then_expr,
        else_expr,
    } = case;

    // Only process CASE WHEN (no operand) with 2+ branches
    if case_expr.is_some() || when_then_expr.len() < 2 {
        return Transformed::no(Expr::Case(Case {
            expr: case_expr,
            when_then_expr,
            else_expr,
        }));
    }

    let mut common_expr: Option<Box<Expr>> = None;
    let mut new_when_then = Vec::with_capacity(when_then_expr.len());

    for (condition, result) in when_then_expr {
        // Each condition must be `expr = literal` (or `literal = expr`)
        let Expr::BinaryExpr(BinaryExpr { left, op, right }) = *condition else {
            return reconstruct_no(common_expr, new_when_then, else_expr);
        };
        if op != Operator::Eq {
            return reconstruct_no(common_expr, new_when_then, else_expr);
        }

        // Determine which side is the literal
        let (expr_side, literal_side) = if is_literal(&right) {
            (left, right)
        } else if is_literal(&left) {
            (right, left)
        } else {
            return reconstruct_no(common_expr, new_when_then, else_expr);
        };

        // All branches must reference the same expression
        match &common_expr {
            None => common_expr = Some(expr_side),
            Some(existing) if **existing == *expr_side => {}
            _ => return reconstruct_no(common_expr, new_when_then, else_expr),
        }

        new_when_then.push((literal_side, result));
    }

    // All THEN results must also be literals for DataFusion's LookupTable
    let all_then_literal = new_when_then.iter().all(|(_, result)| is_literal(result));
    let else_literal = else_expr.as_ref().is_none_or(|e| is_literal(e));

    if !all_then_literal || !else_literal {
        return reconstruct_no(common_expr, new_when_then, else_expr);
    }

    Transformed::yes(Expr::Case(Case {
        expr: common_expr,
        when_then_expr: new_when_then,
        else_expr,
    }))
}

/// Rebuild the original CASE WHEN form (no transformation).
fn reconstruct_no(
    common_expr: Option<Box<Expr>>,
    partial_when_then: Vec<(Box<Expr>, Box<Expr>)>,
    else_expr: Option<Box<Expr>>,
) -> Transformed<Expr> {
    // We partially destructured the original; rebuild equality conditions
    let when_then_expr = match &common_expr {
        Some(col) => partial_when_then
            .into_iter()
            .map(|(literal, result)| {
                let condition = Box::new(Expr::BinaryExpr(BinaryExpr {
                    left: col.clone(),
                    op: Operator::Eq,
                    right: literal,
                }));
                (condition, result)
            })
            .collect(),
        None => partial_when_then,
    };

    Transformed::no(Expr::Case(Case {
        expr: None,
        when_then_expr,
        else_expr,
    }))
}

fn is_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(..))
}

#[cfg(test)]
mod tests {
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::col;

    use super::*;

    fn lit_expr(val: impl Into<ScalarValue>) -> Box<Expr> {
        Box::new(Expr::Literal(val.into(), None))
    }

    fn eq_expr(left: Expr, right: Expr) -> Box<Expr> {
        Box::new(Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op: Operator::Eq,
            right: Box::new(right),
        }))
    }

    #[test]
    fn test_reconstruct_simple_case() {
        let case = Case {
            expr: None,
            when_then_expr: vec![
                (
                    eq_expr(
                        col("status"),
                        Expr::Literal(ScalarValue::from("active"), None),
                    ),
                    lit_expr(ScalarValue::Int32(Some(1))),
                ),
                (
                    eq_expr(
                        col("status"),
                        Expr::Literal(ScalarValue::from("inactive"), None),
                    ),
                    lit_expr(ScalarValue::Int32(Some(0))),
                ),
                (
                    eq_expr(
                        col("status"),
                        Expr::Literal(ScalarValue::from("pending"), None),
                    ),
                    lit_expr(ScalarValue::Int32(Some(2))),
                ),
            ],
            else_expr: Some(lit_expr(ScalarValue::Int32(Some(-1)))),
        };

        let result = try_reconstruct_simple_case(case);
        assert!(result.transformed);

        let Expr::Case(Case {
            expr,
            when_then_expr,
            else_expr,
        }) = result.data
        else {
            unreachable!("expected Case expression");
        };
        assert_eq!(expr, Some(Box::new(col("status"))));
        assert_eq!(when_then_expr.len(), 3);
        for (when, _) in &when_then_expr {
            assert!(is_literal(when), "WHEN should be a literal");
        }
        assert!(else_expr.is_some());
    }

    #[test]
    fn test_no_reconstruct_different_columns() {
        let case = Case {
            expr: None,
            when_then_expr: vec![
                (
                    eq_expr(col("a"), Expr::Literal(ScalarValue::Int32(Some(1)), None)),
                    lit_expr(ScalarValue::from("x")),
                ),
                (
                    eq_expr(col("b"), Expr::Literal(ScalarValue::Int32(Some(2)), None)),
                    lit_expr(ScalarValue::from("y")),
                ),
            ],
            else_expr: None,
        };

        let result = try_reconstruct_simple_case(case);
        assert!(!result.transformed);
    }

    #[test]
    fn test_no_reconstruct_non_literal_then() {
        let case = Case {
            expr: None,
            when_then_expr: vec![
                (
                    eq_expr(col("a"), Expr::Literal(ScalarValue::Int32(Some(1)), None)),
                    Box::new(col("x")), // non-literal THEN
                ),
                (
                    eq_expr(col("a"), Expr::Literal(ScalarValue::Int32(Some(2)), None)),
                    lit_expr(ScalarValue::from("y")),
                ),
            ],
            else_expr: None,
        };

        let result = try_reconstruct_simple_case(case);
        assert!(!result.transformed);
    }

    #[test]
    fn test_no_reconstruct_single_branch() {
        let case = Case {
            expr: None,
            when_then_expr: vec![(
                eq_expr(col("a"), Expr::Literal(ScalarValue::Int32(Some(1)), None)),
                lit_expr(ScalarValue::from("x")),
            )],
            else_expr: None,
        };

        let result = try_reconstruct_simple_case(case);
        assert!(!result.transformed);
    }
}
