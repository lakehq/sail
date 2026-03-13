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

fn try_reconstruct_simple_case(case: Case) -> Transformed<Expr> {
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

    // First pass: validate ALL branches match the pattern by reference.
    // This avoids partially consuming when_then_expr and losing branches on bail-out.
    if !can_reconstruct(&when_then_expr, &else_expr) {
        return Transformed::no(Expr::Case(Case {
            expr: None,
            when_then_expr,
            else_expr,
        }));
    }

    // Second pass: extract common expression and literal WHEN values.
    // Safe to consume since we validated everything above.
    let mut common_expr: Option<Box<Expr>> = None;
    let mut new_when_then = Vec::with_capacity(when_then_expr.len());

    for (condition, result) in when_then_expr {
        let Expr::BinaryExpr(BinaryExpr { left, op: _, right }) = *condition else {
            unreachable!("validated in can_reconstruct");
        };

        let (expr_side, literal_side) = if is_literal(&right) {
            (left, right)
        } else {
            (right, left)
        };

        if common_expr.is_none() {
            common_expr = Some(expr_side);
        }

        new_when_then.push((literal_side, result));
    }

    Transformed::yes(Expr::Case(Case {
        expr: common_expr,
        when_then_expr: new_when_then,
        else_expr,
    }))
}

/// Check if all branches can be reconstructed into simple CASE form,
/// without consuming the data.
fn can_reconstruct(
    when_then_expr: &[(Box<Expr>, Box<Expr>)],
    else_expr: &Option<Box<Expr>>,
) -> bool {
    let mut common_expr: Option<&Expr> = None;

    for (condition, result) in when_then_expr {
        // Each condition must be `expr = literal` (or `literal = expr`)
        let Expr::BinaryExpr(BinaryExpr { left, op, right }) = condition.as_ref() else {
            return false;
        };
        if *op != Operator::Eq {
            return false;
        }

        // Determine which side is the literal
        let expr_side = if is_literal(right) {
            left.as_ref()
        } else if is_literal(left) {
            right.as_ref()
        } else {
            return false;
        };

        // All branches must reference the same expression
        match common_expr {
            None => common_expr = Some(expr_side),
            Some(existing) if *existing == *expr_side => {}
            _ => return false,
        }

        // THEN must be a literal for DataFusion's LookupTable
        if !is_literal(result) {
            return false;
        }
    }

    // ELSE must be a literal (or absent)
    else_expr.as_ref().is_none_or(|e| is_literal(e))
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

    #[test]
    fn test_no_reconstruct_non_equality_conditions() {
        // CASE WHEN col IS NOT NULL THEN 'yes' WHEN col2 THEN 'no' ELSE 'maybe' END
        // This pattern appears in MERGE INTO — must NOT be transformed.
        let case = Case {
            expr: None,
            when_then_expr: vec![
                (
                    Box::new(Expr::IsNotNull(Box::new(col("a")))),
                    lit_expr(ScalarValue::from("yes")),
                ),
                (Box::new(col("b")), lit_expr(ScalarValue::from("no"))),
            ],
            else_expr: Some(lit_expr(ScalarValue::from("maybe"))),
        };

        let result = try_reconstruct_simple_case(case);
        assert!(!result.transformed);

        // Verify the original expression is preserved intact
        let Expr::Case(Case {
            expr,
            when_then_expr,
            else_expr,
        }) = result.data
        else {
            unreachable!("expected Case expression");
        };
        assert!(expr.is_none());
        assert_eq!(when_then_expr.len(), 2);
        assert!(else_expr.is_some());
    }
}
