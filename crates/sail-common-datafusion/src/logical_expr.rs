use datafusion_expr::{Expr, Operator};

/// A logical expression with an optional SQL source string.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct ExprWithSource {
    /// The logical expression.
    pub expr: Expr,
    /// The optional SQL source string for information purposes.
    pub source: Option<String>,
}

impl ExprWithSource {
    pub fn new(expr: Expr, source: Option<String>) -> Self {
        Self { expr, source }
    }
}

/// Whether the operator compares values, including null-safe equality and inequality.
pub fn is_comparison_operator(operator: Operator) -> bool {
    matches!(
        operator,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    )
}
