use datafusion_expr::Expr;

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
