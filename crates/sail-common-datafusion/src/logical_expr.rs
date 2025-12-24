use datafusion_expr::Expr;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct ExprWithSource {
    pub expr: Expr,
    pub source: Option<String>,
}

impl ExprWithSource {
    pub fn new(expr: Expr, source: Option<String>) -> Self {
        Self { expr, source }
    }
}
