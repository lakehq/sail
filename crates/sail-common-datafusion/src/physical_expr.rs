use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::physical_expr::PhysicalExpr;

/// A physical expression with an optional SQL source string.
#[derive(Debug, Clone)]
pub struct PhysicalExprWithSource {
    /// The physical expression.
    pub expr: Arc<dyn PhysicalExpr>,
    /// The optional SQL source string for information purposes.
    pub source: Option<String>,
}

impl PhysicalExprWithSource {
    pub fn new(expr: Arc<dyn PhysicalExpr>, source: Option<String>) -> Self {
        Self { expr, source }
    }
}

impl PartialEq for PhysicalExprWithSource {
    fn eq(&self, other: &Self) -> bool {
        self.expr.as_ref() == other.expr.as_ref() && self.source == other.source
    }
}

impl Eq for PhysicalExprWithSource {}

impl Hash for PhysicalExprWithSource {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expr.as_ref().hash(state);
        self.source.hash(state);
    }
}
