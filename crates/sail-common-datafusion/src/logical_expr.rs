use std::collections::HashMap;

use datafusion_expr::expr::FieldMetadata;
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

/// Attach `name` as an `Alias` to `expr` while preserving any Sail extension
/// `metadata` (e.g. Spark interval qualifier) on the resulting Field. Use
/// this in place of `expr.alias(name)` anywhere we know the Field metadata
/// that should flow downstream — `to_field` on the alias will then surface
/// the metadata on the Logical Field. Pass `HashMap::new()` (or
/// `Default::default()`) when there is no metadata to carry.
///
/// Note: DataFusion's physical planner drops Alias metadata for non-`Literal`
/// inner expressions (see `datafusion-physical-expr/src/planner.rs`), so this
/// alone won't carry metadata onto the physical schema for `BinaryExpr`,
/// `AggregateFunction`, or `ScalarFunction` inner exprs — those need their
/// own `return_field` / `return_field_from_args` to attach metadata.
pub fn alias_preserving_metadata(
    expr: Expr,
    name: impl Into<String>,
    metadata: HashMap<String, String>,
) -> Expr {
    if metadata.is_empty() {
        expr.alias(name)
    } else {
        expr.alias_with_metadata(name, Some(FieldMetadata::from(metadata)))
    }
}
