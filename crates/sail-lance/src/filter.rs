// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Deciding which DataFusion filters a Lance scan can evaluate.
//!
//! Lance and Sail are built against the same DataFusion release, so a filter is
//! pushed down as the DataFusion [`Expr`] itself, through
//! `Scanner::filter_expr`. There is no SQL round trip and no dialect to get
//! wrong: Lance evaluates the very same expression tree with the very same
//! DataFusion version, which is why a pushed down filter is reported as
//! [`Exact`] and DataFusion drops its own `FilterExec`.
//!
//! What still has to be decided is *whether* Lance can evaluate a given
//! expression. That question is answered by Lance itself, by binding the
//! expression against the dataset during planning; this module only rejects
//! the expression shapes that are not a filter over stored columns at all.
//!
//! [`Exact`]: datafusion::logical_expr::TableProviderFilterPushDown::Exact

use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::Expr;

/// Returns true when `expr` is worth offering to a Lance scan.
///
/// Correlated and subquery expressions are not: they are not a predicate over
/// the stored columns of one dataset, and DataFusion evaluates them itself.
pub fn is_scan_filter(expr: &Expr) -> bool {
    let mut is_filter = true;
    let result = expr.apply(|expr| {
        if matches!(
            expr,
            Expr::ScalarSubquery(_)
                | Expr::InSubquery(_)
                | Expr::Exists(_)
                | Expr::OuterReferenceColumn(..)
                | Expr::Placeholder(_)
                | Expr::WindowFunction(_)
                | Expr::AggregateFunction(_)
                | Expr::GroupingSet(_)
        ) {
            is_filter = false;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    });
    result.is_ok() && is_filter
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field};
    use datafusion_common::{Column, ScalarValue};
    use datafusion_expr::{col, lit};

    use super::*;

    #[test]
    fn predicates_over_columns_are_scan_filters() {
        assert!(is_scan_filter(
            &col("id").gt(lit(3_i64)).and(col("name").eq(lit("lance")))
        ));
        assert!(is_scan_filter(&col("id").is_null()));
        assert!(is_scan_filter(&col("name").like(lit("lan%"))));
        assert!(is_scan_filter(
            &col("at").gt(lit(ScalarValue::TimestampMicrosecond(Some(0), None)))
        ));
    }

    #[test]
    fn correlated_references_are_not_scan_filters() {
        let outer = Expr::OuterReferenceColumn(
            Arc::new(Field::new("id", DataType::Int64, true)),
            Column::new_unqualified("id"),
        );
        assert!(!is_scan_filter(&outer.eq(lit(1_i64))));
    }
}
