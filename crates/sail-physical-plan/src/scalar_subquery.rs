// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_expr::scalar_subquery::ScalarSubqueryExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;

/// Returns whether an expression of the operator reads the result of a scalar subquery.
/// The expressions of an operator include those that are pushed into a data source, so
/// a scan with a pushed predicate that reads such a result is found as well. The inputs
/// of the operator are not visited.
pub fn reads_scalar_subquery(plan: &Arc<dyn ExecutionPlan>) -> Result<bool> {
    let mut found = false;
    plan.apply_expressions(&mut |expr| {
        found = expr.exists(|expr| Ok(expr.is::<ScalarSubqueryExpr>()))?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::Operator;
    use datafusion::logical_expr::physical_planning_context::{
        ScalarSubqueryResults, SubqueryIndex,
    };
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, NotExpr};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};

    use super::*;

    #[test]
    fn test_reads_scalar_subquery() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        let column = || Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let subquery = || {
            Arc::new(ScalarSubqueryExpr::new(
                DataType::Int64,
                true,
                SubqueryIndex::new(0),
                ScalarSubqueryResults::new(1),
            )) as Arc<dyn PhysicalExpr>
        };
        let filter = |predicate: Arc<dyn PhysicalExpr>| -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(FilterExec::try_new(
                predicate,
                Arc::clone(&input),
            )?))
        };
        let equal = |right| Arc::new(BinaryExpr::new(column(), Operator::Eq, right));

        assert!(!reads_scalar_subquery(&input)?);
        assert!(!reads_scalar_subquery(&filter(equal(column()))?)?);
        // The result is read inside a larger expression.
        let nested = filter(Arc::new(NotExpr::new(equal(subquery()))))?;
        assert!(reads_scalar_subquery(&nested)?);
        // Each expression of the operator is visited.
        let expr = vec![
            ProjectionExpr::new(column(), "a".to_string()),
            ProjectionExpr::new(subquery(), "b".to_string()),
            ProjectionExpr::new(column(), "c".to_string()),
        ];
        let projection: Arc<dyn ExecutionPlan> =
            Arc::new(ProjectionExec::try_new(expr, Arc::clone(&input))?);
        assert!(reads_scalar_subquery(&projection)?);
        // The expressions of the inputs of the operator are not visited.
        let above: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(equal(column()), nested)?);
        assert!(!reads_scalar_subquery(&above)?);
        Ok(())
    }
}
