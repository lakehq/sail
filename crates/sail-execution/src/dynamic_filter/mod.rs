mod build;
mod graph;
mod runtime;
mod state;
mod wire;

use std::collections::BTreeSet;

pub(crate) use build::{DynamicFilterBuildExec, prepare_join_filters};
use datafusion::common::Result;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
pub(crate) use graph::{DynamicFilterRoute, discover_routes};
pub(crate) use runtime::{DynamicFilterClient, TaskDynamicFilters};
pub(crate) use state::DynamicFilterState;
pub(crate) use wire::DynamicFilterBinding;

fn consumer_filter_ids(plan: &dyn ExecutionPlan) -> Result<BTreeSet<u64>> {
    let produced = plan.dynamic_expressions_produced();
    let mut consumers = BTreeSet::new();
    // ExecutionPlan visits only expression roots; scan predicates can combine
    // several dynamic filters with ordinary predicates.
    plan.apply_expressions(&mut |root| {
        root.apply(|expr| {
            if expr.is::<DynamicFilterPhysicalExpr>() {
                if let Some(id) = expr.expression_id()
                    && !produced
                        .iter()
                        .any(|producer| producer.expression_id() == Some(id))
                {
                    consumers.insert(id);
                }
                return Ok(TreeNodeRecursion::Jump);
            }
            Ok(TreeNodeRecursion::Continue)
        })
    })?;
    Ok(consumers)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, lit};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::filter::FilterExec;

    use super::*;

    #[test]
    fn nested_consumers_exclude_the_nodes_own_producer() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("k", 0));
        let first: Arc<dyn PhysicalExpr> =
            Arc::new(DynamicFilterPhysicalExpr::new(vec![key.clone()], lit(true)));
        let second: Arc<dyn PhysicalExpr> =
            Arc::new(DynamicFilterPhysicalExpr::new(vec![key.clone()], lit(true)));
        let predicate = Arc::new(BinaryExpr::new(
            first.clone(),
            Operator::And,
            Arc::new(BinaryExpr::new(
                Arc::new(BinaryExpr::new(key.clone(), Operator::Gt, lit(0_i64))),
                Operator::And,
                second.clone(),
            )),
        ));
        let consumer = FilterExec::try_new(predicate, Arc::new(EmptyExec::new(schema.clone())))?;
        let ids = [first.expression_id(), second.expression_id()]
            .into_iter()
            .flatten()
            .collect::<BTreeSet<_>>();
        assert_eq!(ids.len(), 2);
        assert_eq!(consumer_filter_ids(&consumer)?, ids);
        let producer = DynamicFilterBuildExec {
            input: Arc::new(consumer),
            keys: vec![key],
            filter: first,
            probe_schema: schema,
            null_equals_null: false,
            null_aware: false,
        };
        assert!(consumer_filter_ids(&producer)?.is_empty());
        Ok(())
    }
}
