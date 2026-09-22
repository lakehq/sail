use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::tree_node::TreeNode;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::filter_pushdown::FilterPushdown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;

#[derive(Debug)]
pub struct PostFilterPushdown;

impl PhysicalOptimizerRule for PostFilterPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut config = config.clone();
        if plan.exists(|node| {
            Ok(node
                .downcast_ref::<AggregateExec>()
                .is_some_and(|aggregate| {
                    aggregate.aggr_expr().len() > 1
                        && !aggregate.dynamic_expressions_produced().is_empty()
                }))
        })? {
            // A NULL-only partition can reset DataFusion's shared MIN bound to NULL.
            // Omitting that bound from a multi-aggregate filter can prune unread minima.
            config.optimizer.enable_aggregate_dynamic_filter_pushdown = false;
        }
        FilterPushdown::new_post_optimization().optimize(plan, &config)
    }

    fn name(&self) -> &str {
        "FilterPushdown(Post)"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_plan::aggregates::{AggregateMode, PhysicalGroupBy};

    use super::*;

    fn aggregate_plan(multiple: bool) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
        let source = Arc::new(ParquetSource::new(Arc::clone(&schema)));
        let scan = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source).build();
        let functions = if multiple {
            vec![min_udaf(), max_udaf()]
        } else {
            vec![min_udaf()]
        };
        let aggregates = functions
            .into_iter()
            .map(|function| {
                let name = function.name().to_string();
                AggregateExprBuilder::new(function, vec![col("v", &schema)?])
                    .schema(Arc::clone(&schema))
                    .alias(name)
                    .build()
                    .map(Arc::new)
            })
            .collect::<Result<Vec<_>>>()?;
        let filters = vec![None; aggregates.len()];
        Ok(Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(vec![]),
            aggregates,
            filters,
            DataSourceExec::from_data_source(scan),
            schema,
        )?))
    }

    #[test]
    fn multi_aggregate_filters_are_not_pushed_down() -> Result<()> {
        let plan = PostFilterPushdown.optimize(aggregate_plan(true)?, &ConfigOptions::default())?;
        assert!(plan.dynamic_expressions_produced().is_empty());
        Ok(())
    }

    #[test]
    fn single_aggregate_filters_remain_enabled() -> Result<()> {
        let plan =
            PostFilterPushdown.optimize(aggregate_plan(false)?, &ConfigOptions::default())?;
        assert_eq!(plan.dynamic_expressions_produced().len(), 1);
        Ok(())
    }
}
