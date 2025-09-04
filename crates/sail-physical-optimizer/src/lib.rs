use std::sync::Arc;

use datafusion::physical_optimizer::aggregate_statistics::AggregateStatistics;
use datafusion::physical_optimizer::coalesce_async_exec_input::CoalesceAsyncExecInput;
use datafusion::physical_optimizer::coalesce_batches::CoalesceBatches;
use datafusion::physical_optimizer::combine_partial_final_agg::CombinePartialFinalAggregate;
use datafusion::physical_optimizer::enforce_distribution::EnforceDistribution;
use datafusion::physical_optimizer::enforce_sorting::EnforceSorting;
use datafusion::physical_optimizer::ensure_coop::EnsureCooperative;
use datafusion::physical_optimizer::filter_pushdown::FilterPushdown;
use datafusion::physical_optimizer::join_selection::JoinSelection;
use datafusion::physical_optimizer::limit_pushdown::LimitPushdown;
use datafusion::physical_optimizer::limited_distinct_aggregation::LimitedDistinctAggregation;
use datafusion::physical_optimizer::output_requirements::OutputRequirements;
use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
use datafusion::physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion::physical_optimizer::topk_aggregation::TopKAggregation;
use datafusion::physical_optimizer::update_aggr_exprs::OptimizeAggregateOrder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
mod join_reorder;

pub fn get_physical_optimizers() -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    vec![
        Arc::new(OutputRequirements::new_add_mode()),
        Arc::new(AggregateStatistics::new()),
        // Custom optimizer
        // Arc::new(JoinReorder::new()),
        Arc::new(JoinSelection::new()),
        Arc::new(LimitedDistinctAggregation::new()),
        Arc::new(FilterPushdown::new()),
        Arc::new(EnforceDistribution::new()),
        Arc::new(CombinePartialFinalAggregate::new()),
        Arc::new(EnforceSorting::new()),
        Arc::new(OptimizeAggregateOrder::new()),
        Arc::new(ProjectionPushdown::new()),
        Arc::new(CoalesceBatches::new()),
        Arc::new(CoalesceAsyncExecInput::new()),
        Arc::new(OutputRequirements::new_remove_mode()),
        Arc::new(TopKAggregation::new()),
        Arc::new(LimitPushdown::new()),
        Arc::new(ProjectionPushdown::new()),
        Arc::new(EnsureCooperative::new()),
        Arc::new(FilterPushdown::new_post_optimization()),
        Arc::new(SanityCheckPlan::new()),
    ]
}

// This function is only needed for the tests to verify the count of optimizers.
pub fn get_custom_sail_optimizers() -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    // vec![Arc::new(JoinReorder::new())]
    vec![]
}

#[cfg(test)]
mod tests {
    use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;

    use super::*;

    #[test]
    fn test_optimizer_count() -> datafusion::common::Result<()> {
        let sail_optimizers = get_custom_sail_optimizers();
        let datafusion_optimizers = PhysicalOptimizer::default().rules;
        let all_optimizers = get_physical_optimizers();
        assert_eq!(
            sail_optimizers.len() + datafusion_optimizers.len(),
            all_optimizers.len(),
            "The total number of optimizers should be the sum of sail and datafusion optimizers"
        );

        Ok(())
    }

    #[test]
    fn test_optimizer_order() -> datafusion::common::Result<()> {
        let optimizers = get_physical_optimizers();
        let custom_optimizers = get_custom_sail_optimizers();
        let datafusion_optimizers = PhysicalOptimizer::default().rules;

        let custom_names: std::collections::HashSet<&str> =
            custom_optimizers.iter().map(|opt| opt.name()).collect();

        let datafusion_names: Vec<&str> =
            datafusion_optimizers.iter().map(|opt| opt.name()).collect();

        let non_custom_names: Vec<&str> = optimizers
            .iter()
            .map(|opt| opt.name())
            .filter(|name| !custom_names.contains(name))
            .collect();

        assert_eq!(
            datafusion_names, non_custom_names,
            "DataFusion optimizers order should be preserved in the complete optimizer list"
        );

        Ok(())
    }
}
