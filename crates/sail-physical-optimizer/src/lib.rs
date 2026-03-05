use std::sync::Arc;

use datafusion::physical_optimizer::aggregate_statistics::AggregateStatistics;
use datafusion::physical_optimizer::coalesce_batches::CoalesceBatches;
use datafusion::physical_optimizer::combine_partial_final_agg::CombinePartialFinalAggregate;
use datafusion::physical_optimizer::enforce_distribution::EnforceDistribution;
use datafusion::physical_optimizer::enforce_sorting::EnforceSorting;
use datafusion::physical_optimizer::ensure_coop::EnsureCooperative;
use datafusion::physical_optimizer::filter_pushdown::FilterPushdown;
use datafusion::physical_optimizer::join_selection::JoinSelection;
use datafusion::physical_optimizer::limit_pushdown::LimitPushdown;
use datafusion::physical_optimizer::limit_pushdown_past_window::LimitPushPastWindows;
use datafusion::physical_optimizer::limited_distinct_aggregation::LimitedDistinctAggregation;
use datafusion::physical_optimizer::output_requirements::OutputRequirements;
use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
use datafusion::physical_optimizer::pushdown_sort::PushdownSort;
use datafusion::physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion::physical_optimizer::topk_aggregation::TopKAggregation;
use datafusion::physical_optimizer::update_aggr_exprs::OptimizeAggregateOrder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;

use crate::explicit_repartition::RewriteExplicitRepartition;
use crate::join_reorder::JoinReorder;

mod explicit_repartition;
mod join_reorder;

#[derive(Debug, Clone, Default)]
pub struct PhysicalOptimizerOptions {
    pub enable_join_reorder: bool,
}

pub fn get_physical_optimizers(
    options: PhysicalOptimizerOptions,
) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    let mut rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> = vec![];

    rules.push(Arc::new(OutputRequirements::new_add_mode()));
    rules.push(Arc::new(AggregateStatistics::new()));
    if options.enable_join_reorder {
        rules.push(Arc::new(JoinReorder::new()));
    }
    rules.push(Arc::new(JoinSelection::new()));
    rules.push(Arc::new(LimitedDistinctAggregation::new()));
    rules.push(Arc::new(FilterPushdown::new()));
    rules.push(Arc::new(EnforceDistribution::new()));
    rules.push(Arc::new(CombinePartialFinalAggregate::new()));
    rules.push(Arc::new(EnforceSorting::new()));
    rules.push(Arc::new(OptimizeAggregateOrder::new()));
    rules.push(Arc::new(ProjectionPushdown::new()));
    rules.push(Arc::new(CoalesceBatches::new()));
    rules.push(Arc::new(OutputRequirements::new_remove_mode()));
    rules.push(Arc::new(TopKAggregation::new()));
    rules.push(Arc::new(LimitPushPastWindows::new()));
    rules.push(Arc::new(LimitPushdown::new()));
    rules.push(Arc::new(ProjectionPushdown::new()));
    rules.push(Arc::new(PushdownSort::new()));
    rules.push(Arc::new(EnsureCooperative::new()));
    rules.push(Arc::new(FilterPushdown::new_post_optimization()));
    rules.push(Arc::new(RewriteExplicitRepartition::new()));
    rules.push(Arc::new(SanityCheckPlan::new()));

    rules
}

#[cfg(test)]
mod tests {
    use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;

    use super::*;

    #[test]
    fn test_optimizer_rules() -> datafusion::common::Result<()> {
        let optimizers = get_physical_optimizers(Default::default());
        let datafusion_optimizers = PhysicalOptimizer::default().rules;

        let datafusion_optimizer_names: Vec<&str> =
            datafusion_optimizers.iter().map(|opt| opt.name()).collect();
        let actual_datafusion_optimizer_names: Vec<&str> = optimizers
            .iter()
            .map(|opt| opt.name())
            .filter(|name| datafusion_optimizer_names.contains(name))
            .collect();
        assert_eq!(
            datafusion_optimizer_names,
            actual_datafusion_optimizer_names,
            "the custom physical optimizer rules should include all the default DataFusion optimizer rules in the same order"
        );

        Ok(())
    }
}
