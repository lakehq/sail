use std::sync::Arc;

use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::aggregate_statistics::AggregateStatistics;
use datafusion::physical_optimizer::combine_partial_final_agg::CombinePartialFinalAggregate;
use datafusion::physical_optimizer::enforce_distribution::EnforceDistribution;
use datafusion::physical_optimizer::enforce_sorting::EnforceSorting;
use datafusion::physical_optimizer::ensure_coop::EnsureCooperative;
use datafusion::physical_optimizer::filter_pushdown::FilterPushdown;
use datafusion::physical_optimizer::hash_join_buffering::HashJoinBuffering;
use datafusion::physical_optimizer::join_selection::JoinSelection;
use datafusion::physical_optimizer::limit_pushdown::LimitPushdown;
use datafusion::physical_optimizer::limit_pushdown_past_window::LimitPushPastWindows;
use datafusion::physical_optimizer::limited_distinct_aggregation::LimitedDistinctAggregation;
use datafusion::physical_optimizer::output_requirements::OutputRequirements;
use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
use datafusion::physical_optimizer::pushdown_sort::PushdownSort;
use datafusion::physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion::physical_optimizer::topk_aggregation::TopKAggregation;
use datafusion::physical_optimizer::topk_repartition::TopKRepartition;
use datafusion::physical_optimizer::update_aggr_exprs::OptimizeAggregateOrder;
use datafusion::physical_optimizer::window_topn::WindowTopN;

use crate::barrier::EnforceBarrierPartitioning;
use crate::collect_left::RewriteCollectLeftHashJoin;
use crate::explicit_repartition::RewriteExplicitRepartition;
use crate::join_reorder::JoinReorder;
pub use crate::join_reorder::JoinReorderOptions;
use crate::wrap_higher_order::WrapHigherOrderFunctions;

mod barrier;
mod collect_left;
mod explicit_repartition;
mod join_reorder;
mod wrap_higher_order;

#[derive(Debug, Clone, Default)]
pub struct PhysicalOptimizerOptions {
    pub enable_join_reorder: bool,
    pub join_reorder: JoinReorderOptions,
}

pub fn get_physical_optimizers(
    options: PhysicalOptimizerOptions,
) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    let mut rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> = vec![];

    rules.push(Arc::new(OutputRequirements::new_add_mode()));
    rules.push(Arc::new(AggregateStatistics::new()));
    if options.enable_join_reorder {
        rules.push(Arc::new(JoinReorder::new(options.join_reorder)));
    }
    rules.push(Arc::new(JoinSelection::new()));
    rules.push(Arc::new(LimitedDistinctAggregation::new()));
    rules.push(Arc::new(FilterPushdown::new()));
    rules.push(Arc::new(EnforceDistribution::new()));
    rules.push(Arc::new(CombinePartialFinalAggregate::new()));
    rules.push(Arc::new(EnforceSorting::new()));
    rules.push(Arc::new(OptimizeAggregateOrder::new()));
    rules.push(Arc::new(WindowTopN::new()));
    rules.push(Arc::new(ProjectionPushdown::new()));
    rules.push(Arc::new(OutputRequirements::new_remove_mode()));
    rules.push(Arc::new(TopKAggregation::new()));
    rules.push(Arc::new(LimitPushPastWindows::new()));
    rules.push(Arc::new(HashJoinBuffering::new()));
    rules.push(Arc::new(LimitPushdown::new()));
    rules.push(Arc::new(TopKRepartition::new()));
    rules.push(Arc::new(ProjectionPushdown::new()));
    rules.push(Arc::new(PushdownSort::new()));
    rules.push(Arc::new(EnsureCooperative::new()));
    rules.push(Arc::new(FilterPushdown::new_post_optimization()));
    rules.push(Arc::new(RewriteExplicitRepartition::new()));
    rules.push(Arc::new(RewriteCollectLeftHashJoin::new()));
    rules.push(Arc::new(EnforceBarrierPartitioning::new()));
    // Wrap higher-order function expressions so they can be serialized for
    // distributed execution. Runs after SanityCheckPlan-relevant rewrites but
    // before the final sanity check validates the wrapped plan.
    rules.push(Arc::new(WrapHigherOrderFunctions::new()));
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
            datafusion_optimizer_names, actual_datafusion_optimizer_names,
            "the custom physical optimizer rules should include all the default DataFusion optimizer rules in the same order"
        );

        Ok(())
    }

    #[test]
    fn test_optimizer_with_join_reorder_enabled() {
        let options = PhysicalOptimizerOptions {
            enable_join_reorder: true,
            join_reorder: Default::default(),
        };
        let optimizers = get_physical_optimizers(options);

        // Check that JoinReorder is included when enabled
        let rule_names: Vec<&str> = optimizers.iter().map(|opt| opt.name()).collect();
        let has_join_reorder = rule_names.contains(&"JoinReorder");
        assert!(
            has_join_reorder,
            "JoinReorder should be present when enabled, got rules: {:?}",
            rule_names
        );
        assert_eq!(
            optimizers.len(),
            27,
            "Expected 27 rules with join reorder enabled"
        );
    }

    #[test]
    fn test_optimizer_without_join_reorder() {
        let options = PhysicalOptimizerOptions {
            enable_join_reorder: false,
            join_reorder: Default::default(),
        };
        let optimizers = get_physical_optimizers(options);

        // Check that JoinReorder is not included when disabled
        let has_join_reorder = optimizers.iter().any(|opt| opt.name() == "JoinReorder");
        assert!(
            !has_join_reorder,
            "JoinReorder should not be present when disabled"
        );
        assert_eq!(
            optimizers.len(),
            26,
            "Expected 26 rules without join reorder"
        );
    }

    #[test]
    fn test_optimizer_rules_order() -> datafusion::common::Result<()> {
        let optimizers = get_physical_optimizers(Default::default());
        let rule_names: Vec<&str> = optimizers.iter().map(|opt| opt.name()).collect();

        // Verify specific expected rules are present
        // Note: Using exact rule names as they appear in DataFusion
        assert!(
            rule_names.contains(&"OutputRequirements"),
            "OutputRequirements should be present, got: {:?}",
            rule_names
        );
        assert!(
            rule_names.contains(&"aggregate_statistics"),
            "aggregate_statistics should be present, got: {:?}",
            rule_names
        );
        assert!(
            rule_names.contains(&"join_selection"),
            "join_selection should be present, got: {:?}",
            rule_names
        );
        assert!(
            rule_names.contains(&"EnforceDistribution"),
            "EnforceDistribution should be present, got: {:?}",
            rule_names
        );
        assert!(
            rule_names.contains(&"EnforceSorting"),
            "EnforceSorting should be present, got: {:?}",
            rule_names
        );
        assert!(
            rule_names.contains(&"wrap_higher_order_functions"),
            "wrap_higher_order_functions should be present, got: {:?}",
            rule_names
        );
        assert!(
            rule_names.contains(&"SanityCheckPlan"),
            "SanityCheckPlan should be present, got: {:?}",
            rule_names
        );

        // Verify WrapHigherOrderFunctions comes before SanityCheckPlan
        let wrap_idx = rule_names
            .iter()
            .position(|&n| n == "wrap_higher_order_functions")
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "WrapHigherOrderFunctions rule is missing".to_string(),
                )
            })?;
        let sanity_idx = rule_names
            .iter()
            .position(|&n| n == "SanityCheckPlan")
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "SanityCheckPlan rule is missing".to_string(),
                )
            })?;
        assert!(
            wrap_idx < sanity_idx,
            "WrapHigherOrderFunctions should come before SanityCheckPlan"
        );
        Ok(())
    }

    #[test]
    fn test_join_reorder_options_default() {
        let options = PhysicalOptimizerOptions::default();
        assert!(!options.enable_join_reorder);
        // Just verify it compiles and has default implementation
        let _ = options.join_reorder;
    }

    #[test]
    fn test_physical_optimizer_options_clone() {
        let options = PhysicalOptimizerOptions {
            enable_join_reorder: true,
            join_reorder: Default::default(),
        };
        let cloned = options.clone();
        assert_eq!(cloned.enable_join_reorder, options.enable_join_reorder);
    }

    #[test]
    fn test_physical_optimizer_options_debug() {
        let options = PhysicalOptimizerOptions {
            enable_join_reorder: true,
            join_reorder: Default::default(),
        };
        let debug_str = format!("{:?}", options);
        assert!(debug_str.contains("enable_join_reorder"));
        assert!(debug_str.contains("true"));
    }
}
