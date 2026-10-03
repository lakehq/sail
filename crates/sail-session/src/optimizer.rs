use std::sync::Arc;

use datafusion::optimizer::{AnalyzerRule, OptimizerRule};
use sail_data_source::listing::partition_bounds::ResolvePartitionBounds;

pub fn default_analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    sail_logical_optimizer::default_analyzer_rules()
}

pub fn default_optimizer_rules(
    partition_bounds_from_listing: bool,
) -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let mut rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> =
        sail_logical_optimizer::default_optimizer_rules()
            .into_iter()
            .filter(|r| r.name() != "push_down_leaf_projections")
            .collect();
    // `ResolvePartitionBounds` runs after the built-in rules so that it sees the
    // final shape of the plan: projection pushdown has reached the table scan and
    // the aggregate input is reduced to the partition column.
    //
    // It is left out entirely when disabled rather than being registered as a no-op,
    // so that a disabled rule does not show up in the optimizer trace of `EXPLAIN`.
    if partition_bounds_from_listing {
        rules.push(Arc::new(ResolvePartitionBounds));
    }
    rules
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_rules_skip_leaf_projection_pushdown() {
        let rules = default_optimizer_rules(false);
        assert!(
            !rules
                .iter()
                .any(|rule| rule.name() == "push_down_leaf_projections")
        );
    }

    #[test]
    fn default_rules_include_partition_bounds_when_enabled() {
        let rules = default_optimizer_rules(true);
        assert!(
            rules
                .iter()
                .any(|rule| rule.name() == "resolve_partition_bounds")
        );
    }

    #[test]
    fn default_rules_omit_partition_bounds_when_disabled() {
        // The rule must be absent, not merely inert: a registered rule appears in the
        // optimizer trace of `EXPLAIN`, which would change plans for every other test.
        let rules = default_optimizer_rules(false);
        assert!(
            !rules
                .iter()
                .any(|rule| rule.name() == "resolve_partition_bounds")
        );
    }
}
