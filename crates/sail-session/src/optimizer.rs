use std::sync::Arc;

use datafusion::optimizer::{AnalyzerRule, OptimizerRule};

pub fn default_analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    sail_logical_optimizer::default_analyzer_rules()
}

pub fn default_optimizer_rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let rules = sail_logical_optimizer::default_optimizer_rules();
    rules
        .into_iter()
        .filter(|r| r.name() != "push_down_leaf_projections")
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_rules_skip_leaf_projection_pushdown() {
        let rules = default_optimizer_rules();
        assert!(
            !rules
                .iter()
                .any(|rule| rule.name() == "push_down_leaf_projections")
        );
    }
}
