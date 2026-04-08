use std::sync::Arc;

use datafusion::optimizer::{AnalyzerRule, OptimizerRule};

pub fn default_analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    sail_logical_optimizer::default_analyzer_rules()
}

pub fn default_optimizer_rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let rules = sail_logical_optimizer::default_optimizer_rules();
    let mut custom = sail_plan_lakehouse::lakehouse_optimizer_rules();
    custom.extend(
        rules
            .into_iter()
            .filter(|r| r.name() != "push_down_leaf_projections"),
    );
    custom
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_rules_start_with_expand_row_level_op() {
        let rules = default_optimizer_rules();
        assert!(
            rules.first().map(|r| r.name()) == Some("expand_row_level_op"),
            "expand_row_level_op should run before built-in optimizers"
        );
    }
}
