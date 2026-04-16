use std::sync::Arc;

use datafusion::optimizer::{AnalyzerRule, OptimizerRule};

pub fn default_analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    sail_logical_optimizer::default_analyzer_rules()
}

pub fn default_optimizer_rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let mut custom = sail_plan_lakehouse::lakehouse_optimizer_rules();
    // `push_down_leaf_projections` breaks generator functions (explode/inline)
    // on nested list-of-struct inputs, so we keep it out of the session pipeline.
    // It is still present in `sail_logical_optimizer::default_optimizer_rules()`
    // to preserve parity with DataFusion's default rule set.
    custom.extend(
        sail_logical_optimizer::default_optimizer_rules()
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
