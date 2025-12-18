use std::sync::Arc;

use datafusion::optimizer::{AnalyzerRule, Optimizer, OptimizerRule};

pub fn default_analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    sail_logical_optimizer::default_analyzer_rules()
}

pub fn default_optimizer_rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let Optimizer { rules } = Optimizer::default();
    let mut custom = sail_plan_lakehouse::lakehouse_optimizer_rules();
    custom.extend(rules);
    custom
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_rules_start_with_expand_merge() {
        let rules = default_optimizer_rules();
        assert!(
            rules.first().map(|r| r.name()) == Some("expand_merge"),
            "expand_merge should run before built-in optimizers"
        );
    }
}
