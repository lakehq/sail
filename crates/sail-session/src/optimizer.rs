use std::sync::Arc;

use datafusion::optimizer::{AnalyzerRule, OptimizerRule};

pub fn default_analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    sail_logical_optimizer::default_analyzer_rules()
}

pub fn default_optimizer_rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let mut custom = sail_plan_lakehouse::lakehouse_optimizer_rules();
    custom.extend(sail_logical_optimizer::default_optimizer_rules());
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

    #[test]
    fn default_rules_include_reconstruct_simple_case_expr() {
        let rules = default_optimizer_rules();
        assert!(
            rules
                .iter()
                .any(|r| r.name() == "reconstruct_simple_case_expr"),
            "reconstruct_simple_case_expr must be registered in the optimizer pipeline"
        );
    }
}
