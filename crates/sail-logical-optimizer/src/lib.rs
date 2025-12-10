use std::sync::Arc;

use datafusion::optimizer::{Analyzer, AnalyzerRule, Optimizer, OptimizerRule};
use sail_plan_lakehouse::lakehouse_optimizer_rules;

pub fn default_analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    let Analyzer {
        function_rewrites: _,
        rules: built_in_rules,
    } = Analyzer::default();

    let mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = vec![];
    rules.extend(built_in_rules);
    rules
}

pub fn default_optimizer_rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let Optimizer { rules } = Optimizer::default();
    let mut custom = lakehouse_optimizer_rules();
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
            rules.first().map(|r| r.name()) == Some("ExpandMergeRule"),
            "ExpandMergeRule should run before built-in optimizers"
        );
    }
}
