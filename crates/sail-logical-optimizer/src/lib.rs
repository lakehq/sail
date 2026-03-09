use std::sync::Arc;

use datafusion::optimizer::{Analyzer, AnalyzerRule, Optimizer, OptimizerRule};

pub mod case_expr;

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
    let mut rules = rules;
    rules.push(Arc::new(case_expr::ReconstructSimpleCaseExpr));
    rules
}
