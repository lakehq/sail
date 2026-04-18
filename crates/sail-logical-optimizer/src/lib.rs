use std::sync::Arc;

use datafusion::optimizer::{Analyzer, AnalyzerRule, Optimizer, OptimizerRule};

mod lateral_join;
pub use lateral_join::DecorrelateLateralProjection;

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
    // Custom rules are prepended so they run before DataFusion's built-in rules.
    // `DecorrelateLateralProjection` must run before `DecorrelateLateralJoin`
    // because it handles the simple case where OuterRef only appears in
    // Projection expressions (e.g. `LATERAL (SELECT t1.a + 1)`), rewriting
    // it into a CrossJoin + Projection. The remaining complex cases (OuterRef
    // in Filter/Aggregate) are left for DataFusion's `DecorrelateLateralJoin`.
    let mut custom: Vec<Arc<dyn OptimizerRule + Send + Sync>> =
        vec![Arc::new(DecorrelateLateralProjection::new())];
    custom.extend(rules);
    custom
}
