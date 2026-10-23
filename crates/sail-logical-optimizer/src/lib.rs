use std::sync::Arc;

use datafusion::optimizer::{Analyzer, AnalyzerRule, Optimizer, OptimizerRule};

mod lateral_join;
mod rewrite_binary_grouping;

use lateral_join::DecorrelateLateralProjection;
use rewrite_binary_grouping::RewriteBinaryGrouping;

pub fn default_analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    // Queries arrive with explicit casts and fully typed lambda bindings.
    Analyzer::default()
        .rules
        .into_iter()
        .filter(|rule| rule.name() != "type_coercion")
        .collect()
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
    custom.push(Arc::new(RewriteBinaryGrouping));
    custom
}
