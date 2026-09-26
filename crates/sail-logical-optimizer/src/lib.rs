use std::sync::Arc;

use datafusion::optimizer::{Analyzer, AnalyzerRule, Optimizer, OptimizerRule};

mod lateral_join;
mod projected_in;
mod resolve_lambda_variables;
mod rewrite_binary_grouping;
mod scalar_iterator_udf;

use lateral_join::DecorrelateLateralProjection;
use projected_in::RewriteProjectedIn;
use resolve_lambda_variables::ResolveLambdaVariables;
use rewrite_binary_grouping::RewriteBinaryGrouping;
use scalar_iterator_udf::ExtractScalarIteratorUDF;

pub fn default_analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    // FIXME: Create analyzer rule for TypeCoercion in Sail
    //  so we don't have to depend on DataFusion's implementation which is incorrect for Spark.
    let Analyzer {
        function_rewrites: _,
        rules: built_in_rules,
    } = Analyzer::default();
    let mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> =
        vec![Arc::new(ResolveLambdaVariables)];
    rules.extend(built_in_rules);
    // Iterator UDFs need partition streams and must be extracted before scalar folding.
    rules.push(Arc::new(ExtractScalarIteratorUDF));
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
    custom.push(Arc::new(RewriteBinaryGrouping));
    // `ResolveLambdaVariables` must run after the built-in rules: constant
    // folding can change the type or nullability of higher-order function
    // arguments, and the lambda variable fields must be refreshed to match.
    custom.push(Arc::new(ResolveLambdaVariables));
    // Fold projected IN with the query's context before filters can push these
    // expressions into a different decorrelation path. The normal optimizer still
    // runs filter pushdown after the projected results have been materialized.
    let normalization = custom
        .iter()
        .filter(|rule| {
            !matches!(
                rule.name(),
                "push_down_leaf_projections" | "push_down_filter"
            )
        })
        .cloned()
        .collect();
    custom.insert(0, Arc::new(RewriteProjectedIn::new(normalization)));
    custom
}
