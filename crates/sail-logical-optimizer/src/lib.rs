use std::sync::Arc;

use datafusion::optimizer::scalar_subquery_to_join::ScalarSubqueryToJoin;
use datafusion::optimizer::{Analyzer, AnalyzerRule, Optimizer, OptimizerRule};

mod correlated_limit;
mod deduplicate;
mod lateral_join;
mod resolve_lambda_variables;
mod rewrite_binary_grouping;
mod scalar_iterator_udf;
mod scalar_subquery_domain;
mod scalar_subquery_keys;
mod subquery_aliases;

use lateral_join::DecorrelateLateralProjection;
use resolve_lambda_variables::ResolveLambdaVariables;
use rewrite_binary_grouping::RewriteBinaryGrouping;
use scalar_iterator_udf::ExtractScalarIteratorUDF;
use scalar_subquery_keys::NormalizeScalarSubqueryKeys;

pub fn default_analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    // FIXME: Create analyzer rule for TypeCoercion in Sail
    //  so we don't have to depend on DataFusion's implementation which is incorrect for Spark.
    let Analyzer {
        function_rewrites: _,
        rules: built_in_rules,
    } = Analyzer::default();
    let mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = vec![
        Arc::new(ResolveLambdaVariables),
        Arc::new(subquery_aliases::NormalizeSubqueryAliases),
    ];
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
    for rule in rules {
        if rule.name() == deduplicate::SimplifyDeduplicate::default().name() {
            custom.push(Arc::new(deduplicate::SimplifyDeduplicate::default()));
            continue;
        }
        if rule.name() == ScalarSubqueryToJoin::new().name() {
            // SimplifyExpressions must first fold LIMIT/OFFSET casts into
            // literals. Decorrelate before limit pushdown can turn the sort
            // into a global top-k or scalar-subquery conversion consumes it.
            custom.push(Arc::new(correlated_limit::DecorrelateLimit));
            custom.push(Arc::new(
                scalar_subquery_domain::NormalizeScalarSubqueryDomain,
            ));
            custom.push(Arc::new(NormalizeScalarSubqueryKeys));
        }
        custom.push(rule);
    }
    custom.push(Arc::new(RewriteBinaryGrouping));
    // `ResolveLambdaVariables` must run after the built-in rules: constant
    // folding can change the type or nullability of higher-order function
    // arguments, and the lambda variable fields must be refreshed to match.
    custom.push(Arc::new(ResolveLambdaVariables));
    custom
}
