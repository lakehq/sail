use std::sync::Arc;

use datafusion::optimizer::analyzer::resolve_grouping_function::ResolveGroupingFunction;
use datafusion::optimizer::{AnalyzerRule, Optimizer, OptimizerRule};

mod lateral_join;
mod resolve_lambda_variables;
mod type_coercion;

use lateral_join::DecorrelateLateralProjection;
use resolve_lambda_variables::ResolveLambdaVariables;

use crate::type_coercion::TypeCoercion;

pub fn default_analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    // Sail's own Spark-compatible `TypeCoercion` (adapted from DataFusion's, which
    // is incorrect for Spark). DataFusion's default analyzer is just
    // `ResolveGroupingFunction` + `TypeCoercion`, so this list mirrors it with our
    // TypeCoercion plus Sail's `ResolveLambdaVariables`.
    vec![
        Arc::new(ResolveLambdaVariables),
        Arc::new(ResolveGroupingFunction::new()),
        Arc::new(TypeCoercion::new()),
    ]
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
    // `ResolveLambdaVariables` must run after the built-in rules: constant
    // folding can change the type or nullability of higher-order function
    // arguments, and the lambda variable fields must be refreshed to match.
    custom.push(Arc::new(ResolveLambdaVariables));
    custom
}
