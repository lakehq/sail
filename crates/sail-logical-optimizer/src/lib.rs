use std::sync::Arc;

use datafusion::optimizer::analyzer::resolve_grouping_function::ResolveGroupingFunction;
use datafusion::optimizer::{Analyzer, AnalyzerRule, Optimizer, OptimizerRule};

mod lateral_join;
mod resolve_lambda_variables;
mod type_coercion;

use crate::type_coercion::TypeCoercion;
use lateral_join::DecorrelateLateralProjection;
use resolve_lambda_variables::ResolveLambdaVariables;

pub fn default_analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    let rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = vec![
        Arc::new(ResolveLambdaVariables),
        Arc::new(ResolveGroupingFunction::new()),
        Arc::new(TypeCoercion::new()),
    ];
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
    // `ResolveLambdaVariables` must run after the built-in rules: constant
    // folding can change the type or nullability of higher-order function
    // arguments, and the lambda variable fields must be refreshed to match.
    custom.push(Arc::new(ResolveLambdaVariables));
    custom
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_analyzer_rules_skip_datafusion_type_coercion() {
        let Analyzer {
            function_rewrites: _,
            rules: datafusion_built_in_rules,
        } = Analyzer::default();
        assert!(datafusion_built_in_rules
            .iter()
            .any(|r| r.name() == "type_coercion"));

        let datafusion_built_in_rules: Vec<_> = datafusion_built_in_rules
            .into_iter()
            .filter(|r| r.name() != "type_coercion")
            .collect();
        assert!(!datafusion_built_in_rules
            .iter()
            .any(|r| r.name() == "type_coercion"));

        let rules = default_analyzer_rules();
        let datafusion_analyzer_names: Vec<&str> = datafusion_built_in_rules
            .iter()
            .map(|rule| rule.name())
            .collect();
        let actual_datafusion_analyzer_names: Vec<&str> = rules
            .iter()
            .map(|rule| rule.name())
            .filter(|name| datafusion_analyzer_names.contains(name))
            .collect();
        assert_eq!(
            datafusion_analyzer_names,
            actual_datafusion_analyzer_names,
            "the custom analyzer rules should include all the default DataFusion analyzer rules except type_coercion in the same order"
        );
    }
}
