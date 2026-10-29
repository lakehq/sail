use std::sync::Arc;

use datafusion::optimizer::scalar_subquery_to_join::ScalarSubqueryToJoin;
use datafusion::optimizer::{Analyzer, AnalyzerRule, Optimizer, OptimizerRule};

mod lateral_join;
mod resolve_lambda_variables;
mod rewrite_binary_grouping;
mod scalar_iterator_udf;
mod scalar_subquery_keys;
mod session_timestamp;
pub mod type_coercion;

use lateral_join::DecorrelateLateralProjection;
use resolve_lambda_variables::ResolveLambdaVariables;
use rewrite_binary_grouping::RewriteBinaryGrouping;
use scalar_iterator_udf::ExtractScalarIteratorUDF;
use scalar_subquery_keys::NormalizeScalarSubqueryKeys;

use crate::type_coercion::TypeCoercion;

pub fn default_analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    // Sail's `TypeCoercion` replaces DataFusion's, whose implementation is incorrect for Spark.
    let Analyzer {
        function_rewrites: _,
        rules: built_in_rules,
    } = Analyzer::default();
    let mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> =
        vec![Arc::new(ResolveLambdaVariables)];
    rules.extend(built_in_rules.into_iter().map(|rule| {
        if rule.name() == datafusion::optimizer::analyzer::type_coercion::TypeCoercion::new().name()
        {
            Arc::new(TypeCoercion::new())
        } else {
            rule
        }
    }));
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
        if rule.name() == ScalarSubqueryToJoin::new().name() {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_analyzer_rules_skip_datafusion_type_coercion() {
        let Analyzer {
            function_rewrites: _,
            rules: datafusion_built_in_rules,
        } = Analyzer::default();
        assert!(
            datafusion_built_in_rules
                .iter()
                .any(|r| r.name() == "type_coercion")
        );

        let datafusion_built_in_rules: Vec<_> = datafusion_built_in_rules
            .into_iter()
            .filter(|r| r.name() != "type_coercion")
            .collect();
        assert!(
            !datafusion_built_in_rules
                .iter()
                .any(|r| r.name() == "type_coercion")
        );

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
            datafusion_analyzer_names, actual_datafusion_analyzer_names,
            "the custom analyzer rules should include all the default DataFusion analyzer rules except type_coercion in the same order"
        );
    }
}
