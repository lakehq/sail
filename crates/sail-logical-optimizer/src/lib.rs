use std::sync::Arc;

use datafusion::optimizer::decorrelate_predicate_subquery::DecorrelatePredicateSubquery;
use datafusion::optimizer::optimize_projections::OptimizeProjections;
use datafusion::optimizer::push_down_limit::PushDownLimit as DataFusionPushDownLimit;
use datafusion::optimizer::scalar_subquery_to_join::ScalarSubqueryToJoin;
use datafusion::optimizer::{Analyzer, AnalyzerRule, Optimizer, OptimizerRule};

mod correlated_limit;
mod deduplicate;
mod eliminate_sorts;
mod input_file_metadata;
mod lateral_join;
mod prune_seeded_random;
mod resolve_lambda_variables;
mod rewrite_binary_grouping;
mod scalar_iterator_udf;
mod scalar_subquery_domain;
mod scalar_subquery_keys;
mod session_timestamp;
mod simplify_expressions;
mod subquery_aliases;
pub mod type_coercion;

use eliminate_sorts::EliminateSorts;
pub use eliminate_sorts::{is_order_irrelevant_aggregate, push_down_left_semi_anti_join};
use input_file_metadata::PushDownLimit;
use lateral_join::DecorrelateLateralProjection;
use prune_seeded_random::PruneSeededRandomColumns;
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
    let mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = vec![
        Arc::new(ResolveLambdaVariables),
        Arc::new(subquery_aliases::NormalizeSubqueryAliases),
    ];
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
        if rule.name() == simplify_expressions::SimplifyExpressions::default().name() {
            custom.push(Arc::new(
                simplify_expressions::SimplifyExpressions::default(),
            ));
            continue;
        }
        if rule.name() == deduplicate::SimplifyDeduplicate::default().name() {
            custom.push(Arc::new(deduplicate::SimplifyDeduplicate::default()));
            continue;
        }
        if rule.name() == DecorrelatePredicateSubquery::new().name() {
            // The top-level sort of a correlated subquery prevents its decorrelation.
            custom.push(Arc::new(EliminateSorts));
            // SimplifyExpressions must first fold LIMIT/OFFSET casts into
            // literals. Decorrelate before limit pushdown can turn the sort
            // into a global top-k, and before predicate or scalar subqueries
            // are converted to joins.
            custom.push(Arc::new(correlated_limit::RemoveExistsLimit));
            custom.push(Arc::new(correlated_limit::DecorrelateLimit));
            custom.push(rule);
            continue;
        }
        if rule.name() == ScalarSubqueryToJoin::new().name() {
            custom.push(Arc::new(
                scalar_subquery_domain::NormalizeScalarSubqueryDomain,
            ));
            custom.push(Arc::new(NormalizeScalarSubqueryKeys));
            custom.push(rule);
            continue;
        }
        if rule.name() == OptimizeProjections::new().name() {
            custom.push(rule);
            custom.push(Arc::new(PruneSeededRandomColumns));
            continue;
        }
        if rule.name() == DataFusionPushDownLimit::new().name() {
            // The sorts below the joins of decorrelated subqueries are removed before limit
            // pushdown would turn them into top-k sorts, which select rows and are kept.
            custom.push(Arc::new(EliminateSorts));
            custom.push(Arc::new(PushDownLimit));
            continue;
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
    fn default_optimizer_rules_eliminate_sorts_before_decorrelation_and_limit_pushdown() {
        let names = default_optimizer_rules()
            .iter()
            .map(|rule| rule.name().to_string())
            .collect::<Vec<_>>();
        let positions = names
            .iter()
            .enumerate()
            .filter(|(_, name)| *name == "eliminate_sorts")
            .map(|(i, _)| i)
            .collect::<Vec<_>>();
        let position = |name: &str| names.iter().position(|n| n == name);
        assert_eq!(positions.len(), 2, "{names:?}");
        assert!(position(DecorrelatePredicateSubquery::new().name()) > Some(positions[0]));
        assert!(position("decorrelate_limit") > Some(positions[0]));
        assert!(position(ScalarSubqueryToJoin::new().name()) > Some(positions[0]));
        assert!(position(ScalarSubqueryToJoin::new().name()) < Some(positions[1]));
        assert_eq!(position(PushDownLimit.name()), Some(positions[1] + 1));
    }

    #[test]
    fn default_optimizer_rules_decorrelate_limits_before_subqueries() {
        let names = default_optimizer_rules()
            .iter()
            .map(|rule| rule.name().to_string())
            .collect::<Vec<_>>();
        let position = |name: &str| names.iter().position(|n| n == name);
        assert!(position("remove_exists_limit").is_some());
        assert!(position("remove_exists_limit") < position("decorrelate_limit"));
        assert!(
            position("decorrelate_limit") < position(DecorrelatePredicateSubquery::new().name())
        );
        assert!(position("decorrelate_limit") < position(ScalarSubqueryToJoin::new().name()));
    }

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
