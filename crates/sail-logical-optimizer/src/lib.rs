use std::sync::Arc;

use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
use datafusion::optimizer::{Analyzer, AnalyzerRule, Optimizer, OptimizerRule};
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::LogicalPlan;

mod lateral_join;
mod output_view_types;
mod resolve_lambda_variables;

use lateral_join::DecorrelateLateralProjection;
use output_view_types::ExpandViewTypesAtOutput;
use resolve_lambda_variables::ResolveLambdaVariables;

#[derive(Debug, Default)]
struct SparkTypeCoercion {
    type_coercion_rule: TypeCoercion,
}

impl AnalyzerRule for SparkTypeCoercion {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let mut coercion_config = config.clone();
        coercion_config.optimizer.expand_views_at_output = false;
        self.type_coercion_rule.analyze(plan, &coercion_config)
    }

    fn name(&self) -> &str {
        self.type_coercion_rule.name()
    }
}

fn spark_analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    // FIXME: Create analyzer rule for TypeCoercion in Sail
    //  so we don't have to depend on DataFusion's implementation which is incorrect for Spark.
    let Analyzer {
        function_rewrites: _,
        rules: built_in_rules,
    } = Analyzer::default();
    let mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> =
        vec![Arc::new(ResolveLambdaVariables)];
    rules.extend(built_in_rules.into_iter().map(|rule| {
        if rule.name() == TypeCoercion::new().name() {
            Arc::new(SparkTypeCoercion::default()) as Arc<dyn AnalyzerRule + Send + Sync>
        } else {
            rule
        }
    }));
    rules
}

pub fn default_analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    let mut rules = spark_analyzer_rules();
    // Protocols without a dedicated Arrow output adapter still materialize view arrays in the
    // logical plan. Spark Connect performs this conversion at its Arrow transport boundary.
    rules.push(Arc::new(ExpandViewTypesAtOutput));
    rules
}

pub fn spark_connect_analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    spark_analyzer_rules()
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

    fn rule_names(rules: &[Arc<dyn AnalyzerRule + Send + Sync>]) -> Vec<String> {
        rules.iter().map(|rule| rule.name().to_string()).collect()
    }

    #[test]
    fn spark_connect_does_not_expand_views_in_the_analyzer() {
        let default_rules = rule_names(&default_analyzer_rules());
        let spark_connect_rules = rule_names(&spark_connect_analyzer_rules());

        assert_eq!(
            default_rules.last().map(String::as_str),
            Some("expand_view_types_at_output")
        );
        assert_eq!(
            &default_rules[..default_rules.len() - 1],
            spark_connect_rules
        );
        assert!(!spark_connect_rules.contains(&"expand_view_types_at_output".to_string()));
    }
}
