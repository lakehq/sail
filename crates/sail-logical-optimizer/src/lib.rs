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

pub fn default_analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
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
    // Run this after the built-in analyzer rules so only the final root schema is expanded;
    // casting scan or intermediate schemas would discard the benefit of keeping view arrays.
    rules.push(Arc::new(ExpandViewTypesAtOutput));
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
