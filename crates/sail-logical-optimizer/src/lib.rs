use std::sync::Arc;

use datafusion::optimizer::analyzer::resolve_grouping_function::ResolveGroupingFunction;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
use datafusion::optimizer::common_subexpr_eliminate::CommonSubexprEliminate;
use datafusion::optimizer::decorrelate_lateral_join::DecorrelateLateralJoin;
use datafusion::optimizer::decorrelate_predicate_subquery::DecorrelatePredicateSubquery;
use datafusion::optimizer::eliminate_cross_join::EliminateCrossJoin;
use datafusion::optimizer::eliminate_duplicated_expr::EliminateDuplicatedExpr;
use datafusion::optimizer::eliminate_filter::EliminateFilter;
use datafusion::optimizer::eliminate_group_by_constant::EliminateGroupByConstant;
use datafusion::optimizer::eliminate_join::EliminateJoin;
use datafusion::optimizer::eliminate_limit::EliminateLimit;
use datafusion::optimizer::eliminate_outer_join::EliminateOuterJoin;
use datafusion::optimizer::extract_equijoin_predicate::ExtractEquijoinPredicate;
use datafusion::optimizer::filter_null_join_keys::FilterNullJoinKeys;
use datafusion::optimizer::optimize_projections::OptimizeProjections;
use datafusion::optimizer::optimize_unions::OptimizeUnions;
use datafusion::optimizer::propagate_empty_relation::PropagateEmptyRelation;
use datafusion::optimizer::push_down_filter::PushDownFilter;
use datafusion::optimizer::push_down_limit::PushDownLimit;
use datafusion::optimizer::replace_distinct_aggregate::ReplaceDistinctWithAggregate;
use datafusion::optimizer::scalar_subquery_to_join::ScalarSubqueryToJoin;
use datafusion::optimizer::simplify_expressions::SimplifyExpressions;
use datafusion::optimizer::single_distinct_to_groupby::SingleDistinctToGroupBy;
use datafusion::optimizer::{AnalyzerRule, OptimizerRule};

use crate::case_expr::ReconstructSimpleCaseExpr;

pub mod case_expr;

pub fn default_analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    vec![
        Arc::new(ResolveGroupingFunction::new()),
        Arc::new(TypeCoercion::new()),
    ]
}

pub fn default_optimizer_rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    vec![
        Arc::new(OptimizeUnions::new()),
        Arc::new(SimplifyExpressions::new()),
        Arc::new(ReplaceDistinctWithAggregate::new()),
        Arc::new(EliminateJoin::new()),
        Arc::new(DecorrelatePredicateSubquery::new()),
        Arc::new(ScalarSubqueryToJoin::new()),
        Arc::new(DecorrelateLateralJoin::new()),
        Arc::new(ExtractEquijoinPredicate::new()),
        Arc::new(EliminateDuplicatedExpr::new()),
        Arc::new(EliminateFilter::new()),
        Arc::new(EliminateCrossJoin::new()),
        Arc::new(EliminateLimit::new()),
        Arc::new(PropagateEmptyRelation::new()),
        // TODO: activate by setting `optimizer.filter_null_join_keys = true` in session config
        Arc::new(FilterNullJoinKeys::default()),
        Arc::new(EliminateOuterJoin::new()),
        // Filters can't be pushed down past Limits, we should do PushDownFilter after PushDownLimit
        Arc::new(PushDownLimit::new()),
        Arc::new(PushDownFilter::new()),
        Arc::new(SingleDistinctToGroupBy::new()),
        // The previous optimizations added expressions and projections,
        // that might benefit from the following rules
        Arc::new(EliminateGroupByConstant::new()),
        Arc::new(CommonSubexprEliminate::new()),
        Arc::new(OptimizeProjections::new()),
        // Sail custom rules
        Arc::new(ReconstructSimpleCaseExpr),
    ]
}

#[cfg(test)]
mod tests {
    use datafusion::optimizer::{Analyzer, Optimizer};

    use super::*;

    #[test]
    fn test_optimizer_rules() -> datafusion::common::Result<()> {
        let optimizers = default_optimizer_rules();
        let datafusion_optimizers = Optimizer::default().rules;

        let datafusion_optimizer_names: Vec<&str> =
            datafusion_optimizers.iter().map(|opt| opt.name()).collect();
        let actual_datafusion_optimizer_names: Vec<&str> = optimizers
            .iter()
            .map(|opt| opt.name())
            .filter(|name| datafusion_optimizer_names.contains(name))
            .collect();
        assert_eq!(
            datafusion_optimizer_names,
            actual_datafusion_optimizer_names,
            "the custom logical optimizer rules should include all the default DataFusion optimizer rules in the same order"
        );

        Ok(())
    }

    #[test]
    fn test_analyzer_rules() -> datafusion::common::Result<()> {
        let analyzers = default_analyzer_rules();
        let datafusion_analyzers = Analyzer::default().rules;

        let datafusion_analyzer_names: Vec<&str> =
            datafusion_analyzers.iter().map(|opt| opt.name()).collect();
        let actual_datafusion_analyzer_names: Vec<&str> = analyzers
            .iter()
            .map(|opt| opt.name())
            .filter(|name| datafusion_analyzer_names.contains(name))
            .collect();
        assert_eq!(
            datafusion_analyzer_names,
            actual_datafusion_analyzer_names,
            "the custom analyzer rules should include all the default DataFusion analyzer rules in the same order"
        );

        Ok(())
    }
}
