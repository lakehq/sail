use std::sync::Arc;

use datafusion::optimizer::push_down_limit::PushDownLimit as DataFusionPushDownLimit;
use datafusion::optimizer::{Analyzer, AnalyzerRule, Optimizer, OptimizerRule};
use sail_common_datafusion::decorrelate::{
    DecorrelateLateralJoin, DecorrelatePredicateSubquery, ScalarSubqueryToJoin,
};

mod correlated_limit;
mod deduplicate;
mod input_file_metadata;
mod lateral_join;
mod resolve_lambda_variables;
mod rewrite_binary_grouping;
mod scalar_iterator_udf;
mod scalar_subquery_domain;
mod scalar_subquery_keys;
mod session_timestamp;
mod simplify_expressions;
mod subquery_aliases;
pub mod type_coercion;

use input_file_metadata::PushDownLimit;
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
    // in Filter/Aggregate) are left for the copied `DecorrelateLateralJoin`.
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
            custom.push(Arc::new(DecorrelatePredicateSubquery::new()));
            continue;
        }
        if rule.name() == DecorrelateLateralJoin::new().name() {
            custom.push(Arc::new(DecorrelateLateralJoin::new()));
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
            custom.push(Arc::new(ScalarSubqueryToJoin::new()));
            continue;
        }
        if rule.name() == DataFusionPushDownLimit::new().name() {
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
    mod decorrelate {
        use std::sync::Arc;

        use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::dataframe::DataFrame;
        use datafusion::datasource::{MemTable, provider_as_source};
        use datafusion::execution::context::SessionConfig;
        use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
        use datafusion::functions::expr_fn::{concat, random};
        use datafusion::functions_aggregate::expr_fn::{count, max, sum};
        use datafusion::optimizer::{Optimizer, OptimizerContext, OptimizerRule};
        use datafusion::physical_plan::displayable;
        use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
        use datafusion_common::{Column, Result, Spans};
        use datafusion_expr::{
            Expr, LogicalPlan, LogicalPlanBuilder, Operator, Subquery, binary_expr, cast, col,
            in_subquery, lit, out_ref_col, scalar_subquery, when,
        };
        use sail_common_datafusion::decorrelate::{
            DecorrelatePredicateSubquery, ScalarSubqueryToJoin,
        };

        fn state() -> SessionState {
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(SessionConfig::new().with_target_partitions(1))
                .with_optimizer_rules(crate::default_optimizer_rules())
                .build()
        }

        fn scan(name: &str, inner: bool) -> Result<LogicalPlan> {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("v", DataType::Int64, true),
            ]));
            let ids = if inner {
                vec![Some(0), Some(0), Some(2), None]
            } else {
                vec![Some(0), Some(1), Some(2), None]
            };
            let values = if inner {
                vec![Some(10), None, None, Some(99)]
            } else {
                vec![None; 4]
            };
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(Int64Array::from(values)),
                ],
            )?;
            let table = MemTable::try_new(schema, vec![vec![batch]])?;
            LogicalPlanBuilder::scan(name, provider_as_source(Arc::new(table)), None)?.build()
        }

        fn aggregate(expr: Expr) -> Result<LogicalPlan> {
            LogicalPlanBuilder::from(scan("i", true)?)
                .filter(col("i.id").eq(out_ref_col(DataType::Int64, "o.id")))?
                .aggregate(Vec::<Expr>::new(), vec![expr.alias("c")])?
                .build()
        }

        fn regex(prefix: &str, number: Expr) -> Expr {
            binary_expr(
                concat(vec![lit(prefix), cast(number, DataType::Utf8)]),
                Operator::RegexMatch,
                lit(r"a\\b"),
            )
        }

        fn assert_regex_plan(plan: &LogicalPlan) -> Result<()> {
            let mut regex_count = 0;
            let mut join_count = 0;
            plan.apply(|node| {
                join_count += usize::from(matches!(node, LogicalPlan::Join(_)));
                node.apply_expressions(|expr| {
                    expr.apply(|expr| {
                        assert!(!matches!(expr, Expr::Like(_)), "{expr:?}");
                        assert!(
                            !matches!(expr, Expr::ScalarSubquery(_) | Expr::InSubquery(_) | Expr::Exists(_)),
                            "subquery was not decorrelated: {expr:?}"
                        );
                        regex_count += usize::from(
                            matches!(expr, Expr::BinaryExpr(b) if matches!(b.op, Operator::RegexMatch | Operator::RegexNotMatch)),
                        );
                        Ok(TreeNodeRecursion::Continue)
                    })
                })?;
                Ok(TreeNodeRecursion::Continue)
            })?;
            assert!(regex_count > 0, "{}", plan.display_indent());
            assert!(join_count > 0, "{}", plan.display_indent());
            Ok(())
        }

        async fn execute(plan: LogicalPlan, check_regex: bool) -> Result<Vec<RecordBatch>> {
            let frame = DataFrame::new(state(), plan);
            let optimized = frame.clone().into_optimized_plan()?;
            if check_regex {
                assert_regex_plan(&optimized)?;
            }
            println!("OPTIMIZED:\n{}", optimized.display_indent());
            let physical = frame.clone().create_physical_plan().await?;
            println!("PHYSICAL:\n{}", displayable(physical.as_ref()).indent(true));
            frame.collect().await
        }

        fn column(batches: &[RecordBatch], index: usize) -> Result<ArrayRef> {
            let arrays = batches
                .iter()
                .map(|b| b.column(index).as_ref())
                .collect::<Vec<_>>();
            Ok(arrow::compute::concat(&arrays)?)
        }

        #[test]
        fn replacements_preserve_upstream_rule_order_and_sail_protections() {
            let original = Optimizer::default().rules;
            let rules = crate::default_optimizer_rules();
            let originals = original.iter().map(|r| r.name()).collect::<Vec<_>>();
            let actual = rules
                .iter()
                .filter(|r| originals.contains(&r.name()))
                .map(|r| r.name())
                .collect::<Vec<_>>();
            assert_eq!(actual, originals);
            for name in [
                "scalar_subquery_to_join",
                "decorrelate_predicate_subquery",
                "decorrelate_lateral_join",
            ] {
                assert_eq!(rules.iter().filter(|r| r.name() == name).count(), 1);
                let before = original
                    .iter()
                    .find(|r| r.name() == name)
                    .map(|r| r.apply_order());
                let after = rules
                    .iter()
                    .find(|r| r.name() == name)
                    .map(|r| r.apply_order());
                assert_eq!(before, after);
            }
            let names = rules.iter().map(|r| r.name()).collect::<Vec<_>>();
            let scalar = names.iter().position(|n| *n == "scalar_subquery_to_join");
            assert!(scalar.is_some());
            let scalar = scalar.unwrap_or_default();
            assert_eq!(
                &names[scalar - 3..scalar],
                &[
                    "decorrelate_limit",
                    "normalize_scalar_subquery_domain",
                    "normalize_scalar_subquery_keys"
                ]
            );
            assert_eq!(names.first(), Some(&"decorrelate_lateral_projection"));
            assert_eq!(names.last(), Some(&"resolve_lambda_variables"));
        }

        #[derive(Clone, Copy, Debug)]
        enum SubqueryKind {
            Scalar,
            Lateral,
        }

        async fn check_dynamic_regex(
            kind: SubqueryKind,
            prefix: &str,
            expected: bool,
        ) -> Result<()> {
            // Scalar/lateral COUNT becomes zero on empty input, but random() remains dynamic.
            let inner = LogicalPlanBuilder::from(aggregate(count(lit(1_i64)))?)
                .project(vec![
                    regex(prefix, cast(col("c"), DataType::Float64) + random()).alias("matched"),
                ])?
                .build()?;
            let outer = scan("o", false)?;
            let plan = match kind {
                SubqueryKind::Scalar => LogicalPlanBuilder::from(outer)
                    .project(vec![
                        col("o.id"),
                        scalar_subquery(Arc::new(inner)).alias("matched"),
                    ])?
                    .build()?,
                SubqueryKind::Lateral => {
                    let refs = inner.all_out_ref_exprs();
                    let right = LogicalPlan::Subquery(Subquery {
                        subquery: Arc::new(inner),
                        outer_ref_columns: refs,
                        spans: Spans::new(),
                    });
                    LogicalPlanBuilder::from(outer)
                        .cross_join(right)?
                        .project(vec![col("o.id"), col("matched")])?
                        .build()?
                }
            };
            let plan = LogicalPlanBuilder::from(plan)
                .sort(vec![col("o.id").sort(true, false)])?
                .build()?;
            println!("CASE {kind:?}, prefix={prefix:?}");
            let batches = execute(plan, true).await?;
            let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(rows, 4);
            assert_eq!(
                column(&batches, 1)?.as_ref(),
                &BooleanArray::from(vec![expected; 4]) as &dyn Array
            );
            Ok(())
        }

        macro_rules! dynamic_regex_tests {
            ($($name:ident: $kind:ident, $prefix:expr, $expected:expr;)*) => {
                $(#[tokio::test]
                async fn $name() -> Result<()> {
                    check_dynamic_regex(SubqueryKind::$kind, $prefix, $expected).await
                })*
            };
        }

        dynamic_regex_tests! {
            scalar_dynamic_regex_matches_backslash: Scalar, r"a\b", true;
            scalar_dynamic_regex_rejects_missing_backslash: Scalar, "ab", false;
            lateral_dynamic_regex_matches_backslash: Lateral, r"a\b", true;
            lateral_dynamic_regex_rejects_missing_backslash: Lateral, "ab", false;
        }

        #[test]
        fn scalar_count_in_predicate_retains_upstream_compensation_guard() -> Result<()> {
            let inner = LogicalPlanBuilder::from(aggregate(count(lit(1_i64)))?)
                .project(vec![
                    regex(r"a\b", cast(col("c"), DataType::Float64) + random()).alias("matched"),
                ])?
                .build()?;
            let plan = LogicalPlanBuilder::from(scan("o", false)?)
                .filter(in_subquery(lit(true), Arc::new(inner)))?
                .build()?;
            let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = vec![
                Arc::new(DecorrelatePredicateSubquery::new()),
                Arc::new(datafusion::optimizer::decorrelate_predicate_subquery::DecorrelatePredicateSubquery::new()),
            ];
            for rule in rules {
                let optimized = Optimizer::with_rules(vec![rule]).optimize(
                    plan.clone(),
                    &OptimizerContext::new(),
                    |_, _| {},
                )?;
                assert_eq!(optimized, plan);
                let LogicalPlan::Filter(filter) = optimized else {
                    panic!("expected the unsupported predicate to remain a filter");
                };
                assert!(matches!(filter.predicate, Expr::InSubquery(_)));
            }
            Ok(())
        }

        #[tokio::test]
        async fn non_strong_projection_compensates_missing_and_null_groups() -> Result<()> {
            let aggregate_expr = max(col("i.v"));
            // An aggregate's expression name is one unqualified column, even without
            // DataFusion's SQL identifier parser feature.
            let value = Expr::Column(Column::new_unqualified(
                aggregate_expr.schema_name().to_string(),
            ));
            let inner = LogicalPlanBuilder::from(scan("i", true)?)
                .filter(col("i.id").eq(out_ref_col(DataType::Int64, "o.id")))?
                .aggregate(Vec::<Expr>::new(), vec![aggregate_expr])?
                .project(vec![
                    when(cast(value, DataType::Boolean), lit("a"))
                        .otherwise(lit("b"))?
                        .alias("result"),
                ])?
                .build()?;
            let plan = LogicalPlanBuilder::from(scan("o", false)?)
                .project(vec![
                    col("o.id"),
                    scalar_subquery(Arc::new(inner)).alias("result"),
                ])?
                .sort(vec![col("o.id").sort(true, false)])?
                .build()?;
            let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = vec![
                Arc::new(ScalarSubqueryToJoin::new()),
                Arc::new(
                    datafusion::optimizer::scalar_subquery_to_join::ScalarSubqueryToJoin::new(),
                ),
            ];
            for rule in rules {
                let optimized = Optimizer::with_rules(vec![rule]).optimize(
                    plan.clone(),
                    &OptimizerContext::new(),
                    |_, _| {},
                )?;
                let mut compensates_empty_input = false;
                optimized.apply(|node| {
                    node.apply_expressions(|expr| {
                        expr.apply(|expr| {
                            if let Expr::Case(case) = expr {
                                compensates_empty_input |= case.when_then_expr.iter().any(|(when, _)| {
                                    matches!(when.as_ref(), Expr::IsNull(value)
                                        if matches!(value.as_ref(), Expr::Column(c) if c.name == "__always_true"))
                                });
                            }
                            Ok(TreeNodeRecursion::Continue)
                        })
                    })?;
                    Ok(TreeNodeRecursion::Continue)
                })?;
                assert!(compensates_empty_input, "{}", optimized.display_indent());
                let batches = execute(optimized, false).await?;
                assert_eq!(
                    column(&batches, 1)?.as_ref(),
                    &StringArray::from(vec!["a", "b", "b", "b"]) as &dyn Array
                );
            }
            Ok(())
        }

        #[tokio::test]
        async fn correlated_count_and_nulls_keep_empty_input_semantics() -> Result<()> {
            let mut expressions = vec![col("o.id")];
            for (name, expr) in [
                ("rows", count(lit(1_i64))),
                ("nonnull", count(col("i.v"))),
                ("total", sum(col("i.v"))),
            ] {
                expressions.push(scalar_subquery(Arc::new(aggregate(expr)?)).alias(name));
            }
            let plan = LogicalPlanBuilder::from(scan("o", false)?)
                .project(expressions)?
                .sort(vec![col("o.id").sort(true, false)])?
                .build()?;
            let batches = execute(plan, false).await?;
            for (index, expected) in [
                (1, vec![Some(2), Some(0), Some(1), Some(0)]),
                (2, vec![Some(1), Some(0), Some(0), Some(0)]),
                (3, vec![Some(10), None, None, None]),
            ] {
                assert_eq!(
                    column(&batches, index)?.as_ref(),
                    &Int64Array::from(expected) as &dyn Array
                );
            }
            Ok(())
        }
    }

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
