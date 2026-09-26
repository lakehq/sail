use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::optimizer::eliminate_cross_join::EliminateCrossJoin as DataFusionEliminateCrossJoin;
use datafusion::optimizer::extract_equijoin_predicate::ExtractEquijoinPredicate as DataFusionExtractEquijoinPredicate;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{DFSchemaRef, NullEquality, Result, internal_err};
use datafusion_expr::{Expr, Extension, JoinType, LogicalPlan, UserDefinedLogicalNodeCore};

#[derive(Debug)]
pub struct EliminateCrossJoin;

impl OptimizerRule for EliminateCrossJoin {
    fn name(&self) -> &str {
        "eliminate_cross_join"
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let join_root = match &plan {
            LogicalPlan::Filter(filter) => filter.input.as_ref(),
            _ => &plan,
        };
        if !is_ordinary_inner_join(join_root) {
            return Ok(Transformed::no(plan));
        }

        // Flatten only ordinary inner joins. Hide independent inputs from the
        // delegated rule's own recursion; the optimizer visits them after restoration.
        let region = isolate_join_region(plan)?;
        DataFusionEliminateCrossJoin::new()
            .rewrite(region, config)?
            .map_data(|plan| {
                plan.transform_up_with_subqueries(|plan| {
                    if let LogicalPlan::Extension(extension) = &plan
                        && let Some(input) =
                            extension.node.as_any().downcast_ref::<JoinRegionInput>()
                    {
                        return Ok(Transformed::yes(input.plan.as_ref().clone()));
                    }
                    Ok(Transformed::no(plan))
                })
                .data()
            })
    }
}

fn is_ordinary_inner_join(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Join(join)
        if join.join_type == JoinType::Inner
            && join.null_equality == NullEquality::NullEqualsNothing)
}

fn isolate_join_region(plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.map_uncorrelated_subqueries(|subquery| {
        subquery.map_children(|input| Ok(Transformed::yes(JoinRegionInput::wrap(input))))
    })?
    .transform_sibling(|plan| {
        plan.map_children(|input| {
            let input = if is_ordinary_inner_join(&input) {
                isolate_join_region(input)?
            } else {
                JoinRegionInput::wrap(input)
            };
            Ok(Transformed::yes(input))
        })
    })
    .data()
}

/// An opaque input used only while rewriting one join region.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
struct JoinRegionInput {
    plan: Arc<LogicalPlan>,
}

impl JoinRegionInput {
    fn wrap(plan: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(Self {
                plan: Arc::new(plan),
            }),
        })
    }
}

impl UserDefinedLogicalNodeCore for JoinRegionInput {
    fn name(&self) -> &str {
        "JoinRegionInput"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.plan.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if !exprs.is_empty() || !inputs.is_empty() {
            return internal_err!("JoinRegionInput must remain opaque");
        }
        Ok(self.clone())
    }
}

#[derive(Debug)]
pub struct ExtractEquijoinPredicate;

impl OptimizerRule for ExtractEquijoinPredicate {
    fn name(&self) -> &str {
        "extract_equijoin_predicate"
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Ordinary equality residuals must not inherit existing null-safe keys'
        // comparison mode when filter pushdown introduces them in a later pass.
        if let LogicalPlan::Join(join) = &plan
            && join.null_equality == NullEquality::NullEqualsNull
            && !join.on.is_empty()
        {
            return Ok(Transformed::no(plan));
        }
        DataFusionExtractEquijoinPredicate::new().rewrite(plan, config)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::provider_as_source;
    use datafusion::optimizer::{Optimizer, OptimizerContext};
    use datafusion_common::tree_node::TreeNodeRecursion;
    use datafusion_expr::expr_fn::exists;
    use datafusion_expr::{LogicalPlanBuilder, col};

    use super::*;

    fn scan(name: &str) -> Result<LogicalPlanBuilder> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
        LogicalPlanBuilder::scan(
            name,
            provider_as_source(Arc::new(EmptyTable::new(schema))),
            None,
        )
    }

    fn null_safe_input() -> Result<LogicalPlanBuilder> {
        scan("x")?
            .join_detailed(
                scan("y")?.build()?,
                JoinType::Inner,
                (vec!["x.k"], vec!["y.k"]),
                Some(col("x.k").eq(col("y.k"))),
                NullEquality::NullEqualsNull,
            )?
            .join_detailed(
                scan("z")?.build()?,
                JoinType::Inner,
                (vec!["y.k"], vec!["z.k"]),
                None,
                NullEquality::NullEqualsNull,
            )
    }

    fn outer_joins(input: LogicalPlan) -> Result<LogicalPlan> {
        scan("a")?
            .cross_join(scan("b")?.build()?)?
            .cross_join(input)?
            .filter(col("a.k").eq(col("d.k")).and(col("b.k").eq(col("d.k"))))?
            .build()
    }

    fn assert_join_regions(
        plan: &LogicalPlan,
        expected_ordinary: usize,
        expected_null_safe: usize,
    ) -> Result<()> {
        let mut ordinary_joins = 0;
        let mut null_safe_joins = 0;
        plan.apply_with_subqueries(|node| {
            match node {
                LogicalPlan::Join(join) => {
                    if join.join_type == JoinType::Inner {
                        assert!(!join.on.is_empty(), "unexpected cross join");
                        ordinary_joins +=
                            usize::from(join.null_equality == NullEquality::NullEqualsNothing);
                    }
                    null_safe_joins +=
                        usize::from(join.null_equality == NullEquality::NullEqualsNull);
                }
                LogicalPlan::Extension(extension) => {
                    assert!(!extension.node.as_any().is::<JoinRegionInput>());
                }
                _ => {}
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert_eq!(
            ordinary_joins,
            expected_ordinary,
            "{}",
            plan.display_indent()
        );
        assert_eq!(
            null_safe_joins,
            expected_null_safe,
            "{}",
            plan.display_indent()
        );
        Ok(())
    }

    #[test]
    fn cross_join_elimination_preserves_independent_null_safe_regions() -> Result<()> {
        let config = OptimizerContext::new();
        let source = null_safe_input()?.project(vec![col("x.k")])?;
        let inputs = [
            source.clone().limit(0, Some(10))?.alias("d")?.build()?,
            source
                .clone()
                .aggregate(vec![col("x.k")], Vec::<Expr>::new())?
                .alias("d")?
                .build()?,
            source.clone().alias("d")?.build()?,
            source
                .join(
                    scan("w")?.build()?,
                    JoinType::Left,
                    (vec!["x.k"], vec!["w.k"]),
                    None,
                )?
                .project(vec![col("x.k")])?
                .alias("d")?
                .build()?,
        ];
        for input in inputs {
            let plan = outer_joins(input.clone())?;
            let expected_schema = plan.schema().clone();
            let rewritten = EliminateCrossJoin.rewrite(plan.clone(), &config)?;
            assert!(rewritten.transformed);
            assert_eq!(rewritten.data.schema(), &expected_schema);
            assert_join_regions(&rewritten.data, 2, 2)?;
            let mut retained_input = false;
            rewritten.data.apply(|node| {
                retained_input |= node == &input;
                Ok(TreeNodeRecursion::Continue)
            })?;
            assert!(retained_input, "independent input must remain unchanged");

            let optimizer = Optimizer::with_rules(crate::default_optimizer_rules());
            let optimized = optimizer.optimize(plan, &config, |_, _| {})?;
            assert_eq!(optimized.schema(), &expected_schema);
            assert_join_regions(&optimized, 2, 2)?;
        }
        Ok(())
    }

    #[test]
    fn null_safe_join_is_a_boundary_without_a_subquery_wrapper() -> Result<()> {
        let input = null_safe_input()?.build()?;
        let plan = scan("a")?
            .cross_join(scan("b")?.build()?)?
            .cross_join(input.clone())?
            .filter(col("a.k").eq(col("x.k")).and(col("b.k").eq(col("x.k"))))?
            .build()?;
        let rewritten = EliminateCrossJoin.rewrite(plan, &OptimizerContext::new())?;
        assert_join_regions(&rewritten.data, 2, 2)?;
        let mut retained_input = false;
        rewritten.data.apply(|node| {
            retained_input |= node == &input;
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert!(retained_input);
        Ok(())
    }

    #[test]
    fn cross_join_elimination_preserves_expression_subqueries() -> Result<()> {
        let subquery = null_safe_input()?
            .project(vec![col("x.k")])?
            .limit(0, Some(10))?
            .build()?;
        let plan = scan("a")?
            .cross_join(scan("b")?.build()?)?
            .filter(
                col("a.k")
                    .eq(col("b.k"))
                    .and(exists(Arc::new(subquery.clone()))),
            )?
            .build()?;
        let rewritten = EliminateCrossJoin.rewrite(plan, &OptimizerContext::new())?;
        assert_join_regions(&rewritten.data, 1, 2)?;
        let mut retained_subquery = false;
        rewritten.data.apply_with_subqueries(|node| {
            retained_subquery |= node == &subquery;
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert!(retained_subquery);
        Ok(())
    }

    #[test]
    fn ordinary_region_below_null_safe_join_is_optimized() -> Result<()> {
        let plan = LogicalPlanBuilder::from(outer_joins(scan("d")?.build()?)?)
            .join_detailed(
                scan("e")?.build()?,
                JoinType::Inner,
                (vec!["d.k"], vec!["e.k"]),
                None,
                NullEquality::NullEqualsNull,
            )?
            .build()?;
        let optimizer = Optimizer::with_rules(vec![Arc::new(EliminateCrossJoin)]);
        let optimized = optimizer.optimize(plan, &OptimizerContext::new(), |_, _| {})?;
        assert_join_regions(&optimized, 2, 1)
    }
}
