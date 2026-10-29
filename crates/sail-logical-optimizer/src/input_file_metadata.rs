use std::sync::Arc;

use datafusion::optimizer::push_down_limit::PushDownLimit as DataFusionPushDownLimit;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::utils::combine_limit;
use datafusion_expr::{Expr, FetchType, Limit, LogicalPlan, Projection, SkipType, lit};
use sail_common_datafusion::input_file::is_input_file_metadata_function;

/// Extends DataFusion's limit pushdown while preserving input-file metadata evaluation context.
#[derive(Debug)]
pub(crate) struct PushDownLimit;

impl OptimizerRule for PushDownLimit {
    fn name(&self) -> &str {
        "push_down_limit"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Limit(limit) = plan else {
            return DataFusionPushDownLimit::new().rewrite(plan, config);
        };

        // DataFusion recursively rewrites merged limits. Merge them here so that
        // recursion cannot bypass the metadata projection guard.
        merge_limits(limit)?.transform_data(|limit| {
            if let LogicalPlan::Projection(projection) = limit.input.as_ref()
                && references_input_file_metadata(projection)?
            {
                // Moving the limit below this projection would separate metadata
                // evaluation from the scan that supplies it.
                return Ok(Transformed::no(LogicalPlan::Limit(limit)));
            }
            DataFusionPushDownLimit::new().rewrite(LogicalPlan::Limit(limit), config)
        })
    }
}

fn merge_limits(mut limit: Limit) -> Result<Transformed<Limit>> {
    let mut merged = false;
    loop {
        let SkipType::Literal(skip) = limit.get_skip_type()? else {
            break;
        };
        let FetchType::Literal(fetch) = limit.get_fetch_type()? else {
            break;
        };
        let LogicalPlan::Limit(child) = limit.input.as_ref() else {
            break;
        };
        let SkipType::Literal(child_skip) = child.get_skip_type()? else {
            break;
        };
        let FetchType::Literal(child_fetch) = child.get_fetch_type()? else {
            break;
        };
        let (skip, fetch) = combine_limit(skip, fetch, child_skip, child_fetch);
        limit = Limit {
            skip: Some(Box::new(lit(skip as i64))),
            fetch: fetch.map(|fetch| Box::new(lit(fetch as i64))),
            input: Arc::clone(&child.input),
        };
        merged = true;
    }
    Ok(Transformed::new_transformed(limit, merged))
}

fn references_input_file_metadata(projection: &Projection) -> Result<bool> {
    for expression in &projection.expr {
        if expression.exists(|expression| {
            Ok(matches!(expression, Expr::ScalarFunction(function)
                if is_input_file_metadata_function(&function.func)))
        })? {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::provider_as_source;
    use datafusion::functions::core::expr_fn::{coalesce, input_file_name};
    use datafusion::optimizer::{Optimizer, OptimizerContext};
    use datafusion_common::internal_err;
    use datafusion_common::tree_node::TreeNodeRecursion;
    use datafusion_expr::{LogicalPlanBuilder, ScalarUDF, col};
    use sail_common_datafusion::input_file::{InputFileBlockLengthFunc, InputFileBlockStartFunc};

    use super::*;

    fn scan() -> Result<LogicalPlanBuilder> {
        let table = EmptyTable::new(Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )])));
        LogicalPlanBuilder::scan("t", provider_as_source(Arc::new(table)), None)
    }

    fn metadata_expressions() -> Vec<Expr> {
        vec![
            input_file_name(),
            // Sail's filename expression also supplies Spark's non-nullable schema.
            coalesce(vec![input_file_name(), lit("")]),
            ScalarUDF::from(InputFileBlockStartFunc::new()).call(vec![]),
            ScalarUDF::from(InputFileBlockLengthFunc::new()).call(vec![]),
        ]
    }

    fn optimize(plan: LogicalPlan) -> Result<LogicalPlan> {
        Optimizer::with_rules(vec![Arc::new(PushDownLimit)]).optimize(
            plan,
            &OptimizerContext::new().with_skip_failing_rules(false),
            |_, _| {},
        )
    }

    fn limit_input(plan: &LogicalPlan, skip: usize, fetch: Option<usize>) -> Result<&LogicalPlan> {
        let LogicalPlan::Limit(limit) = plan else {
            return internal_err!("expected a limit, got {plan:?}");
        };
        assert!(matches!(limit.get_skip_type()?, SkipType::Literal(value) if value == skip));
        assert!(matches!(limit.get_fetch_type()?, FetchType::Literal(value) if value == fetch));
        Ok(&limit.input)
    }

    fn projection(plan: &LogicalPlan) -> Result<&Projection> {
        let LogicalPlan::Projection(projection) = plan else {
            return internal_err!("expected a projection, got {plan:?}");
        };
        Ok(projection)
    }

    fn assert_scan_fetch(plan: &LogicalPlan, fetch: Option<usize>) -> Result<()> {
        let LogicalPlan::TableScan(scan) = plan else {
            return internal_err!("expected a scan, got {plan:?}");
        };
        assert_eq!(scan.fetch, fetch);
        Ok(())
    }

    #[test]
    fn keeps_metadata_below_limits_and_offsets() -> Result<()> {
        for expression in metadata_expressions() {
            for expression in [expression.clone(), expression.is_not_null()] {
                for (skip, fetch) in [(0, Some(3)), (1, Some(3)), (1, None), (0, Some(0))] {
                    let plan = scan()?
                        .project(vec![col("id"), expression.clone().alias("metadata")])?
                        .limit(skip, fetch)?
                        .build()?;
                    let result = PushDownLimit.rewrite(plan.clone(), &OptimizerContext::new())?;
                    assert!(!result.transformed);
                    assert_eq!(result.data, plan);
                    let optimized = optimize(plan.clone())?;
                    assert_eq!(optimized, plan);
                    let projection = projection(limit_input(&optimized, skip, fetch)?)?;
                    assert_scan_fetch(&projection.input, None)?;
                }
            }
        }
        Ok(())
    }

    #[test]
    fn merges_nested_limits_without_crossing_metadata() -> Result<()> {
        for expression in metadata_expressions() {
            for (limits, skip, fetch) in [
                ([(2, Some(9)), (3, None), (1, Some(3))], 6, Some(3)),
                ([(2, Some(3)), (4, Some(2)), (1, None)], 7, Some(0)),
                ([(2, None), (3, None), (1, None)], 6, None),
            ] {
                let projected = scan()?
                    .project(vec![col("id"), expression.clone().alias("metadata")])?
                    .build()?;
                let mut builder = LogicalPlanBuilder::from(projected.clone());
                for (skip, fetch) in limits {
                    builder = builder.limit(skip, fetch)?;
                }
                let result = PushDownLimit.rewrite(builder.build()?, &OptimizerContext::new())?;
                assert!(result.transformed);
                assert_eq!(limit_input(&result.data, skip, fetch)?, &projected);
                let repeated =
                    PushDownLimit.rewrite(result.data.clone(), &OptimizerContext::new())?;
                assert!(!repeated.transformed);
                assert_eq!(repeated.data, result.data);
                assert_eq!(optimize(result.data.clone())?, result.data);
            }
        }
        Ok(())
    }

    #[test]
    fn keeps_metadata_above_existing_limits() -> Result<()> {
        for expression in metadata_expressions() {
            let plan = scan()?
                .limit(1, Some(3))?
                .project(vec![col("id"), expression.alias("metadata")])?
                .build()?;
            let optimized = optimize(plan.clone())?;
            let projected = projection(&optimized)?;
            assert_eq!(projected.expr, projection(&plan)?.expr);
            assert_scan_fetch(limit_input(&projected.input, 1, Some(3))?, Some(4))?;
            assert_eq!(optimize(optimized.clone())?, optimized);
        }
        Ok(())
    }

    #[test]
    fn preserves_deterministic_projection_pushdown() -> Result<()> {
        let plan = scan()?
            .project(vec![(col("id") + lit(1_i64)).alias("next_id")])?
            .limit(2, Some(10))?
            .limit(1, Some(4))?
            .build()?;
        let upstream = Optimizer::with_rules(vec![Arc::new(DataFusionPushDownLimit::new())])
            .optimize(
                plan.clone(),
                &OptimizerContext::new().with_skip_failing_rules(false),
                |_, _| {},
            )?;
        let optimized = optimize(plan)?;
        assert_eq!(optimized, upstream);
        let projection = projection(&optimized)?;
        assert_scan_fetch(limit_input(&projection.input, 3, Some(4))?, Some(7))?;
        assert_eq!(optimize(optimized.clone())?, optimized);
        Ok(())
    }

    #[test]
    fn pushes_through_ordinary_projection_until_metadata_boundary() -> Result<()> {
        let metadata = scan()?
            .project(vec![col("id"), input_file_name().alias("metadata")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(metadata.clone())
            .project(vec![col("metadata").alias("filename")])?
            .limit(0, Some(3))?
            .build()?;
        let optimized = optimize(plan)?;
        assert_eq!(
            limit_input(&projection(&optimized)?.input, 0, Some(3))?,
            &metadata
        );
        Ok(())
    }

    #[test]
    fn does_not_merge_nonliteral_limits() -> Result<()> {
        let projected = scan()?
            .project(vec![input_file_name().alias("metadata")])?
            .build()?;
        for (skip, fetch) in [
            (Some(lit(1_i64) + lit(1_i64)), Some(lit(3_i64))),
            (None, Some(lit(1_i64) + lit(2_i64))),
        ] {
            let nonliteral = LogicalPlanBuilder::from(projected.clone())
                .limit_by_expr(skip.clone(), fetch.clone())?
                .limit(0, Some(2))?
                .build()?;
            let literal = LogicalPlanBuilder::from(projected.clone())
                .limit(0, Some(2))?
                .limit_by_expr(skip, fetch)?
                .build()?;
            for plan in [nonliteral, literal] {
                let result = PushDownLimit.rewrite(plan.clone(), &OptimizerContext::new())?;
                assert!(!result.transformed);
                assert_eq!(result.data, plan);
                assert_eq!(optimize(plan.clone())?, plan);
            }
        }
        Ok(())
    }

    #[test]
    fn replaces_upstream_rule_in_place() {
        let upstream = Optimizer::default().rules;
        let rules = crate::default_optimizer_rules();
        let replacement = PushDownLimit;
        let original = DataFusionPushDownLimit::new();
        assert_eq!(replacement.name(), original.name());
        let expected: Vec<_> = upstream.iter().map(|rule| rule.name()).collect();
        let actual: Vec<_> = rules
            .iter()
            .map(|rule| rule.name())
            .filter(|name| expected.contains(name))
            .collect();
        assert_eq!(actual, expected);
        assert_eq!(
            rules
                .iter()
                .filter(|rule| rule.name() == replacement.name())
                .count(),
            1
        );
        assert_eq!(replacement.apply_order(), original.apply_order());
    }

    #[test]
    fn default_optimizer_preserves_metadata_below_limit() -> Result<()> {
        let optimizer = Optimizer::with_rules(crate::default_optimizer_rules());
        let config = OptimizerContext::new().with_skip_failing_rules(false);
        for expression in metadata_expressions() {
            let plan = scan()?
                .project(vec![col("id"), expression.alias("metadata")])?
                .limit(1, Some(3))?
                .build()?;
            let schema = Arc::clone(plan.schema());
            let optimized = optimizer.optimize(plan, &config, |_, _| {})?;
            assert_eq!(optimized.schema(), &schema);
            assert!(optimized.exists(|node| Ok(matches!(node, LogicalPlan::Limit(_))))?);
            let mut metadata_projections = 0;
            optimized.apply(|node| {
                if let LogicalPlan::Projection(projection) = node
                    && references_input_file_metadata(projection)?
                {
                    metadata_projections += 1;
                    assert!(
                        !projection
                            .input
                            .exists(|node| Ok(matches!(node, LogicalPlan::Limit(_))))?
                    );
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            assert!(metadata_projections > 0);
            assert_eq!(
                optimizer.optimize(optimized.clone(), &config, |_, _| {})?,
                optimized
            );
        }
        Ok(())
    }
}
