use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_expr::projection::ProjectionExpr;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use sail_physical_plan::higher_order::wrap_distributed_higher_order;

/// Wraps every `HigherOrderFunctionExpr` in the plan with a
/// `DistributedHigherOrderExpr` so it can be serialized for distributed
/// execution (DataFusion does not serialize higher-order function expressions,
/// and rebuilding them needs the input schema, which the wrapper carries).
///
/// The wrapper delegates evaluation and display to the inner expression, so it
/// is transparent for local execution and plan snapshots.
///
/// Higher-order functions are rewritten in `ProjectionExec`, `FilterExec` and
/// `SortExec` expressions, which covers `filter`/`transform` (projection),
/// `exists`/`forall` (filter) and higher-order functions in `ORDER BY` (sort).
/// Higher-order functions in other plan nodes (aggregate, window, join filters)
/// are left unwrapped; serializing such a plan fails with a clear error from the
/// codec rather than executing incorrectly.
///
/// TODO: extend coverage to `AggregateExec` and `WindowAggExec` so HOFs in
/// aggregate args / `GROUP BY` / window functions distribute. Acceptance
/// queries (fail in cluster today, work in local + Spark):
/// `sum(size(filter(arr, x -> x > 0)))`, `GROUP BY filter(arr, x -> x > 1)`,
/// `max(size(filter(arr, x -> x > 0))) OVER ()`. For `AggregateExec`: wrap only
/// in Partial/Single mode; group exprs and `filter_expr` are plain
/// `Arc<dyn PhysicalExpr>`; aggregate args go through
/// `AggregateFunctionExpr::with_new_expressions` (may return `None` — fall back
/// to leaving it unwrapped). PREREQUISITE: the codec must first deserialize the
/// scalar UDFs HOF bodies use (e.g. `size`/`array_length`/`cardinality`) — see
/// the registry TODO in `RemoteExecutionCodec::try_decode_udf` — otherwise
/// `sum(size(filter(...)))` still fails to deserialize even once wrapped.
#[derive(Debug, Default)]
pub struct WrapHigherOrderFunctions {}

impl WrapHigherOrderFunctions {
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for WrapHigherOrderFunctions {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            if let Some(projection) = node.downcast_ref::<ProjectionExec>() {
                let input_schema = projection.input().schema();
                let mut changed = false;
                let exprs = projection
                    .expr()
                    .iter()
                    .map(|pe| {
                        let wrapped =
                            wrap_distributed_higher_order(Arc::clone(&pe.expr), &input_schema)?;
                        if !Arc::ptr_eq(&wrapped, &pe.expr) {
                            changed = true;
                        }
                        Ok(ProjectionExpr {
                            expr: wrapped,
                            alias: pe.alias.clone(),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                if changed {
                    let new_projection =
                        ProjectionExec::try_new(exprs, Arc::clone(projection.input()))?;
                    Ok(Transformed::yes(Arc::new(new_projection) as _))
                } else {
                    Ok(Transformed::no(node))
                }
            } else if let Some(filter) = node.downcast_ref::<FilterExec>() {
                let input_schema = filter.input().schema();
                let wrapped =
                    wrap_distributed_higher_order(Arc::clone(filter.predicate()), &input_schema)?;
                if Arc::ptr_eq(&wrapped, filter.predicate()) {
                    Ok(Transformed::no(node))
                } else {
                    let new_filter = FilterExec::try_new(wrapped, Arc::clone(filter.input()))?
                        .with_default_selectivity(filter.default_selectivity())?;
                    Ok(Transformed::yes(Arc::new(new_filter) as _))
                }
            } else if let Some(sort) = node.downcast_ref::<SortExec>() {
                let input_schema = sort.input().schema();
                let mut changed = false;
                let mut sort_exprs = Vec::with_capacity(sort.expr().len());
                for se in sort.expr().iter() {
                    let wrapped =
                        wrap_distributed_higher_order(Arc::clone(&se.expr), &input_schema)?;
                    if !Arc::ptr_eq(&wrapped, &se.expr) {
                        changed = true;
                    }
                    sort_exprs.push(PhysicalSortExpr::new(wrapped, se.options));
                }
                if changed {
                    let ordering = LexOrdering::new(sort_exprs).ok_or_else(|| {
                        datafusion::error::DataFusionError::Internal(
                            "SortExec ordering became empty while wrapping".to_string(),
                        )
                    })?;
                    let new_sort = SortExec::new(ordering, Arc::clone(sort.input()))
                        .with_preserve_partitioning(sort.preserve_partitioning())
                        .with_fetch(sort.fetch());
                    Ok(Transformed::yes(Arc::new(new_sort) as _))
                } else {
                    Ok(Transformed::no(node))
                }
            } else {
                Ok(Transformed::no(node))
            }
        })
        .data()
    }

    fn name(&self) -> &str {
        "wrap_higher_order_functions"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::execution_props::ExecutionProps;
    use datafusion::logical_expr::expr::{HigherOrderFunction, LambdaVariable};
    use datafusion::logical_expr::{col, lambda, lit, Expr, HigherOrderUDF};
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_plan::empty::EmptyExec;
    use sail_function::scalar::array::spark_array_filter::SparkArrayFilter;
    use sail_physical_plan::higher_order::DistributedHigherOrderExpr;

    use super::*;

    /// Returns `(filter_physical_expr, input_plan)` for `filter(arr, v -> v > 2)`
    /// over a single `List<Int32>` column "arr".
    fn filter_expr_and_input() -> Result<(Arc<dyn PhysicalExpr>, Arc<dyn ExecutionPlan>)> {
        let list_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
        let schema = Schema::new(vec![Field::new("arr", list_type.clone(), true)]);
        let dfschema = DFSchema::from_unqualified_fields(
            vec![Field::new("arr", list_type, true)].into(),
            HashMap::new(),
        )?;
        let body = Expr::LambdaVariable(LambdaVariable::new(
            "v".to_string(),
            Some(Arc::new(Field::new("v", DataType::Int32, true))),
        ))
        .gt(lit(2i32));
        let func = Arc::new(HigherOrderUDF::new_from_impl(SparkArrayFilter::new()));
        let logical = Expr::HigherOrderFunction(HigherOrderFunction::new(
            func,
            vec![col("arr"), lambda(["v"], body)],
        ));
        let physical = datafusion::physical_expr::create_physical_expr(
            &logical,
            &dfschema,
            &ExecutionProps::new(),
        )?;
        let schema_ref: SchemaRef = Arc::new(schema);
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema_ref));
        Ok((physical, input))
    }

    /// True if a `DistributedHigherOrderExpr` appears anywhere in `expr`'s tree.
    fn contains_wrapper(expr: &Arc<dyn PhysicalExpr>) -> Result<bool> {
        expr.exists(|node| Ok(node.downcast_ref::<DistributedHigherOrderExpr>().is_some()))
    }

    // Contract: projection/filter/sort nodes are covered by this rule.
    // Aggregate/window/join-filter nodes are intentionally NOT wrapped;
    // serializing those fails with a clear codec error rather than running
    // incorrectly. The three tests below pin the covered nodes.

    #[test]
    fn test_wrap_higher_order_in_projection() -> Result<()> {
        let (physical, input) = filter_expr_and_input()?;
        let projection = ProjectionExec::try_new(
            vec![ProjectionExpr {
                expr: physical,
                alias: "result".to_string(),
            }],
            input,
        )?;

        let optimized = WrapHigherOrderFunctions::new()
            .optimize(Arc::new(projection), &ConfigOptions::default())?;

        let projection = optimized.downcast_ref::<ProjectionExec>().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(
                "optimized plan is not a ProjectionExec".to_string(),
            )
        })?;
        assert!(projection.expr()[0]
            .expr
            .downcast_ref::<DistributedHigherOrderExpr>()
            .is_some());

        Ok(())
    }

    #[test]
    fn test_wrap_higher_order_in_filter() -> Result<()> {
        // Predicate `filter(...) IS NOT NULL` is boolean and nests the HOF.
        let (physical, input) = filter_expr_and_input()?;
        let predicate = Arc::new(datafusion::physical_expr::expressions::IsNotNullExpr::new(
            physical,
        )) as Arc<dyn PhysicalExpr>;
        let filter = FilterExec::try_new(predicate, input)?;

        let optimized = WrapHigherOrderFunctions::new()
            .optimize(Arc::new(filter), &ConfigOptions::default())?;

        let filter = optimized.downcast_ref::<FilterExec>().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(
                "optimized plan is not a FilterExec".to_string(),
            )
        })?;
        assert!(contains_wrapper(filter.predicate())?);

        Ok(())
    }

    #[test]
    fn test_wrap_higher_order_in_sort() -> Result<()> {
        let (physical, input) = filter_expr_and_input()?;
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            physical,
            SortOptions::default(),
        )])
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Internal("empty ordering".to_string())
        })?;
        let sort = SortExec::new(ordering, input);

        let optimized =
            WrapHigherOrderFunctions::new().optimize(Arc::new(sort), &ConfigOptions::default())?;

        let sort = optimized.downcast_ref::<SortExec>().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(
                "optimized plan is not a SortExec".to_string(),
            )
        })?;
        let first = sort.expr().iter().next().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal("empty sort ordering".to_string())
        })?;
        assert!(contains_wrapper(&first.expr)?);

        Ok(())
    }
}
