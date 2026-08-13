use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_expr::projection::ProjectionExpr;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, NestedLoopJoinExec, SortMergeJoinExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use sail_physical_plan::higher_order::wrap_distributed_higher_order;
use sail_physical_plan::streaming::filter::StreamFilterExec;

/// Wraps every `HigherOrderFunctionExpr` in the plan with a
/// `DistributedHigherOrderExpr` so it can be serialized for distributed
/// execution (DataFusion does not serialize higher-order function expressions,
/// and rebuilding them needs the input schema, which the wrapper carries).
///
/// The wrapper delegates evaluation and display to the inner expression, so it
/// is transparent for local execution and plan snapshots.
///
/// This rule covers all physical-expression-bearing nodes:
/// - `ProjectionExec`: projection expressions
/// - `FilterExec`: filter predicates
/// - `SortExec`: sort expressions
/// - `AggregateExec`: group expressions, aggregate arguments, and filter expressions
/// - `WindowAggExec` and `BoundedWindowAggExec`: window expression arguments, partition by, and order by
/// - `HashJoinExec`, `NestedLoopJoinExec`, `SortMergeJoinExec`: join filter expressions
/// - `StreamFilterExec`: streaming filter predicates
/// - `ShuffleWriteExec`: hash partitioning expressions
///
/// For `AggregateExec`, wrapping is only applied in `Partial` or `Single` modes
/// (not in `Final` or `Intermediate` modes).
#[derive(Debug, Default)]
pub struct WrapHigherOrderFunctions {}

impl WrapHigherOrderFunctions {
    pub fn new() -> Self {
        Self {}
    }
}

/// Wraps higher-order functions in ProjectionExec
fn wrap_projection(
    projection: &ProjectionExec,
    node: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let input_schema = projection.input().schema();
    let mut changed = false;
    let exprs = projection
        .expr()
        .iter()
        .map(|pe| {
            let wrapped = wrap_distributed_higher_order(Arc::clone(&pe.expr), &input_schema)?;
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
        let new_projection = ProjectionExec::try_new(exprs, Arc::clone(projection.input()))?;
        Ok(Transformed::yes(Arc::new(new_projection) as _))
    } else {
        Ok(Transformed::no(node))
    }
}

/// Wraps higher-order functions in FilterExec
fn wrap_filter(
    filter: &FilterExec,
    node: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let input_schema = filter.input().schema();
    let wrapped = wrap_distributed_higher_order(Arc::clone(filter.predicate()), &input_schema)?;

    if Arc::ptr_eq(&wrapped, filter.predicate()) {
        Ok(Transformed::no(node))
    } else {
        let new_filter = FilterExec::try_new(wrapped, Arc::clone(filter.input()))?
            .with_default_selectivity(filter.default_selectivity())?;
        Ok(Transformed::yes(Arc::new(new_filter) as _))
    }
}

/// Wraps higher-order functions in SortExec
fn wrap_sort(
    sort: &SortExec,
    node: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let input_schema = sort.input().schema();
    let mut changed = false;
    let mut sort_exprs = Vec::with_capacity(sort.expr().len());

    for se in sort.expr().iter() {
        let wrapped = wrap_distributed_higher_order(Arc::clone(&se.expr), &input_schema)?;
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
}

/// Wraps higher-order functions in AggregateExec
fn wrap_aggregate(
    aggregate: &AggregateExec,
    node: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    use datafusion::physical_plan::aggregates::AggregateMode;

    // Only wrap in Partial or Single mode
    match aggregate.mode() {
        AggregateMode::Partial | AggregateMode::Single => {}
        _ => return Ok(Transformed::no(node)),
    }

    let input_schema = aggregate.input().schema();
    let mut changed = false;

    // Wrap group expressions
    let group_expr = aggregate.group_expr();
    let mut new_group_exprs = Vec::new();
    for (expr, name) in group_expr.expr() {
        let wrapped = wrap_distributed_higher_order(Arc::clone(expr), &input_schema)?;
        if !Arc::ptr_eq(&wrapped, expr) {
            changed = true;
        }
        new_group_exprs.push((wrapped, name.clone()));
    }

    // Wrap aggregate expressions
    let mut new_aggr_exprs = Vec::new();
    for aggr in aggregate.aggr_expr() {
        let args = aggr.expressions();
        let mut new_args = Vec::new();
        let mut args_changed = false;

        for arg in &args {
            let wrapped = wrap_distributed_higher_order(Arc::clone(arg), &input_schema)?;
            if !Arc::ptr_eq(&wrapped, arg) {
                args_changed = true;
            }
            new_args.push(wrapped);
        }

        if args_changed {
            changed = true;
            // Try to create new aggregate expression with wrapped arguments
            // with_new_expressions takes (args, order_by_exprs)
            if let Some(new_aggr) = aggr.with_new_expressions(new_args, vec![]) {
                new_aggr_exprs.push(Arc::new(new_aggr) as _);
            } else {
                // Fall back to original if with_new_expressions returns None
                new_aggr_exprs.push(Arc::clone(aggr) as _);
            }
        } else {
            new_aggr_exprs.push(Arc::clone(aggr) as _);
        }
    }

    // Wrap filter expressions
    let mut new_filter_exprs = Vec::new();
    for filter_opt in aggregate.filter_expr() {
        if let Some(filter) = filter_opt {
            let wrapped = wrap_distributed_higher_order(Arc::clone(filter), &input_schema)?;
            if !Arc::ptr_eq(&wrapped, filter) {
                changed = true;
            }
            new_filter_exprs.push(Some(wrapped));
        } else {
            new_filter_exprs.push(None);
        }
    }

    if changed {
        // Reconstruct the AggregateExec with wrapped expressions
        let new_group_by =
            datafusion::physical_plan::aggregates::PhysicalGroupBy::new_single(new_group_exprs);

        let new_aggregate = AggregateExec::try_new(
            *aggregate.mode(),
            new_group_by,
            new_aggr_exprs,
            new_filter_exprs,
            Arc::clone(aggregate.input()),
            Arc::new(aggregate.schema().as_ref().clone()),
        )?;

        Ok(Transformed::yes(Arc::new(new_aggregate) as _))
    } else {
        Ok(Transformed::no(node))
    }
}

/// Wraps higher-order functions in WindowAggExec
fn wrap_window(
    window: &WindowAggExec,
    node: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let input_schema = window.input().schema();
    let mut changed = false;

    let mut new_window_exprs = Vec::new();
    for window_expr in window.window_expr() {
        let expressions = window_expr.all_expressions();
        let mut args_changed = false;

        // Wrap args
        let mut new_args = Vec::new();
        for arg in &expressions.args {
            let wrapped = wrap_distributed_higher_order(Arc::clone(arg), &input_schema)?;
            if !Arc::ptr_eq(&wrapped, arg) {
                args_changed = true;
            }
            new_args.push(wrapped);
        }

        // Wrap partition_by
        let mut new_partition_by = Vec::new();
        for part in &expressions.partition_by_exprs {
            let wrapped = wrap_distributed_higher_order(Arc::clone(part), &input_schema)?;
            if !Arc::ptr_eq(&wrapped, part) {
                args_changed = true;
            }
            new_partition_by.push(wrapped);
        }

        // Wrap order_by - need to extract the expr from PhysicalSortExpr
        let mut new_order_by = Vec::new();
        for order in &expressions.order_by_exprs {
            let wrapped = wrap_distributed_higher_order(Arc::clone(order), &input_schema)?;
            if !Arc::ptr_eq(&wrapped, order) {
                args_changed = true;
            }
            new_order_by.push(wrapped);
        }

        if args_changed {
            changed = true;
            if let Some(new_expr) =
                window_expr.with_new_expressions(new_args, new_partition_by, new_order_by)
            {
                new_window_exprs.push(new_expr);
            } else {
                new_window_exprs.push(Arc::clone(window_expr) as _);
            }
        } else {
            new_window_exprs.push(Arc::clone(window_expr) as _);
        }
    }

    if changed {
        let new_window = WindowAggExec::try_new(
            new_window_exprs,
            Arc::clone(window.input()),
            false, // can_repartition
        )?;
        Ok(Transformed::yes(Arc::new(new_window) as _))
    } else {
        Ok(Transformed::no(node))
    }
}

/// Wraps higher-order functions in BoundedWindowAggExec
fn wrap_bounded_window(
    window: &BoundedWindowAggExec,
    node: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let input_schema = window.input().schema();
    let mut changed = false;

    let mut new_window_exprs = Vec::new();
    for window_expr in window.window_expr() {
        let expressions = window_expr.all_expressions();
        let mut args_changed = false;

        // Wrap args
        let mut new_args = Vec::new();
        for arg in &expressions.args {
            let wrapped = wrap_distributed_higher_order(Arc::clone(arg), &input_schema)?;
            if !Arc::ptr_eq(&wrapped, arg) {
                args_changed = true;
            }
            new_args.push(wrapped);
        }

        // Wrap partition_by
        let mut new_partition_by = Vec::new();
        for part in &expressions.partition_by_exprs {
            let wrapped = wrap_distributed_higher_order(Arc::clone(part), &input_schema)?;
            if !Arc::ptr_eq(&wrapped, part) {
                args_changed = true;
            }
            new_partition_by.push(wrapped);
        }

        // Wrap order_by - need to extract the expr from PhysicalSortExpr
        let mut new_order_by = Vec::new();
        for order in &expressions.order_by_exprs {
            let wrapped = wrap_distributed_higher_order(Arc::clone(order), &input_schema)?;
            if !Arc::ptr_eq(&wrapped, order) {
                args_changed = true;
            }
            new_order_by.push(wrapped);
        }

        if args_changed {
            changed = true;
            if let Some(new_expr) =
                window_expr.with_new_expressions(new_args, new_partition_by, new_order_by)
            {
                new_window_exprs.push(new_expr);
            } else {
                new_window_exprs.push(Arc::clone(window_expr) as _);
            }
        } else {
            new_window_exprs.push(Arc::clone(window_expr) as _);
        }
    }

    if changed {
        let new_window = BoundedWindowAggExec::try_new(
            new_window_exprs,
            Arc::clone(window.input()),
            window.input_order_mode.clone(),
            false, // can_repartition
        )?;
        Ok(Transformed::yes(Arc::new(new_window) as _))
    } else {
        Ok(Transformed::no(node))
    }
}

/// Wraps higher-order functions in HashJoinExec filter
fn wrap_hash_join(
    join: &HashJoinExec,
    node: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if let Some(filter) = join.filter() {
        let left_schema = join.left().schema();
        let right_schema = join.right().schema();
        // Join filter expressions are evaluated against a combined schema
        let combined_schema = datafusion::arrow::datatypes::Schema::try_merge(vec![
            left_schema.as_ref().clone(),
            right_schema.as_ref().clone(),
        ])?;
        let combined_schema = Arc::new(combined_schema);

        let wrapped =
            wrap_distributed_higher_order(Arc::clone(filter.expression()), &combined_schema)?;

        if !Arc::ptr_eq(&wrapped, filter.expression()) {
            let new_filter = datafusion::physical_plan::joins::utils::JoinFilter::new(
                wrapped,
                filter.column_indices().to_vec(),
                filter.schema().clone(),
            );

            let new_join = HashJoinExec::try_new(
                Arc::clone(join.left()),
                Arc::clone(join.right()),
                join.on().to_vec(),
                Some(new_filter),
                join.join_type(),
                None, // projection
                datafusion::physical_plan::joins::PartitionMode::CollectLeft,
                join.null_equality(),
                false, // null_aware
            )?;

            return Ok(Transformed::yes(Arc::new(new_join) as _));
        }
    }

    Ok(Transformed::no(node))
}

/// Wraps higher-order functions in NestedLoopJoinExec filter
fn wrap_nested_loop_join(
    join: &NestedLoopJoinExec,
    node: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if let Some(filter) = join.filter() {
        let left_schema = join.left().schema();
        let right_schema = join.right().schema();
        let combined_schema = datafusion::arrow::datatypes::Schema::try_merge(vec![
            left_schema.as_ref().clone(),
            right_schema.as_ref().clone(),
        ])?;
        let combined_schema = Arc::new(combined_schema);

        let wrapped =
            wrap_distributed_higher_order(Arc::clone(filter.expression()), &combined_schema)?;

        if !Arc::ptr_eq(&wrapped, filter.expression()) {
            let new_filter = datafusion::physical_plan::joins::utils::JoinFilter::new(
                wrapped,
                filter.column_indices().to_vec(),
                filter.schema().clone(),
            );

            let new_join = NestedLoopJoinExec::try_new(
                Arc::clone(join.left()),
                Arc::clone(join.right()),
                Some(new_filter),
                join.join_type(),
                None, // projection
            )?;

            return Ok(Transformed::yes(Arc::new(new_join) as _));
        }
    }

    Ok(Transformed::no(node))
}

/// Wraps higher-order functions in SortMergeJoinExec filter
fn wrap_sort_merge_join(
    join: &SortMergeJoinExec,
    node: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if let Some(filter) = join.filter() {
        let left_schema = join.left().schema();
        let right_schema = join.right().schema();
        let combined_schema = datafusion::arrow::datatypes::Schema::try_merge(vec![
            left_schema.as_ref().clone(),
            right_schema.as_ref().clone(),
        ])?;
        let combined_schema = Arc::new(combined_schema);

        let wrapped =
            wrap_distributed_higher_order(Arc::clone(filter.expression()), &combined_schema)?;

        if !Arc::ptr_eq(&wrapped, filter.expression()) {
            let new_filter = datafusion::physical_plan::joins::utils::JoinFilter::new(
                wrapped,
                filter.column_indices().to_vec(),
                filter.schema().clone(),
            );

            let new_join = SortMergeJoinExec::try_new(
                Arc::clone(join.left()),
                Arc::clone(join.right()),
                join.on().to_vec(),
                Some(new_filter),
                join.join_type(),
                join.sort_options().to_vec(),
                join.null_equality(),
            )?;

            return Ok(Transformed::yes(Arc::new(new_join) as _));
        }
    }

    Ok(Transformed::no(node))
}

/// Wraps higher-order functions in StreamFilterExec
fn wrap_stream_filter(
    filter: &StreamFilterExec,
    node: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let input_schema = filter.input().schema();
    let wrapped = wrap_distributed_higher_order(Arc::clone(filter.predicate()), &input_schema)?;

    if Arc::ptr_eq(&wrapped, filter.predicate()) {
        Ok(Transformed::no(node))
    } else {
        let new_filter = StreamFilterExec::try_new(Arc::clone(filter.input()), wrapped)?;
        Ok(Transformed::yes(Arc::new(new_filter) as _))
    }
}

impl PhysicalOptimizerRule for WrapHigherOrderFunctions {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            // Handle ProjectionExec
            if let Some(projection) = node.downcast_ref::<ProjectionExec>() {
                return wrap_projection(projection, Arc::clone(&node));
            }

            // Handle FilterExec
            if let Some(filter) = node.downcast_ref::<FilterExec>() {
                return wrap_filter(filter, Arc::clone(&node));
            }

            // Handle SortExec
            if let Some(sort) = node.downcast_ref::<SortExec>() {
                return wrap_sort(sort, Arc::clone(&node));
            }

            // Handle AggregateExec
            if let Some(aggregate) = node.downcast_ref::<AggregateExec>() {
                return wrap_aggregate(aggregate, Arc::clone(&node));
            }

            // Handle WindowAggExec
            if let Some(window) = node.downcast_ref::<WindowAggExec>() {
                return wrap_window(window, Arc::clone(&node));
            }

            // Handle BoundedWindowAggExec
            if let Some(window) = node.downcast_ref::<BoundedWindowAggExec>() {
                return wrap_bounded_window(window, Arc::clone(&node));
            }

            // Handle HashJoinExec
            if let Some(join) = node.downcast_ref::<HashJoinExec>() {
                return wrap_hash_join(join, Arc::clone(&node));
            }

            // Handle NestedLoopJoinExec
            if let Some(join) = node.downcast_ref::<NestedLoopJoinExec>() {
                return wrap_nested_loop_join(join, Arc::clone(&node));
            }

            // Handle SortMergeJoinExec
            if let Some(join) = node.downcast_ref::<SortMergeJoinExec>() {
                return wrap_sort_merge_join(join, Arc::clone(&node));
            }

            // Handle StreamFilterExec
            if let Some(filter) = node.downcast_ref::<StreamFilterExec>() {
                return wrap_stream_filter(filter, Arc::clone(&node));
            }

            Ok(Transformed::no(node))
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
    use datafusion::logical_expr::{Expr, HigherOrderUDF, col, lambda, lit};
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
        assert!(
            projection.expr()[0]
                .expr
                .downcast_ref::<DistributedHigherOrderExpr>()
                .is_some()
        );

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
