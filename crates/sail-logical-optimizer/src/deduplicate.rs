use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::functions_aggregate::min_max::{Max, Min};
use datafusion::optimizer::replace_distinct_aggregate::ReplaceDistinctWithAggregate;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchema, Result};
use datafusion_expr::{
    Aggregate, Distinct, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Operator,
    Projection, SubqueryAlias,
};

/// Simplifies deduplication and removes duplicate-only grouping beneath MIN/MAX.
#[derive(Debug, Default)]
pub struct SimplifyDeduplicate(ReplaceDistinctWithAggregate);

impl OptimizerRule for SimplifyDeduplicate {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if let LogicalPlan::Distinct(Distinct::On(distinct)) = &plan
            && distinct.sort_expr.is_none()
            && distinct.on_expr == distinct.select_expr
        {
            // DISTINCT ON carries recovered DataFrame attributes. When all
            // outputs are keys, ordinary DISTINCT avoids first_value aggregates.
            let input = LogicalPlanBuilder::from(distinct.input.as_ref().clone())
                .project(distinct.select_expr.clone())?
                .distinct()?
                .build()?;
            return self.0.rewrite(input, config);
        }
        self.0
            .rewrite(plan, config)?
            .transform_data(remove_grouping_below_extrema)
    }
}

fn remove_grouping_below_extrema(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Aggregate(upper) = &plan else {
        return Ok(Transformed::no(plan));
    };
    if upper.aggr_expr.is_empty() || upper.group_expr.iter().any(|expr| !cheap_scalar(expr)) {
        return Ok(Transformed::no(plan));
    }
    for expr in &upper.aggr_expr {
        let Expr::AggregateFunction(function) = unalias(expr) else {
            return Ok(Transformed::no(plan));
        };
        let params = &function.params;
        // Identity, rather than the name, excludes user-defined "min"/"max".
        if !(function.func.inner().is::<Min>() || function.func.inner().is::<Max>())
            || params.args.len() != 1
            || !params.order_by.is_empty()
            || params.null_treatment.is_some()
            || !cheap_scalar(&params.args[0])
            || params
                .filter
                .as_deref()
                .is_some_and(|expr| !cheap_scalar(expr))
            // Grouped floating-point extrema can depend on NaN ordering.
            || contains_float(&params.args[0].get_type(upper.input.schema())?)
        {
            return Ok(Transformed::no(plan));
        }
    }

    let required = referenced_indices(
        upper.group_expr.iter().chain(&upper.aggr_expr),
        upper.input.schema(),
    )?;
    let Some(input) = remove_duplicate_grouping(&upper.input, &required)? else {
        return Ok(Transformed::no(plan));
    };
    let replacement = LogicalPlan::Aggregate(Aggregate::try_new(
        input,
        upper.group_expr.clone(),
        upper.aggr_expr.clone(),
    )?);
    if !same_fields(plan.schema(), replacement.schema()) {
        return Ok(Transformed::no(plan));
    }
    Ok(Transformed::yes(replacement))
}

/// Only projection and alias nodes are transparent. In particular, HAVING,
/// LIMIT, windows, and DISTINCT ON selecting payloads stay above their grouping.
fn remove_duplicate_grouping(
    plan: &Arc<LogicalPlan>,
    required: &HashSet<usize>,
) -> Result<Option<Arc<LogicalPlan>>> {
    let replacement = match plan.as_ref() {
        LogicalPlan::Projection(projection) if projection.expr.iter().all(cheap_scalar) => {
            let required = referenced_indices(
                projection
                    .expr
                    .iter()
                    .enumerate()
                    .filter_map(|(i, expr)| required.contains(&i).then_some(expr)),
                projection.input.schema(),
            )?;
            let Some(input) = remove_duplicate_grouping(&projection.input, &required)? else {
                return Ok(None);
            };
            LogicalPlan::Projection(Projection::try_new(projection.expr.clone(), input)?)
        }
        LogicalPlan::SubqueryAlias(alias) => {
            let Some(input) = remove_duplicate_grouping(&alias.input, required)? else {
                return Ok(None);
            };
            LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(input, alias.alias.clone())?)
        }
        LogicalPlan::Aggregate(lower)
            if !lower.group_expr.is_empty() && lower.aggr_expr.is_empty() =>
        {
            for (i, expr) in lower.group_expr.iter().enumerate() {
                if !repeatable_scalar(expr)
                    // Grouping normalizes floating-point zero representatives.
                    || contains_float(&expr.get_type(lower.input.schema())?)
                    // A later projection-pruning pass must not discard a
                    // fallible grouping expression that was evaluated before.
                    || (!required.contains(&i)
                        && !infallible_unused_key(expr, &lower.input)?)
                {
                    return Ok(None);
                }
            }
            let expr = lower
                .group_expr
                .iter()
                .zip(lower.schema.iter())
                .map(|(expr, (qualifier, field))| {
                    expr.clone().alias_qualified_with_metadata(
                        qualifier.cloned(),
                        field.name(),
                        Some(field.metadata().into()),
                    )
                })
                .collect();
            // Do not reuse the aggregate schema: its unique-key functional
            // dependencies are false after this projection restores duplicates.
            LogicalPlan::Projection(Projection::try_new(expr, Arc::clone(&lower.input))?)
        }
        _ => return Ok(None),
    };
    if !same_fields(plan.schema(), replacement.schema()) {
        return Ok(None);
    }
    Ok(Some(Arc::new(replacement)))
}

/// An unused grouping column may hide a fallible expression in its input.
/// Follow only transparent lineage; unknown producers retain their grouping.
fn infallible_unused_key(expr: &Expr, input: &LogicalPlan) -> Result<bool> {
    let Expr::Column(column) = unalias(expr) else {
        return Ok(matches!(unalias(expr), Expr::Literal(..)));
    };
    let index = input.schema().index_of_column(column)?;
    match input {
        LogicalPlan::Projection(projection) => {
            infallible_unused_key(&projection.expr[index], &projection.input)
        }
        LogicalPlan::SubqueryAlias(alias) => {
            let (qualifier, field) = alias.input.schema().qualified_field(index);
            let column = Expr::Column(datafusion_common::Column::new(
                qualifier.cloned(),
                field.name(),
            ));
            infallible_unused_key(&column, &alias.input)
        }
        LogicalPlan::TableScan(scan) => Ok(scan.source.get_logical_plan().is_none()),
        LogicalPlan::Values(values) => Ok(values
            .values
            .iter()
            .all(|row| matches!(unalias(&row[index]), Expr::Literal(..)))),
        LogicalPlan::EmptyRelation(_) => Ok(true),
        _ => Ok(false),
    }
}

fn referenced_indices<'a>(
    expressions: impl IntoIterator<Item = &'a Expr>,
    schema: &DFSchema,
) -> Result<HashSet<usize>> {
    expressions
        .into_iter()
        .flat_map(Expr::column_refs)
        .map(|column| schema.index_of_column(column))
        .collect()
}

fn unalias(mut expr: &Expr) -> &Expr {
    while let Expr::Alias(alias) = expr {
        expr = &alias.expr;
    }
    expr
}

/// Restoring duplicates increases evaluations above the removed grouping.
/// Keep costly functions and casts evaluated once per distinct row.
fn cheap_scalar(expr: &Expr) -> bool {
    expr.exists(|expr| {
        let cheap = match expr {
            Expr::Column(_)
            | Expr::Literal(..)
            | Expr::Alias(_)
            | Expr::Negative(_)
            | Expr::Not(_)
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_) => true,
            Expr::BinaryExpr(binary) => matches!(
                binary.op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
                    | Operator::IsDistinctFrom
                    | Operator::IsNotDistinctFrom
                    | Operator::Plus
                    | Operator::Minus
                    | Operator::Multiply
                    | Operator::Divide
                    | Operator::Modulo
                    | Operator::And
                    | Operator::Or
            ),
            _ => false,
        };
        Ok(!cheap)
    })
    .is_ok_and(|expensive_expression| !expensive_expression)
}

fn repeatable_scalar(expr: &Expr) -> bool {
    // This traversal does not inspect the unchanged subtree below the grouping.
    expr.exists(|expr| {
        Ok(expr.is_volatile_node()
            || matches!(
                expr,
                Expr::AggregateFunction(_)
                    | Expr::WindowFunction(_)
                    | Expr::Exists(_)
                    | Expr::InSubquery(_)
                    | Expr::SetComparison(_)
                    | Expr::ScalarSubquery(_)
                    | Expr::OuterReferenceColumn(..)
                    | Expr::GroupingSet(_)
                    | Expr::Unnest(_)
            ))
    })
    .is_ok_and(|unsafe_expression| !unsafe_expression)
}

fn contains_float(data_type: &DataType) -> bool {
    match data_type {
        DataType::Float16 | DataType::Float32 | DataType::Float64 => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => contains_float(field.data_type()),
        DataType::Struct(fields) => fields.iter().any(|field| contains_float(field.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| contains_float(field.data_type())),
        DataType::Dictionary(_, value) => contains_float(value),
        DataType::RunEndEncoded(_, value) => contains_float(value.data_type()),
        _ => false,
    }
}

fn same_fields(left: &DFSchema, right: &DFSchema) -> bool {
    left.iter().eq(right.iter()) && left.metadata() == right.metadata()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::provider_as_source;
    use datafusion::functions::crypto::expr_fn::sha256;
    use datafusion::functions::math::expr_fn::random;
    use datafusion::functions_aggregate::expr_fn::{count, max, min, sum};
    use datafusion::optimizer::{Optimizer, OptimizerContext};
    use datafusion_common::internal_err;
    use datafusion_common::tree_node::TreeNodeRecursion;
    use datafusion_expr::expr_fn::ExprFunctionExt;
    use datafusion_expr::{Volatility, cast, col, create_udaf, grouping_set, lit};

    use super::*;

    fn scan() -> Result<LogicalPlanBuilder> {
        let schema = Schema::new_with_metadata(
            vec![
                Field::new("k", DataType::Int32, false),
                Field::new("v", DataType::Int64, true)
                    .with_metadata(HashMap::from([("field".into(), "retained".into())])),
            ],
            HashMap::from([("schema".into(), "retained".into())]),
        );
        LogicalPlanBuilder::scan(
            "t",
            provider_as_source(Arc::new(EmptyTable::new(Arc::new(schema)))),
            None,
        )
    }

    fn grouped() -> Result<LogicalPlanBuilder> {
        scan()?.aggregate(vec![col("k"), col("v")], Vec::<Expr>::new())
    }

    fn aggregate_count(plan: &LogicalPlan) -> Result<usize> {
        let mut count = 0;
        plan.apply(|plan| {
            count += usize::from(matches!(plan, LogicalPlan::Aggregate(_)));
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(count)
    }

    fn rewrite(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        SimplifyDeduplicate::default().rewrite(plan, &OptimizerContext::new())
    }

    fn assert_unchanged(plan: LogicalPlan) -> Result<()> {
        let result = rewrite(plan.clone())?;
        assert!(!result.transformed);
        assert_eq!(result.data, plan);
        Ok(())
    }

    #[test]
    fn removes_grouping_below_min_and_max() -> Result<()> {
        let plan = grouped()?
            .aggregate(
                vec![col("k")],
                vec![min(col("v")).alias("lo"), max(col("v")).alias("hi")],
            )?
            .build()?;
        let result = rewrite(plan.clone())?;
        assert!(result.transformed);
        assert_eq!(aggregate_count(&result.data)?, 1);
        assert!(same_fields(plan.schema(), result.data.schema()));
        assert_unchanged(result.data)
    }

    #[test]
    fn removes_grouping_created_by_the_existing_distinct_rule() -> Result<()> {
        let plan = scan()?
            .distinct()?
            .aggregate(Vec::<Expr>::new(), vec![max(col("v"))])?
            .build()?;
        let optimized = Optimizer::with_rules(vec![Arc::new(SimplifyDeduplicate::default())])
            .optimize(
                plan.clone(),
                &OptimizerContext::new().with_skip_failing_rules(false),
                |_, _| {},
            )?;
        assert_eq!(aggregate_count(&optimized)?, 1);
        assert!(same_fields(plan.schema(), optimized.schema()));
        Ok(())
    }

    #[test]
    fn preserves_aliases_computed_keys_nullability_and_metadata() -> Result<()> {
        let plan = scan()?
            .aggregate(
                vec![col("k"), (col("v") + lit(1_i64)).alias("key")],
                Vec::<Expr>::new(),
            )?
            .alias("d")?
            .project(vec![
                col("d.k").alias("bucket"),
                col("d.key").alias("value"),
            ])?
            .alias("e")?
            .aggregate(vec![col("e.bucket")], vec![max(col("e.value"))])?
            .build()?;
        let result = rewrite(plan.clone())?;
        assert!(result.transformed);
        assert_eq!(aggregate_count(&result.data)?, 1);
        assert!(same_fields(plan.schema(), result.data.schema()));
        let LogicalPlan::Aggregate(upper) = result.data else {
            return internal_err!("expected an upper aggregate");
        };
        upper.input.apply(|plan| {
            if matches!(
                plan,
                LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_)
            ) {
                assert!(plan.schema().functional_dependencies().is_empty());
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(())
    }

    #[test]
    fn removed_grouping_does_not_claim_its_input_is_unique() -> Result<()> {
        let plan = grouped()?
            .aggregate(Vec::<Expr>::new(), vec![max(col("v"))])?
            .build()?;
        let result = rewrite(plan)?;
        assert!(result.transformed);
        let LogicalPlan::Aggregate(upper) = result.data else {
            return internal_err!("expected an upper aggregate");
        };
        assert!(upper.input.schema().functional_dependencies().is_empty());
        // Keeping the old aggregate's uniqueness dependency would incorrectly
        // make the existing DISTINCT rule remove this necessary deduplication.
        let distinct = LogicalPlanBuilder::from(upper.input.as_ref().clone())
            .distinct()?
            .build()?;
        let result = rewrite(distinct)?;
        assert!(matches!(result.data, LogicalPlan::Aggregate(_)));
        Ok(())
    }

    #[test]
    fn accepts_deterministic_filter_and_distinct_extrema() -> Result<()> {
        let aggregate = max(col("v"))
            .filter(col("k").gt(lit(0_i32)))
            .distinct()
            .build()?;
        let plan = grouped()?
            .aggregate(Vec::<Expr>::new(), vec![aggregate])?
            .build()?;
        let result = rewrite(plan.clone())?;
        assert!(result.transformed);
        assert_eq!(aggregate_count(&result.data)?, 1);
        assert!(same_fields(plan.schema(), result.data.schema()));
        Ok(())
    }

    #[test]
    fn keeps_expensive_scalar_arguments_and_projections_above_grouping() -> Result<()> {
        let expensive = sha256(cast(col("v"), DataType::Utf8));
        let plan = grouped()?
            .aggregate(Vec::<Expr>::new(), vec![max(expensive.clone())])?
            .build()?;
        assert_unchanged(plan)?;
        let plan = grouped()?
            .project(vec![expensive.alias("value")])?
            .aggregate(Vec::<Expr>::new(), vec![min(col("value"))])?
            .build()?;
        assert_unchanged(plan)
    }

    #[test]
    fn keeps_expensive_filters_and_upper_grouping_keys() -> Result<()> {
        let expensive = sha256(cast(col("v"), DataType::Utf8));
        let aggregate = max(col("v"))
            .filter(expensive.clone().is_not_null())
            .build()?;
        let plan = grouped()?
            .aggregate(Vec::<Expr>::new(), vec![aggregate])?
            .build()?;
        assert_unchanged(plan)?;
        let plan = grouped()?
            .aggregate(vec![expensive.alias("bucket")], vec![max(col("v"))])?
            .build()?;
        assert_unchanged(plan)
    }

    #[test]
    fn accepts_simple_arithmetic_above_grouping() -> Result<()> {
        let plan = grouped()?
            .aggregate(Vec::<Expr>::new(), vec![max(col("v") + lit(1_i64))])?
            .build()?;
        let result = rewrite(plan.clone())?;
        assert!(result.transformed);
        assert_eq!(aggregate_count(&result.data)?, 1);
        assert!(same_fields(plan.schema(), result.data.schema()));
        Ok(())
    }

    #[test]
    fn keeps_unused_computed_grouping_keys() -> Result<()> {
        let plan = scan()?
            .aggregate(
                vec![(col("k") + lit(1_i32)).alias("unused"), col("v")],
                Vec::<Expr>::new(),
            )?
            .project(vec![col("v")])?
            .alias("d")?
            .aggregate(Vec::<Expr>::new(), vec![max(col("d.v"))])?
            .build()?;
        assert_unchanged(plan)
    }

    #[test]
    fn full_optimizer_keeps_fallible_unused_input_columns() -> Result<()> {
        let fallible = cast(col("v"), DataType::Int32).alias("bad");
        let direct_key = scan()?
            .aggregate(vec![col("k"), fallible.clone()], Vec::<Expr>::new())?
            .build()?;
        let projected_key = scan()?
            .project(vec![col("k"), fallible])?
            .alias("source")?
            .aggregate(vec![col("k"), col("bad")], Vec::<Expr>::new())?
            .build()?;
        let optimizer = Optimizer::with_rules(crate::default_optimizer_rules());
        let config = OptimizerContext::new().with_skip_failing_rules(false);
        for lower in [direct_key, projected_key] {
            let plan = LogicalPlanBuilder::from(lower)
                .project(vec![col("k")])?
                .aggregate(Vec::<Expr>::new(), vec![min(col("k"))])?
                .build()?;
            assert_unchanged(plan.clone())?;
            let optimized = optimizer.optimize(plan.clone(), &config, |_, _| {})?;
            assert_eq!(aggregate_count(&optimized)?, 2);
            assert!(optimized.display_indent().to_string().contains("CAST"));
            assert!(same_fields(plan.schema(), optimized.schema()));
        }
        Ok(())
    }

    #[test]
    fn keeps_duplicate_sensitive_and_payload_aggregates() -> Result<()> {
        for aggregate in [count(col("v")), sum(col("v"))] {
            let plan = grouped()?
                .aggregate(Vec::<Expr>::new(), vec![max(col("v")), aggregate])?
                .build()?;
            assert_unchanged(plan)?;
        }
        let plan = scan()?
            .aggregate(vec![col("k")], vec![max(col("v")).alias("value")])?
            .aggregate(Vec::<Expr>::new(), vec![min(col("value"))])?
            .build()?;
        assert_unchanged(plan)?;
        // A global lower aggregate returns a row even when its input is empty.
        let plan = scan()?
            .aggregate(Vec::<Expr>::new(), vec![max(col("v")).alias("value")])?
            .aggregate(Vec::<Expr>::new(), vec![min(col("value"))])?
            .build()?;
        assert_unchanged(plan)
    }

    #[test]
    fn preserves_having_limit_and_sort_barriers() -> Result<()> {
        let input = grouped()?.build()?;
        let barriers = [
            LogicalPlanBuilder::from(input.clone())
                .filter(col("v").gt(lit(0_i64)))?
                .build()?,
            LogicalPlanBuilder::from(input.clone())
                .limit(0, Some(3))?
                .build()?,
            LogicalPlanBuilder::from(input)
                .sort(vec![col("v").sort(true, true)])?
                .build()?,
        ];
        for input in barriers {
            let plan = LogicalPlanBuilder::from(input)
                .aggregate(Vec::<Expr>::new(), vec![min(col("v"))])?
                .build()?;
            assert_unchanged(plan)?;
        }
        Ok(())
    }

    #[test]
    fn keeps_grouping_sets() -> Result<()> {
        let plan = scan()?
            .aggregate(
                vec![grouping_set(vec![vec![col("k")], Vec::<Expr>::new()])],
                Vec::<Expr>::new(),
            )?
            .aggregate(Vec::<Expr>::new(), vec![max(col("k"))])?
            .build()?;
        assert_unchanged(plan)?;
        let plan = grouped()?
            .aggregate(
                vec![grouping_set(vec![vec![col("k")], Vec::<Expr>::new()])],
                vec![max(col("v"))],
            )?
            .build()?;
        assert_unchanged(plan)
    }

    #[test]
    fn keeps_volatile_grouping_projection_argument_and_filter() -> Result<()> {
        let volatile = cast(random(), DataType::Int64);
        let plan = scan()?
            .aggregate(vec![volatile.clone().alias("key")], Vec::<Expr>::new())?
            .aggregate(Vec::<Expr>::new(), vec![max(col("key"))])?
            .build()?;
        assert_unchanged(plan)?;
        let plan = grouped()?
            .project(vec![volatile.clone().alias("key")])?
            .aggregate(Vec::<Expr>::new(), vec![min(col("key"))])?
            .build()?;
        assert_unchanged(plan)?;
        let plan = grouped()?
            .aggregate(Vec::<Expr>::new(), vec![max(volatile)])?
            .build()?;
        assert_unchanged(plan)?;
        let aggregate = max(col("v")).filter(random().lt(lit(0.5))).build()?;
        let plan = grouped()?
            .aggregate(Vec::<Expr>::new(), vec![aggregate])?
            .build()?;
        assert_unchanged(plan)
    }

    #[test]
    fn preserves_float_and_nested_float_representatives() -> Result<()> {
        let float = Arc::new(Field::new("float", DataType::Float64, true));
        let map = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Float64, false),
                        Field::new("value", DataType::Int32, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        for data_type in [
            DataType::Float32,
            DataType::Float64,
            DataType::List(Arc::clone(&float)),
            DataType::Struct(vec![float].into()),
            map,
        ] {
            let schema = Arc::new(Schema::new(vec![Field::new("key", data_type, true)]));
            let plan = LogicalPlanBuilder::scan(
                "t",
                provider_as_source(Arc::new(EmptyTable::new(schema))),
                None,
            )?
            .aggregate(vec![col("key")], Vec::<Expr>::new())?
            .aggregate(Vec::<Expr>::new(), vec![max(lit(1_i64))])?
            .build()?;
            assert_unchanged(plan)?;
        }
        // Floating values may also be introduced above an integer grouping.
        let plan = grouped()?
            .aggregate(
                Vec::<Expr>::new(),
                vec![min(cast(col("v"), DataType::Float64))],
            )?
            .build()?;
        assert_unchanged(plan)
    }

    #[test]
    fn keeps_outer_references_in_changed_expressions() -> Result<()> {
        let outer = Expr::OuterReferenceColumn(
            Arc::new(Field::new("v", DataType::Int64, true)),
            datafusion_common::Column::from_name("v"),
        );
        let plan = grouped()?
            .aggregate(Vec::<Expr>::new(), vec![max(outer)])?
            .build()?;
        assert_unchanged(plan)
    }

    #[test]
    fn does_not_match_user_aggregate_by_name() -> Result<()> {
        let aggregate = create_udaf(
            "min",
            vec![DataType::Int64],
            Arc::new(DataType::Int64),
            Volatility::Immutable,
            Arc::new(|_| internal_err!("not executed by this logical-plan test")),
            Arc::new(vec![DataType::Int64]),
        );
        let plan = grouped()?
            .aggregate(Vec::<Expr>::new(), vec![aggregate.call(vec![col("v")])])?
            .build()?;
        assert_unchanged(plan)
    }

    #[test]
    fn existing_projection_pruning_exposes_unused_lower_aggregate() -> Result<()> {
        let plan = scan()?
            .aggregate(
                vec![col("k"), col("v")],
                vec![sum(col("v")).alias("unused")],
            )?
            .project(vec![col("v")])?
            .aggregate(Vec::<Expr>::new(), vec![max(col("v"))])?
            .build()?;
        assert_unchanged(plan.clone())?;
        let optimizer = Optimizer::with_rules(crate::default_optimizer_rules());
        let config = OptimizerContext::new().with_skip_failing_rules(false);
        let optimized = optimizer.optimize(plan.clone(), &config, |_, _| {})?;
        assert_eq!(aggregate_count(&optimized)?, 1);
        assert!(same_fields(plan.schema(), optimized.schema()));
        let repeated = optimizer.optimize(optimized.clone(), &config, |_, _| {})?;
        assert_eq!(optimized, repeated);
        Ok(())
    }
}
