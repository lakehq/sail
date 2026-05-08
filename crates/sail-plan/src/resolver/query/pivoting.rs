use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::{
    col, expr, lit, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Projection, ScalarUDF,
};
use datafusion_functions_nested::expr_fn as nested_fn;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;
use sail_function::scalar::explode;
use sail_function::scalar::struct_function::StructFunction;

use crate::error::{PlanError, PlanResult};
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::explode::ExplodeRewriter;
use crate::resolver::tree::monotonic_id::MonotonicIdRewriter;
use crate::resolver::tree::spark_partition_id::SparkPartitionIdRewriter;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_pivot(
        &self,
        pivot: spec::Pivot,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::Pivot {
            input,
            grouping,
            aggregate: aggregate_exprs,
            columns: pivot_columns,
            values: pivot_values,
        } = pivot;

        // 1. Resolve the input plan.
        let input = self
            .resolve_query_plan_with_hidden_fields(*input, state)
            .await?;
        let schema = input.schema();

        // 2. Resolve pivot column expressions.
        let (pivot_col_names, pivot_col_exprs) = self
            .resolve_expressions_and_names(pivot_columns, schema, state)
            .await?;
        if pivot_col_exprs.len() != 1 {
            return Err(PlanError::todo("multi-column pivot"));
        }
        let pivot_col_expr = pivot_col_exprs[0].clone();

        // 3. Resolve aggregate expressions.
        let aggregate_named = self
            .resolve_named_expressions(aggregate_exprs, schema, state)
            .await?;

        // 4. Determine grouping columns.
        //    SQL PIVOT: infer from input minus pivot/aggregate columns.
        //    DataFrame API: use the explicit `groupBy(...)` list.
        let grouping_named = if grouping.is_empty() {
            self.infer_pivot_grouping_columns(schema, &pivot_col_names, &aggregate_named, state)?
        } else {
            self.resolve_named_expressions(grouping, schema, state)
                .await?
        };

        // 5. Determine pivot values.
        //    If no values are specified, eagerly compute the distinct values.
        let service = self.ctx.extension::<PlanService>()?;
        let formatter = service.plan_formatter();
        let pivot_scalar_values = if pivot_values.is_empty() {
            self.compute_distinct_pivot_values(&input, &pivot_col_expr, state)
                .await?
        } else {
            let mut values = Vec::with_capacity(pivot_values.len());
            for pv in &pivot_values {
                if pv.values.len() != 1 {
                    return Err(PlanError::todo("multi-value pivot"));
                }
                let scalar = self.resolve_literal(pv.values[0].clone(), state)?;
                let name = match &pv.alias {
                    Some(alias) => alias.as_ref().to_string(),
                    None => formatter.literal_to_string(&scalar, &self.config.session_timezone)?,
                };
                values.push((scalar, name));
            }
            values
        };

        // 6. Build the pivot aggregates.
        //    For each pivot value and each aggregate function, add a filter predicate:
        //    agg_func(...) FILTER (WHERE pivot_col = value)
        let mut pivot_projections: Vec<NamedExpr> = Vec::new();
        let single_agg = aggregate_named.len() == 1;

        for (scalar_value, value_name) in &pivot_scalar_values {
            for agg_named in &aggregate_named {
                let agg_display_name = &agg_named.name;
                let filtered_agg = rewrite_aggregate_with_pivot_filter(
                    &agg_named.expr,
                    &pivot_col_expr,
                    scalar_value,
                )?;

                // Column naming follows Spark convention:
                // - Single aggregate: column name = <pivot_value>
                // - Multiple aggregates: column name = <pivot_value>_<agg_name>
                let col_name: String = if single_agg {
                    value_name.clone()
                } else {
                    let agg_name = agg_display_name.join(", ");
                    format!("{value_name}_{agg_name}")
                };

                pivot_projections.push(NamedExpr {
                    name: vec![col_name],
                    expr: filtered_agg,
                    metadata: vec![],
                });
            }
        }

        // 7. Build the final plan using rewrite_aggregate.
        //    The grouping expressions become the group-by,
        //    and pivot_projections become the projection (aggregates).
        self.rewrite_aggregate(
            input,
            pivot_projections,
            grouping_named,
            None,
            true, // with_grouping_expressions: include group-by columns in output
            state,
        )
    }

    fn infer_pivot_grouping_columns(
        &self,
        schema: &DFSchemaRef,
        pivot_col_names: &[String],
        aggregate_named: &[NamedExpr],
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<NamedExpr>> {
        // Skip hidden fields: `resolve_columns` filters them out, so listing
        // their names would produce `[UNRESOLVED_COLUMN]` errors.
        let input_names: Vec<String> = schema
            .fields()
            .iter()
            .filter_map(|field| {
                let info = state.get_field_info(field.name()).ok()?;
                (!info.is_hidden()).then(|| info.name().to_string())
            })
            .collect();
        let columns = Self::resolve_columns(self, schema, &input_names, state)?;

        let mut used_cols: HashSet<String> = HashSet::new();
        for name in pivot_col_names {
            used_cols.insert(name.clone());
        }
        // Aggregate expressions reference opaque field IDs; map back to user-visible names.
        for agg in aggregate_named {
            let mut opaque_ids: HashSet<String> = HashSet::new();
            collect_column_names(&agg.expr, &mut opaque_ids);
            for id in &opaque_ids {
                if let Ok(info) = state.get_field_info(id) {
                    used_cols.insert(info.name().to_string());
                }
            }
        }

        Ok(columns
            .iter()
            .zip(input_names.iter())
            .filter(|(_, name)| !used_cols.contains(name.as_str()))
            .map(|(c, name)| NamedExpr {
                name: vec![name.clone()],
                expr: col(c.flat_name()),
                metadata: vec![],
            })
            .collect::<Vec<_>>())
    }

    /// Compute distinct values for the pivot column by executing a query.
    async fn compute_distinct_pivot_values(
        &self,
        input: &LogicalPlan,
        pivot_col_expr: &Expr,
        _state: &mut PlanResolverState,
    ) -> PlanResult<Vec<(ScalarValue, String)>> {
        let service = self.ctx.extension::<PlanService>()?;
        let formatter = service.plan_formatter();

        // Alias the projection so the sort can reference it by name. Without
        // the alias, sorting by `pivot_col_expr` would re-evaluate against
        // source columns that the project just dropped, breaking computed
        // pivot expressions like `col("year") + 1`.
        const PIVOT_VALUE_ALIAS: &str = "__pivot_value";
        let distinct_plan = LogicalPlanBuilder::from(input.clone())
            .project(vec![pivot_col_expr.clone().alias(PIVOT_VALUE_ALIAS)])?
            .distinct()?
            .sort(vec![col(PIVOT_VALUE_ALIAS).sort(true, true)])?
            .build()?;

        let batches = self
            .ctx
            .execute_logical_plan(distinct_plan)
            .await?
            .collect()
            .await?;

        let mut values = Vec::new();
        for batch in &batches {
            let array = batch.column(0);
            for i in 0..array.len() {
                if array.is_valid(i) {
                    let scalar = ScalarValue::try_from_array(array, i)?;
                    let name =
                        formatter.literal_to_string(&scalar, &self.config.session_timezone)?;
                    values.push((scalar, name));
                    if values.len() > self.config.pivot_max_values {
                        return Err(PlanError::AnalysisError(format!(
                            "The pivot column {pivot_col_expr} has more than {} distinct values, \
                             this could indicate an error. If this was intended, set \
                             spark.sql.pivotMaxValues to at least the number of distinct values \
                             of the pivot column.",
                            self.config.pivot_max_values,
                        )));
                    }
                }
            }
        }
        Ok(values)
    }

    pub(super) async fn resolve_query_unpivot(
        &self,
        unpivot: spec::Unpivot,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan(unpivot.input.as_ref().clone(), state)
            .await?;

        let input_names = Self::get_field_names(input.schema(), state)?;
        let columns = Self::resolve_columns(self, input.schema(), &input_names, state)?;
        let col_name_map: HashMap<&str, String> = (columns
            .iter()
            .map(|c| c.name())
            .zip(input_names.iter().cloned()))
        .collect();

        let expr_with_real_name = |expr: expr::Expr| {
            let col_name = expr.qualified_name().1;
            let real_name = col_name_map.get(col_name.as_str()).ok_or_else(|| {
                PlanError::AnalysisError(format!("unpivot: cannot find column name for {col_name}"))
            })?;
            Ok((expr, real_name.clone()))
        };

        let remaining_exprs = |input_names: &Vec<String>,
                               chosen_names_set: &HashSet<String>,
                               state: &mut PlanResolverState|
         -> PlanResult<Vec<(expr::Expr, String)>> {
            let remaining_names = input_names
                .iter()
                .filter(|&name| !chosen_names_set.contains(name))
                .collect::<Vec<_>>();
            let remaining_columns =
                Self::resolve_columns(self, input.schema(), &remaining_names, state)?;
            Ok(remaining_columns
                .iter()
                .zip(remaining_names.iter())
                .map(|(c, &name)| (col(c.flat_name()), name.clone()))
                .collect())
        };

        let all_columns_are_ids_err = || {
            Err(PlanError::AnalysisError(
            "[UNPIVOT_REQUIRES_VALUE_COLUMNS] At least one value column needs to be specified for UNPIVOT, all columns specified as ids.".to_string()
        ))
        };

        let ids_opt = match unpivot.ids {
            Some(ids) => {
                let ids = self.resolve_expressions(ids, input.schema(), state).await?;
                let ids = ids
                    .into_iter()
                    .map(expr_with_real_name)
                    .collect::<Result<Vec<_>, PlanError>>()?;
                Some(ids)
            }
            None => None,
        };

        let values_opt = match unpivot.values {
            Some(values) => {
                let values: Vec<spec::Expr> = values
                    .iter()
                    .flat_map(|value| &value.columns)
                    .cloned()
                    .collect();

                let values = self
                    .resolve_expressions(values, input.schema(), state)
                    .await?;

                let values = values
                    .into_iter()
                    .map(expr_with_real_name)
                    .collect::<Result<Vec<_>, PlanError>>()?;
                Some(values)
            }
            None => None,
        };

        let (ids, values) = match (ids_opt, values_opt) {
            (Some(ids), _) if ids.len() == input_names.len() => all_columns_are_ids_err(),
            (_, Some(values)) if values.is_empty() => all_columns_are_ids_err(),
            (None, None) => all_columns_are_ids_err(),
            (Some(ids), Some(values)) => Ok((ids, values)),
            (None, Some(values)) => {
                let values_names: HashSet<String> =
                    values.iter().map(|expr_name| expr_name.1.clone()).collect();
                Ok((remaining_exprs(&input_names, &values_names, state)?, values))
            }
            (Some(ids), None) => {
                let ids_names: HashSet<String> =
                    ids.iter().map(|expr_name| expr_name.1.clone()).collect();
                Ok((ids, remaining_exprs(&input_names, &ids_names, state)?))
            }
        }?;

        let values_types = values
            .iter()
            .map(|(value, _name)| value.get_type(input.schema()))
            .collect::<Result<Vec<_>, _>>()?;

        if !types_are_coercible(&values_types) {
            let type_names: Vec<String> =
                values_types.iter().map(|dt| format!("{:?}", dt)).collect();
            return Err(PlanError::AnalysisError(format!(
                "[UNPIVOT_VALUE_DATA_TYPE_MISMATCH] Unpivot value columns must share a least common type, some types do not: {}",
                type_names.join(", ")
            )));
        }

        let variable_column_name = unpivot.variable_column_name.as_ref();
        let value_column_name = unpivot.value_column_names[0].as_ref();

        let structs = values
            .into_iter()
            .map(|(value, name)| {
                ScalarUDF::from(StructFunction::new(vec![
                    variable_column_name.to_string(),
                    value_column_name.to_string(),
                ]))
                .call(vec![lit(name), value])
            })
            .collect::<Vec<_>>();

        let structs_arr = nested_fn::make_array(structs);
        let inline_expr = ScalarUDF::from(explode::Explode::new(explode::ExplodeKind::Inline))
            .call(vec![structs_arr]);
        let inline_name = state.register_field_name("");

        let projections = ids
            .into_iter()
            .chain(std::iter::once((inline_expr, inline_name)))
            .map(|(expr, name)| NamedExpr {
                name: vec![name.to_string()],
                expr: expr.clone(),
                metadata: vec![],
            })
            .collect::<Vec<_>>();

        let (input, expr) =
            self.rewrite_projection::<MonotonicIdRewriter>(input.clone(), projections, state)?;
        let (input, expr) =
            self.rewrite_projection::<SparkPartitionIdRewriter>(input, expr, state)?;
        let (input, expr) = self.rewrite_projection::<ExplodeRewriter>(input, expr, state)?;

        let expr = self.rewrite_multi_expr(expr)?;
        let expr = self.rewrite_named_expressions(expr, state)?;

        Ok(LogicalPlan::Projection(Projection::try_new(
            expr,
            Arc::new(input),
        )?))
    }
}

fn types_are_coercible(data_types: &[DataType]) -> bool {
    if data_types.is_empty() {
        return true;
    }
    data_types
        .iter()
        .skip(1)
        .try_fold(data_types[0].clone(), |acc, dt| match (&acc, dt) {
            (DataType::Utf8 | DataType::LargeUtf8, DataType::Utf8 | DataType::LargeUtf8)
            | (DataType::Null, _)
            | (_, DataType::Null)
            | (_, _)
                if (&acc == dt) || (acc.is_numeric() && dt.is_numeric()) =>
            {
                Some(acc)
            }
            _ => None,
        })
        .is_some()
}

/// Rewrite an aggregate expression to include a pivot filter.
/// Instead of wrapping aggregate arguments in CASE WHEN, this uses the
/// aggregate function's `filter` parameter: `agg_func(...) FILTER (WHERE pivot_col = value)`.
/// This approach correctly handles nested/composed aggregates (e.g. `sum(x) + 1`),
/// `count(*)`, and preserves correct semantics for all aggregate types.
///
/// The function walks the expression tree (bottom-up) and rewrites each
/// `AggregateFunction` node, combining any pre-existing filter with the pivot
/// predicate via AND.  Top-level aliases are stripped because the caller
/// controls output column naming.
fn rewrite_aggregate_with_pivot_filter(
    agg_expr: &Expr,
    pivot_col: &Expr,
    pivot_value: &ScalarValue,
) -> PlanResult<Expr> {
    let pivot_predicate = build_pivot_predicate(pivot_col, pivot_value);
    // Use transform_up so that inner AggregateFunction nodes are rewritten
    // before outer Alias nodes are stripped.
    let result = agg_expr
        .clone()
        .transform_up(|e| match e {
            Expr::AggregateFunction(agg_func) => {
                let new_filter = match &agg_func.params.filter {
                    Some(existing) => {
                        Box::new(existing.as_ref().clone().and(pivot_predicate.clone()))
                    }
                    None => Box::new(pivot_predicate.clone()),
                };
                Ok(Transformed::yes(Expr::AggregateFunction(
                    expr::AggregateFunction {
                        func: agg_func.func,
                        params: expr::AggregateFunctionParams {
                            args: agg_func.params.args,
                            distinct: agg_func.params.distinct,
                            filter: Some(new_filter),
                            order_by: agg_func.params.order_by,
                            null_treatment: agg_func.params.null_treatment,
                        },
                    },
                )))
            }
            Expr::Alias(alias) => {
                // Strip aliases; the caller controls output column naming.
                Ok(Transformed::yes(*alias.expr))
            }
            other => Ok(Transformed::no(other)),
        })
        .data()?;
    Ok(result)
}

/// Build the predicate expression for pivot filtering.
/// Uses `IS NULL` for NULL pivot values (since `col = NULL` is always NULL in SQL)
/// and standard equality for non-NULL values.
fn build_pivot_predicate(pivot_col: &Expr, pivot_value: &ScalarValue) -> Expr {
    if pivot_value.is_null() {
        pivot_col.clone().is_null()
    } else {
        pivot_col
            .clone()
            .eq(Expr::Literal(pivot_value.clone(), None))
    }
}

/// Collect all column names referenced by an expression.
fn collect_column_names(expr: &Expr, names: &mut HashSet<String>) {
    let _ = expr.apply(|e| {
        if let Expr::Column(col) = e {
            names.insert(col.name().to_string());
        }
        Ok(TreeNodeRecursion::Continue)
    });
}
