use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DFSchemaRef, ScalarValue};
use datafusion_expr::{
    col, expr, lit, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Projection, ScalarUDF,
};
use datafusion_functions_nested::expr_fn as nested_fn;
use sail_common::spec;
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
            aggregate,
            columns,
            values,
        } = pivot;

        let input = self.resolve_query_plan(*input, state).await?;
        let schema = input.schema().clone();

        // Only a single pivot column is supported.
        let mut pivot_columns = self.resolve_expressions(columns, &schema, state).await?;
        if pivot_columns.len() != 1 {
            return Err(PlanError::todo("pivot with multiple pivot columns"));
        }
        let pivot_column = pivot_columns.swap_remove(0);

        let aggregates = self
            .resolve_named_expressions(aggregate, &schema, state)
            .await?;

        // For SQL pivots the grouping list is empty; the implicit grouping columns are
        // all input columns not referenced by the pivot column or the aggregates.
        let grouping = self
            .resolve_named_expressions(grouping, &schema, state)
            .await?;
        let grouping = if grouping.is_empty() {
            Self::implicit_pivot_grouping(&schema, &pivot_column, &aggregates, state)?
        } else {
            grouping
        };

        // Each pivot value becomes one output column per aggregate. Values may be
        // given explicitly (`... IN (v1, v2)`); otherwise infer the distinct values
        // from the data (matching Spark's `pivot(column)`).
        let pivot_values: Vec<(ScalarValue, Option<String>)> = if values.is_empty() {
            self.infer_pivot_values(&input, &pivot_column)
                .await?
                .into_iter()
                .map(|scalar| (scalar, None))
                .collect()
        } else {
            let mut resolved = Vec::with_capacity(values.len());
            for value in values {
                let spec::PivotValue {
                    values: literals,
                    alias,
                } = value;
                if literals.len() != 1 {
                    return Err(PlanError::todo(
                        "pivot value with multiple literals (struct pivot)",
                    ));
                }
                let Some(literal) = literals.into_iter().next() else {
                    return Err(PlanError::AnalysisError(
                        "pivot value must have a literal".to_string(),
                    ));
                };
                resolved.push((self.resolve_literal(literal, state)?, alias.map(String::from)));
            }
            resolved
        };

        let single_aggregate = aggregates.len() == 1;
        let mut projections = grouping.clone();
        for (scalar, alias) in pivot_values {
            // A NULL pivot value forms its own group in Spark: match rows where the
            // pivot column IS NULL (equality with NULL never holds) and name the
            // output column `null`.
            let is_null = scalar.is_null();
            let value_name = match alias {
                Some(alias) => alias,
                None if is_null => "null".to_string(),
                None => scalar.to_string(),
            };
            let predicate = if is_null {
                pivot_column.clone().is_null()
            } else {
                pivot_column.clone().eq(lit(scalar))
            };
            for agg in &aggregates {
                let expr = inject_pivot_filter(agg.expr.clone(), &predicate)?;
                let name = if single_aggregate {
                    value_name.clone()
                } else {
                    format!("{value_name}_{}", agg.name.join("_"))
                };
                projections.push(NamedExpr {
                    name: vec![name],
                    expr,
                    metadata: vec![],
                });
            }
        }

        self.rewrite_aggregate(input, projections, grouping, None, false, state)
    }

    /// Infer pivot values by collecting the distinct values of the pivot column
    /// from the input, sorted ascending (matching Spark's `pivot(column)`).
    async fn infer_pivot_values(
        &self,
        input: &LogicalPlan,
        pivot_column: &expr::Expr,
    ) -> PlanResult<Vec<ScalarValue>> {
        // `GROUP BY pivot_column` with no aggregates is equivalent to
        // `SELECT DISTINCT pivot_column`.
        let plan = LogicalPlanBuilder::from(input.clone())
            .aggregate(vec![pivot_column.clone()], Vec::<expr::Expr>::new())?
            .build()?;
        let batches = self
            .ctx
            .execute_logical_plan(plan)
            .await?
            .collect()
            .await?;
        let mut values = Vec::new();
        for batch in &batches {
            let column = batch.column(0);
            for row in 0..batch.num_rows() {
                values.push(ScalarValue::try_from_array(column, row)?);
            }
        }
        // Spark rejects pivots whose distinct-value count exceeds the configured
        // maximum (`spark.sql.pivotMaxValues`, default 10000).
        const MAX_PIVOT_VALUES: usize = 10000;
        if values.len() > MAX_PIVOT_VALUES {
            return Err(PlanError::AnalysisError(format!(
                "Too many distinct pivot values ({}); maximum is {MAX_PIVOT_VALUES}",
                values.len()
            )));
        }
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        Ok(values)
    }

    /// Compute the implicit grouping columns for a SQL `PIVOT` with no explicit
    /// grouping: every input column that is not referenced by the pivot column or
    /// any of the aggregate expressions.
    fn implicit_pivot_grouping(
        schema: &DFSchemaRef,
        pivot_column: &expr::Expr,
        aggregates: &[NamedExpr],
        state: &PlanResolverState,
    ) -> PlanResult<Vec<NamedExpr>> {
        let mut referenced: HashSet<Column> = HashSet::new();
        for column in pivot_column.column_refs() {
            referenced.insert(column.clone());
        }
        for agg in aggregates {
            for column in agg.expr.column_refs() {
                referenced.insert(column.clone());
            }
        }
        // The schema field names are internal ids (e.g. `#6`); map each to its
        // user-facing name so the grouping columns keep their original names.
        let names = Self::get_field_names(schema, state)?;
        Ok(schema
            .columns()
            .into_iter()
            .zip(names)
            .filter(|(column, _)| !referenced.contains(column))
            .map(|(column, name)| NamedExpr {
                name: vec![name],
                expr: expr::Expr::Column(column),
                metadata: vec![],
            })
            .collect())
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

/// Add `predicate` as a FILTER to every aggregate function inside `expr`,
/// AND-combining with any existing filter. Implements Spark's pivot semantics:
/// each aggregate is computed only over rows matching the pivot value.
fn inject_pivot_filter(expr: expr::Expr, predicate: &expr::Expr) -> PlanResult<expr::Expr> {
    Ok(expr
        .transform(|e| match e {
            expr::Expr::AggregateFunction(mut func) => {
                let filter = match func.params.filter.take() {
                    Some(existing) => (*existing).and(predicate.clone()),
                    None => predicate.clone(),
                };
                func.params.filter = Some(Box::new(filter));
                Ok(Transformed::yes(expr::Expr::AggregateFunction(func)))
            }
            other => Ok(Transformed::no(other)),
        })?
        .data)
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
