use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, DFSchemaRef, ScalarValue};
use datafusion_expr::expr::NullTreatment;
use datafusion_expr::{
    col, expr, lit, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Projection, ScalarUDF,
};
use datafusion_functions_nested::expr_fn as nested_fn;
use sail_common::spec;
use sail_common_datafusion::display::{ArrayFormatter, FormatOptions};
use sail_common_datafusion::literal::LiteralEvaluator;
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

        let mut pivot_columns = self.resolve_expressions(columns, &schema, state).await?;
        let column_arity = pivot_columns.len();
        let pivot_column = match column_arity {
            0 => {
                return Err(PlanError::AnalysisError(
                    "pivot column required".to_string(),
                ))
            }
            1 => pivot_columns.swap_remove(0),
            _ => make_pivot_struct(pivot_columns),
        };

        let aggregates = self
            .resolve_named_expressions(aggregate, &schema, state)
            .await?;

        // Spark allows aggregate expressions and pure literals, but rejects columns outside an
        // aggregate function.
        for agg in &aggregates {
            check_valid_pivot_aggregate(&agg.expr, state)?;
        }

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
            let pivot_column_name = pivot_column_display_name(&pivot_column, state)?;
            self.infer_pivot_values(&input, &pivot_column, &pivot_column_name)
                .await?
                .into_iter()
                .map(|scalar| (scalar, None))
                .collect()
        } else {
            // Each pivot value is a foldable expression (literal, typed literal such as
            // `DATE'...'`, or cast). Resolve it against an empty schema and fold it to a scalar
            // with `LiteralEvaluator`, mirroring how SHOW PARTITIONS resolves partition values.
            let empty_schema = Arc::new(DFSchema::empty());
            let evaluator = LiteralEvaluator::new();
            let mut resolved = Vec::with_capacity(values.len());
            for value in values {
                let spec::PivotValue {
                    values: exprs,
                    alias,
                } = value;
                if exprs.len() != column_arity {
                    return Err(PlanError::AnalysisError(format!(
                        "pivot value has {} expressions, but pivot column has {}",
                        exprs.len(),
                        column_arity
                    )));
                }
                let mut exprs = self
                    .resolve_expressions(exprs, &empty_schema, state)
                    .await?;
                let expr = if column_arity == 1 {
                    exprs.swap_remove(0)
                } else {
                    make_pivot_struct(exprs)
                };
                let scalar = evaluator
                    .evaluate(&expr)
                    .map_err(|e| PlanError::invalid(e.to_string()))?;
                resolved.push((scalar, alias.map(String::from)));
            }
            resolved
        };

        // Spark casts each pivot value to the pivot column's type before comparing
        // (`Cast(value, pivotColumn.dataType)`), so an `int` literal matches a `bigint` column and
        // a struct value matches the struct column's field types. The output name still uses the
        // raw value (Spark casts the original value to string), so it is taken from `scalar` below
        // before the cast.
        let column_type = pivot_column.get_type(&schema)?;
        let spark_uses_pivot_first = aggregates
            .iter()
            .map(|agg| agg.expr.get_type(&schema))
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .all(spark_pivot_first_supports_data_type);
        let force_first_last_ignore_nulls = !spark_uses_pivot_first;
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
                None => pivot_value_name(&scalar)?,
            };
            let predicate = if is_null {
                pivot_column.clone().is_null()
            } else {
                let scalar = scalar
                    .cast_to(&column_type)
                    .map_err(|e| PlanError::invalid(e.to_string()))?;
                // Spark's PivotFirst fast path creates a NaN column but does not match NaN rows into it.
                if spark_uses_pivot_first && scalar_is_nan(&scalar) {
                    lit(false)
                } else {
                    pivot_column.clone().eq(lit(scalar))
                }
            };
            for agg in &aggregates {
                let expr = inject_pivot_filter(
                    agg.expr.clone(),
                    &predicate,
                    force_first_last_ignore_nulls,
                )?;
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
        pivot_column_name: &str,
    ) -> PlanResult<Vec<ScalarValue>> {
        // Spark rejects pivots whose distinct-value count exceeds the configured maximum
        // (`spark.sql.pivotMaxValues`, default 10000), defined as `DATAFRAME_PIVOT_MAX_VALUES`
        // in `SQLConf`. Its `collectPivotValues` bounds the distinct-value scan with
        // `.limit(maxValues + 1)` before collecting, which is what the LIMIT below mirrors:
        //   https://github.com/apache/spark/blob/v4.0.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L1951
        //   https://github.com/apache/spark/blob/v4.0.0/sql/core/src/main/scala/org/apache/spark/sql/classic/RelationalGroupedDataset.scala#L637-L655
        let max_pivot_values = self.config.pivot_max_values;
        // `GROUP BY pivot_column` with no aggregates is equivalent to
        // `SELECT DISTINCT pivot_column`. Apply a LIMIT of one past the cap so a
        // high-cardinality pivot column never pulls an unbounded number of distinct
        // values into the planner process; the extra row still lets us detect overflow.
        let plan = LogicalPlanBuilder::from(input.clone())
            .aggregate(vec![pivot_column.clone()], Vec::<expr::Expr>::new())?
            .limit(0, Some(max_pivot_values.saturating_add(1)))?
            .build()?;
        let batches = self.ctx.execute_logical_plan(plan).await?.collect().await?;
        let mut values = Vec::new();
        for batch in &batches {
            let column = batch.column(0);
            for row in 0..batch.num_rows() {
                values.push(ScalarValue::try_from_array(column, row)?);
            }
        }
        // The plan is limited to `max_pivot_values + 1` rows, so report "more than" rather than
        // an exact count (which would be capped and misleading) — mirroring Spark's message.
        if values.len() > max_pivot_values {
            return Err(PlanError::AnalysisError(format!(
                "The pivot column {pivot_column_name} has more than {max_pivot_values} distinct \
                 values, this could indicate an error. If this was intended, set \
                 spark.sql.pivotMaxValues to at least the number of distinct values of the pivot \
                 column."
            )));
        }
        // `partial_cmp` returns `None` only for incomparable values such as float `NaN`.
        // Fall back to a deterministic tiebreaker so the inferred pivot column order is stable
        // across runs (the distinct values arrive in non-deterministic aggregate order). The
        // string form also places `NaN` last, matching Spark's "NaN is greatest" ordering.
        values.sort_by(|a, b| {
            a.partial_cmp(b)
                .unwrap_or_else(|| a.to_string().cmp(&b.to_string()))
        });
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

/// Formats a pivot value into its output column name via Sail's `CAST(... AS STRING)` arrow
/// formatter (keeps `.0` on whole doubles, formats decimals) rather than `ScalarValue::Display`.
fn pivot_value_name(scalar: &ScalarValue) -> PlanResult<String> {
    let array = scalar
        .to_array()
        .map_err(|e| PlanError::invalid(e.to_string()))?;
    let formatter = ArrayFormatter::try_new(array.as_ref(), &FormatOptions::default())
        .map_err(|e| PlanError::invalid(e.to_string()))?;
    formatter
        .value(0)
        .try_to_string()
        .map_err(|e| PlanError::invalid(e.to_string()))
}

fn make_pivot_struct(exprs: Vec<expr::Expr>) -> expr::Expr {
    ScalarUDF::from(StructFunction::new(
        (0..exprs.len()).map(|i| format!("col{}", i + 1)).collect(),
    ))
    .call(exprs)
}

fn pivot_column_display_name(expr: &expr::Expr, state: &PlanResolverState) -> PlanResult<String> {
    if let expr::Expr::Column(column) = expr {
        Ok(state.get_field_info(column.name())?.name().to_string())
    } else {
        Ok(expr.to_string())
    }
}

fn check_valid_pivot_aggregate(expr: &expr::Expr, state: &PlanResolverState) -> PlanResult<()> {
    let mut bare_column = None;

    expr.apply(|e| match e {
        expr::Expr::AggregateFunction(_) => Ok(TreeNodeRecursion::Jump),
        expr::Expr::Column(column) => {
            bare_column = Some(column.clone());
            Ok(TreeNodeRecursion::Stop)
        }
        _ => Ok(TreeNodeRecursion::Continue),
    })?;

    if let Some(column) = bare_column {
        let name = state
            .get_field_info(column.name())
            .map(|info| info.name().to_string())
            .unwrap_or_else(|_| column.name().to_string());
        return Err(PlanError::AnalysisError(format!(
            "Aggregate expression required for pivot, but '{name}' did not appear in any \
             aggregate function."
        )));
    }

    Ok(())
}

fn spark_pivot_first_supports_data_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

fn scalar_is_nan(scalar: &ScalarValue) -> bool {
    match scalar {
        ScalarValue::Float16(Some(value)) => value.is_nan(),
        ScalarValue::Float32(Some(value)) => value.is_nan(),
        ScalarValue::Float64(Some(value)) => value.is_nan(),
        _ => false,
    }
}

/// Add `predicate` as a FILTER to every aggregate function inside `expr`,
/// AND-combining with any existing filter. Implements Spark's pivot semantics:
/// each aggregate is computed only over rows matching the pivot value.
fn inject_pivot_filter(
    expr: expr::Expr,
    predicate: &expr::Expr,
    force_first_last_ignore_nulls: bool,
) -> PlanResult<expr::Expr> {
    Ok(expr
        .transform(|e| match e {
            expr::Expr::AggregateFunction(mut func) => {
                let filter = match func.params.filter.take() {
                    Some(existing) => (*existing).and(predicate.clone()),
                    None => predicate.clone(),
                };
                func.params.filter = Some(Box::new(filter));
                if force_first_last_ignore_nulls
                    && matches!(func.func.name(), "first_value" | "last_value")
                {
                    func.params.null_treatment = Some(NullTreatment::IgnoreNulls);
                }
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
