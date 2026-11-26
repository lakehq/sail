// SPDX-License-Identifier: Apache-2.0
// SPDX-FileNotice: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership.
// SPDX-FileComment: Credit: https://github.com/apache/datafusion/blob/454bf2978660f69744eb220bed19b0a1d60f4128/datafusion/physical-optimizer/src/window_topn.rs
// SPDX-FileAttributionText: Notice: https://github.com/apache/datafusion/blob/454bf2978660f69744eb220bed19b0a1d60f4128/NOTICE.txt

//! [`WindowTopN`] optimizer rule for per-partition top-K window queries.
//!
//! Detects queries of the form:
//!
//! ```sql
//! SELECT * FROM (
//!     SELECT *, ROW_NUMBER() OVER (PARTITION BY pk ORDER BY val) as rn
//!     FROM t
//! ) WHERE rn <= K;
//! ```
//!
//! or with `RANK()` / `DENSE_RANK()` in place of `ROW_NUMBER()`:
//!
//! ```sql
//! SELECT * FROM (
//!     SELECT *, RANK() OVER (PARTITION BY pk ORDER BY val) as rk
//!     FROM t
//! ) WHERE rk <= K;
//! ```
//!
//! And replaces the `FilterExec → BoundedWindowAggExec` pipeline with
//! `PartitionedTopKExec(fetch=K)`, removing the `FilterExec`. When the window
//! node holds a single ranking expression the operator emits that expression's
//! column itself and the `BoundedWindowAggExec` is removed too; with two or
//! more it is kept above the operator to compute its columns.
//!
//! The appropriate [`WindowFnKind`] is forwarded to `PartitionedTopKExec`.
//! `RANK` and `DENSE_RANK` require a non-empty `ORDER BY` clause (otherwise
//! all rows tie at rank 1 and the optimization is degenerate).
//!
//! See [`PartitionedTopKExec`] for details on the replacement operator.
//!
//! [`PartitionedTopKExec`]: datafusion_physical_plan::sorts::partitioned_topk::PartitionedTopKExec
//! [`WindowFnKind`]: datafusion_physical_plan::sorts::partitioned_topk::WindowFnKind

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::functions_window::rank::{dense_rank_udwf, rank_udwf};
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::execution_plan::replace_children_if_necessary;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::partitioned_topk::{PartitionedTopKExec, WindowFnKind};
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowUDFExpr};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, CastExpr, Column, Literal, TryCastExpr};
use datafusion_physical_expr::window::StandardWindowExpr;
use datafusion_physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};

use crate::PhysicalOptimizerRule;

/// Physical optimizer rule that converts per-partition `ROW_NUMBER`,
/// `RANK`, and `DENSE_RANK` top-K queries into a more efficient plan
/// using [`PartitionedTopKExec`].
///
/// # Pattern Detected
///
/// ```text
/// FilterExec(<ranking fn output> <= K)
///   [optional ProjectionExec]
///     BoundedWindowAggExec(<ranking fn> PARTITION BY ... ORDER BY ...)
/// ```
///
/// # Replacement
///
/// ```text
/// [optional ProjectionExec]
///   PartitionedTopKExec(fn=<row_number|rank|dense_rank>, partition_keys, order_keys, fetch=K, emit=[<ranking fn> PARTITION BY ... ORDER BY ...])
/// ```
///
/// or, when the window node holds two or more window expressions and so still
/// has columns to compute:
///
/// ```text
/// [optional ProjectionExec]
///   BoundedWindowAggExec(<ranking fns> PARTITION BY ... ORDER BY ...)
///     PartitionedTopKExec(fn=<row_number|rank|dense_rank>, partition_keys, order_keys, fetch=K)
/// ```
///
/// The `FilterExec` is removed entirely, and `PartitionedTopKExec` maintains
/// per-partition top-K state (a heap for `ROW_NUMBER`, a heap plus boundary
/// ties for `RANK`, a K-bounded distinct-ob map for `DENSE_RANK`) instead of
/// sorting the whole dataset. In the first shape it also appends the ranking
/// column, which it can derive from the retained rows as it emits them, so
/// nothing is left for the window node to do and it is removed as well. The
/// column keeps the window expression's name, not the query's alias (`rn`):
/// the alias is applied by the `ProjectionExec` above.
///
/// # Supported Predicates
///
/// - `rn <= K` → fetch = K
/// - `rn < K` → fetch = K - 1
/// - `K >= rn` (flipped) → fetch = K
/// - `K > rn` (flipped) → fetch = K - 1
///
/// `rn` may also be wrapped in an integer cast that cannot change the result,
/// such as Sail's `CAST(rn AS Int32) <= K` (see [`ranking_column`]).
///
/// # When the Rule Fires
///
/// All of the following must be true:
/// - Config flag `enable_window_topn` is `true`
/// - The plan matches `FilterExec → [ProjectionExec] → BoundedWindowAggExec`
/// - Every `ProjectionExec` between the filter and the window passes the
///   filtered column through as a bare column, possibly at another position
///   (as when a column the query no longer needs is dropped)
/// - The window function is `ROW_NUMBER`, `RANK`, or `DENSE_RANK`
/// - Every window expression in the `BoundedWindowAggExec` is `ROW_NUMBER`,
///   `RANK`, or `DENSE_RANK` over the same `PARTITION BY` / `ORDER BY`. A
///   sibling that reads pruned rows (e.g. `LEAD`, or an aggregate whose
///   frame is not strictly backward-looking) would be computed over the
///   pruned input and give wrong results.
/// - The window function has a `PARTITION BY` clause (global top-K is
///   already handled by `SortExec` with `fetch`)
/// - At least one `ORDER BY` key survives past the `PARTITION BY` prefix
///   (so the operator has a non-empty ORDER BY). This rejects both a
///   missing `ORDER BY` and one fully covered by the partition prefix
///   such as `PARTITION BY pk ORDER BY pk`; for `RANK` / `DENSE_RANK`
///   such orderings also make every row tie at rank 1 (degenerate).
/// - The filter predicate compares the window output column to an integer
///   literal using `<=`, `<`, `>=`, or `>`
///
/// [`PartitionedTopKExec`]: datafusion_physical_plan::sorts::partitioned_topk::PartitionedTopKExec
#[derive(Default, Clone, Debug)]
pub struct WindowTopN;

impl WindowTopN {
    pub fn new() -> Self {
        Self
    }

    /// Attempt to transform a single plan node.
    ///
    /// Returns `Some(new_plan)` if the node matches the
    /// `FilterExec → [ProjectionExec] → BoundedWindowAggExec`
    /// pattern and can be rewritten, or `None` if the node should be
    /// left unchanged.
    fn try_transform(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        // Step 1: Match FilterExec at the top
        let filter = plan.downcast_ref::<FilterExec>()?;

        // Don't handle filters with projections
        if filter.projection().is_some() {
            return None;
        }

        // The rewrite replaces the `FilterExec` entirely, so anything it
        // carries beyond the predicate has to be reproduced or declined.
        // `fetch` is applied by `FilterExec::execute` *after* the predicate,
        // and the rewritten plan has nowhere to put it: a
        // `PartitionedTopKExec` bounds rows per partition, which is not the
        // same as a row limit over the filtered output. Declining keeps the
        // rule from silently returning more rows than were asked for.
        //
        // The default rule list runs `LimitPushdown` after this rule, so no
        // plan reaches here with a fetch today. The guard is cheap insurance
        // for pipelines that reorder the two.
        if filter.fetch().is_some() {
            return None;
        }

        // Step 2: Extract limit from predicate (rn <= K, rn < K, etc.)
        let (col_idx, limit_n) = extract_window_limit(filter.predicate())?;

        // A predicate such as `rn < 1` (or the flipped `1 > rn`) yields a fetch of
        // 0. `ROW_NUMBER`/`RANK`/`DENSE_RANK` are always >= 1, so no row can satisfy
        // it and the correct result is empty. `PartitionedTopKExec` requires `k > 0`
        // and would panic on `k = 0`, so bail out here and let the regular
        // `FilterExec` produce the (empty) result instead of rewriting.
        if limit_n == 0 {
            return None;
        }

        // Step 3: Walk through optional ProjectionExec and RepartitionExec to find BoundedWindowAggExec
        let child = filter.input();
        let (window_exec, intermediates) = find_window_below(child)?;

        // Step 4: Verify col_idx references a supported window function output column.
        // `col_idx` indexes the filter's input; resolve it to the window's output
        // first, since a projection in between can drop or reorder columns.
        let col_idx = column_index_below(col_idx, &intermediates)?;
        let window_exec_typed = window_exec.downcast_ref::<BoundedWindowAggExec>()?;
        let input_field_count = window_exec_typed.input().schema().fields().len();
        if col_idx < input_field_count {
            return None; // Filter is on an input column, not a window column
        }
        let window_expr_idx = col_idx - input_field_count;
        let window_exprs = window_exec_typed.window_expr();
        if window_expr_idx >= window_exprs.len() {
            return None;
        }
        let fn_kind = supported_window_fn(&window_exprs[window_expr_idx])?;

        // Tail-pruning drops the rows that rank after the retained top-K,
        // and every window expression in this `BoundedWindowAggExec` is
        // then evaluated over the *pruned* input. The rewrite is only valid
        // if each expression's value for a *retained* row is unaffected by
        // the dropped rows. ROW_NUMBER / RANK / DENSE_RANK over the same
        // PARTITION BY / ORDER BY satisfy this — each depends only on rows
        // at or before the current row in that order, all of which are
        // retained. A sibling like `LEAD(x)` reads following (pruned) rows,
        // so at the retained boundary it would resolve to a dropped row and
        // give a wrong result. Bail out unless every window expression is a
        // supported ranking function sharing the matched expression's
        // partition/order keys.
        let matched_expr = &window_exprs[window_expr_idx];
        let all_prune_safe = window_exprs.iter().all(|e| {
            supported_window_fn(e).is_some()
                && e.partition_by() == matched_expr.partition_by()
                && e.order_by() == matched_expr.order_by()
        });
        if !all_prune_safe {
            return None;
        }

        // Step 5: Validate PARTITION BY / ORDER BY and collect sort keys from the window expr
        let partition_by = window_exprs[window_expr_idx].partition_by();
        let partition_prefix_len = partition_by.len();

        // Without PARTITION BY, this is just a global top-K which
        // SortExec with fetch already handles efficiently.
        if partition_prefix_len == 0 {
            return None;
        }

        // Step 6: Build PartitionedTopKExec from the window's partition/order keys
        let order_by = window_exprs[window_expr_idx].order_by();
        let expr_iterator = partition_by
            .iter()
            .map(|e| PhysicalSortExpr::new_default(Arc::clone(e)))
            .chain(order_by.iter().cloned());
        let expr = LexOrdering::new(expr_iterator)?;

        // `PartitionedTopKExec` derives its ORDER BY keys from the ordering
        // *beyond* the partition prefix (`expr[partition_prefix_len..]`).
        // That slice is empty in two cases:
        //   * no ORDER BY at all (e.g. `ROW_NUMBER() OVER (PARTITION BY pk)`);
        //   * ORDER BY keys fully covered by the partition prefix (e.g.
        //     `DENSE_RANK() OVER (PARTITION BY pk ORDER BY pk)`, whose
        //     deduplicated ordering is just `[pk]`).
        // With zero order keys the operator panics on execution (it
        // requires at least one), and for RANK / DENSE_RANK every row
        // would tie at rank 1 (a degenerate, unbounded retained set).
        // `order_by()` alone does not catch the second case — it reports
        // `[pk]` even though no order key survives past the partition
        // prefix — so guard on the effective order-key count instead.
        if expr.len() <= partition_prefix_len {
            return None;
        }

        // The operator can emit the ranking column itself, which makes the
        // `BoundedWindowAggExec` above it redundant — see step 7. All three
        // ranking functions are derivable from the retained set as it is
        // emitted (`PartitionedTopKExec` documents how per policy), so the only
        // condition is that there is exactly one window expression: the
        // operator appends a single column, and it has to reproduce the whole
        // of the window node's output schema. With two or more the window node
        // stays and computes them as before.
        let ranking_field = if window_exprs.len() == 1 {
            Some(matched_expr.field().ok()?)
        } else {
            None
        };

        let mut partitioned_topk = PartitionedTopKExec::try_new(
            Arc::clone(window_exec_typed.input()),
            expr,
            partition_prefix_len,
            limit_n,
            fn_kind,
        )
        .ok()?;

        if let Some(field) = &ranking_field {
            partitioned_topk = partitioned_topk
                .with_ranking_field(Arc::clone(field))
                .ok()?;
        }

        // Step 7: Rebuild the pipeline above the operator.
        //
        // When the operator emits the ranking column itself, the
        // `BoundedWindowAggExec` has nothing left to compute: it would
        // re-derive, over the retained rows, exactly the values already in
        // that column. Its output schema is the input's fields followed by
        // its window expressions' fields, which is what the operator now
        // produces, so the nodes above it keep referencing the same column
        // indices. Drop it.
        let mut result: Arc<dyn ExecutionPlan> = Arc::new(partitioned_topk);
        if ranking_field.is_none() {
            result = replace_children_if_necessary(window_exec, vec![result]).ok()?;
        }

        // Step 8: Rebuild intermediate nodes (ProjectionExec/RepartitionExec)
        for node in intermediates.into_iter().rev() {
            result = replace_children_if_necessary(node, vec![result]).ok()?;
        }

        Some(result)
    }
}

impl PhysicalOptimizerRule for WindowTopN {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !config.optimizer.enable_window_topn {
            return Ok(plan);
        }

        plan.transform_down(|node| {
            Ok(
                if let Some(transformed) = WindowTopN::try_transform(&node) {
                    Transformed::yes(transformed)
                } else {
                    Transformed::no(node)
                },
            )
        })
        .data()
    }

    fn name(&self) -> &str {
        "WindowTopN"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Extract a window limit from a predicate expression.
///
/// Returns `(column_index, fetch)` if the predicate constrains a column
/// to at most N rows.
///
/// # Supported Patterns
///
/// | Predicate | Returns |
/// |-----------|---------|
/// | `Column(idx) <= Literal(N)` | `(idx, N)` |
/// | `Column(idx) < Literal(N)` | `(idx, N-1)` |
/// | `Literal(N) >= Column(idx)` | `(idx, N)` |
/// | `Literal(N) > Column(idx)` | `(idx, N-1)` |
///
/// `Column(idx)` may also be a cast of it accepted by [`ranking_column`].
///
/// # Examples
///
/// - `rn <= 5` → `Some((2, 5))` (assuming rn is column index 2)
/// - `rn < 3` → `Some((2, 2))`
/// - `10 >= rn` → `Some((2, 10))`
/// - `rn = 1` → `None` (equality not supported)
/// - `val <= 5` → `Some((1, 5))` (caller must verify it's a window column)
fn extract_window_limit(
    predicate: &Arc<dyn datafusion_physical_expr::PhysicalExpr>,
) -> Option<(usize, usize)> {
    let binary = predicate.downcast_ref::<BinaryExpr>()?;
    let op = binary.op();
    let left = binary.left();
    let right = binary.right();

    // Try Column op Literal
    if let Some(lit_val) = right.downcast_ref::<Literal>()
        && let Some(col) = ranking_column(left, lit_val.value())
    {
        let n = scalar_to_usize(lit_val.value())?;
        return match *op {
            Operator::LtEq => Some((col.index(), n)),
            Operator::Lt => Some((col.index(), n - 1)),
            _ => None,
        };
    }

    // Try Literal op Column (flipped)
    if let Some(lit_val) = left.downcast_ref::<Literal>()
        && let Some(col) = ranking_column(right, lit_val.value())
    {
        let n = scalar_to_usize(lit_val.value())?;
        return match *op {
            Operator::GtEq => Some((col.index(), n)),
            Operator::Gt => Some((col.index(), n - 1)),
            _ => None,
        };
    }

    None
}

/// Returns the column compared with `limit` by a window limit predicate: a bare
/// [`Column`], or one under a cast to `limit`'s integer type that keeps the
/// rows ranked up to `limit` unchanged and raises no error the pruned plan
/// would hide. The caller only accepts a `ROW_NUMBER`/`RANK`/`DENSE_RANK`
/// output column, whose values are positive `UInt64`s.
///
/// - A null-on-overflow cast ([`TryCastExpr`], or [`CastExpr`] with `safe`
///   options): `limit` fits the target type, so ranks up to it are unchanged
///   and ranks that overflow become NULL, which the filter drops anyway.
/// - A strict cast to `Int32`: Sail's representation of Spark's `IntegerType`
///   ranking column. It can only fail on a rank past `i32::MAX`, which Spark,
///   applying its window group limit for the same filter, never computes.
///
/// Other strict casts are rejected: `CAST(rank AS Int8) <= 2` fails at rank
/// 128, and pruning that row would turn the error into a result.
fn ranking_column<'a>(expr: &'a Arc<dyn PhysicalExpr>, limit: &ScalarValue) -> Option<&'a Column> {
    let (input, cast_type, null_on_overflow) = if let Some(cast) = expr.downcast_ref::<CastExpr>() {
        (cast.expr(), cast.cast_type(), cast.cast_options().safe)
    } else if let Some(cast) = expr.downcast_ref::<TryCastExpr>() {
        (cast.expr(), cast.cast_type(), true)
    } else {
        return expr.downcast_ref::<Column>();
    };
    let transparent = cast_type.is_integer()
        && *cast_type == limit.data_type()
        && (null_on_overflow || *cast_type == DataType::Int32);
    if transparent {
        input.downcast_ref::<Column>()
    } else {
        None
    }
}

/// Convert a [`ScalarValue`] to `usize` if it's a positive integer.
///
/// Returns `None` for null values, zero, negative integers, and
/// non-integer types (floats, strings, decimals, etc.).
fn scalar_to_usize(value: &ScalarValue) -> Option<usize> {
    if !value.data_type().is_integer() {
        return None;
    }
    let casted = value.cast_to(&DataType::UInt64).ok()?;
    match casted {
        ScalarValue::UInt64(Some(v)) if v > 0 => usize::try_from(v).ok(),
        _ => None,
    }
}

/// Identify which supported ranking window function `expr` is.
///
/// Downcasts through `StandardWindowExpr` → `WindowUDFExpr` and compares
/// the UDF with the built-in one. Returns:
/// - `Some(WindowFnKind::RowNumber)` for the built-in `row_number`
/// - `Some(WindowFnKind::Rank)` for the built-in `rank`
/// - `Some(WindowFnKind::DenseRank)` for the built-in `dense_rank`
/// - `None` for everything else
///
/// The comparison is `WindowUDF` equality rather than the name: a user-defined
/// function registered as `row_number` (say) can compute anything, and the
/// rewrite both prunes its input and, with a single window expression, drops
/// its evaluator.
fn supported_window_fn(
    expr: &Arc<dyn datafusion_physical_expr::window::WindowExpr>,
) -> Option<WindowFnKind> {
    let swe = expr.as_any().downcast_ref::<StandardWindowExpr>()?;
    let swfe = swe.get_standard_func_expr();
    let udf = swfe.as_any().downcast_ref::<WindowUDFExpr>()?;
    let fun = udf.fun();
    if fun == &row_number_udwf() {
        Some(WindowFnKind::RowNumber)
    } else if fun == &rank_udwf() {
        Some(WindowFnKind::Rank)
    } else if fun == &dense_rank_udwf() {
        Some(WindowFnKind::DenseRank)
    } else {
        None
    }
}

type PlanAndIntermediates = (Arc<dyn ExecutionPlan>, Vec<Arc<dyn ExecutionPlan>>);

/// Walk below a plan node looking for a [`BoundedWindowAggExec`].
///
/// Handles sequences of `ProjectionExec` and `RepartitionExec`.
/// This is safe because `PartitionedTopKExec` can be pushed below them:
/// projections only rename, drop, reorder or compute over the window's
/// columns (the filter's column is mapped back through them by
/// [`column_index_below`]), and pushing the limit below repartitions is safe
/// because the limit is computed per-partition.
///
/// Returns the window exec and a list of intermediate nodes to rebuild,
/// or `None` if no `BoundedWindowAggExec` is found.
fn find_window_below(plan: &Arc<dyn ExecutionPlan>) -> Option<PlanAndIntermediates> {
    let mut current = Arc::clone(plan);
    let mut intermediates = Vec::new();

    loop {
        if current.downcast_ref::<BoundedWindowAggExec>().is_some() {
            return Some((current, intermediates));
        } else if current.downcast_ref::<ProjectionExec>().is_some()
            || current.downcast_ref::<RepartitionExec>().is_some()
        {
            let next = Arc::clone(current.children().first()?);
            intermediates.push(current);
            current = next;
        } else {
            return None;
        }
    }
}

/// Maps `col_idx`, an index into the output of `intermediates[0]` (the
/// filter's input), to the index of the same column in the output of the
/// node below the last intermediate (the window).
///
/// `intermediates` is top-down, as [`find_window_below`] returns it, so each
/// step reads an index into the node's output and yields one into its input.
/// A `RepartitionExec` keeps its input's schema. A `ProjectionExec` must pass
/// the column through as a bare [`Column`]: one it computes (e.g. `rn + 1`)
/// has no single input column to follow, so this returns `None`.
fn column_index_below(col_idx: usize, intermediates: &[Arc<dyn ExecutionPlan>]) -> Option<usize> {
    intermediates.iter().try_fold(col_idx, |idx, node| {
        if let Some(projection) = node.downcast_ref::<ProjectionExec>() {
            let expr = &projection.expr().get(idx)?.expr;
            Some(expr.downcast_ref::<Column>()?.index())
        } else if node.downcast_ref::<RepartitionExec>().is_some() {
            Some(idx)
        } else {
            None
        }
    })
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::compute::{CastOptions, concat_batches};
    use arrow::datatypes::{Field, Schema};
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::windows::create_window_expr;
    use datafusion::physical_plan::{InputOrderMode, collect, displayable};
    use datafusion_datasource::memory::MemorySourceConfig;
    use datafusion_expr::{WindowFrame, WindowFunctionDefinition};

    use super::*;

    /// `FilterExec(<predicate>)` over `RANK() OVER (PARTITION BY pk ORDER BY v)`,
    /// where the window's output column `rank` is at index 2. The input has
    /// `rows` rows with `pk = 0` and `v = 0, 1, ...`, so `rank` runs from 1.
    fn plan(rows: i32, predicate: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![0; rows as usize])),
                Arc::new(Int32Array::from_iter_values(0..rows)),
            ],
        )?;
        let source = MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;
        let pk: Arc<dyn PhysicalExpr> = Arc::new(Column::new("pk", 0));
        let v = PhysicalSortExpr::new_default(Arc::new(Column::new("v", 1)));
        let ordering = LexOrdering::new(vec![
            PhysicalSortExpr::new_default(Arc::clone(&pk)),
            v.clone(),
        ])
        .ok_or_else(|| datafusion_common::internal_datafusion_err!("empty ordering"))?;
        let input = Arc::new(SortExec::new(ordering, source));
        let rank = create_window_expr(
            &WindowFunctionDefinition::WindowUDF(rank_udwf()),
            "rank".to_string(),
            &[],
            &[pk],
            &[v],
            Arc::new(WindowFrame::new(Some(true))),
            schema,
            false,
            false,
            None,
        )?;
        let window = Arc::new(BoundedWindowAggExec::try_new(
            vec![rank],
            input,
            InputOrderMode::Sorted,
            true,
        )?);
        Ok(Arc::new(FilterExec::try_new(predicate, window)?))
    }

    fn rank() -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new("rank", 2))
    }

    /// A `CastExpr`, strict unless `safe`.
    fn cast(expr: Arc<dyn PhysicalExpr>, cast_type: DataType, safe: bool) -> Arc<dyn PhysicalExpr> {
        let options = CastOptions {
            safe,
            ..Default::default()
        };
        Arc::new(CastExpr::new(expr, cast_type, Some(options)))
    }

    fn compare(
        left: Arc<dyn PhysicalExpr>,
        op: Operator,
        right: Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(left, op, right))
    }

    /// The integer literal `value` of type `data_type`.
    fn literal(value: i64, data_type: &DataType) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Literal::new(
            ScalarValue::Int64(Some(value)).cast_to(data_type)?,
        )))
    }

    fn optimize_plan(plan: &Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let mut config = ConfigOptions::default();
        config.optimizer.enable_window_topn = true;
        let optimized = WindowTopN::new().optimize(Arc::clone(plan), &config)?;
        assert_eq!(plan.schema(), optimized.schema());
        Ok(optimized)
    }

    /// Runs the rule and returns the rewritten plan's display, asserting that
    /// the schema is unchanged.
    fn optimize(plan: Arc<dyn ExecutionPlan>) -> Result<String> {
        let optimized = optimize_plan(&plan)?;
        Ok(displayable(optimized.as_ref()).indent(true).to_string())
    }

    /// Executes `plan` to completion and concatenates its output.
    async fn execute(plan: Arc<dyn ExecutionPlan>) -> Result<RecordBatch> {
        let schema = plan.schema();
        let batches = collect(plan, Arc::new(TaskContext::default())).await?;
        Ok(concat_batches(&schema, &batches)?)
    }

    #[test]
    fn test_window_topn_through_integer_cast() -> Result<()> {
        let int32 = DataType::Int32;
        let mut cases = vec![
            // Sail's ranking column: `CAST(rank AS Int32) <= 2`, `< 3`, `2 >=`, `3 >`
            compare(
                cast(rank(), int32.clone(), false),
                Operator::LtEq,
                literal(2, &int32)?,
            ),
            compare(
                cast(rank(), int32.clone(), false),
                Operator::Lt,
                literal(3, &int32)?,
            ),
            compare(
                literal(2, &int32)?,
                Operator::GtEq,
                cast(rank(), int32.clone(), false),
            ),
            compare(
                literal(3, &int32)?,
                Operator::Gt,
                cast(rank(), int32.clone(), false),
            ),
            // `rank <= 2`
            compare(rank(), Operator::LtEq, literal(2, &DataType::UInt64)?),
        ];
        for t in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ] {
            // Null-on-overflow casts: `CAST(rank AS t, safe) <= 2`, `TRY_CAST(rank AS t) <= 2`
            cases.push(compare(
                cast(rank(), t.clone(), true),
                Operator::LtEq,
                literal(2, &t)?,
            ));
            cases.push(compare(
                Arc::new(TryCastExpr::new(rank(), t.clone())),
                Operator::LtEq,
                literal(2, &t)?,
            ));
        }
        for predicate in cases {
            let actual = optimize(plan(0, predicate)?)?;
            assert!(!actual.contains("FilterExec"), "{actual}");
            assert!(
                actual.contains("PartitionedTopKExec: fn=rank, fetch=2"),
                "{actual}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_window_topn_ignores_unsafe_cast() -> Result<()> {
        let mut cases = vec![];
        // Strict casts other than Sail's `Int32` (e.g. `Int8` fails at rank 128).
        for t in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ] {
            cases.push(compare(
                cast(rank(), t.clone(), false),
                Operator::LtEq,
                literal(2, &t)?,
            ));
            cases.push(compare(
                literal(2, &t)?,
                Operator::GtEq,
                cast(rank(), t, false),
            ));
        }
        let (int8, int32, int64) = (DataType::Int8, DataType::Int32, DataType::Int64);
        cases.extend([
            // Nested casts: `CAST(CAST(rank AS Int32) AS Int8, safe) <= 2`
            compare(
                cast(cast(rank(), int32.clone(), false), int8.clone(), true),
                Operator::LtEq,
                literal(2, &int8)?,
            ),
            // Non-integer casts
            compare(
                cast(rank(), DataType::Float64, true),
                Operator::LtEq,
                Arc::new(Literal::new(ScalarValue::Float64(Some(2.0)))),
            ),
            compare(
                cast(rank(), DataType::Decimal128(20, 0), true),
                Operator::LtEq,
                Arc::new(Literal::new(ScalarValue::Decimal128(Some(2), 20, 0))),
            ),
            // Unsupported comparisons: `= 2`, `< 1`
            compare(
                cast(rank(), int32.clone(), false),
                Operator::Eq,
                literal(2, &int32)?,
            ),
            compare(
                cast(rank(), int32.clone(), false),
                Operator::Lt,
                literal(1, &int32)?,
            ),
        ]);
        // `K` does not fit the cast's type, e.g. `CAST(rank AS Int8, safe) <= 200`.
        // `FilterExec` rejects such ill-typed predicates, so check the matcher.
        for predicate in [
            compare(
                cast(rank(), int8.clone(), true),
                Operator::LtEq,
                literal(200, &DataType::Int16)?,
            ),
            compare(
                Arc::new(TryCastExpr::new(rank(), int8.clone())),
                Operator::LtEq,
                literal(200, &DataType::Int16)?,
            ),
            compare(
                cast(rank(), int32.clone(), false),
                Operator::LtEq,
                literal(2, &int64)?,
            ),
        ] {
            assert_eq!(extract_window_limit(&predicate), None);
        }
        for predicate in cases {
            let actual = optimize(plan(0, predicate)?)?;
            assert!(actual.contains("FilterExec"), "{actual}");
            assert!(!actual.contains("PartitionedTopKExec"), "{actual}");
        }
        Ok(())
    }

    /// With 128 rows in one partition, `rank` reaches 128, which a strict
    /// `Int8` cast cannot hold. The rewrite must neither hide that error nor
    /// change the rows returned through Sail's `Int32` cast.
    #[tokio::test]
    async fn test_window_topn_cast_execution() -> Result<()> {
        let (int8, int32) = (DataType::Int8, DataType::Int32);

        let strict_int8 = plan(
            128,
            compare(
                cast(rank(), int8.clone(), false),
                Operator::LtEq,
                literal(2, &int8)?,
            ),
        )?;
        let result = execute(optimize_plan(&strict_int8)?).await;
        assert!(
            matches!(&result, Err(e) if e.to_string().contains("Can't cast value 128 to type Int8")),
            "{result:?}"
        );

        for predicate in [
            compare(
                cast(rank(), int32.clone(), false),
                Operator::LtEq,
                literal(2, &int32)?,
            ),
            compare(
                cast(rank(), int8.clone(), true),
                Operator::LtEq,
                literal(2, &int8)?,
            ),
        ] {
            let original = plan(128, predicate)?;
            let optimized = optimize_plan(&original)?;
            let display = displayable(optimized.as_ref()).indent(true).to_string();
            assert!(display.contains("PartitionedTopKExec"), "{display}");
            let expected = execute(original).await?;
            assert_eq!(expected.num_rows(), 2);
            assert_eq!(execute(optimized).await?, expected);
        }
        Ok(())
    }
}
