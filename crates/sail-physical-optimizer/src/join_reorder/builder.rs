use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{JoinType, Operator};
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use log::trace;

use datafusion::common::stats::Precision;
use datafusion_physical_expr::intervals::utils::check_support;
use datafusion_physical_expr::{analyze, AnalysisContext};

use crate::join_reorder::graph::{JoinEdge, QueryGraph, RelationNode, StableColumn};
use crate::join_reorder::join_set::JoinSet;

type PhysicalExprRef = Arc<dyn PhysicalExpr>;
type EquiPair = (StableColumn, StableColumn);
type PhysicalExprWithEquiPairs = (PhysicalExprRef, Vec<EquiPair>);
type GroupedPredicates = HashMap<JoinSet, Vec<PhysicalExprWithEquiPairs>>;

/// Maps an output column from an ExecutionPlan back to a stable identifier.
/// The vector is indexed by the column index in the plan's output schema.
pub type ColumnMap = Vec<ColumnMapEntry>;

/// Represents how a column is derived.
#[derive(Debug, Clone)]
pub enum ColumnMapEntry {
    /// The column is a direct reference to a column from a base relation.
    Stable {
        relation_id: usize,
        column_index: usize,
    },
    /// The column is a derived expression (e.g., a + b, or a literal).
    /// We need to store the expression itself and its input context to reconstruct it later.
    Expression {
        expr: Arc<dyn PhysicalExpr>,
        /// This map describes the input schema for the expression `expr`.
        input_map: ColumnMap,
    },
}

/// Builder for constructing query graph from ExecutionPlan.
pub struct GraphBuilder {
    /// The query graph being built.
    graph: QueryGraph,
    /// Counter for assigning unique relation IDs.
    relation_counter: usize,
    /// Maps original PhysicalExprs (specifically Columns) to their stable IDs.
    /// This helps resolve join conditions that reference columns by their expression object.
    /// Key: A Column expression (which is hashable). Value: Stable ID.
    expr_to_stable_id: HashMap<Column, (usize, usize)>,
}

impl GraphBuilder {
    pub fn new() -> Self {
        Self {
            graph: QueryGraph::new(),
            relation_counter: 0,
            expr_to_stable_id: HashMap::new(),
        }
    }

    /// Build query graph from the given execution plan.
    /// Returns None if the plan contains no reorderable joins.
    /// Returns (QueryGraph, ColumnMap) where ColumnMap represents the original plan's output columns.
    pub fn build(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Option<(QueryGraph, ColumnMap)>> {
        trace!("Building query graph from execution plan");
        // Attempt to build a graph starting from this node.
        // visit_plan will return an error if the node is not part of a reorderable region.
        let result = self.visit_plan(plan);

        if let Ok(original_column_map) = result {
            // A graph was successfully built.
            // Check if the built graph is worth reordering (at least 2 relations needed for reordering)
            if self.graph.relation_count() >= 2 {
                Ok(Some((self.graph.clone(), original_column_map)))
            } else {
                // If too few relations, no need to reorder, return None
                Ok(None)
            }
        } else {
            // This node is not the start of a reorderable join chain.
            Ok(None)
        }
    }

    /// Recursively traverses the execution plan, building the query graph.
    /// Returns a map of the plan's output columns to our stable IDs.
    fn visit_plan(&mut self, plan: Arc<dyn ExecutionPlan>) -> Result<ColumnMap> {
        trace!("Visiting plan: {}", plan.name());
        let any_plan = plan.as_any();

        // TODO: Extend to support `SortMergeJoinExec`.
        if let Some(join_plan) = any_plan.downcast_ref::<HashJoinExec>() {
            if join_plan.join_type() == &JoinType::Inner {
                trace!("Visiting inner join: {}", join_plan.name());
                return self.visit_inner_join(join_plan);
            } else {
                trace!(
                    "Skipping non-inner join ({:?}): {}",
                    join_plan.join_type(),
                    join_plan.name()
                );
            }
        }

        if let Some(proj_plan) = any_plan.downcast_ref::<ProjectionExec>() {
            trace!("Visiting projection: {}", proj_plan.name());
            return self.visit_projection(proj_plan);
        }

        // If it's not a reorderable join or a projection we can see through,
        // it's either a boundary (leaf of our graph) or not part of a reorderable region.
        // AggregateExec and other transformation nodes are not part of reorderable regions.

        if any_plan.is::<AggregateExec>() {
            trace!("AggregateExec encountered - not part of reorderable region");
            return Err(DataFusionError::Internal(
                "AggregateExec is not part of a reorderable join region".to_string(),
            ));
        }

        // Treat any other node type as a potential leaf for the query graph.
        // This includes DataSourceExec, non-inner joins, etc.
        // The key is that we *stop* descending here.
        self.visit_boundary_or_leaf(plan)
    }

    /// This function is called when a node is a boundary of the reorderable region.
    /// It creates a `RelationNode` for this plan and stops further recursion.
    fn visit_boundary_or_leaf(&mut self, plan: Arc<dyn ExecutionPlan>) -> Result<ColumnMap> {
        // Boundary nodes are treated as single relations in the query graph.
        self.create_relation_node(plan)
    }

    fn visit_inner_join(&mut self, join_plan: &HashJoinExec) -> Result<ColumnMap> {
        // Recursively build the graph from both children.
        // This continues building the join chain or hits a boundary to create a RelationNode.
        let left_map = self.visit_plan(join_plan.left().clone())?;
        let right_map = self.visit_plan(join_plan.right().clone())?;

        // Build join predicates and add edges.
        //
        // Note: avoid creating a single hyperedge that unions unrelated binary predicates
        // (e.g. a join node with ON conditions that touch different base table pairs). Instead,
        // split AND-conjuncts by their base-relation dependencies and create one edge per group.

        // Collect conjunct predicates originating from join `on` conditions.
        // These are equi-join predicates by construction (HashJoin keys), and we require that
        // each side resolves to a single base StableColumn so we can reconstruct HashJoinExec.
        let mut conjuncts: Vec<PhysicalExprWithEquiPairs> = Vec::new();

        for (left_on, right_on) in join_plan.on() {
            let left_stable_col = self
                .resolve_to_single_stable_col(left_on, &left_map)?
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "JoinReorder: join key is not a simple column reference".to_string(),
                    )
                })?;
            let right_stable_col = self
                .resolve_to_single_stable_col(right_on, &right_map)?
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "JoinReorder: join key is not a simple column reference".to_string(),
                    )
                })?;

            // Represent the equality in stable-name form so downstream dependency analysis and
            // join-filter rewriting can work without relying on transient schema names.
            let l: PhysicalExprRef = Arc::new(Column::new(&left_stable_col.name, 0));
            let r: PhysicalExprRef = Arc::new(Column::new(&right_stable_col.name, 0));
            let eq = Arc::new(BinaryExpr::new(l, Operator::Eq, r)) as PhysicalExprRef;

            conjuncts.push((eq, vec![(left_stable_col, right_stable_col)]));
        }

        // Collect conjunct predicates from join filter (non-equi predicates / residuals).
        if let Some(join_filter) = join_plan.filter() {
            // Rewrite join filter to stable column names.
            let extra = self.rewrite_join_filter_to_stable(
                join_plan,
                join_filter.expression(),
                &left_map,
                &right_map,
            )?;

            for c in Self::decompose_conjuncts(&extra) {
                conjuncts.push((c, vec![]));
            }
        }

        // Group conjuncts by their base-relation dependency set and add one edge per group.
        //
        // This avoids generating hyperedges for join nodes where each conjunct is actually
        // a binary predicate between two base relations.
        let mut grouped: GroupedPredicates = HashMap::new();

        for (pred, pairs) in conjuncts {
            let deps = self.relations_for_expr(&pred, &left_map, &right_map)?;

            // NOTE: If a predicate depends on <2 base relations, keep it associated with the full
            // join node so it is not lost (it may be a single-side residual predicate).
            let deps = if deps.cardinality() < 2 {
                self.all_relations_in_maps(&left_map, &right_map)?
            } else {
                deps
            };

            grouped.entry(deps).or_default().push((pred, pairs));
        }

        for (join_set, preds) in grouped {
            let (filter, equi_pairs) = Self::combine_predicates(preds)?;
            let edge = JoinEdge::new(join_set, filter, *join_plan.join_type(), equi_pairs);
            self.graph.add_edge(edge)?;
        }

        // Build and return the output ColumnMap for current Join node
        // Inner Join output is concatenation of left and right child outputs
        let mut output_map = left_map;
        output_map.extend(right_map);
        // A projection may be embedded into HashJoinExec; apply it so ColumnMap matches
        // the actual join output schema (reorder/drop columns).
        if let Some(projection) = join_plan.projection.as_ref() {
            let mut projected: ColumnMap = Vec::with_capacity(projection.len());
            for &idx in projection.iter() {
                projected.push(output_map.get(idx).cloned().ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "HashJoinExec projection index {} out of bounds (len {})",
                        idx,
                        output_map.len()
                    ))
                })?);
            }
            output_map = projected;
        }
        Ok(output_map)
    }

    /// Parse stable column name like "R{rel}.C{col}" -> (rel, col)
    fn parse_stable_name(name: &str) -> Option<(usize, usize)> {
        if !name.starts_with('R') {
            return None;
        }
        let dot = name.find('.')?;
        let rel_str = &name[1..dot];
        if !name[dot + 1..].starts_with('C') {
            return None;
        }
        let col_str = &name[dot + 2..];
        let rel = rel_str.parse::<usize>().ok()?;
        let col = col_str.parse::<usize>().ok()?;
        Some((rel, col))
    }

    fn decompose_conjuncts(expr: &Arc<dyn PhysicalExpr>) -> Vec<Arc<dyn PhysicalExpr>> {
        let mut result = Vec::new();
        if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>() {
            match binary.op() {
                Operator::And => {
                    result.extend(Self::decompose_conjuncts(binary.left()));
                    result.extend(Self::decompose_conjuncts(binary.right()));
                }
                _ => result.push(Arc::clone(expr)),
            }
        } else {
            result.push(Arc::clone(expr));
        }
        result
    }

    /// Return the set of base relations referenced by `expr`.
    ///
    /// The expression can refer to:
    /// - stable names ("R{rel}.C{col}") emitted by join-filter rewriting, or
    /// - Column indices into either `left_map` or `right_map` (for expressions that have not
    ///   been rewritten yet).
    fn relations_for_expr(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        left_map: &ColumnMap,
        right_map: &ColumnMap,
    ) -> Result<JoinSet> {
        let cols = collect_columns(expr);
        let mut bits: u64 = 0;

        for c in &cols {
            // Prefer parsing stable names if present.
            if let Some((rel, _cidx)) = Self::parse_stable_name(c.name()) {
                bits |= 1u64 << rel;
                continue;
            }

            // Otherwise, fall back to column index mapping. The same expression can exist in
            // either the left or the right join input; if present in both, it's ambiguous.
            let l = left_map.get(c.index());
            let r = right_map.get(c.index());

            match (l, r) {
                (Some(entry), None) => self.add_relation_bits_from_entry(entry, &mut bits)?,
                (None, Some(entry)) => self.add_relation_bits_from_entry(entry, &mut bits)?,
                (Some(_), Some(_)) => {
                    return Err(DataFusionError::Internal(format!(
                        "JoinReorder: ambiguous column index {} found in both left and right maps while analyzing predicate dependencies",
                        c.index()
                    )));
                }
                (None, None) => {
                    return Err(DataFusionError::Internal(format!(
                        "JoinReorder: column index {} out of bounds for both left_map (len {}) and right_map (len {})",
                        c.index(),
                        left_map.len(),
                        right_map.len()
                    )));
                }
            }
        }

        Ok(JoinSet::from_bits(bits))
    }

    fn add_relation_bits_from_entry(&self, entry: &ColumnMapEntry, bits: &mut u64) -> Result<()> {
        match entry {
            ColumnMapEntry::Stable { relation_id, .. } => {
                *bits |= 1u64 << *relation_id;
                Ok(())
            }
            ColumnMapEntry::Expression { expr, input_map } => {
                // Conservative: if a predicate references a derived column, analyze its base
                // dependencies so we can still attach the predicate at the right time.
                self.add_relation_bits_from_expr(expr, input_map, bits)
            }
        }
    }

    fn add_relation_bits_from_expr(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        column_map: &ColumnMap,
        bits: &mut u64,
    ) -> Result<()> {
        let cols = collect_columns(expr);
        for c in &cols {
            if let Some((rel, _)) = Self::parse_stable_name(c.name()) {
                *bits |= 1u64 << rel;
                continue;
            }

            let entry = column_map.get(c.index()).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "JoinReorder: expression column index {} out of bounds (len {}) while analyzing predicate dependencies",
                    c.index(),
                    column_map.len()
                ))
            })?;

            self.add_relation_bits_from_entry(entry, bits)?;
        }
        Ok(())
    }

    /// Return all base relations present in either column map.
    fn all_relations_in_maps(
        &self,
        left_map: &ColumnMap,
        right_map: &ColumnMap,
    ) -> Result<JoinSet> {
        let mut bits: u64 = 0;
        for e in left_map.iter().chain(right_map.iter()) {
            self.add_relation_bits_from_entry(e, &mut bits)?;
        }
        Ok(JoinSet::from_bits(bits))
    }

    fn combine_predicates(
        preds: Vec<PhysicalExprWithEquiPairs>,
    ) -> Result<(PhysicalExprRef, Vec<EquiPair>)> {
        if preds.is_empty() {
            return Err(DataFusionError::Internal(
                "JoinReorder: cannot combine empty predicate list".to_string(),
            ));
        }

        let mut filter = preds[0].0.clone();
        let mut equi_pairs: Vec<EquiPair> = Vec::new();
        equi_pairs.extend_from_slice(&preds[0].1);

        for (pred, pairs) in preds.into_iter().skip(1) {
            filter = Arc::new(BinaryExpr::new(filter, Operator::And, pred)) as PhysicalExprRef;
            equi_pairs.extend(pairs);
        }

        Ok((filter, equi_pairs))
    }

    /// Rewrite a HashJoin's JoinFilter expression so that any Column references are
    /// converted to stable names based on base relations: "R{relation_id}.C{column_index}".
    /// This avoids depending on transient projection aliases like "#37" and local indices.
    fn rewrite_join_filter_to_stable(
        &self,
        join_plan: &HashJoinExec,
        expr: &Arc<dyn PhysicalExpr>,
        left_map: &ColumnMap,
        right_map: &ColumnMap,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if join_plan.filter().is_none() {
            return Ok(Arc::clone(expr));
        }

        let filter = join_plan.filter().ok_or_else(|| {
            DataFusionError::Internal(
                "Filter should exist when join_plan.filter() is not None".to_string(),
            )
        })?;
        let indices = filter.column_indices();

        let expr_arc = Arc::clone(expr);
        let transformed = expr_arc.transform(|node| {
            if let Some(col) = node.as_any().downcast_ref::<Column>() {
                let i = col.index();
                // Safeguard: if index out of bounds, return error
                if i >= indices.len() {
                    return Err(DataFusionError::Internal(
                        "Column index out of bounds in join filter rewrite".to_string(),
                    ));
                }
                let ci = &indices[i];
                // Determine which stable column this refers to
                let stable_entry_opt = match ci.side {
                    datafusion::common::JoinSide::Left => left_map.get(ci.index),
                    datafusion::common::JoinSide::Right => right_map.get(ci.index),
                    _ => None,
                };

                if let Some(ColumnMapEntry::Stable {
                    relation_id,
                    column_index,
                }) = stable_entry_opt.cloned()
                {
                    // Build a stable name like R{relation_id}.C{column_index}
                    // TODO: Consider implement PhysicalExpr trait for StableColumn.
                    let stable_name = format!("R{}.C{}", relation_id, column_index);
                    // TODO: Reconstructor will retarget indices to its compact schema
                    let new_col = Column::new(&stable_name, 0);
                    return Ok(Transformed::yes(Arc::new(new_col) as Arc<dyn PhysicalExpr>));
                }
            }
            Ok(Transformed::no(node))
        })?;

        Ok(transformed.data)
    }

    fn visit_projection(&mut self, proj_plan: &ProjectionExec) -> Result<ColumnMap> {
        // Recursively visit child node
        let input_map = self.visit_plan(proj_plan.input().clone())?;

        // Build output ColumnMap for current projection node
        let mut output_map = Vec::with_capacity(proj_plan.expr().len());
        for proj_expr in proj_plan.expr() {
            let expr = &proj_expr.expr;
            // Try to parse expression directly as a single stable column
            if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                // This is a simple column reference, like `SELECT a FROM ...`
                // `col.index()` is its index in the input Schema
                let entry = input_map.get(col.index()).cloned().ok_or_else(|| {
                    DataFusionError::Internal("Projection column index out of bounds".to_string())
                })?;
                output_map.push(entry);
            } else {
                // This is a complex expression, like `SELECT a + 1 FROM ...`
                // We cannot map it back to a single stable column, so save the entire expression
                // and its input context (input_map).
                output_map.push(ColumnMapEntry::Expression {
                    expr: expr.clone(),
                    input_map: input_map.clone(),
                });
            }
        }

        Ok(output_map)
    }

    fn create_relation_node(&mut self, plan: Arc<dyn ExecutionPlan>) -> Result<ColumnMap> {
        // Assign new relation_id
        let relation_id = self.relation_counter;
        self.relation_counter += 1;

        // Estimate initial cardinality and choose statistics for downstream estimation.
        //
        // Special-case FilterExec: treat it as a boundary leaf (same as before), but pull
        // statistics from its *input* (pre-filter) so we retain the most original/accurate
        // datasource stats (e.g., Parquet), and apply the filter's selectivity as a penalty
        // factor to initial cardinality.
        let (stats, initial_cardinality) = if plan.as_any().is::<FilterExec>() {
            // NOTE: We still keep FilterExec as the boundary leaf (strategy A), but we must avoid
            // an inconsistent stats state where num_rows is "post-filter" while distinct_count
            // (and thus TDom) remains "pre-filter". That mismatch can cause greedy join ordering
            // to prefer catastrophic NLJs.
            //
            // We therefore:
            // - read base (pre-filter) datasource statistics from beneath the filter chain, and
            // - estimate a selectivity factor for the filter predicate(s), and
            // - apply that selectivity to num_rows / total_byte_size (inexact) while preserving
            //   base column stats as a best-effort proxy for join planning.
            let (pre_filter_plan, selectivity) =
                self.peel_filter_chain_and_estimate_selectivity(plan.clone())?;
            let pre_stats = pre_filter_plan.partition_statistics(None)?;
            let base = match pre_stats.num_rows {
                Precision::Exact(count) => count as f64,
                Precision::Inexact(count) => count as f64,
                Precision::Absent => 1000.0,
            };

            let mut adjusted = pre_stats.to_inexact();
            adjusted.num_rows = adjusted.num_rows.with_estimated_selectivity(selectivity);
            adjusted.total_byte_size = adjusted
                .total_byte_size
                .with_estimated_selectivity(selectivity);

            (adjusted, base * selectivity)
        } else {
            let stats = plan.partition_statistics(None)?;
            let initial_cardinality = match stats.num_rows {
                Precision::Exact(count) => count as f64,
                Precision::Inexact(count) => count as f64,
                Precision::Absent => 1000.0, // Default estimation
            };
            (stats, initial_cardinality)
        };

        let relation_node =
            RelationNode::new(plan.clone(), relation_id, initial_cardinality, stats);
        self.graph.add_relation(relation_node);

        // Create stable IDs for all output columns of this new relation and build ColumnMap
        let mut output_map = Vec::with_capacity(plan.schema().fields().len());
        for i in 0..plan.schema().fields().len() {
            let entry = ColumnMapEntry::Stable {
                relation_id,
                column_index: i,
            };
            output_map.push(entry);

            // Update expr_to_stable_id mapping for subsequent join condition resolution
            let col_expr = Column::new(plan.schema().field(i).name(), i);
            self.expr_to_stable_id.insert(col_expr, (relation_id, i));
        }

        Ok(output_map)
    }

    /// If `plan` is a FilterExec (possibly a chain of stacked FilterExecs), returns:
    /// - the first non-Filter child plan (the "pre-filter" plan), and
    /// - a multiplicative selectivity factor estimated from filter predicates.
    ///
    /// This is used to keep base datasource statistics (e.g., Parquet) while still
    /// penalizing cardinality for table/subplan filters.
    fn peel_filter_chain_and_estimate_selectivity(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<(Arc<dyn ExecutionPlan>, f64)> {
        let mut cur = plan;
        let mut selectivity: f64 = 1.0;

        while let Some(filter) = cur.as_any().downcast_ref::<FilterExec>() {
            let input = filter.input().clone();
            let input_stats = input.partition_statistics(None)?;
            let input_schema = input.schema();

            let sel = self.estimate_filter_selectivity(
                filter.predicate(),
                &input_schema,
                &input_stats,
                filter.default_selectivity(),
            );
            selectivity *= sel;
            cur = input;
        }

        Ok((cur, selectivity.clamp(0.0, 1.0)))
    }

    /// Estimate selectivity for a Filter predicate using DataFusion's interval analysis when
    /// possible, falling back to FilterExec's default selectivity otherwise.
    fn estimate_filter_selectivity(
        &self,
        predicate: &Arc<dyn PhysicalExpr>,
        schema: &datafusion::arrow::datatypes::SchemaRef,
        input_stats: &datafusion::common::Statistics,
        default_selectivity: u8,
    ) -> f64 {
        // Default: `FilterExec.default_selectivity` is expressed as percent [0, 100].
        //
        // In practice this can be configured to 100 (no reduction), which is too optimistic
        // for join ordering when the predicate is selective but cannot be analyzed.
        // Cap our fallback to a conservative upper bound.
        let configured = (default_selectivity as f64 / 100.0).clamp(0.0, 1.0);
        // If configured to 100% ("no filtering"), use a conservative fallback when we can't
        // estimate from stats to avoid catastrophic join orderings.
        let fallback = if configured >= 0.999 { 0.2 } else { configured };

        // First try a small set of cheap, deterministic heuristics that work well for common
        // predicates (e.g. col = literal, range filters, conjunctions).
        if let Some(sel) = self.estimate_selectivity_from_stats(predicate, schema, input_stats) {
            return sel.clamp(0.0, 1.0);
        }

        if !check_support(predicate, schema) {
            return fallback;
        }

        // Best effort: analyze predicate to derive a selectivity from column statistics.
        let Ok(input_ctx) =
            AnalysisContext::try_from_statistics(schema, &input_stats.column_statistics)
        else {
            return fallback;
        };

        let Ok(ctx) = analyze(predicate, input_ctx, schema) else {
            return fallback;
        };

        ctx.selectivity.unwrap_or(fallback).clamp(0.0, 1.0)
    }

    fn estimate_selectivity_from_stats(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        schema: &datafusion::arrow::datatypes::SchemaRef,
        input_stats: &datafusion::common::Statistics,
    ) -> Option<f64> {
        let bin = expr.as_any().downcast_ref::<BinaryExpr>()?;
        match bin.op() {
            Operator::And => {
                let l = self.estimate_selectivity_from_stats(bin.left(), schema, input_stats)?;
                let r = self.estimate_selectivity_from_stats(bin.right(), schema, input_stats)?;
                Some((l * r).clamp(0.0, 1.0))
            }
            Operator::Or => {
                let l = self.estimate_selectivity_from_stats(bin.left(), schema, input_stats)?;
                let r = self.estimate_selectivity_from_stats(bin.right(), schema, input_stats)?;
                // Independence assumption: P(A ∪ B) = P(A) + P(B) - P(A)P(B)
                Some((l + r - l * r).clamp(0.0, 1.0))
            }
            Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
                self.estimate_binary_selectivity_from_stats(bin, schema, input_stats)
            }
            _ => None,
        }
    }

    fn estimate_binary_selectivity_from_stats(
        &self,
        bin: &BinaryExpr,
        schema: &datafusion::arrow::datatypes::SchemaRef,
        input_stats: &datafusion::common::Statistics,
    ) -> Option<f64> {
        use datafusion::common::ScalarValue;
        use datafusion::physical_expr::expressions::Literal;

        // Normalize into (Column, Literal) if possible.
        let (col, lit) = if let (Some(c), Some(l)) = (
            bin.left().as_any().downcast_ref::<Column>(),
            bin.right().as_any().downcast_ref::<Literal>(),
        ) {
            (c, l)
        } else if let (Some(l), Some(c)) = (
            bin.left().as_any().downcast_ref::<Literal>(),
            bin.right().as_any().downcast_ref::<Column>(),
        ) {
            // Flip operator direction if needed.
            // For Eq it's symmetric; for inequalities we can invert.
            let flipped_op = match bin.op() {
                Operator::Lt => Operator::Gt,
                Operator::LtEq => Operator::GtEq,
                Operator::Gt => Operator::Lt,
                Operator::GtEq => Operator::LtEq,
                Operator::Eq => Operator::Eq,
                _ => return None,
            };
            let tmp = BinaryExpr::new(
                Arc::new(Column::new(c.name(), c.index())),
                flipped_op,
                Arc::new(Literal::new(l.value().clone())),
            );
            return self.estimate_binary_selectivity_from_stats(&tmp, schema, input_stats);
        } else {
            return None;
        };

        let col_idx = schema.index_of(col.name()).ok()?;
        let stats = input_stats.column_statistics.get(col_idx)?;

        // Prefer distinct_count for equality predicates.
        if bin.op() == &Operator::Eq {
            let ndv = match stats.distinct_count {
                Precision::Exact(v) => v as f64,
                Precision::Inexact(v) => v as f64,
                Precision::Absent => 0.0,
            };
            if ndv.is_finite() && ndv > 0.0 {
                return Some((1.0 / ndv).clamp(0.0, 1.0));
            }
        }

        // Best-effort range reasoning from min/max for numeric scalars.
        let (min, max) = match (&stats.min_value, &stats.max_value) {
            (Precision::Exact(min), Precision::Exact(max))
            | (Precision::Inexact(min), Precision::Inexact(max))
            | (Precision::Exact(min), Precision::Inexact(max))
            | (Precision::Inexact(min), Precision::Exact(max)) => (min, max),
            _ => return None,
        };

        // Only implement a small int range model.
        let (min, max, v) = match (min, max, lit.value()) {
            (
                ScalarValue::Int32(Some(min)),
                ScalarValue::Int32(Some(max)),
                ScalarValue::Int32(Some(v)),
            ) => (*min as f64, *max as f64, *v as f64),
            (
                ScalarValue::Int64(Some(min)),
                ScalarValue::Int64(Some(max)),
                ScalarValue::Int64(Some(v)),
            ) => (*min as f64, *max as f64, *v as f64),
            (
                ScalarValue::UInt32(Some(min)),
                ScalarValue::UInt32(Some(max)),
                ScalarValue::UInt32(Some(v)),
            ) => (*min as f64, *max as f64, *v as f64),
            (
                ScalarValue::UInt64(Some(min)),
                ScalarValue::UInt64(Some(max)),
                ScalarValue::UInt64(Some(v)),
            ) => (*min as f64, *max as f64, *v as f64),
            _ => return None,
        };

        if !(min.is_finite() && max.is_finite() && v.is_finite()) || max < min {
            return None;
        }

        let width = (max - min + 1.0).max(1.0);
        let frac = match bin.op() {
            Operator::Eq => {
                if v < min || v > max {
                    0.0
                } else {
                    1.0 / width
                }
            }
            Operator::Lt => ((v - min) / width).clamp(0.0, 1.0),
            Operator::LtEq => (((v - min) + 1.0) / width).clamp(0.0, 1.0),
            Operator::Gt => ((max - v) / width).clamp(0.0, 1.0),
            Operator::GtEq => (((max - v) + 1.0) / width).clamp(0.0, 1.0),
            _ => return None,
        };

        Some(frac.clamp(0.0, 1.0))
    }

    /// Helper function to resolve an expression to the set of relation IDs it references.
    /// Traverses the expression tree to find all underlying Stable columns.
    /// Helper function to resolve an expression to a single StableColumn if possible.
    /// Returns None if the expression is not a simple column reference.
    fn resolve_to_single_stable_col(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        column_map: &ColumnMap,
    ) -> Result<Option<StableColumn>> {
        if let Some(col) = expr.as_any().downcast_ref::<Column>() {
            // This is a direct column reference
            if let Some(entry) = column_map.get(col.index()) {
                match entry {
                    ColumnMapEntry::Stable {
                        relation_id,
                        column_index,
                    } => {
                        return Ok(Some(StableColumn {
                            relation_id: *relation_id,
                            column_index: *column_index,
                            // Use a stable, parseable name that uniquely identifies the base column.
                            name: format!("R{}.C{}", relation_id, column_index),
                        }));
                    }
                    ColumnMapEntry::Expression { .. } => {
                        // This column comes from a complex expression
                        return Ok(None);
                    }
                }
            }
        }
        // For complex expressions, return None
        Ok(None)
    }
}

impl Default for GraphBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::ScalarValue;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::joins::HashJoinExec;
    use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;

    use super::*;

    #[test]
    fn test_graph_builder_creation() {
        let builder = GraphBuilder::new();
        assert_eq!(builder.relation_counter, 0);
        assert!(builder.graph.relations.is_empty() && builder.graph.edges.is_empty());
    }

    #[test]
    fn test_build_with_simple_plan() -> Result<()> {
        let mut builder = GraphBuilder::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));
        let plan = Arc::new(EmptyExec::new(schema));

        let result = builder.build(plan)?;
        // Simple plan contains no joins, returns None
        // With the new top-down approach, single relations are still built but not returned as reorderable
        assert!(result.is_none());

        Ok(())
    }

    #[test]
    fn test_build_with_single_relation() -> Result<()> {
        let mut builder = GraphBuilder::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let plan = Arc::new(EmptyExec::new(schema));

        let result = builder.build(plan)?;
        // Single relation returns None (no joins to reorder)
        assert!(result.is_none());

        Ok(())
    }

    #[test]
    fn test_visit_plan_identifies_base_relations() {
        let mut builder = GraphBuilder::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));
        let plan = Arc::new(EmptyExec::new(schema));

        // EmptyExec is handled as a base relation in visit_plan

        // Test the visit_plan method creates a relation node
        let column_map = match builder.visit_plan(plan) {
            Ok(map) => map,
            Err(_) => unreachable!("visit_plan should succeed in test"),
        };
        assert_eq!(column_map.len(), 1);
        assert_eq!(builder.graph.relation_count(), 1);

        // Verify the column map entry
        match &column_map[0] {
            ColumnMapEntry::Stable {
                relation_id,
                column_index,
            } => {
                assert_eq!(*relation_id, 0);
                assert_eq!(*column_index, 0);
            }
            _ => unreachable!("Expected Stable column map entry"),
        }
    }

    #[test]
    fn test_visit_inner_join_simple() -> Result<()> {
        use datafusion::common::NullEquality;
        use datafusion::physical_plan::joins::PartitionMode;

        let mut builder = GraphBuilder::new();

        // Create two base relations
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let left_plan = Arc::new(EmptyExec::new(schema1.clone()));
        let right_plan = Arc::new(EmptyExec::new(schema2.clone()));

        // Create join conditions (id = id)
        let left_col = Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
        let right_col = Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
        let on_conditions = vec![(left_col.clone(), right_col.clone())];

        // Create HashJoinExec
        let join_plan = Arc::new(HashJoinExec::try_new(
            left_plan,
            right_plan,
            on_conditions,
            None, // No filter initially
            &JoinType::Inner,
            None, // projection
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
        )?);

        // Test that visit_inner_join correctly handles HashJoinExec
        let result = builder.visit_inner_join(&join_plan);
        assert!(result.is_ok());

        // Check that an edge was created
        assert_eq!(builder.graph.edges.len(), 1);
        // Check that two relations were created (left and right)
        assert_eq!(builder.graph.relation_count(), 2);

        Ok(())
    }

    #[test]
    fn test_filter_leaf_adjusts_statistics_and_penalizes_cardinality() -> Result<()> {
        // Build an input plan with exact row count statistics.
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let input = Arc::new(PlaceholderRowExec::new(schema));

        // Use a predicate shape that the interval analysis doesn't support so we deterministically
        // fall back to `default_selectivity`:
        //
        // (a > 0) OR (a < 0)
        let a = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let gt0 = Arc::new(BinaryExpr::new(
            a.clone(),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
        )) as Arc<dyn PhysicalExpr>;
        let lt0 = Arc::new(BinaryExpr::new(
            a,
            Operator::Lt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
        )) as Arc<dyn PhysicalExpr>;
        let pred: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(gt0, Operator::Or, lt0));

        let filter = FilterExec::try_new(pred, input)?.with_default_selectivity(50)?;
        let filter: Arc<dyn ExecutionPlan> = Arc::new(filter);

        let mut builder = GraphBuilder::new();
        let _ = builder.visit_plan(filter.clone())?;

        assert_eq!(builder.graph.relation_count(), 1);

        let rel = &builder.graph.relations[0];
        // Keep the boundary plan as FilterExec (structural behavior unchanged for strategy A).
        assert_eq!(rel.plan.name(), "FilterExec");

        // Statistics should reflect the estimated selectivity.
        match rel.statistics.num_rows {
            datafusion::common::stats::Precision::Inexact(n) => assert_eq!(n, 1),
            other => panic!("expected Inexact(1) filtered stats, got {other:?}"),
        }

        // Penalize initial cardinality by default_selectivity (50% here).
        assert!(
            (rel.initial_cardinality - 0.5).abs() < 1e-9,
            "expected initial_cardinality ~= 0.5, got {}",
            rel.initial_cardinality
        );

        Ok(())
    }

    #[test]
    fn test_penetrate_complex_plan_structure() -> Result<()> {
        use datafusion::common::NullEquality;
        use datafusion::physical_plan::joins::PartitionMode;

        let mut builder = GraphBuilder::new();

        // Create three base relations for complex join testing
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let schema3 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("category", DataType::Utf8, false),
        ]));

        let table1 = Arc::new(EmptyExec::new(schema1.clone()));
        let table2 = Arc::new(EmptyExec::new(schema2.clone()));
        let table3 = Arc::new(EmptyExec::new(schema3.clone()));

        // Create first join: table1 ⋈ table2
        let left_col1 = Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
        let right_col1 = Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
        let on_conditions1 = vec![(left_col1, right_col1)];

        let join1 = Arc::new(HashJoinExec::try_new(
            table1,
            table2,
            on_conditions1,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
        )?);

        // Create second join: (table1 ⋈ table2) ⋈ table3
        let left_col2 = Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
        let right_col2 = Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
        let on_conditions2 = vec![(left_col2, right_col2)];

        let join2 = Arc::new(HashJoinExec::try_new(
            join1,
            table3,
            on_conditions2,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
        )?);

        // Test that our enhanced visit_plan can find the joins directly
        // With the new top-down approach, we test the join structure directly
        let result = builder.build(join2)?;

        // Should find the joins and return a graph with multiple relations
        assert!(
            result.is_some(),
            "Should find reorderable joins in complex plan"
        );

        let (graph, _column_map) = match result {
            Some(result) => result,
            None => unreachable!("Should have reorderable joins in test"),
        };

        // Should have 3 relations (table1, table2, table3)
        assert_eq!(graph.relation_count(), 3, "Should find 3 base relations");

        // Should have 2 join edges
        assert_eq!(graph.edges.len(), 2, "Should find 2 join edges");

        Ok(())
    }

    #[test]
    fn test_builder_splits_multi_pair_join_into_binary_edges() -> Result<()> {
        use datafusion::common::NullEquality;
        use datafusion::physical_plan::joins::PartitionMode;

        let mut builder = GraphBuilder::new();

        // Each table has a single column "id" to keep join key construction simple.
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let t1 = Arc::new(EmptyExec::new(schema.clone()));
        let t2 = Arc::new(EmptyExec::new(schema.clone()));
        let t3 = Arc::new(EmptyExec::new(schema.clone()));
        let t4 = Arc::new(EmptyExec::new(schema.clone()));

        // Join t1 ⋈ t2 on id = id
        let join12 = Arc::new(HashJoinExec::try_new(
            t1,
            t2,
            vec![(
                Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>,
                Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>,
            )],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
        )?);

        // Join t3 ⋈ t4 on id = id
        let join34 = Arc::new(HashJoinExec::try_new(
            t3,
            t4,
            vec![(
                Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>,
                Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>,
            )],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
        )?);

        // Root join: (t1 ⋈ t2) ⋈ (t3 ⋈ t4) with two independent equi predicates:
        // - t1.id = t3.id  (left index 0, right index 0)
        // - t2.id = t4.id  (left index 1, right index 1)
        //
        // GraphBuilder should emit two *binary* edges ({t1,t3} and {t2,t4}),
        // not a single hyperedge {t1,t2,t3,t4}.
        let root = Arc::new(HashJoinExec::try_new(
            join12,
            join34,
            vec![
                (
                    Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>,
                    Arc::new(Column::new("id", 0)) as Arc<dyn PhysicalExpr>,
                ),
                (
                    Arc::new(Column::new("id", 1)) as Arc<dyn PhysicalExpr>,
                    Arc::new(Column::new("id", 1)) as Arc<dyn PhysicalExpr>,
                ),
            ],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
        )?);

        let (graph, _map) = builder
            .build(root)?
            .ok_or_else(|| DataFusionError::Internal("expected Some(graph)".to_string()))?;

        assert_eq!(graph.relation_count(), 4);

        // We expect:
        // - edge {0,1} from join12
        // - edge {2,3} from join34
        // - edges {0,2} and {1,3} from the root join's two predicates
        assert_eq!(graph.edges.len(), 4);

        assert!(
            graph.edges.iter().all(|e| e.join_set.cardinality() <= 2),
            "expected all edges to be binary (no hyperedge) for this plan"
        );

        let s02 = JoinSet::from_iter([0, 2])?;
        let s13 = JoinSet::from_iter([1, 3])?;
        assert!(
            graph.edges.iter().any(|e| e.join_set == s02),
            "expected an edge connecting relations {{0,2}}"
        );
        assert!(
            graph.edges.iter().any(|e| e.join_set == s13),
            "expected an edge connecting relations {{1,3}}"
        );

        Ok(())
    }

    #[test]
    fn test_builder_keeps_true_multi_relation_predicate_as_hyperedge() -> Result<()> {
        use datafusion::common::{JoinSide, NullEquality};
        use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
        use datafusion::physical_plan::joins::PartitionMode;

        let mut builder = GraphBuilder::new();

        // Three base relations with distinct column names to avoid name ambiguity.
        let schema_a = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let schema_b = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, false)]));
        let schema_c = Arc::new(Schema::new(vec![Field::new("c", DataType::Int32, false)]));

        let t1 = Arc::new(EmptyExec::new(schema_a.clone()));
        let t2 = Arc::new(EmptyExec::new(schema_b.clone()));
        let t3 = Arc::new(EmptyExec::new(schema_c.clone()));

        // Join t1 ⋈ t2 on a = b
        let join12 = Arc::new(HashJoinExec::try_new(
            t1,
            t2,
            vec![(
                Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                Arc::new(Column::new("b", 0)) as Arc<dyn PhysicalExpr>,
            )],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
        )?);

        // Root join: (t1 ⋈ t2) ⋈ t3 on a = c, plus a filter that truly depends on all 3 relations:
        // (a + b) > c
        //
        // This conjunct cannot be split into binary predicates, so GraphBuilder must keep it as a
        // hyperedge with join_set {t1,t2,t3}.
        let filter_column_indices = vec![
            ColumnIndex {
                side: JoinSide::Left,
                index: 0, // a
            },
            ColumnIndex {
                side: JoinSide::Left,
                index: 1, // b
            },
            ColumnIndex {
                side: JoinSide::Right,
                index: 0, // c
            },
        ];

        let filter_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Plus,
                Arc::new(Column::new("b", 1)),
            )),
            Operator::Gt,
            Arc::new(Column::new("c", 2)),
        ));

        let filter_schema = Arc::new(Schema::new(vec![
            join12.schema().field(0).clone(),
            join12.schema().field(1).clone(),
            schema_c.field(0).clone(),
        ]));

        let join_filter = JoinFilter::new(filter_expr, filter_column_indices, filter_schema);

        let root = Arc::new(HashJoinExec::try_new(
            join12,
            t3,
            vec![(
                Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                Arc::new(Column::new("c", 0)) as Arc<dyn PhysicalExpr>,
            )],
            Some(join_filter),
            &JoinType::Inner,
            None,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
        )?);

        let (graph, _map) = builder
            .build(root)?
            .ok_or_else(|| DataFusionError::Internal("expected Some(graph)".to_string()))?;

        assert_eq!(graph.relation_count(), 3);

        // Ensure we have at least one hyperedge with 3 relations.
        assert!(
            graph.edges.iter().any(|e| e.join_set.cardinality() == 3),
            "expected a 3-relation hyperedge from the (a + b) > c predicate"
        );

        Ok(())
    }

    #[test]
    fn test_visit_inner_join_applies_hash_join_projection() -> Result<()> {
        use datafusion::common::NullEquality;
        use datafusion::logical_expr::JoinType;
        use datafusion::physical_plan::joins::PartitionMode;

        let mut builder = GraphBuilder::new();

        // Two base relations, each with 2 columns, to test projection reorder/drop.
        let schema_left = Arc::new(Schema::new(vec![
            Field::new("l0", DataType::Int32, false),
            Field::new("l1", DataType::Int32, false),
        ]));
        let schema_right = Arc::new(Schema::new(vec![
            Field::new("r0", DataType::Int32, false),
            Field::new("r1", DataType::Int32, false),
        ]));

        let left_plan = Arc::new(EmptyExec::new(schema_left.clone()));
        let right_plan = Arc::new(EmptyExec::new(schema_right.clone()));

        // Join on l0 = r0
        let on_conditions = vec![(
            Arc::new(Column::new("l0", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("r0", 0)) as Arc<dyn PhysicalExpr>,
        )];

        // Projection selects [r1, l0, r0] from the join output:
        // join output (no projection) would be [l0, l1, r0, r1]
        let projection = Some(vec![3usize, 0usize, 2usize]);

        let join_plan = Arc::new(HashJoinExec::try_new(
            left_plan,
            right_plan,
            on_conditions,
            None,
            &JoinType::Inner,
            projection,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
        )?);

        let output_map = builder.visit_inner_join(&join_plan)?;

        // After applying projection [3,0,2], output_map should have length 3.
        assert_eq!(output_map.len(), 3);

        // The first projected column is r1, which is stable (relation 1, col 1).
        match &output_map[0] {
            ColumnMapEntry::Stable {
                relation_id,
                column_index,
            } => {
                assert_eq!(*relation_id, 1);
                assert_eq!(*column_index, 1);
            }
            _ => unreachable!("expected Stable for projected r1"),
        }

        // Second is l0 => (relation 0, col 0)
        match &output_map[1] {
            ColumnMapEntry::Stable {
                relation_id,
                column_index,
            } => {
                assert_eq!(*relation_id, 0);
                assert_eq!(*column_index, 0);
            }
            _ => unreachable!("expected Stable for projected l0"),
        }

        // Third is r0 => (relation 1, col 0)
        match &output_map[2] {
            ColumnMapEntry::Stable {
                relation_id,
                column_index,
            } => {
                assert_eq!(*relation_id, 1);
                assert_eq!(*column_index, 0);
            }
            _ => unreachable!("expected Stable for projected r0"),
        }

        Ok(())
    }
}
