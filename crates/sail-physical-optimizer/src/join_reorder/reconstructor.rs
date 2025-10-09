use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::{JoinSide, NullEquality};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{JoinType, Operator};
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::ExecutionPlan;

use crate::join_reorder::builder::{ColumnMap, ColumnMapEntry};
use crate::join_reorder::dp_plan::{DPPlan, PlanType};
use crate::join_reorder::find_physical_index;
use crate::join_reorder::graph::{QueryGraph, StableColumn};
use crate::join_reorder::join_set::JoinSet;

/// Type alias for join condition pairs
type JoinConditionPairs = Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>;

/// Plan reconstructor, converting the optimal DPPlan back to ExecutionPlan.
pub struct PlanReconstructor<'a> {
    /// Reference to the complete DP table for looking up subproblems
    dp_table: &'a HashMap<JoinSet, Arc<DPPlan>>,
    /// Reference to the query graph
    query_graph: &'a QueryGraph,
    /// Cache for reconstructed plans to avoid duplicate construction
    plan_cache: HashMap<JoinSet, (Arc<dyn ExecutionPlan>, ColumnMap)>,
    /// Pending filters that couldn't be applied yet due to missing dependencies
    pending_filters: Vec<PendingFilter>,
}

/// Represents a filter that couldn't be applied yet due to missing table dependencies
#[derive(Debug, Clone)]
struct PendingFilter {
    /// The filter expression
    expr: Arc<dyn PhysicalExpr>,
    /// Set of relations this filter depends on
    required_relations: JoinSet,
}

impl<'a> PlanReconstructor<'a> {
    pub fn new(dp_table: &'a HashMap<JoinSet, Arc<DPPlan>>, query_graph: &'a QueryGraph) -> Self {
        Self {
            dp_table,
            query_graph,
            plan_cache: HashMap::new(),
            pending_filters: Vec::new(),
        }
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

    /// Main entry point: recursively reconstruct ExecutionPlan from DPPlan
    /// Returns tuple: (reconstructed plan, column mapping for that plan's output)
    pub fn reconstruct(&mut self, dp_plan: &DPPlan) -> Result<(Arc<dyn ExecutionPlan>, ColumnMap)> {
        // Check cache
        if let Some(cached) = self.plan_cache.get(&dp_plan.join_set) {
            return Ok(cached.clone());
        }

        let mut result = match &dp_plan.plan_type {
            PlanType::Leaf { relation_id } => self.reconstruct_leaf(*relation_id)?,
            PlanType::Join {
                left_set,
                right_set,
                edge_indices,
            } => self.reconstruct_join(*left_set, *right_set, edge_indices)?,
        };

        // If we just reconstructed the root join set (entire reorderable region),
        // attach any remaining pending filters as a top-level FilterExec to ensure correctness.
        if dp_plan.join_set.cardinality() as usize == self.query_graph.relation_count() {
            if !self.pending_filters.is_empty() {
                let (plan, col_map) = &result;
                match self.apply_remaining_pending_filters(plan.clone(), col_map)? {
                    Some(new_plan) => {
                        result = (new_plan, col_map.clone());
                        // All remaining were applied; clear them
                        self.pending_filters.clear();
                    }
                    None => {
                        // Could not apply any; warn below
                    }
                }
            }

            // Final sanity check if there are still pending filters after reconstruction
            if !self.pending_filters.is_empty() && dp_plan.join_set.cardinality() > 1u32 {
                Err(DataFusionError::Internal(
                    "Some pending filters could not be applied after full reconstruction"
                        .to_string(),
                ))?;
            }
        }

        // Store in cache and return
        self.plan_cache.insert(dp_plan.join_set, result.clone());

        Ok(result)
    }

    /// Reconstruct leaf node (single relation).
    fn reconstruct_leaf(&self, relation_id: usize) -> Result<(Arc<dyn ExecutionPlan>, ColumnMap)> {
        let relation_node = self.query_graph.get_relation(relation_id).ok_or_else(|| {
            DataFusionError::Internal(format!("Relation {} not found in query graph", relation_id))
        })?;

        let plan = relation_node.plan.clone();

        // Create a fresh ColumnMap for this base relation
        let column_map = (0..plan.schema().fields().len())
            .map(|i| ColumnMapEntry::Stable {
                relation_id,
                column_index: i,
            })
            .collect();

        Ok((plan, column_map))
    }

    /// Reconstruct Join node.
    fn reconstruct_join(
        &mut self,
        left_set: JoinSet,
        right_set: JoinSet,
        edge_indices: &[usize],
    ) -> Result<(Arc<dyn ExecutionPlan>, ColumnMap)> {
        // Find left and right subplans from DP table
        let left_dp_plan = self.dp_table.get(&left_set).ok_or_else(|| {
            DataFusionError::Internal("Left subplan not found in DP table".to_string())
        })?;
        let right_dp_plan = self.dp_table.get(&right_set).ok_or_else(|| {
            DataFusionError::Internal("Right subplan not found in DP table".to_string())
        })?;

        // Recursively reconstruct left and right subplans
        let (left_plan, left_map) = self.reconstruct(left_dp_plan)?;
        let (right_plan, right_map) = self.reconstruct(right_dp_plan)?;

        // Build physical join conditions
        let on_conditions = self.build_join_conditions(
            edge_indices,
            &left_map,
            &right_map,
            &left_plan,
            &right_plan,
        )?;

        // Determine join type from edge information
        let join_type = self.determine_join_type(edge_indices)?;

        // Build join filter for non-equi conditions
        let join_filter = self.build_join_filter(
            edge_indices,
            &left_map,
            &right_map,
            &left_plan,
            &right_plan,
            left_set,
            right_set,
        )?;

        // Create HashJoinExec
        let join_plan = Arc::new(HashJoinExec::try_new(
            left_plan,
            right_plan,
            on_conditions,
            join_filter,         // Use JoinEdge.filter for non-equi conditions
            &join_type,          // Use determined join type
            None,                // projection
            PartitionMode::Auto, // partition_mode
            NullEquality::NullEqualsNothing, // TODO: Skip the optimizer completely
                                 // if NullEquality is something else in the input region.
        )?);

        // Merge left and right ColumnMap to create output ColumnMap for new Join plan
        let mut join_output_map = left_map;
        join_output_map.extend(right_map);

        Ok((join_plan, join_output_map))
    }

    /// Builds physical join conditions (`on` clause) from specific edge indices.
    fn build_join_conditions(
        &self,
        edge_indices: &[usize],
        left_map: &ColumnMap,
        right_map: &ColumnMap,
        left_plan: &Arc<dyn ExecutionPlan>,
        right_plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<JoinConditionPairs> {
        let mut on_conditions = vec![];

        // Directly iterate over the edges that the DP solver told us connect these subplans
        for &edge_index in edge_indices {
            let edge = self.query_graph.edges.get(edge_index).ok_or_else(|| {
                DataFusionError::Internal(format!("Edge with index {} not found", edge_index))
            })?;

            // Process each equi-join pair in this edge
            for (col1_stable, col2_stable) in &edge.equi_pairs {
                // Try to locate these two columns in the left and right maps
                let col1_left_idx = find_physical_index(col1_stable, left_map);
                let col1_right_idx = find_physical_index(col1_stable, right_map);
                let col2_left_idx = find_physical_index(col2_stable, left_map);
                let col2_right_idx = find_physical_index(col2_stable, right_map);

                // Determine which column is on which side and create the join condition
                let (left_col_expr, right_col_expr) = if let (Some(left_idx), Some(right_idx)) =
                    (col1_left_idx, col2_right_idx)
                {
                    // col1 is on the left, col2 is on the right
                    let left_name = left_plan.schema().field(left_idx).name().to_string();
                    let right_name = right_plan.schema().field(right_idx).name().to_string();
                    (
                        Arc::new(Column::new(&left_name, left_idx)) as Arc<dyn PhysicalExpr>,
                        Arc::new(Column::new(&right_name, right_idx)) as Arc<dyn PhysicalExpr>,
                    )
                } else if let (Some(left_idx), Some(right_idx)) = (col2_left_idx, col1_right_idx) {
                    // col2 is on the left, col1 is on the right
                    let left_name = left_plan.schema().field(left_idx).name().to_string();
                    let right_name = right_plan.schema().field(right_idx).name().to_string();
                    (
                        Arc::new(Column::new(&left_name, left_idx)) as Arc<dyn PhysicalExpr>,
                        Arc::new(Column::new(&right_name, right_idx)) as Arc<dyn PhysicalExpr>,
                    )
                } else {
                    // Skip equi-pairs that don't span across left/right plans
                    continue;
                };

                on_conditions.push((left_col_expr, right_col_expr));
            }
        }

        // Verify that connecting edges produce join conditions
        if on_conditions.is_empty() && !edge_indices.is_empty() {
            return Err(DataFusionError::Internal(
                "Failed to reconstruct any 'on' conditions for a join that should have them"
                    .to_string(),
            ));
        }
        Ok(on_conditions)
    }

    /// Determines join type from edge information.
    fn determine_join_type(&self, edge_indices: &[usize]) -> Result<JoinType> {
        // Use the join type from the first edge, or default to Inner
        if let Some(&edge_index) = edge_indices.iter().next() {
            let edge = self.query_graph.edges.get(edge_index).ok_or_else(|| {
                DataFusionError::Internal(format!("Edge with index {} not found", edge_index))
            })?;

            return Ok(edge.join_type);
        }

        // Default to Inner join if no edges found
        Ok(JoinType::Inner)
    }

    /// Builds join filter for non-equi conditions from edge information.
    fn build_join_filter(
        &mut self,
        edge_indices: &[usize],
        left_map: &ColumnMap,
        right_map: &ColumnMap,
        left_plan: &Arc<dyn ExecutionPlan>,
        right_plan: &Arc<dyn ExecutionPlan>,
        left_set: JoinSet,
        right_set: JoinSet,
    ) -> Result<Option<JoinFilter>> {
        if edge_indices.is_empty() {
            return Ok(None);
        }

        let mut non_equi_filters: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
        let current_join_set = left_set | right_set;

        // First, process filters from current edges
        for &edge_index in edge_indices {
            let edge = self.query_graph.edges.get(edge_index).ok_or_else(|| {
                DataFusionError::Internal(format!("Edge with index {} not found", edge_index))
            })?;

            if let Some(non_equi_expr) =
                self.remove_equi_conditions_from_filter(&edge.filter, &edge.equi_pairs)?
            {
                let sub_preds = Self::decompose_conjuncts(&non_equi_expr);
                for pred in sub_preds {
                    let required = self.analyze_predicate_dependencies(
                        &pred, left_map, right_map, left_plan, right_plan,
                    )?;
                    // Check if all required relations are available in current join set
                    if required.is_subset(&current_join_set) {
                        non_equi_filters.push(pred);
                    } else {
                        // Store filters that can't be applied yet
                        log::trace!(
                            "JoinReorder: Deferring filter application - required relations: {:?}, current join set: {:?}",
                            required.iter().collect::<Vec<_>>(),
                            current_join_set.iter().collect::<Vec<_>>()
                        );
                        self.pending_filters.push(PendingFilter {
                            expr: pred,
                            required_relations: required,
                        });
                    }
                }
            }
        }

        // Second, check if any pending filters can now be applied
        let mut applicable_pending = Vec::new();
        let mut remaining_pending = Vec::new();

        for pending in std::mem::take(&mut self.pending_filters) {
            if pending.required_relations.is_subset(&current_join_set) {
                // This pending filter can now be applied
                log::trace!(
                    "JoinReorder: Applying previously deferred filter - required relations: {:?}",
                    pending.required_relations.iter().collect::<Vec<_>>()
                );
                if let Ok(rewritten_pred) = self.rewrite_pending_filter_for_current_join(
                    &pending.expr,
                    left_map,
                    right_map,
                    left_plan,
                    right_plan,
                ) {
                    applicable_pending.push(rewritten_pred);
                } else {
                    log::warn!("JoinReorder: Failed to rewrite pending filter, skipping");
                }
            } else {
                // Keep in pending list
                remaining_pending.push(pending);
            }
        }

        // Restore the remaining pending filters
        self.pending_filters = remaining_pending;

        // Add applicable pending filters
        non_equi_filters.extend(applicable_pending);

        if non_equi_filters.is_empty() {
            return Ok(None);
        }

        // Combine multiple filters with AND logic
        let combined_filter = if non_equi_filters.len() == 1 {
            non_equi_filters.into_iter().next().ok_or_else(|| {
                DataFusionError::Internal(
                    "non_equi_filters should have exactly one element".to_string(),
                )
            })?
        } else {
            self.combine_filters_with_and(non_equi_filters)?
        };

        use datafusion::common::tree_node::{Transformed, TreeNode};
        use datafusion::physical_expr::expressions::Column;

        // Find side and base index for a column, supporting stable names and schema field names
        let find_side_and_index = |col: &Column| -> Option<(JoinSide, usize)> {
            if let Some((rel, cidx)) = Self::parse_stable_name(col.name()) {
                // Look up in left_map by stable, else right_map
                if let Some(pos) = left_map.iter().position(|e| matches!(e, ColumnMapEntry::Stable{ relation_id, column_index } if *relation_id==rel && *column_index==cidx)) {
                    return Some((JoinSide::Left, pos));
                }
                if let Some(pos) = right_map.iter().position(|e| matches!(e, ColumnMapEntry::Stable{ relation_id, column_index } if *relation_id==rel && *column_index==cidx)) {
                    return Some((JoinSide::Right, pos));
                }
            }
            // Fallback by matching current plan schema names
            let lname = left_plan.schema();
            for (i, f) in lname.fields().iter().enumerate() {
                if f.name() == col.name() {
                    return Some((JoinSide::Left, i));
                }
            }
            let rname = right_plan.schema();
            for (i, f) in rname.fields().iter().enumerate() {
                if f.name() == col.name() {
                    return Some((JoinSide::Right, i));
                }
            }
            None
        };

        // Build compact column index list in first-appearance order
        let mut compact_indices: Vec<ColumnIndex> = Vec::new();
        let mut seen: Vec<(JoinSide, usize)> = Vec::new();
        let cols_in_expr = collect_columns(&combined_filter);
        for c in &cols_in_expr {
            if let Some((side, idx)) = find_side_and_index(c) {
                if !seen.iter().any(|(s, i)| *s == side && *i == idx) {
                    seen.push((side, idx));
                    compact_indices.push(ColumnIndex { side, index: idx });
                }
            }
        }

        let index_of = |side: JoinSide, base_idx: usize| -> Option<usize> {
            compact_indices
                .iter()
                .position(|ci| ci.side == side && ci.index == base_idx)
        };

        let retargeted = combined_filter.transform(|expr| {
            if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                if let Some((side, base_idx)) = find_side_and_index(col) {
                    if let Some(new_pos) = index_of(side, base_idx) {
                        let new_col = Column::new(col.name(), new_pos);
                        return Ok(Transformed::yes(Arc::new(new_col)));
                    }
                }
            }
            Ok(Transformed::no(expr))
        })?;
        let rewritten_expr = retargeted.data;

        use datafusion::arrow::datatypes::Schema;
        let mut fields = Vec::with_capacity(compact_indices.len());
        for ci in &compact_indices {
            match ci.side {
                JoinSide::Left => fields.push(left_plan.schema().field(ci.index).clone()),
                JoinSide::Right => fields.push(right_plan.schema().field(ci.index).clone()),
                JoinSide::None => unreachable!(),
            }
        }
        let intermediate_schema = Arc::new(Schema::new(fields));

        Ok(Some(JoinFilter::new(
            rewritten_expr,
            compact_indices,
            intermediate_schema,
        )))
    }

    /// Attempt to apply any remaining pending filters as a top-level FilterExec
    /// on the provided plan. Returns Some(new_plan) if at least one filter has
    /// been applied, or None if none could be rewritten.
    fn apply_remaining_pending_filters(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        output_map: &ColumnMap,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if self.pending_filters.is_empty() {
            return Ok(None);
        }

        // Try rewriting each pending filter to reference the final plan's schema
        let mut rewritten: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
        for pending in &self.pending_filters {
            if let Ok(expr) = self.rewrite_expr_to_output_schema(&pending.expr, &plan, output_map) {
                rewritten.push(expr);
            }
        }

        if rewritten.is_empty() {
            return Ok(None);
        }

        // Combine with AND and attach FilterExec
        let combined = if rewritten.len() == 1 {
            rewritten[0].clone()
        } else {
            self.combine_filters_with_and(rewritten)?
        };

        let new_plan = Arc::new(FilterExec::try_new(combined, plan)?);
        Ok(Some(new_plan))
    }

    /// Rewrite an expression that uses stable column names (e.g. "R{rel}.C{col}")
    /// to use actual output column indices and names of the final plan schema.
    fn rewrite_expr_to_output_schema(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        plan: &Arc<dyn ExecutionPlan>,
        output_map: &ColumnMap,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        use datafusion::common::tree_node::{Transformed, TreeNode};
        use datafusion::physical_expr::expressions::Column;

        let expr_arc = Arc::clone(expr);
        let transformed = expr_arc.transform(|node| {
            if let Some(col) = node.as_any().downcast_ref::<Column>() {
                // Prefer stable name mapping first
                if let Some((rel, cidx)) = self.parse_stable_column_name(col.name()) {
                    if let Some(pos) = output_map.iter().position(|e| {
                        matches!(
                            e,
                            ColumnMapEntry::Stable {
                                relation_id,
                                column_index
                            } if *relation_id == rel && *column_index == cidx
                        )
                    }) {
                        // Use the final plan's schema field name and index
                        let field_name = plan.schema().field(pos).name().to_string();
                        let new_col = Column::new(&field_name, pos);
                        return Ok(Transformed::yes(Arc::new(new_col)));
                    }
                }

                // Fallback: try to match by current schema field name
                if let Some(pos) = plan
                    .schema()
                    .fields()
                    .iter()
                    .position(|f| f.name() == col.name())
                {
                    let new_col = Column::new(col.name(), pos);
                    return Ok(Transformed::yes(Arc::new(new_col)));
                }
            }
            Ok(Transformed::no(node))
        })?;

        Ok(transformed.data)
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

    fn analyze_predicate_dependencies(
        &self,
        predicate: &Arc<dyn PhysicalExpr>,
        left_map: &ColumnMap,
        right_map: &ColumnMap,
        left_plan: &Arc<dyn ExecutionPlan>,
        right_plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<JoinSet> {
        let mut bits: u64 = 0;
        let cols = collect_columns(predicate);
        for c in &cols {
            if let Some((rel, _)) = Self::parse_stable_name(c.name()) {
                bits |= 1u64 << rel;
                continue;
            }
            let mut matched = false;
            for (i, f) in left_plan.schema().fields().iter().enumerate() {
                if f.name() == c.name() {
                    if let Some(ColumnMapEntry::Stable { relation_id, .. }) = left_map.get(i) {
                        bits |= 1u64 << *relation_id;
                        matched = true;
                        break;
                    }
                }
            }
            if matched {
                continue;
            }
            for (i, f) in right_plan.schema().fields().iter().enumerate() {
                if f.name() == c.name() {
                    if let Some(ColumnMapEntry::Stable { relation_id, .. }) = right_map.get(i) {
                        bits |= 1u64 << *relation_id;
                        matched = true;
                        break;
                    }
                }
            }
            let _ = matched;
        }

        Ok(JoinSet::from_bits(bits))
    }

    /// Remove equi-join conditions from the filter expression, returning only non-equi parts.
    fn remove_equi_conditions_from_filter(
        &self,
        filter: &Arc<dyn PhysicalExpr>,
        equi_pairs: &[(StableColumn, StableColumn)],
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        // Separate equi and non-equi conditions
        let non_equi_expr = self.extract_non_equi_from_expression(filter, equi_pairs)?;
        Ok(non_equi_expr)
    }

    /// Extract non-equi conditions from a complex expression by removing equi-join conditions.
    fn extract_non_equi_from_expression(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        equi_pairs: &[(StableColumn, StableColumn)],
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        use datafusion::physical_expr::expressions::BinaryExpr;

        if let Some(binary_expr) = expr.as_any().downcast_ref::<BinaryExpr>() {
            match binary_expr.op() {
                Operator::And => {
                    // For AND expressions, recursively process left and right sides
                    let left_non_equi =
                        self.extract_non_equi_from_expression(binary_expr.left(), equi_pairs)?;
                    let right_non_equi =
                        self.extract_non_equi_from_expression(binary_expr.right(), equi_pairs)?;

                    match (left_non_equi, right_non_equi) {
                        (Some(left), Some(right)) => {
                            // Both sides have non-equi conditions, combine them with AND
                            Ok(Some(Arc::new(BinaryExpr::new(left, Operator::And, right))))
                        }
                        (Some(expr), None) | (None, Some(expr)) => {
                            // Only one side has non-equi conditions
                            Ok(Some(expr))
                        }
                        (None, None) => {
                            // No non-equi conditions found
                            Ok(None)
                        }
                    }
                }
                Operator::Eq => {
                    // Check if this equality is part of the equi-join conditions
                    if self.is_equi_join_condition(binary_expr, equi_pairs) {
                        Ok(None) // This is an equi-join condition, exclude it
                    } else {
                        Ok(Some(expr.clone())) // This is a non-equi condition
                    }
                }
                _ => {
                    // All other operators (>, <, >=, <=, !=, etc.) are non-equi conditions
                    Ok(Some(expr.clone()))
                }
            }
        } else {
            // Non-binary expressions are considered non-equi conditions
            Ok(Some(expr.clone()))
        }
    }

    /// Check if a binary equality expression matches any of the equi-join pairs.
    fn is_equi_join_condition(
        &self,
        binary_expr: &BinaryExpr,
        equi_pairs: &[(StableColumn, StableColumn)],
    ) -> bool {
        use datafusion::physical_expr::expressions::Column;

        // Extract column references from both sides of the equality
        let left_col = binary_expr.left().as_any().downcast_ref::<Column>();
        let right_col = binary_expr.right().as_any().downcast_ref::<Column>();

        if let (Some(left), Some(right)) = (left_col, right_col) {
            // Check if this column pair matches any equi-join pair
            for (col1, col2) in equi_pairs {
                if (left.name() == col1.name && right.name() == col2.name)
                    || (left.name() == col2.name && right.name() == col1.name)
                {
                    return true;
                }
            }
        }
        false
    }

    /// Rewrite a pending filter expression to work with the current join's column mapping
    fn rewrite_pending_filter_for_current_join(
        &self,
        pending_expr: &Arc<dyn PhysicalExpr>,
        left_map: &ColumnMap,
        right_map: &ColumnMap,
        left_plan: &Arc<dyn ExecutionPlan>,
        right_plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        // Verify that all columns in the pending expression can be resolved
        // in the current join context before applying the filter
        let cols_in_expr = collect_columns(pending_expr);
        for col in &cols_in_expr {
            let found_in_left = self.find_column_in_plan_schema(col, left_plan, left_map);
            let found_in_right = self.find_column_in_plan_schema(col, right_plan, right_map);

            if !found_in_left && !found_in_right {
                return Err(DataFusionError::Internal(format!(
                    "Column '{}' in pending filter cannot be resolved in current join context",
                    col.name()
                )));
            }
        }

        // Return the expression as-is since the column mapping logic
        // in build_join_filter already handles the rewriting correctly
        Ok(pending_expr.clone())
    }

    /// Helper to check if a column can be found in a plan's schema or column map
    fn find_column_in_plan_schema(
        &self,
        col: &Column,
        plan: &Arc<dyn ExecutionPlan>,
        column_map: &ColumnMap,
    ) -> bool {
        // Check by stable column name format (R{rel}.C{col})
        if let Some((rel, cidx)) = self.parse_stable_column_name(col.name()) {
            return column_map.iter().any(|entry| {
                matches!(entry, ColumnMapEntry::Stable { relation_id, column_index }
                    if *relation_id == rel && *column_index == cidx)
            });
        }

        // Check by schema field name
        plan.schema()
            .fields()
            .iter()
            .any(|f| f.name() == col.name())
    }

    /// Parse stable column name format "R{rel}.C{col}" -> (rel, col)
    fn parse_stable_column_name(&self, name: &str) -> Option<(usize, usize)> {
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

    /// Combine multiple filter expressions with AND logic.
    fn combine_filters_with_and(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::BinaryExpr;

        let mut result = filters[0].clone();
        for filter in filters.into_iter().skip(1) {
            result = Arc::new(BinaryExpr::new(result, Operator::And, filter));
        }
        Ok(result)
    }

    /// Clear plan cache.
    #[cfg(test)]
    pub fn clear_cache(&mut self) {
        self.plan_cache.clear();
        self.pending_filters.clear();
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Statistics;
    use datafusion::physical_plan::empty::EmptyExec;

    use super::*;
    use crate::join_reorder::graph::{QueryGraph, RelationNode};
    use crate::join_reorder::join_set::JoinSet;

    fn create_test_graph() -> QueryGraph {
        let mut graph = QueryGraph::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));

        let plan = Arc::new(EmptyExec::new(schema.clone()));
        let relation = RelationNode::new(plan, 0, 1000.0, Statistics::new_unknown(&schema));
        graph.add_relation(relation);

        graph
    }

    #[test]
    fn test_reconstructor_creation() {
        let dp_table = HashMap::new();
        let graph = QueryGraph::new();
        let reconstructor = PlanReconstructor::new(&dp_table, &graph);
        assert!(reconstructor.plan_cache.is_empty());
    }

    #[test]
    fn test_reconstruct_leaf() -> Result<()> {
        let mut dp_table = HashMap::new();
        let graph = create_test_graph();
        let leaf_plan = Arc::new(DPPlan::new_leaf(0, 1000.0).unwrap());
        dp_table.insert(leaf_plan.join_set, leaf_plan.clone());

        let mut reconstructor = PlanReconstructor::new(&dp_table, &graph);
        let result = reconstructor.reconstruct(&leaf_plan);

        assert!(result.is_ok());
        Ok(())
    }

    #[test]
    fn test_reconstruct_join_missing_subplans() {
        let dp_table = HashMap::new(); // Empty table
        let graph = create_test_graph();

        let left_set = JoinSet::new_singleton(0).unwrap();
        let right_set = JoinSet::new_singleton(1).unwrap();
        let join_plan = Arc::new(DPPlan::new_join(
            left_set,
            right_set,
            vec![0],
            2000.0,
            500.0,
        ));

        let mut reconstructor = PlanReconstructor::new(&dp_table, &graph);
        let result = reconstructor.reconstruct(&join_plan);
        assert!(result.is_err());

        // Should return Internal error about missing subplan
        if let Err(DataFusionError::Internal(_)) = result {
            // Expected error type
        } else {
            unreachable!("Expected Internal error about missing subplan");
        }
    }

    #[test]
    fn test_clear_cache() {
        let dp_table = HashMap::new();
        let graph = QueryGraph::new();
        let mut reconstructor = PlanReconstructor::new(&dp_table, &graph);

        // Add some cache items (simulated)
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));
        let plan = Arc::new(EmptyExec::new(schema));
        let column_map = vec![];
        let join_set = JoinSet::new_singleton(0).unwrap();
        reconstructor
            .plan_cache
            .insert(join_set, (plan, column_map));

        assert!(!reconstructor.plan_cache.is_empty());

        reconstructor.clear_cache();
        assert!(reconstructor.plan_cache.is_empty());
    }
}
