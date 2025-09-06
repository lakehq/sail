//! # Decomposer Module
//!
//! This module is responsible for the **decomposition phase** of the DPHyp algorithm.
//! It traverses an ExecutionPlan and breaks it down into:
//!
//! - A set of base relations ([`JoinRelation`]).
//! - A query graph ([`QueryGraph`]) representing join conditions.
//! - A list of non-equi conditions to be applied as filters later.
//!
//! ## Decomposition Strategy
//!
//! - **Inner Joins**: The decomposer traverses through inner joins, extracting
//!   equi-join conditions to build the query graph.
//! - **Filters & Simple Projections**: These are traversed, with filters contributing
//!   to non-equi conditions.
//! - **Complex Operators**: When a node that is not part of a simple inner join chain
//!   (e.g., non-inner join, aggregate, sort, complex projection) is encountered,
//!   it's treated as a boundary. The optimizer is called recursively on its children
//!   to optimize them independently. The operator with its optimized children is
//!   then treated as a single, atomic base relation for the current optimization context.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::common::{DataFusionError, JoinType};
use datafusion::error::Result;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use log::debug;

use crate::join_reorder::graph::QueryGraph;
use crate::join_reorder::plan::{JoinRelation, MappedFilterExpr, MappedJoinKey, RelationSetTree};
use crate::join_reorder::utils::is_simple_projection;
use crate::join_reorder::JoinReorder;

/// Result of the decomposition phase.
pub(crate) struct DecomposedPlan {
    /// Base relations extracted from the original plan.
    pub(crate) join_relations: Vec<JoinRelation>,
    /// Query graph representing equi-join conditions between relations.
    pub(crate) query_graph: QueryGraph,
    /// Non-equi conditions that should be applied as filters.
    pub(crate) non_equi_conditions: Vec<MappedFilterExpr>,
    /// Mapping from original plan output columns to (relation_id, base_col_idx).
    pub(crate) original_output_map: HashMap<usize, (usize, usize)>,
    /// Trie structure for managing canonical relation sets.
    pub(crate) relation_set_tree: RelationSetTree,
}

/// Main decomposer that handles the decomposition phase.
pub(crate) struct Decomposer<'a> {
    join_relations: Vec<JoinRelation>,
    query_graph: QueryGraph,
    non_equi_conditions: Vec<MappedFilterExpr>,
    relation_set_tree: RelationSetTree,
    /// Reference to the main optimizer for recursive calls.
    optimizer: &'a JoinReorder,
}

impl<'a> Decomposer<'a> {
    /// Creates a new decomposer.
    pub(crate) fn new(optimizer: &'a JoinReorder) -> Self {
        Self {
            join_relations: Vec::new(),
            query_graph: QueryGraph::new(),
            non_equi_conditions: Vec::new(),
            relation_set_tree: RelationSetTree::new(),
            optimizer,
        }
    }

    /// Decomposes an ExecutionPlan into its constituent components.
    pub(crate) fn decompose(mut self, plan: Arc<dyn ExecutionPlan>) -> Result<DecomposedPlan> {
        let original_output_map = self.recursive_decompose(plan)?;

        debug!(
            "Decomposition completed: {} relations, {} non-equi conditions",
            self.join_relations.len(),
            self.non_equi_conditions.len(),
        );

        Ok(DecomposedPlan {
            join_relations: self.join_relations,
            query_graph: self.query_graph,
            non_equi_conditions: self.non_equi_conditions,
            original_output_map,
            relation_set_tree: self.relation_set_tree,
        })
    }

    /// Recursively traverses the plan, decomposing joins and building the query graph.
    /// Returns a map from output column indices of the current plan to `(relation_id, original_column_index)`.
    fn recursive_decompose(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<HashMap<usize, (usize, usize)>> {
        if let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() {
            if join.join_type() == &JoinType::Inner {
                let filter_expr = join.filter().as_ref().map(|f| f.expression());
                return self.decompose_inner_join(
                    join.left(),
                    join.right(),
                    join.on(),
                    filter_expr,
                );
            }
        }
        if let Some(join) = plan.as_any().downcast_ref::<SortMergeJoinExec>() {
            if join.join_type() == JoinType::Inner {
                let filter_expr = join.filter().as_ref().map(|f| f.expression());
                return self.decompose_inner_join(
                    join.left(),
                    join.right(),
                    join.on(),
                    filter_expr,
                );
            }
        }
        if let Some(filter) = plan.as_any().downcast_ref::<FilterExec>() {
            return self.decompose_filter(filter);
        }
        if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
            return self.decompose_projection(projection);
        }

        // Any other plan type is a "complex operator" that forms a boundary.
        // It will be treated as a single base relation after its children are recursively optimized.
        self.decompose_complex_operator(plan)
    }

    /// Decomposes an inner join by recursively processing its children.
    fn decompose_inner_join(
        &mut self,
        left: &Arc<dyn ExecutionPlan>,
        right: &Arc<dyn ExecutionPlan>,
        on: &[(PhysicalExprRef, PhysicalExprRef)],
        filter: Option<&PhysicalExprRef>,
    ) -> Result<HashMap<usize, (usize, usize)>> {
        let left_map = self.recursive_decompose(left.clone())?;
        let right_map = self.recursive_decompose(right.clone())?;

        for (left_expr, right_expr) in on.iter() {
            self.process_join_condition(left_expr, right_expr, &left_map, &right_map)?;
        }

        if let Some(filter_expr) = filter {
            // Combine left and right child maps to provide complete context for the filter
            let combined_input_map = self.combine_column_maps(
                left_map.clone(),
                right_map.clone(),
                left.schema().fields().len(),
            )?;
            let filter_local_column_map =
                self.extract_local_column_map_for_expr(filter_expr, &combined_input_map)?;
            self.non_equi_conditions.push(MappedFilterExpr::new(
                filter_expr.clone(),
                filter_local_column_map,
            ));
        }

        self.combine_column_maps(left_map, right_map, left.schema().fields().len())
    }

    /// Decomposes a FilterExec by collecting its predicate and recursing on its input.
    fn decompose_filter(&mut self, filter: &FilterExec) -> Result<HashMap<usize, (usize, usize)>> {
        // First recursively decompose the input to get its column mapping
        let input_map = self.recursive_decompose(filter.input().clone())?;
        debug!(
            "Decomposer: Found filter, adding to non-equi conditions: {}",
            filter.predicate()
        );

        // Extract the local column mapping for the filter predicate
        let filter_local_column_map =
            self.extract_local_column_map_for_expr(filter.predicate(), &input_map)?;
        self.non_equi_conditions.push(MappedFilterExpr::new(
            filter.predicate().clone(),
            filter_local_column_map,
        ));

        // FilterExec's output schema is the same as input schema
        Ok(input_map)
    }

    /// Decomposes a ProjectionExec.
    fn decompose_projection(
        &mut self,
        projection: &ProjectionExec,
    ) -> Result<HashMap<usize, (usize, usize)>> {
        if is_simple_projection(projection) {
            // A simple projection (only column reordering/selection) can be traversed.
            let input_map = self.recursive_decompose(projection.input().clone())?;
            let mut projection_map = HashMap::new();

            for (i, (expr, _name)) in projection.expr().iter().enumerate() {
                let col = expr.as_any().downcast_ref::<Column>().ok_or_else(|| {
                    DataFusionError::Internal("Expected Column in simple projection".to_string())
                })?;
                let origin = input_map.get(&col.index()).ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Projection references unknown column index {}",
                        col.index()
                    ))
                })?;
                projection_map.insert(i, *origin);
            }
            Ok(projection_map)
        } else {
            // A projection with expressions is a "complex operator".
            self.decompose_complex_operator(Arc::new(projection.clone()))
        }
    }

    /// Decomposes a complex operator by treating it as an atomic relation.
    /// Its children are recursively optimized first.
    fn decompose_complex_operator(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<HashMap<usize, (usize, usize)>> {
        debug!(
            "Complex operator '{}' detected, treating as an atomic relation.",
            plan.name()
        );

        // Recursively optimize all children of this complex operator.
        let optimized_children: Result<Vec<_>> = plan
            .children()
            .iter()
            .map(|child| self.optimizer.optimize_join_chain((*child).clone()))
            .collect();
        let optimized_children = optimized_children?;

        // Create a new version of the plan with its children replaced by their optimized versions.
        let optimized_plan = if optimized_children.is_empty() {
            plan
        } else {
            plan.with_new_children(optimized_children)?
        };

        // Add the entire optimized plan as a single base relation.
        self.add_base_relation(optimized_plan)
    }

    /// Adds a plan as a new base relation and returns its column map.
    fn add_base_relation(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<HashMap<usize, (usize, usize)>> {
        let stats = plan.partition_statistics(Some(0))?;
        let relation_id = self.join_relations.len();

        self.join_relations.push(JoinRelation {
            plan: plan.clone(),
            stats,
            id: relation_id,
        });
        debug!(
            "Added base relation [id: {}]: name={}, schema_fields={}",
            relation_id,
            plan.name(),
            plan.schema().fields().len()
        );

        let map = (0..plan.schema().fields().len())
            .map(|i| (i, (relation_id, i)))
            .collect();
        Ok(map)
    }

    /// Processes a join condition, adding an edge to the query graph.
    fn process_join_condition(
        &mut self,
        left_expr: &PhysicalExprRef,
        right_expr: &PhysicalExprRef,
        left_map: &HashMap<usize, (usize, usize)>,
        right_map: &HashMap<usize, (usize, usize)>,
    ) -> Result<()> {
        debug!(
            "Processing join condition: left_expr='{}', right_expr='{}'. Left map: {:?}, Right map: {:?}",
            left_expr,
            right_expr,
            left_map,
            right_map
        );
        let left_columns = collect_columns(left_expr);
        let right_columns = collect_columns(right_expr);

        let mut left_column_map = HashMap::new();
        let mut left_rel_ids = HashSet::new();
        for col in &left_columns {
            if let Some(&(rel_id, base_idx)) = left_map.get(&col.index()) {
                left_column_map.insert(col.index(), (rel_id, base_idx));
                left_rel_ids.insert(rel_id);
            } else {
                return Err(DataFusionError::Internal(format!(
                    "Left join key column '{}' not found in column map",
                    col.name()
                )));
            }
        }

        let mut right_column_map = HashMap::new();
        let mut right_rel_ids = HashSet::new();
        for col in &right_columns {
            if let Some(&(rel_id, base_idx)) = right_map.get(&col.index()) {
                right_column_map.insert(col.index(), (rel_id, base_idx));
                right_rel_ids.insert(rel_id);
            } else {
                return Err(DataFusionError::Internal(format!(
                    "Right join key column '{}' not found in column map",
                    col.name()
                )));
            }
        }

        // An equi-join connects disjoint sets of relations.
        if left_rel_ids.is_disjoint(&right_rel_ids) {
            debug!(
                "Creating equi-join edge: left_rel_ids={:?}, right_rel_ids={:?}, left_column_map={:?}, right_column_map={:?}",
                left_rel_ids, right_rel_ids, left_column_map, right_column_map
            );
            let left_key = MappedJoinKey::new(left_expr.clone(), left_column_map);
            let right_key = MappedJoinKey::new(right_expr.clone(), right_column_map);
            let left_rel_set = self.relation_set_tree.get_relation_set(&left_rel_ids);
            let right_rel_set = self.relation_set_tree.get_relation_set(&right_rel_ids);

            self.query_graph
                .create_edge(left_rel_set, right_rel_set, (left_key, right_key));
        } else {
            // This is a filter predicate (e.g., t1.a = t1.b). Treat as a non-equi condition.
            debug!(
                "Creating non-equi condition (same relation): left_rel_ids={:?}, right_rel_ids={:?}",
                left_rel_ids, right_rel_ids
            );
            let filter_expr = Arc::new(BinaryExpr::new(
                left_expr.clone(),
                Operator::Eq,
                right_expr.clone(),
            )) as PhysicalExprRef;
            // For same-relation filters, we need to create the combined map context
            let mut combined_map = left_map.clone();
            for (&right_idx, &origin) in right_map {
                combined_map.insert(right_idx, origin);
            }
            let filter_local_column_map =
                self.extract_local_column_map_for_expr(&filter_expr, &combined_map)?;
            self.non_equi_conditions
                .push(MappedFilterExpr::new(filter_expr, filter_local_column_map));
        }
        Ok(())
    }

    /// Combines column maps from left and right children of a join.
    fn combine_column_maps(
        &self,
        left_map: HashMap<usize, (usize, usize)>,
        right_map: HashMap<usize, (usize, usize)>,
        left_schema_len: usize,
    ) -> Result<HashMap<usize, (usize, usize)>> {
        let mut combined_map = left_map;
        for (right_idx, origin) in right_map {
            combined_map.insert(left_schema_len + right_idx, origin);
        }
        Ok(combined_map)
    }

    /// Helper function: Extract local column mapping for a PhysicalExprRef
    /// This mapping stores: (expr's local index) -> (global relation_id, base_col_idx)
    fn extract_local_column_map_for_expr(
        &self,
        expr: &PhysicalExprRef,
        input_map_from_parent: &HashMap<usize, (usize, usize)>, // Maps expr input schema index to (rel_id, base_idx)
    ) -> Result<HashMap<usize, (usize, usize)>> {
        let mut local_map = HashMap::new();
        for col in collect_columns(expr) {
            if let Some(&origin) = input_map_from_parent.get(&col.index()) {
                local_map.insert(col.index(), origin); // Map expr's local index to (rel_id, base_idx)
            } else {
                return Err(DataFusionError::Internal(format!(
                    "Column '{}' with index {} in expression not found in input map. Input map: {:?}",
                    col.name(), col.index(), input_map_from_parent
                )));
            }
        }
        Ok(local_map)
    }
}
