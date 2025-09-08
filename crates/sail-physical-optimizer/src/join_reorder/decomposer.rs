use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::common::{DataFusionError, JoinType};
use datafusion::error::Result;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;

use crate::join_reorder::graph::QueryGraph;
use crate::join_reorder::plan::{JoinRelation, MappedJoinKey, RelationSetTree};
use crate::join_reorder::utils::is_simple_projection;

fn create_plan_identifier(plan: &Arc<dyn ExecutionPlan>) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();

    plan.name().hash(&mut hasher);
    plan.schema().fields().len().hash(&mut hasher);

    for field in plan.schema().fields() {
        field.name().hash(&mut hasher);
        format!("{:?}", field.data_type()).hash(&mut hasher);
    }

    if plan.children().is_empty() {
        format!("{:?}", plan).hash(&mut hasher);
    }

    format!("plan_{:016x}", hasher.finish())
}

pub(crate) struct DecomposedPlan {
    pub(crate) column_catalog: crate::join_reorder::plan::ColumnCatalog,
    pub(crate) join_relations: Vec<JoinRelation>,
    pub(crate) query_graph: QueryGraph,
    pub(crate) original_output_map: HashMap<usize, (usize, usize)>,
    pub(crate) relation_set_tree: RelationSetTree,
}

pub(crate) struct Decomposer {
    column_catalog: crate::join_reorder::plan::ColumnCatalog,
    join_relations: Vec<JoinRelation>,
    query_graph: QueryGraph,
    relation_set_tree: RelationSetTree,
}

impl Decomposer {
    pub(crate) fn new() -> Self {
        Self {
            column_catalog: crate::join_reorder::plan::ColumnCatalog::new(),
            join_relations: Vec::new(),
            query_graph: QueryGraph::new(),
            relation_set_tree: RelationSetTree::new(),
        }
    }

    pub(crate) fn decompose(mut self, plan: Arc<dyn ExecutionPlan>) -> Result<DecomposedPlan> {
        let original_output_map = self.recursive_decompose(plan)?;

        Ok(DecomposedPlan {
            column_catalog: self.column_catalog,
            join_relations: self.join_relations,
            query_graph: self.query_graph,
            original_output_map,
            relation_set_tree: self.relation_set_tree,
        })
    }

    fn trace_and_catalog_columns(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<HashMap<usize, usize>> {
        if plan.children().is_empty() {
            let mut output_map = HashMap::new();
            for (idx, field) in plan.schema().fields().iter().enumerate() {
                let stable_id = self
                    .column_catalog
                    .add_column(field.name().clone(), field.data_type().clone());
                output_map.insert(idx, stable_id);
            }
            return Ok(output_map);
        }

        if let Some(proj) = plan.as_any().downcast_ref::<ProjectionExec>() {
            let input_map = self.trace_and_catalog_columns(proj.input().clone())?;
            let mut output_map = HashMap::new();
            for (output_idx, (expr, _name)) in proj.expr().iter().enumerate() {
                if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                    if let Some(stable_id) = input_map.get(&col.index()) {
                        output_map.insert(output_idx, *stable_id);
                    } else {
                        return Err(DataFusionError::Internal(
                            "Projection references an untraceable column".to_string(),
                        ));
                    }
                } else {
                    let schema = plan.schema();
                    let field = schema.field(output_idx);
                    let stable_id = self
                        .column_catalog
                        .add_column(field.name().clone(), field.data_type().clone());
                    output_map.insert(output_idx, stable_id);
                }
            }
            return Ok(output_map);
        }

        if let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() {
            let left_map = self.trace_and_catalog_columns(join.left().clone())?;
            let right_map = self.trace_and_catalog_columns(join.right().clone())?;

            let mut output_map = left_map;
            let left_len = join.left().schema().fields().len();
            for (right_idx, stable_id) in right_map {
                output_map.insert(left_len + right_idx, stable_id);
            }
            return Ok(output_map);
        }

        let mut output_map = HashMap::new();
        for (idx, field) in plan.schema().fields().iter().enumerate() {
            let stable_id = self
                .column_catalog
                .add_column(field.name().clone(), field.data_type().clone());
            output_map.insert(idx, stable_id);
        }
        Ok(output_map)
    }

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
                    plan.clone(),
                );
            } else {
                return self.add_base_relation(plan);
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
                    plan.clone(),
                );
            } else {
                return self.add_base_relation(plan);
            }
        }
        if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
            return self.decompose_projection(projection);
        }

        self.add_base_relation(plan)
    }

    fn decompose_inner_join(
        &mut self,
        left: &Arc<dyn ExecutionPlan>,
        right: &Arc<dyn ExecutionPlan>,
        on: &[(PhysicalExprRef, PhysicalExprRef)],
        filter: Option<&PhysicalExprRef>,
        _original_join_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<HashMap<usize, (usize, usize)>> {
        let left_map = self.recursive_decompose(left.clone())?;
        let right_map = self.recursive_decompose(right.clone())?;

        for (left_expr, right_expr) in on.iter() {
            self.process_join_condition(left_expr, right_expr, &left_map, &right_map)?;
        }

        if let Some(filter_expr) = filter {
            let left_len = left.schema().fields().len();
            self.store_join_filter(filter_expr, &left_map, &right_map, left_len)?;
        }

        self.combine_column_maps(left_map, right_map, left.schema().fields().len())
    }

    fn decompose_projection(
        &mut self,
        projection: &ProjectionExec,
    ) -> Result<HashMap<usize, (usize, usize)>> {
        if is_simple_projection(projection) {
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
            self.add_base_relation(Arc::new(projection.clone()))
        }
    }

    fn add_base_relation(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<HashMap<usize, (usize, usize)>> {
        let stats = plan.partition_statistics(Some(0))?;
        let relation_id = self.join_relations.len();

        let output_to_stable_id = self.trace_and_catalog_columns(plan.clone())?;

        let mut leaf_to_stable_map: HashMap<String, HashMap<usize, (usize, usize)>> =
            HashMap::new();

        Self::build_leaf_mappings(&plan, relation_id, &mut leaf_to_stable_map)?;

        self.join_relations.push(JoinRelation {
            plan: plan.clone(),
            stats,
            id: relation_id,
            output_to_stable_id: output_to_stable_id.clone(),
            leaf_to_stable_map,
        });

        let output_to_join_graph_id = (0..plan.schema().fields().len())
            .map(|output_idx| (output_idx, (relation_id, output_idx)))
            .collect();

        Ok(output_to_join_graph_id)
    }

    fn build_leaf_mappings(
        plan: &Arc<dyn ExecutionPlan>,
        relation_id: usize,
        leaf_to_stable_map: &mut HashMap<String, HashMap<usize, (usize, usize)>>,
    ) -> Result<()> {
        if plan.children().is_empty() {
            let leaf_key = create_plan_identifier(plan);
            let mut column_map = HashMap::new();

            for i in 0..plan.schema().fields().len() {
                column_map.insert(i, (relation_id, i));
            }

            leaf_to_stable_map.insert(leaf_key, column_map);
        } else {
            for child in plan.children() {
                Self::build_leaf_mappings(child, relation_id, leaf_to_stable_map)?;
            }
        }

        Ok(())
    }

    fn process_join_condition(
        &mut self,
        left_expr: &PhysicalExprRef,
        right_expr: &PhysicalExprRef,
        left_map: &HashMap<usize, (usize, usize)>,
        right_map: &HashMap<usize, (usize, usize)>,
    ) -> Result<()> {
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

        if left_rel_ids.is_disjoint(&right_rel_ids) {
            let left_key = MappedJoinKey::new_legacy(
                left_expr.clone(),
                left_column_map,
                &self.join_relations,
            )?;
            let right_key = MappedJoinKey::new_legacy(
                right_expr.clone(),
                right_column_map,
                &self.join_relations,
            )?;
            let left_rel_set = self.relation_set_tree.get_relation_set(&left_rel_ids);
            let right_rel_set = self.relation_set_tree.get_relation_set(&right_rel_ids);

            self.query_graph
                .create_edge(left_rel_set, right_rel_set, (left_key, right_key));
        }
        Ok(())
    }

    fn store_join_filter(
        &mut self,
        filter_expr: &PhysicalExprRef,
        left_map: &HashMap<usize, (usize, usize)>,
        right_map: &HashMap<usize, (usize, usize)>,
        left_schema_len: usize,
    ) -> Result<()> {
        use datafusion::physical_expr::utils::collect_columns;

        let filter_columns = collect_columns(filter_expr);
        let mut filter_column_map = HashMap::new();
        let mut filter_rel_ids = HashSet::new();

        for col in &filter_columns {
            let idx = col.index();
            if let Some(&(rel_id, base_idx)) = left_map.get(&idx) {
                filter_column_map.insert(idx, (rel_id, base_idx));
                filter_rel_ids.insert(rel_id);
                continue;
            }
            if let Some(&(rel_id, base_idx)) = right_map.get(&idx) {
                filter_column_map.insert(idx, (rel_id, base_idx));
                filter_rel_ids.insert(rel_id);
                continue;
            }

            if idx >= left_schema_len {
                let ridx = idx - left_schema_len;
                if let Some(&(rel_id, base_idx)) = right_map.get(&ridx) {
                    filter_column_map.insert(idx, (rel_id, base_idx));
                    filter_rel_ids.insert(rel_id);
                    continue;
                }
            }

            return Err(DataFusionError::Internal(format!(
                "Filter column '{}' not found in column maps",
                col.name()
            )));
        }

        let filter_key = MappedJoinKey::new_legacy(
            filter_expr.clone(),
            filter_column_map,
            &self.join_relations,
        )?;

        let filter_rel_set = self.relation_set_tree.get_relation_set(&filter_rel_ids);
        self.query_graph
            .add_join_filter(filter_rel_set.clone(), filter_key.clone());

        Ok(())
    }

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
}
