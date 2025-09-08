use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion::common::{DataFusionError, JoinType, NullEquality, Statistics};
use datafusion::error::Result;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::ExecutionPlan;

use crate::join_reorder::placeholder::placeholder_column;
use crate::join_reorder::utils::is_subset;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnIdentity {
    pub original_name: String,
    pub data_type: DataType,
}

#[derive(Debug, Default)]
pub struct ColumnCatalog {
    catalog: HashMap<usize, ColumnIdentity>,
    next_id: usize,
}

impl ColumnCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_column(&mut self, name: String, data_type: DataType) -> usize {
        let id = self.next_id;
        self.catalog.insert(
            id,
            ColumnIdentity {
                original_name: name,
                data_type,
            },
        );
        self.next_id += 1;
        id
    }

    pub fn get(&self, id: usize) -> Option<&ColumnIdentity> {
        self.catalog.get(&id)
    }
}

pub(crate) type PlanWithColumnMap = (Arc<dyn ExecutionPlan>, HashMap<usize, (usize, usize)>);

pub(crate) struct JoinRelation {
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    pub(crate) stats: Statistics,
    pub(crate) id: usize,
    pub(crate) output_to_stable_id: HashMap<usize, usize>,
    pub(crate) leaf_to_stable_map: HashMap<String, HashMap<usize, (usize, usize)>>,
}

#[derive(Debug, Clone)]
pub(crate) struct MappedJoinKey {
    pub(crate) expr: PhysicalExprRef,
    pub(crate) column_map: HashMap<usize, usize>,
}

impl MappedJoinKey {
    #[allow(dead_code)]
    pub(crate) fn new(expr: PhysicalExprRef, column_map: HashMap<usize, usize>) -> Self {
        Self { expr, column_map }
    }

    pub(crate) fn new_legacy(
        expr: PhysicalExprRef,
        legacy_column_map: HashMap<usize, (usize, usize)>,
        relations: &[JoinRelation],
    ) -> Result<Self> {
        let mut stable_column_map = HashMap::new();
        for (expr_idx, (relation_id, base_col_idx)) in legacy_column_map {
            if let Some(relation) = relations.get(relation_id) {
                if let Some(&stable_id) = relation.output_to_stable_id.get(&base_col_idx) {
                    stable_column_map.insert(expr_idx, stable_id);
                } else {
                    return Err(DataFusionError::Internal(format!(
                        "Cannot find stable_id for relation {} base_col_idx {}",
                        relation_id, base_col_idx
                    )));
                }
            } else {
                return Err(DataFusionError::Internal(format!(
                    "Cannot find relation with id {}",
                    relation_id
                )));
            }
        }
        Ok(Self {
            expr,
            column_map: stable_column_map,
        })
    }

    pub(crate) fn rewrite_for_target(
        &self,
        target_stable_to_physical: &HashMap<usize, usize>,
        column_catalog: &ColumnCatalog,
    ) -> Result<PhysicalExprRef> {
        struct IndexRewriter<'a> {
            key_column_map: &'a HashMap<usize, usize>,
            target_stable_to_physical: &'a HashMap<usize, usize>,
            column_catalog: &'a ColumnCatalog,
        }

        impl<'a> TreeNodeRewriter for IndexRewriter<'a> {
            type Node = PhysicalExprRef;

            fn f_up(&mut self, expr: Self::Node) -> Result<Transformed<Self::Node>> {
                if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                    let original_idx = col.index();
                    if let Some(&stable_id) = self.key_column_map.get(&original_idx) {
                        if let Some(&new_idx) = self.target_stable_to_physical.get(&stable_id) {
                            let identity = self.column_catalog.get(stable_id).ok_or_else(|| {
                                DataFusionError::Internal(format!(
                                    "Cannot find column identity for stable_id {}",
                                    stable_id
                                ))
                            })?;

                            return Ok(Transformed::yes(Arc::new(Column::new(
                                &identity.original_name,
                                new_idx,
                            ))));
                        } else {
                            return Err(DataFusionError::Internal(format!(
                                "Column rewrite failed: Cannot find mapping for column '{}' (stable_id={}) in target_stable_to_physical. Available mappings: {:?}",
                                col.name(), stable_id, self.target_stable_to_physical
                            )));
                        }
                    } else {
                        return Err(DataFusionError::Internal(format!(
                            "Column rewrite failed: Cannot find column '{}' with index {} in key_column_map. Available mappings: {:?}",
                            col.name(), original_idx, self.key_column_map
                        )));
                    }
                }
                Ok(Transformed::no(expr))
            }
        }

        let mut rewriter = IndexRewriter {
            key_column_map: &self.column_map,
            target_stable_to_physical,
            column_catalog,
        };
        self.expr.clone().rewrite(&mut rewriter).map(|t| t.data)
    }

    pub(crate) fn rewrite_for_target_legacy(
        &self,
        target_inv_map: &HashMap<(usize, usize), usize>,
        _target_schema: &datafusion::arrow::datatypes::Schema,
        relations: &[JoinRelation],
        column_catalog: &ColumnCatalog,
    ) -> Result<PhysicalExprRef> {
        let mut stable_to_physical = HashMap::new();
        for (&(relation_id, base_col_idx), &physical_idx) in target_inv_map {
            if let Some(relation) = relations.get(relation_id) {
                if let Some(&stable_id) = relation.output_to_stable_id.get(&base_col_idx) {
                    stable_to_physical.insert(stable_id, physical_idx);
                }
            }
        }

        self.rewrite_for_target(&stable_to_physical, column_catalog)
    }

    pub(crate) fn get_relation_ids(&self, relations: &[JoinRelation]) -> Vec<usize> {
        let mut relation_ids = Vec::new();
        for &stable_id in self.column_map.values() {
            for relation in relations {
                if relation
                    .output_to_stable_id
                    .values()
                    .any(|&id| id == stable_id)
                {
                    relation_ids.push(relation.id);
                    break;
                }
            }
        }
        relation_ids.sort_unstable();
        relation_ids.dedup();
        relation_ids
    }

    pub(crate) fn to_legacy_column_map(
        &self,
        relations: &[JoinRelation],
    ) -> HashMap<usize, (usize, usize)> {
        let mut legacy_map = HashMap::new();
        for (&expr_idx, &stable_id) in &self.column_map {
            for relation in relations {
                for (&base_col_idx, &rel_stable_id) in &relation.output_to_stable_id {
                    if rel_stable_id == stable_id {
                        legacy_map.insert(expr_idx, (relation.id, base_col_idx));
                        break;
                    }
                }
            }
        }
        legacy_map
    }
}

#[derive(Debug, Clone)]
pub(crate) struct JoinNode {
    pub(crate) leaves: Arc<Vec<usize>>,

    pub(crate) children: Vec<Arc<JoinNode>>,

    pub(crate) join_conditions: Vec<(MappedJoinKey, MappedJoinKey)>,

    pub(crate) join_filter: Option<PhysicalExprRef>,

    pub(crate) join_filter_keys: Vec<MappedJoinKey>,

    pub(crate) join_type: JoinType,

    pub(crate) partition_mode: PartitionMode,

    pub(crate) stats: Statistics,

    pub(crate) cost: f64,
}

impl JoinNode {
    pub fn build_prototype_plan_recursive(
        &self,
        relations: &[JoinRelation],
        column_catalog: &ColumnCatalog,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if self.children.is_empty() {
            assert_eq!(self.leaves.len(), 1, "Leaf node must have one relation");
            let relation_id = self.leaves[0];
            let relation = relations
                .iter()
                .find(|r| r.id == relation_id)
                .ok_or_else(|| {
                    DataFusionError::Internal(format!("Relation with id {} not found", relation_id))
                })?;

            return Ok(relation.plan.clone());
        }

        assert_eq!(self.children.len(), 2, "JoinNode must have 2 children");

        let build_child = &self.children[0];
        let probe_child = &self.children[1];

        let build_plan = build_child.build_prototype_plan_recursive(relations, column_catalog)?;
        let probe_plan = probe_child.build_prototype_plan_recursive(relations, column_catalog)?;

        let on = self.create_placeholder_join_conditions(relations, column_catalog)?;

        let joined_plan: Arc<dyn ExecutionPlan> = if on.is_empty() {
            use datafusion::physical_plan::joins::CrossJoinExec;
            Arc::new(CrossJoinExec::new(build_plan, probe_plan))
        } else {
            let filter_wrapper = {
                use datafusion::physical_plan::joins::utils::JoinFilter;
                if !self.join_filter_keys.is_empty() {
                    use datafusion::logical_expr::Operator;
                    use datafusion::physical_expr::expressions::BinaryExpr;

                    let iter = self
                        .join_filter_keys
                        .iter()
                        .map(|k| self.create_placeholder_expr_for_key(k, relations, column_catalog))
                        .peekable();
                    let mut combined: Option<PhysicalExprRef> = None;
                    for expr_res in iter {
                        let expr = expr_res?;
                        combined = Some(if let Some(acc) = combined.take() {
                            Arc::new(BinaryExpr::new(acc, Operator::And, expr))
                        } else {
                            expr
                        });
                    }
                    combined.map(|e| {
                        JoinFilter::new(
                            e,
                            vec![],
                            Arc::new(datafusion::arrow::datatypes::Schema::empty()),
                        )
                    })
                } else {
                    self.join_filter.as_ref().map(|filter_expr| {
                        JoinFilter::new(
                            filter_expr.clone(),
                            vec![],
                            Arc::new(datafusion::arrow::datatypes::Schema::empty()),
                        )
                    })
                }
            };

            Arc::new(HashJoinExec::try_new(
                build_plan,
                probe_plan,
                on,
                filter_wrapper,
                &self.join_type,
                None,
                self.partition_mode,
                NullEquality::NullEqualsNull,
            )?)
        };

        Ok(joined_plan)
    }

    fn create_placeholder_join_conditions(
        &self,
        relations: &[JoinRelation],
        column_catalog: &ColumnCatalog,
    ) -> Result<Vec<(PhysicalExprRef, PhysicalExprRef)>> {
        let mut on_conditions = vec![];

        for (key1, key2) in &self.join_conditions {
            let key1_rels = key1.get_relation_ids(relations);

            if is_subset(&key1_rels, &self.children[0].leaves) {
                let build_expr =
                    self.create_placeholder_expr_for_key(key1, relations, column_catalog)?;
                let probe_expr =
                    self.create_placeholder_expr_for_key(key2, relations, column_catalog)?;
                on_conditions.push((build_expr, probe_expr));
            } else {
                let build_expr =
                    self.create_placeholder_expr_for_key(key2, relations, column_catalog)?;
                let probe_expr =
                    self.create_placeholder_expr_for_key(key1, relations, column_catalog)?;
                on_conditions.push((build_expr, probe_expr));
            }
        }

        Ok(on_conditions)
    }

    fn create_placeholder_expr_for_key(
        &self,
        key: &MappedJoinKey,
        relations: &[JoinRelation],
        column_catalog: &ColumnCatalog,
    ) -> Result<PhysicalExprRef> {
        use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
        use datafusion::physical_expr::expressions::Column;

        let legacy_column_map = key.to_legacy_column_map(relations);
        struct PlaceholderRewriter<'a> {
            key_column_map: &'a HashMap<usize, (usize, usize)>,
            relations: &'a [JoinRelation],
            column_catalog: &'a ColumnCatalog,
        }

        impl<'a> TreeNodeRewriter for PlaceholderRewriter<'a> {
            type Node = PhysicalExprRef;

            fn f_up(&mut self, expr: Self::Node) -> Result<Transformed<Self::Node>> {
                if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                    let original_idx = col.index();
                    if let Some(&(rel_id, base_idx)) = self.key_column_map.get(&original_idx) {
                        let relation =
                            self.relations
                                .iter()
                                .find(|r| r.id == rel_id)
                                .ok_or_else(|| {
                                    DataFusionError::Internal(format!(
                                        "Relation {} not found",
                                        rel_id
                                    ))
                                })?;

                        let stable_id =
                            relation.output_to_stable_id.get(&base_idx).ok_or_else(|| {
                                DataFusionError::Internal(format!(
                                    "Cannot find stable_id for relation {} base_col_idx {}",
                                    rel_id, base_idx
                                ))
                            })?;

                        let stable_id_val = *stable_id;
                        let identity = self.column_catalog.get(stable_id_val).ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "Cannot find column identity for stable_id {}",
                                stable_id_val
                            ))
                        })?;

                        let placeholder = placeholder_column(
                            *stable_id,
                            identity.original_name.clone(),
                            identity.data_type.clone(),
                        );

                        return Ok(Transformed::yes(placeholder));
                    } else {
                        return Err(DataFusionError::Internal(format!(
                            "Column '{}' with index {} not found in key column map",
                            col.name(),
                            original_idx
                        )));
                    }
                }
                Ok(Transformed::no(expr))
            }
        }

        let mut rewriter = PlaceholderRewriter {
            key_column_map: &legacy_column_map,
            relations,
            column_catalog,
        };

        key.expr.clone().rewrite(&mut rewriter).map(|t| t.data)
    }

    pub fn build_plan_recursive(
        &self,
        relations: &[JoinRelation],
        column_catalog: &ColumnCatalog,
    ) -> Result<PlanWithColumnMap> {
        if self.children.is_empty() {
            assert_eq!(self.leaves.len(), 1, "Leaf node must have one relation");
            let relation_id = self.leaves[0];
            let plan = relations
                .iter()
                .find(|r| r.id == relation_id)
                .ok_or_else(|| {
                    DataFusionError::Internal(format!("Relation with id {} not found", relation_id))
                })?
                .plan
                .clone();

            let column_map = (0..plan.schema().fields().len())
                .map(|i| (i, (relation_id, i)))
                .collect();

            return Ok((plan, column_map));
        }

        assert_eq!(self.children.len(), 2, "JoinNode must have 2 children");

        let build_child = &self.children[0];
        let probe_child = &self.children[1];

        let (build_plan, build_map) =
            build_child.build_plan_recursive(relations, column_catalog)?;
        let (probe_plan, probe_map) =
            probe_child.build_plan_recursive(relations, column_catalog)?;

        let on = self.recreate_join_conditions(
            &build_child.leaves,
            &build_map,
            &probe_map,
            &build_plan,
            &probe_plan,
            relations,
            column_catalog,
        )?;

        let joined_plan: Arc<dyn ExecutionPlan> = if on.is_empty() {
            use datafusion::physical_plan::joins::CrossJoinExec;
            Arc::new(CrossJoinExec::new(build_plan, probe_plan))
        } else {
            let join_filter = if let Some(filter_expr) = &self.join_filter {
                let mut combined_column_map = HashMap::new();

                for (&build_idx, &(rel_id, base_idx)) in build_map.iter() {
                    combined_column_map.insert((rel_id, base_idx), build_idx);
                }

                let build_plan_cols = build_plan.schema().fields().len();
                for (&probe_idx, &(rel_id, base_idx)) in probe_map.iter() {
                    combined_column_map.insert((rel_id, base_idx), build_plan_cols + probe_idx);
                }

                let mut combined_fields = build_plan.schema().fields().to_vec();
                combined_fields.extend(probe_plan.schema().fields().iter().cloned());
                let combined_schema =
                    Arc::new(datafusion::arrow::datatypes::Schema::new(combined_fields));

                let mut filter_column_map = HashMap::new();
                use datafusion::physical_expr::utils::collect_columns;
                let filter_columns = collect_columns(filter_expr);

                for col in filter_columns {
                    if let Some(_new_idx) = combined_column_map.get(&(0, col.index())) {
                        filter_column_map.insert(col.index(), (0, col.index()));
                    }
                }

                if !filter_column_map.is_empty() {
                    let temp_filter_key =
                        MappedJoinKey::new_legacy(filter_expr.clone(), filter_column_map, &[])?;
                    temp_filter_key
                        .rewrite_for_target_legacy(
                            &combined_column_map,
                            combined_schema.as_ref(),
                            &[],
                            &ColumnCatalog::new(),
                        )
                        .ok()
                } else {
                    None
                }
            } else {
                None
            };

            let filter_wrapper = if let Some(filter_expr) = join_filter {
                use datafusion::physical_plan::joins::utils::JoinFilter;
                Some(JoinFilter::new(
                    filter_expr,
                    vec![],
                    Arc::new(datafusion::arrow::datatypes::Schema::empty()),
                ))
            } else {
                None
            };

            Arc::new(HashJoinExec::try_new(
                build_plan,
                probe_plan,
                on,
                filter_wrapper,
                &self.join_type,
                None,
                self.partition_mode,
                NullEquality::NullEqualsNull,
            )?)
        };

        let mut new_column_map = build_map;
        let build_plan_num_cols = joined_plan.children()[0].schema().fields().len();
        for (probe_idx, origin) in probe_map {
            new_column_map.insert(build_plan_num_cols + probe_idx, origin);
        }

        Ok((joined_plan, new_column_map))
    }

    pub(crate) fn recreate_join_conditions(
        &self,
        build_leaves: &[usize],
        build_map: &HashMap<usize, (usize, usize)>,
        probe_map: &HashMap<usize, (usize, usize)>,
        build_plan: &Arc<dyn ExecutionPlan>,
        probe_plan: &Arc<dyn ExecutionPlan>,
        relations: &[JoinRelation],
        column_catalog: &ColumnCatalog,
    ) -> Result<Vec<(PhysicalExprRef, PhysicalExprRef)>> {
        Self::recreate_on_conditions(
            &self.join_conditions,
            build_leaves,
            build_plan,
            probe_plan,
            build_map,
            probe_map,
            relations,
            column_catalog,
        )
    }

    pub(crate) fn recreate_on_conditions(
        join_conditions: &[(MappedJoinKey, MappedJoinKey)],
        build_leaves: &[usize],
        build_plan: &Arc<dyn ExecutionPlan>,
        probe_plan: &Arc<dyn ExecutionPlan>,
        build_map: &HashMap<usize, (usize, usize)>,
        probe_map: &HashMap<usize, (usize, usize)>,
        relations: &[JoinRelation],
        column_catalog: &ColumnCatalog,
    ) -> Result<Vec<(PhysicalExprRef, PhysicalExprRef)>> {
        let mut probe_leaves: Vec<usize> = probe_map
            .values()
            .map(|(rel_id, _)| *rel_id)
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        probe_leaves.sort_unstable();

        let build_rev_map: HashMap<(usize, usize), usize> =
            build_map.iter().map(|(k, v)| (*v, *k)).collect();
        let probe_rev_map: HashMap<(usize, usize), usize> =
            probe_map.iter().map(|(k, v)| (*v, *k)).collect();

        let build_leaves_set: HashSet<usize> = build_leaves.iter().cloned().collect();
        let probe_leaves_set: HashSet<usize> = probe_leaves.iter().cloned().collect();

        let mut on_conditions = vec![];

        for (key1, key2) in join_conditions.iter() {
            let key1_rels_set: HashSet<usize> =
                key1.get_relation_ids(relations).into_iter().collect();
            let key2_rels_set: HashSet<usize> =
                key2.get_relation_ids(relations).into_iter().collect();

            if key1_rels_set.is_subset(&build_leaves_set)
                && key2_rels_set.is_subset(&probe_leaves_set)
            {
                let build_expr = key1.rewrite_for_target_legacy(&build_rev_map, &build_plan.schema(), relations, column_catalog).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "Failed to rewrite build key (case 1): {}. Build key: {:?}, Build reverse map: {:?}",
                        e, key1.column_map, build_rev_map
                    ))
                })?;
                let probe_expr = key2.rewrite_for_target_legacy(&probe_rev_map, &probe_plan.schema(), relations, column_catalog).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "Failed to rewrite probe key (case 1): {}. Probe key: {:?}, Probe reverse map: {:?}",
                        e, key2.column_map, probe_rev_map
                    ))
                })?;
                on_conditions.push((build_expr, probe_expr));
            } else if key2_rels_set.is_subset(&build_leaves_set)
                && key1_rels_set.is_subset(&probe_leaves_set)
            {
                let build_expr = key2.rewrite_for_target_legacy(&build_rev_map, &build_plan.schema(), relations, column_catalog).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "Failed to rewrite build key (case 2): {}. Build key: {:?}, Build reverse map: {:?}",
                        e, key2.column_map, build_rev_map
                    ))
                })?;
                let probe_expr = key1.rewrite_for_target_legacy(&probe_rev_map, &probe_plan.schema(), relations, column_catalog).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "Failed to rewrite probe key (case 2): {}. Probe key: {:?}, Probe reverse map: {:?}",
                        e, key1.column_map, probe_rev_map
                    ))
                })?;
                on_conditions.push((build_expr, probe_expr));
            } else {
                continue;
            }
        }

        Ok(on_conditions)
    }
}

#[derive(Debug, Default)]
pub(crate) struct RelationSetTree {
    root: RelationSetNode,
}

impl RelationSetTree {
    pub(crate) fn new() -> Self {
        Self {
            root: RelationSetNode::default(),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct RelationSetNode {
    relations: Option<Arc<Vec<usize>>>,
    children: HashMap<usize, RelationSetNode>,
}

impl RelationSetTree {
    pub fn get_relation_set(&mut self, ids: &HashSet<usize>) -> Arc<Vec<usize>> {
        if ids.is_empty() {
            return Arc::new(vec![]);
        }

        let mut sorted_ids: Vec<usize> = ids.iter().copied().collect();
        sorted_ids.sort_unstable();

        let mut current_node = &mut self.root;
        for id in &sorted_ids {
            current_node = current_node.children.entry(*id).or_default();
        }

        current_node
            .relations
            .get_or_insert_with(|| Arc::new(sorted_ids))
            .clone()
    }
}
