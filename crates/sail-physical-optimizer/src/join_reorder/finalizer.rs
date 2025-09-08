use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion::common::{DataFusionError, JoinSide};
use datafusion::error::Result;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr::{PhysicalExpr, PhysicalExprRef};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::utils::ColumnIndex;
use datafusion::physical_plan::joins::{CrossJoinExec, HashJoinExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;

use crate::join_reorder::placeholder::PlaceholderColumn;
use crate::join_reorder::plan::{ColumnCatalog, JoinRelation};

type FinalizedPlanWithMap = (Arc<dyn ExecutionPlan>, HashMap<usize, (usize, usize)>);

pub(crate) struct PlanFinalizer<'a> {
    column_catalog: &'a ColumnCatalog,
    relations: &'a [JoinRelation],
}

impl<'a> PlanFinalizer<'a> {
    pub(crate) fn new(column_catalog: &'a ColumnCatalog, relations: &'a [JoinRelation]) -> Self {
        Self {
            column_catalog,
            relations,
        }
    }

    fn convert_legacy_map_to_stable_map(
        &self,
        legacy_map: &HashMap<(usize, usize), usize>,
        relations: &[JoinRelation],
    ) -> Result<HashMap<usize, usize>> {
        let mut stable_map = HashMap::new();
        for (&(relation_id, base_col_idx), &physical_idx) in legacy_map {
            if let Some(relation) = relations.get(relation_id) {
                if let Some(&stable_id) = relation.output_to_stable_id.get(&base_col_idx) {
                    stable_map.insert(stable_id, physical_idx);
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
        Ok(stable_map)
    }

    fn create_plan_identifier(&self, plan: &Arc<dyn ExecutionPlan>) -> String {
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

    pub(crate) fn finalize(
        &self,
        proto_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (final_plan, _map) = self.finalize_recursive(proto_plan)?;

        Ok(final_plan)
    }

    fn finalize_recursive(&self, plan: Arc<dyn ExecutionPlan>) -> Result<FinalizedPlanWithMap> {
        let mut finalized_children_with_maps = Vec::new();
        for child in plan.children() {
            finalized_children_with_maps.push(self.finalize_recursive(child.clone())?);
        }

        let mut stable_to_physical = HashMap::new();
        let mut current_offset = 0;
        for (child_plan, child_output_map) in &finalized_children_with_maps {
            for (local_idx, stable_id) in child_output_map {
                stable_to_physical.insert(*stable_id, current_offset + local_idx);
            }
            current_offset += child_plan.schema().fields().len();
        }

        let children_plans: Vec<Arc<dyn ExecutionPlan>> = finalized_children_with_maps
            .iter()
            .map(|(p, _)| p.clone())
            .collect();
        let new_plan = plan.with_new_children(children_plans)?;
        let rewritten_plan = self.rewrite_plan_expressions(new_plan, &stable_to_physical)?;

        let output_map = self.calculate_output_map(&rewritten_plan)?;

        Ok((rewritten_plan, output_map))
    }

    fn calculate_output_map(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<HashMap<usize, (usize, usize)>> {
        if plan.children().is_empty() {
            if let Some(relation) = self.find_relation_containing_leaf(plan) {
                let leaf_key = self.create_plan_identifier(plan);
                if let Some(mapping) = relation.leaf_to_stable_map.get(&leaf_key) {
                    return Ok(mapping.clone());
                }
            }
            return Err(DataFusionError::Internal(format!(
                "calculate_output_map: Could not find base relation for leaf '{}'",
                plan.name()
            )));
        }

        if let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() {
            let left_map = self.calculate_output_map(join.left())?;
            let right_map = self.calculate_output_map(join.right())?;

            let mut combined_map = left_map;
            let left_len = join.left().schema().fields().len();
            for (right_idx, stable_id) in right_map {
                combined_map.insert(left_len + right_idx, stable_id);
            }
            return Ok(combined_map);
        }

        if let Some(join) = plan.as_any().downcast_ref::<CrossJoinExec>() {
            let left_map = self.calculate_output_map(&join.left().clone())?;
            let right_map = self.calculate_output_map(&join.right().clone())?;

            let mut combined_map = left_map;
            let left_len = join.left().schema().fields().len();
            for (right_idx, stable_id) in right_map {
                combined_map.insert(left_len + right_idx, stable_id);
            }
            return Ok(combined_map);
        }

        if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
            let input_map = self.calculate_output_map(projection.input())?;
            let mut projection_map = HashMap::new();
            for (i, (expr, _name)) in projection.expr().iter().enumerate() {
                if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                    if let Some(&stable_id) = input_map.get(&col.index()) {
                        projection_map.insert(i, stable_id);
                    }
                } else {
                    return Err(DataFusionError::Internal(
                        "Finalized projection has non-column expr".to_string(),
                    ));
                }
            }
            return Ok(projection_map);
        }

        if let Some(filter) = plan.as_any().downcast_ref::<FilterExec>() {
            return self.calculate_output_map(filter.input());
        }

        if plan.children().len() == 1 {
            return self.calculate_output_map(plan.children()[0]);
        }

        Err(DataFusionError::Internal(format!(
            "calculate_output_map not implemented for {}",
            plan.name()
        )))
    }

    fn find_relation_containing_leaf(
        &self,
        leaf_plan: &Arc<dyn ExecutionPlan>,
    ) -> Option<&JoinRelation> {
        self.relations
            .iter()
            .find(|&relation| self.plan_contains_leaf(&relation.plan, leaf_plan))
    }

    fn plan_contains_leaf(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        target_leaf: &Arc<dyn ExecutionPlan>,
    ) -> bool {
        if self.create_plan_identifier(plan) == self.create_plan_identifier(target_leaf) {
            return true;
        }

        if plan.children().is_empty() {
            return false;
        }

        for child in plan.children() {
            if self.plan_contains_leaf(child, target_leaf) {
                return true;
            }
        }

        false
    }

    fn expression_belongs_to_mapping(
        &self,
        expr: &PhysicalExprRef,
        mapping: &HashMap<usize, usize>,
    ) -> Result<bool> {
        let mut belongs = true;
        expr.apply(|e| {
            if let Some(placeholder) = e.as_any().downcast_ref::<PlaceholderColumn>() {
                if !mapping.contains_key(&placeholder.stable_id) {
                    belongs = false;
                    return Ok(datafusion::common::tree_node::TreeNodeRecursion::Stop);
                }
            }
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        })?;

        if !belongs {
            return Ok(false);
        }

        let mut has_other_columns = false;
        expr.apply(|e| {
            if e.as_any()
                .is::<datafusion::physical_expr::expressions::Column>()
            {
                has_other_columns = true;
                return Ok(datafusion::common::tree_node::TreeNodeRecursion::Stop);
            }
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        })?;

        if has_other_columns {
            return Err(DataFusionError::Internal(
                "Expression contains unexpected concrete Column during finalization".to_string(),
            ));
        }

        Ok(true)
    }

    fn rewrite_plan_expressions(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        stable_to_physical: &HashMap<(usize, usize), usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() {
            let left_map_phys_to_stable = self.calculate_output_map(join.left())?;
            let right_map_phys_to_stable = self.calculate_output_map(join.right())?;

            let left_stable_to_local: HashMap<(usize, usize), usize> = left_map_phys_to_stable
                .into_iter()
                .map(|(i, s)| (s, i))
                .collect();
            let right_stable_to_local: HashMap<(usize, usize), usize> = right_map_phys_to_stable
                .into_iter()
                .map(|(i, s)| (s, i))
                .collect();

            let left_stable_map =
                self.convert_legacy_map_to_stable_map(&left_stable_to_local, self.relations)?;
            let right_stable_map =
                self.convert_legacy_map_to_stable_map(&right_stable_to_local, self.relations)?;

            let mut left_rewriter = PlaceholderRewriter {
                stable_to_physical: &left_stable_map,
                column_catalog: self.column_catalog,
                has_transformed: false,
            };
            let mut right_rewriter = PlaceholderRewriter {
                stable_to_physical: &right_stable_map,
                column_catalog: self.column_catalog,
                has_transformed: false,
            };

            let mut new_on = Vec::new();
            for (expr1, expr2) in join.on() {
                let expr1_belongs_to_left =
                    self.expression_belongs_to_mapping(expr1, &left_stable_map)?;

                if expr1_belongs_to_left {
                    let new_left = expr1.clone().rewrite(&mut left_rewriter)?.data;
                    let new_right = expr2.clone().rewrite(&mut right_rewriter)?.data;
                    new_on.push((new_left, new_right));
                } else {
                    let new_left = expr2.clone().rewrite(&mut left_rewriter)?.data;
                    let new_right = expr1.clone().rewrite(&mut right_rewriter)?.data;
                    new_on.push((new_left, new_right));
                }
            }

            let new_filter = if let Some(filter) = join.filter() {
                let mut combined_stable_to_physical: HashMap<(usize, usize), usize> =
                    left_stable_to_local.clone();
                let left_len = join.left().schema().fields().len();
                for (stable, idx) in right_stable_to_local.iter() {
                    combined_stable_to_physical.insert(*stable, left_len + *idx);
                }

                let combined_stable_map = self.convert_legacy_map_to_stable_map(
                    &combined_stable_to_physical,
                    self.relations,
                )?;

                let mut filter_rewriter = PlaceholderRewriter {
                    stable_to_physical: &combined_stable_map,
                    column_catalog: self.column_catalog,
                    has_transformed: false,
                };

                let rewritten_expr = filter
                    .expression()
                    .clone()
                    .rewrite(&mut filter_rewriter)?
                    .data;

                let required_columns = collect_columns(&rewritten_expr);
                if required_columns.is_empty() {
                    log::warn!("Join filter rewrite resulted in an expression with no column references. Original: '{}'", filter.expression());
                }

                let mut column_indices = Vec::new();
                let mut intermediate_fields = Vec::new();
                let left_schema = join.left().schema();
                let right_schema = join.right().schema();

                let mut physical_to_intermediate_map = HashMap::new();

                for col in required_columns {
                    let physical_idx = col.index();
                    if physical_to_intermediate_map.contains_key(&physical_idx) {
                        continue;
                    }

                    let new_intermediate_idx = column_indices.len();
                    physical_to_intermediate_map.insert(physical_idx, new_intermediate_idx);

                    if physical_idx < left_len {
                        let stable_id = self
                            .calculate_output_map(join.left())?
                            .get(&physical_idx)
                            .copied()
                            .ok_or_else(|| {
                                DataFusionError::Internal(
                                    "Stable ID not found for left filter column".to_string(),
                                )
                            })?;
                        let relation = self.relations.get(stable_id.0).ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "Relation with id {} not found",
                                stable_id.0
                            ))
                        })?;
                        let actual_stable_id = relation
                            .output_to_stable_id
                            .get(&stable_id.1)
                            .ok_or_else(|| {
                                DataFusionError::Internal(format!(
                                    "Stable ID not found for relation {} column {}",
                                    stable_id.0, stable_id.1
                                ))
                            })?;
                        let identity =
                            self.column_catalog.get(*actual_stable_id).ok_or_else(|| {
                                DataFusionError::Internal(format!(
                                    "Column identity not found for stable_id {}",
                                    actual_stable_id
                                ))
                            })?;
                        let name = identity.original_name.clone();
                        let data_type = identity.data_type.clone();

                        column_indices.push(ColumnIndex {
                            index: physical_idx,
                            side: JoinSide::Left,
                        });
                        intermediate_fields.push(datafusion::arrow::datatypes::Field::new(
                            name,
                            data_type,
                            left_schema.field(physical_idx).is_nullable(),
                        ));
                    } else {
                        let right_idx = physical_idx - left_len;
                        let stable_id = self
                            .calculate_output_map(join.right())?
                            .get(&right_idx)
                            .copied()
                            .ok_or_else(|| {
                                DataFusionError::Internal(
                                    "Stable ID not found for right filter column".to_string(),
                                )
                            })?;
                        let relation = self.relations.get(stable_id.0).ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "Relation with id {} not found",
                                stable_id.0
                            ))
                        })?;
                        let actual_stable_id = relation
                            .output_to_stable_id
                            .get(&stable_id.1)
                            .ok_or_else(|| {
                                DataFusionError::Internal(format!(
                                    "Stable ID not found for relation {} column {}",
                                    stable_id.0, stable_id.1
                                ))
                            })?;
                        let identity =
                            self.column_catalog.get(*actual_stable_id).ok_or_else(|| {
                                DataFusionError::Internal(format!(
                                    "Column identity not found for stable_id {}",
                                    actual_stable_id
                                ))
                            })?;
                        let name = identity.original_name.clone();
                        let data_type = identity.data_type.clone();
                        column_indices.push(ColumnIndex {
                            index: right_idx,
                            side: JoinSide::Right,
                        });
                        intermediate_fields.push(datafusion::arrow::datatypes::Field::new(
                            name,
                            data_type,
                            right_schema.field(right_idx).is_nullable(),
                        ));
                    }
                }

                let final_filter_expr = rewritten_expr
                    .transform(
                        &|e: Arc<dyn PhysicalExpr>| -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
                            if let Some(col) = e.as_any().downcast_ref::<Column>() {
                                let intermediate_idx = physical_to_intermediate_map
                                    .get(&col.index())
                                    .ok_or_else(|| {
                                        DataFusionError::Internal(format!(
                                            "Column with index {} not found in intermediate map",
                                            col.index()
                                        ))
                                    })?;
                                Ok(Transformed::yes(Arc::new(Column::new(
                                    col.name(),
                                    *intermediate_idx,
                                ))))
                            } else {
                                Ok(Transformed::no(e))
                            }
                        },
                    )?
                    .data;

                let intermediate_schema = Arc::new(Schema::new(intermediate_fields));

                Some(datafusion::physical_plan::joins::utils::JoinFilter::new(
                    final_filter_expr,
                    column_indices,
                    intermediate_schema,
                ))
            } else {
                if join.filter().is_some() {
                    log::warn!(
                        "Original plan had a join filter, but it was lost during finalization!"
                    );
                }
                None
            };

            let new_join = HashJoinExec::try_new(
                join.left().clone(),
                join.right().clone(),
                new_on,
                new_filter,
                join.join_type(),
                join.projection.clone(),
                *join.partition_mode(),
                datafusion::common::NullEquality::NullEqualsNull,
            )?;
            Ok(Arc::new(new_join))
        } else if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
            let stable_map =
                self.convert_legacy_map_to_stable_map(stable_to_physical, self.relations)?;

            let mut rewriter = PlaceholderRewriter {
                stable_to_physical: &stable_map,
                column_catalog: self.column_catalog,
                has_transformed: false,
            };

            let mut new_exprs = Vec::new();
            for (expr, name) in projection.expr() {
                let new_expr = expr.clone().rewrite(&mut rewriter)?.data;
                let new_name = if name.starts_with("_reorder_filter_col_") {
                    if let Some(col) = new_expr.as_any().downcast_ref::<Column>() {
                        col.name().to_string()
                    } else {
                        name.clone()
                    }
                } else {
                    name.clone()
                };
                new_exprs.push((new_expr, new_name));
            }
            let new_projection = ProjectionExec::try_new(new_exprs, projection.input().clone())?;
            Ok(Arc::new(new_projection))
        } else if let Some(filter) = plan.as_any().downcast_ref::<FilterExec>() {
            let stable_map =
                self.convert_legacy_map_to_stable_map(stable_to_physical, self.relations)?;

            let mut rewriter = PlaceholderRewriter {
                stable_to_physical: &stable_map,
                column_catalog: self.column_catalog,
                has_transformed: false,
            };

            let new_predicate = filter.predicate().clone().rewrite(&mut rewriter)?.data;
            let new_filter = FilterExec::try_new(new_predicate, filter.input().clone())?;
            Ok(Arc::new(new_filter))
        } else {
            Ok(plan)
        }
    }
}

struct PlaceholderRewriter<'a> {
    stable_to_physical: &'a HashMap<usize, usize>,
    column_catalog: &'a ColumnCatalog,
    has_transformed: bool,
}

impl<'a> TreeNodeRewriter for PlaceholderRewriter<'a> {
    type Node = PhysicalExprRef;

    fn f_up(&mut self, expr: Self::Node) -> Result<Transformed<Self::Node>> {
        if let Some(placeholder) = expr.as_any().downcast_ref::<PlaceholderColumn>() {
            let stable_id = placeholder.stable_id;

            let physical_index = self
                .stable_to_physical
                .get(&stable_id)
                .copied()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Cannot find physical index for stable_id {}",
                        stable_id
                    ))
                })?;

            let identity = self.column_catalog.get(stable_id).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Cannot find column identity for stable_id {}",
                    stable_id
                ))
            })?;

            let column = Arc::new(Column::new(&identity.original_name, physical_index));
            self.has_transformed = true;
            Ok(Transformed::yes(column))
        } else {
            Ok(Transformed::no(expr))
        }
    }
}
