use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::PartitioningSatisfaction;
use datafusion::physical_optimizer::join_selection::JoinSelection;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizerContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;

use crate::PhysicalOptimizerRule;
use crate::join_reorder::JoinReorderOptions;
use crate::join_reorder::builder::{ColumnMap, GraphBuilder};
use crate::join_reorder::cardinality_estimator::CardinalityEstimator;
use crate::join_reorder::cost_model::CostModel;
use crate::join_reorder::dp_plan::{DPPlan, PlanType};
use crate::join_reorder::graph::{QueryGraph, RelationNode};
use crate::join_reorder::join_set::JoinSet;
use crate::join_reorder::reconstructor::PlanReconstructor;

type ReconstructedPlan = (Arc<dyn ExecutionPlan>, ColumnMap);

struct PhysicalCandidate {
    cost: f64,
    root: Arc<DPPlan>,
    table: HashMap<JoinSet, Arc<DPPlan>>,
}

/// Refine the DP winner with bounded tree rotations priced after JoinSelection.
pub fn refine_join_tree(
    root: &Arc<DPPlan>,
    table: &HashMap<JoinSet, Arc<DPPlan>>,
    graph: &QueryGraph,
    target: &ColumnMap,
    options: &JoinReorderOptions,
    context: &dyn PhysicalOptimizerContext,
    initial: ReconstructedPlan,
) -> Result<ReconstructedPlan> {
    let selected = JoinSelection::new().optimize_with_context(Arc::clone(&initial.0), context)?;
    if !contains_partitioned_join(&selected) {
        return Ok(initial);
    }
    let Some(mut best_cost) =
        estimate_cost_after_selection(selected, &graph.relations, options, context)?
    else {
        return Ok(initial);
    };
    let mut best = initial;
    let mut frontier = vec![PhysicalCandidate {
        cost: best_cost,
        root: Arc::clone(root),
        table: table.clone(),
    }];
    let mut estimator = CardinalityEstimator::new(graph.clone());
    let mut seen = HashSet::new();
    let mut remaining = options.emit_threshold.min(32);
    seen.insert(tree_signature(root, table));

    while remaining > 0 && !frontier.is_empty() {
        frontier.sort_by(|a, b| a.cost.total_cmp(&b.cost));
        let current = frontier.remove(0);
        'rotations: for parent in reachable_joins(&current.root, &current.table) {
            let PlanType::Join {
                left_set,
                right_set,
                ..
            } = parent.plan_type
            else {
                continue;
            };
            for (pivot, sibling) in [(left_set, right_set), (right_set, left_set)] {
                let Some(pivot) = current.table.get(&pivot) else {
                    continue;
                };
                let PlanType::Join {
                    left_set: a,
                    right_set: b,
                    ..
                } = pivot.plan_type
                else {
                    continue;
                };
                for (moved, retained) in [(a, b), (b, a)] {
                    let Some(group) = rotation_join(moved, sibling, graph, &mut estimator)? else {
                        continue;
                    };
                    let Some(replacement) =
                        rotation_join(group.join_set, retained, graph, &mut estimator)?
                    else {
                        continue;
                    };
                    let mut candidate_table = current.table.clone();
                    candidate_table.insert(group.join_set, group);
                    candidate_table.insert(parent.join_set, replacement);
                    let candidate_root =
                        candidate_table
                            .get(&root.join_set)
                            .cloned()
                            .ok_or_else(|| {
                                DataFusionError::Internal(
                                    "Missing root during physical join refinement".into(),
                                )
                            })?;
                    if !seen.insert(tree_signature(&candidate_root, &candidate_table)) {
                        continue;
                    }
                    remaining -= 1;
                    let mut reconstructor = PlanReconstructor::new(&candidate_table, graph);
                    let config = context.config_options();
                    if !config.optimizer.repartition_joins
                        || config.execution.target_partitions == 1
                    {
                        reconstructor.partition_mode = PartitionMode::CollectLeft;
                    }
                    reconstructor.validate_reconstruction_plan(&candidate_root)?;
                    reconstructor.prepare_required_output_columns(&candidate_root, target)?;
                    let candidate = reconstructor.reconstruct(&candidate_root)?;
                    if let Some(cost) = estimate_selected_cost(
                        Arc::clone(&candidate.0),
                        &graph.relations,
                        options,
                        context,
                    )? {
                        if cost < best_cost * (1.0 - 1e-9) {
                            log::trace!(
                                "JoinReorder: Physical refinement reduced cost from {best_cost:.2} to {cost:.2}"
                            );
                            best_cost = cost;
                            best = candidate;
                        }
                        frontier.push(PhysicalCandidate {
                            cost,
                            root: candidate_root,
                            table: candidate_table,
                        });
                        frontier.sort_by(|a, b| a.cost.total_cmp(&b.cost));
                        frontier.truncate(4);
                    }
                    if remaining == 0 {
                        break 'rotations;
                    }
                }
            }
        }
    }
    Ok(best)
}

fn contains_partitioned_join(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.downcast_ref::<HashJoinExec>()
        .is_some_and(|join| *join.partition_mode() == PartitionMode::Partitioned)
        || plan.children().into_iter().any(contains_partitioned_join)
}

fn rotation_join(
    left: JoinSet,
    right: JoinSet,
    graph: &QueryGraph,
    estimator: &mut CardinalityEstimator,
) -> Result<Option<Arc<DPPlan>>> {
    let edges = graph.get_connecting_edge_indices(left, right);
    if edges.is_empty() || !graph.is_join_pair_legal(left, right, &edges) {
        return Ok(None);
    }
    let (left, right) = if graph.can_swap_physical_order(&edges)
        && estimator.estimate_cardinality(right)? < estimator.estimate_cardinality(left)?
    {
        (right, left)
    } else {
        (left, right)
    };
    Ok(Some(Arc::new(DPPlan::new_join(
        left,
        right,
        edges,
        0.0,
        estimator.estimate_cardinality(left | right)?,
    ))))
}

fn reachable_joins(root: &Arc<DPPlan>, table: &HashMap<JoinSet, Arc<DPPlan>>) -> Vec<Arc<DPPlan>> {
    let mut stack = vec![Arc::clone(root)];
    let mut joins = vec![];
    while let Some(plan) = stack.pop() {
        if let PlanType::Join {
            left_set,
            right_set,
            ..
        } = plan.plan_type
        {
            for child in [right_set, left_set] {
                if let Some(plan) = table.get(&child) {
                    stack.push(Arc::clone(plan));
                }
            }
            joins.push(plan);
        }
    }
    joins
}

fn tree_signature(
    root: &Arc<DPPlan>,
    table: &HashMap<JoinSet, Arc<DPPlan>>,
) -> Vec<(u64, u64, u64)> {
    let mut signature = reachable_joins(root, table)
        .into_iter()
        .filter_map(|plan| {
            if let PlanType::Join {
                left_set,
                right_set,
                ..
            } = plan.plan_type
            {
                Some((plan.join_set.bits(), left_set.bits(), right_set.bits()))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    signature.sort_unstable();
    signature
}

/// Evaluate the build sides and modes that downstream JoinSelection will actually choose.
pub fn estimate_selected_cost(
    plan: Arc<dyn ExecutionPlan>,
    relations: &[RelationNode],
    options: &JoinReorderOptions,
    context: &dyn PhysicalOptimizerContext,
) -> Result<Option<f64>> {
    let selected = JoinSelection::new().optimize_with_context(plan, context)?;
    estimate_cost_after_selection(selected, relations, options, context)
}

fn estimate_cost_after_selection(
    selected: Arc<dyn ExecutionPlan>,
    relations: &[RelationNode],
    options: &JoinReorderOptions,
    context: &dyn PhysicalOptimizerContext,
) -> Result<Option<f64>> {
    let config = context.config_options();
    let Some((graph, _)) = GraphBuilder::new(options.clone())
        .cache_relation_statistics(relations)
        .build(Arc::clone(&selected))?
    else {
        return Ok(None);
    };
    // A newly introduced non-hash join must not disappear into a zero-cost leaf.
    if graph.relation_count() != relations.len() || relations.len() <= 2 {
        return Ok(None);
    }
    let mut estimator = CardinalityEstimator::new(graph.clone());
    let model = CostModel::new(options, config);
    estimate_subtree(&selected, &graph, &mut estimator, &model, config).map(|plan| Some(plan.cost))
}

fn estimate_subtree(
    plan: &Arc<dyn ExecutionPlan>,
    graph: &QueryGraph,
    estimator: &mut CardinalityEstimator,
    model: &CostModel,
    config: &ConfigOptions,
) -> Result<DPPlan> {
    if let Some(relation) = graph.relations.iter().find(|r| Arc::ptr_eq(&r.plan, plan)) {
        return DPPlan::new_leaf(relation.relation_id, relation.initial_cardinality);
    }
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        return estimate_subtree(projection.input(), graph, estimator, model, config);
    }
    let join = plan.downcast_ref::<HashJoinExec>().ok_or_else(|| {
        DataFusionError::Internal(format!("Unexpected join-cost region node: {}", plan.name()))
    })?;
    let left = estimate_subtree(join.left(), graph, estimator, model, config)?;
    let right = estimate_subtree(join.right(), graph, estimator, model, config)?;
    let cardinality = estimator.estimate_cardinality(left.join_set | right.join_set)?;
    let key_count = join.on().len();
    let mut redistribution = 0.0;
    if *join.partition_mode() == PartitionMode::Partitioned
        && config.optimizer.repartition_joins
        && config.execution.target_partitions > 1
    {
        for ((input, rows), required) in [
            (join.left(), left.cardinality),
            (join.right(), right.cardinality),
        ]
        .into_iter()
        .zip(join.input_distribution_requirements().into_per_child())
        {
            let properties = input.properties();
            let partitioning = properties.output_partitioning();
            let satisfied = partitioning.partition_count() == config.execution.target_partitions
                && partitioning.satisfaction(&required, properties.equivalence_properties(), false)
                    == PartitioningSatisfaction::Exact;
            if !satisfied {
                // Use fixed Arrow widths where available and an estimate for variable values.
                let width = input
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| match field.data_type() {
                        DataType::Null => 0.0,
                        DataType::Boolean => 0.125,
                        DataType::FixedSizeBinary(size) => (*size).max(0) as f64,
                        data_type => data_type.primitive_width().unwrap_or(32) as f64,
                    })
                    .sum::<f64>()
                    .max(8.0);
                redistribution += model.redistribution_cost(rows, width, key_count);
            }
        }
    }
    let cost =
        model.compute_cost_for_distribution(&left, &right, cardinality, key_count, redistribution);
    Ok(DPPlan::new_join(
        left.join_set,
        right.join_set,
        vec![],
        cost,
        cardinality,
    ))
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::stats::Precision;
    use datafusion::common::{JoinSide, NullEquality, ScalarValue, Statistics};
    use datafusion::logical_expr::{JoinType, Operator};
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion::physical_optimizer::optimizer::ConfigOnlyContext;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::joins::NestedLoopJoinExec;
    use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
    use datafusion::physical_plan::operator_statistics::{
        ExtendedStatistics, StatisticsProvider, StatisticsRegistry, StatisticsResult,
    };
    use datafusion::physical_plan::test::exec::StatisticsExec;

    use super::*;

    fn selected_cost(
        plan: Arc<dyn ExecutionPlan>,
        relation_count: usize,
        options: &JoinReorderOptions,
        context: &dyn PhysicalOptimizerContext,
    ) -> Result<Option<f64>> {
        let (graph, _) = GraphBuilder::new(options.clone())
            .build(plan.clone())?
            .unwrap();
        assert_eq!(graph.relation_count(), relation_count);
        estimate_selected_cost(plan, &graph.relations, options, context)
    }

    #[test]
    fn selected_cost_rejects_a_new_uncosted_join_boundary() -> Result<()> {
        let addresses = relation(&["a_key"], 250_000, 250_000);
        let sales = relation(&["s_customer", "s_date"], 10_000_000, 500_000);
        let customers = relation(&["c_key", "c_address"], 500_000, 250_000);
        let dates = relation(&["d_key"], 100, 100);
        let original = join(
            join(
                addresses.clone(),
                customers.clone(),
                &[("a_key", "c_address")],
            )?,
            join(dates.clone(), sales.clone(), &[("d_key", "s_date")])?,
            &[("c_key", "s_customer")],
        )?;
        let options = JoinReorderOptions::default();
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a_key", 0)),
                Operator::Lt,
                Arc::new(Literal::new(ScalarValue::Int64(Some(50)))),
            )),
            Operator::Or,
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("s_customer", 1)),
                Operator::Gt,
                Arc::new(Literal::new(ScalarValue::Int64(Some(1000)))),
            )),
        ));
        let original = original
            .downcast_ref::<HashJoinExec>()
            .unwrap()
            .builder()
            .with_filter(Some(JoinFilter::new(
                predicate,
                vec![
                    ColumnIndex {
                        side: JoinSide::Left,
                        index: 0,
                    },
                    ColumnIndex {
                        side: JoinSide::Right,
                        index: 1,
                    },
                ],
                Arc::new(Schema::new(vec![
                    Field::new("a_key", DataType::Int64, false),
                    Field::new("s_customer", DataType::Int64, false),
                ])),
            )))
            .build_exec()?;
        let (graph, _) = GraphBuilder::new(options.clone())
            .build(original.clone())?
            .unwrap();
        let pairs: Arc<dyn ExecutionPlan> = Arc::new(NestedLoopJoinExec::try_new(
            addresses,
            sales,
            None,
            &JoinType::Inner,
            None,
        )?);
        let plan = join(
            customers,
            join(dates, pairs, &[("d_key", "s_date")])?,
            &[("c_key", "s_customer"), ("c_address", "a_key")],
        )?;
        let config = ConfigOptions::new();
        let context = ConfigOnlyContext::new(&config);
        assert!(
            estimate_selected_cost(plan.clone(), &graph.relations, &options, &context)?.is_none()
        );
        // An existing boundary is shared by both alternatives and may stay opaque.
        assert!(selected_cost(plan, 3, &options, &context)?.is_some());
        let optimized =
            crate::join_reorder::JoinReorder::new(options.clone()).optimize(original, &config)?;
        let (optimized_graph, _) = GraphBuilder::new(options).build(optimized)?.unwrap();
        assert_eq!(optimized_graph.relation_count(), 4);
        Ok(())
    }

    #[test]
    fn physical_refinement_avoids_a_downstream_large_build() -> Result<()> {
        let sales = relation(&["s_key", "s_other"], 1_000_000, 100_000);
        let returns = relation(&["r_key", "r_other"], 100_000, 100_000);
        let catalog = relation(&["c_key"], 200_000, 200_000);
        let facts = join(
            returns,
            sales,
            &[("r_key", "s_key"), ("r_other", "s_other")],
        )?;
        let plan = join(facts, catalog, &[("r_key", "c_key")])?;
        let options = JoinReorderOptions::default();
        let mut config = ConfigOptions::new();
        config.execution.target_partitions = 10;
        let context = ConfigOnlyContext::new(&config);
        let (graph, target) = GraphBuilder::new(options.clone()).build(plan)?.unwrap();
        let mut enumerator =
            crate::join_reorder::enumerator::PlanEnumerator::new(graph, options.clone(), &config);
        let root = enumerator.solve()?.unwrap();
        let mut reconstructor =
            PlanReconstructor::new(&enumerator.dp_table, &enumerator.query_graph);
        reconstructor.prepare_required_output_columns(&root, &target)?;
        let initial = reconstructor.reconstruct(&root)?;
        let selected =
            JoinSelection::new().optimize_with_context(Arc::clone(&initial.0), &context)?;
        assert!(contains_partitioned_join(&selected));
        let before = selected_cost(Arc::clone(&initial.0), 3, &options, &context)?.unwrap();
        let disabled = JoinReorderOptions {
            emit_threshold: 0,
            ..options.clone()
        };
        let unchanged = refine_join_tree(
            &root,
            &enumerator.dp_table,
            &enumerator.query_graph,
            &target,
            &disabled,
            &context,
            initial.clone(),
        )?;
        assert!(Arc::ptr_eq(&unchanged.0, &initial.0));
        let refined = refine_join_tree(
            &root,
            &enumerator.dp_table,
            &enumerator.query_graph,
            &target,
            &options,
            &context,
            initial,
        )?;
        let after = selected_cost(Arc::clone(&refined.0), 3, &options, &context)?.unwrap();
        assert!(after < before);
        let selected = JoinSelection::new().optimize_with_context(refined.0, &context)?;
        let mut root_plan = &selected;
        while let Some(projection) = root_plan.downcast_ref::<ProjectionExec>() {
            root_plan = projection.input();
        }
        let root = root_plan.downcast_ref::<HashJoinExec>().unwrap();
        assert!(root.left().schema().index_of("r_key").is_ok());
        Ok(())
    }

    fn relation(names: &[&str], rows: usize, ndv: usize) -> Arc<dyn ExecutionPlan> {
        let schema = Schema::new(
            names
                .iter()
                .map(|name| Field::new(*name, DataType::Int64, false))
                .collect::<Vec<_>>(),
        );
        let mut stats = Statistics::new_unknown(&schema);
        stats.num_rows = Precision::Exact(rows);
        stats.total_byte_size = Precision::Exact(rows * names.len() * 8);
        for column in &mut stats.column_statistics {
            column.distinct_count = Precision::Exact(ndv);
            column.null_count = Precision::Exact(0);
            column.min_value = Precision::Exact(ScalarValue::Int64(Some(0)));
            column.max_value = Precision::Exact(ScalarValue::Int64(Some(ndv as i64 - 1)));
        }
        Arc::new(StatisticsExec::new(stats, schema))
    }

    fn join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        keys: &[(&str, &str)],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let on = keys
            .iter()
            .map(|(l, r)| {
                Ok((
                    Arc::new(Column::new(l, left.schema().index_of(l)?)) as Arc<dyn PhysicalExpr>,
                    Arc::new(Column::new(r, right.schema().index_of(r)?)) as Arc<dyn PhysicalExpr>,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
            false,
        )?))
    }

    #[test]
    fn selected_cost_keeps_selective_dimension_before_multikey_fact_join() -> Result<()> {
        let sales = relation(&["s_key", "s_other", "s_date"], 1_000_000, 100_000);
        let returns = relation(&["r_key", "r_other"], 100_000, 100_000);
        let dates = relation(&["d_key"], 100, 100);
        let selected_date: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("d_key", 0)),
                Operator::Eq,
                Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
            )),
            dates,
        )?);
        let early = join(
            returns.clone(),
            join(selected_date.clone(), sales.clone(), &[("d_key", "s_date")])?,
            &[("r_key", "s_key"), ("r_other", "s_other")],
        )?;
        let late = join(
            selected_date,
            join(
                returns,
                sales,
                &[("r_key", "s_key"), ("r_other", "s_other")],
            )?,
            &[("d_key", "s_date")],
        )?;
        let mut config = ConfigOptions::new();
        config.execution.target_partitions = 10;
        let options = JoinReorderOptions::default();
        assert!(
            selected_cost(early.clone(), 3, &options, &ConfigOnlyContext::new(&config))?.unwrap()
                < selected_cost(late.clone(), 3, &options, &ConfigOnlyContext::new(&config))?
                    .unwrap()
        );
        let optimized = crate::join_reorder::JoinReorder::new(options).optimize(late, &config)?;
        fn check_date_join(plan: &Arc<dyn ExecutionPlan>) -> usize {
            let mut found = 0;
            if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
                for (left_key, right_key) in join.on() {
                    for (key, other) in [(left_key, join.right()), (right_key, join.left())] {
                        if key
                            .downcast_ref::<Column>()
                            .is_some_and(|c| c.name() == "d_key")
                        {
                            assert!(other.schema().index_of("s_key").is_ok());
                            assert!(other.schema().index_of("r_key").is_err());
                            found += 1;
                        }
                    }
                }
            }
            found
                + plan
                    .children()
                    .into_iter()
                    .map(check_date_join)
                    .sum::<usize>()
        }
        assert_eq!(check_date_join(&optimized), 1);
        Ok(())
    }

    #[test]
    fn selected_cost_observes_datafusion_build_side_swap() -> Result<()> {
        let sales = relation(&["s_key", "s_other"], 1_000_000, 100_000);
        let returns = relation(&["r_key", "r_other"], 100_000, 100_000);
        let catalog = relation(&["c_key"], 200_000, 200_000);
        let facts = join(
            returns,
            sales,
            &[("r_key", "s_key"), ("r_other", "s_other")],
        )?;
        let plan = join(facts, catalog, &[("s_key", "c_key")])?;
        let mut config = ConfigOptions::new();
        config.execution.target_partitions = 10;
        let selected = JoinSelection::new().optimize(plan.clone(), &config)?;
        let selected = selected
            .downcast_ref::<ProjectionExec>()
            .map_or(&selected, |p| p.input());
        let root = selected.downcast_ref::<HashJoinExec>().unwrap();
        assert_eq!(root.left().schema().field(0).name(), "c_key");
        assert_eq!(*root.partition_mode(), PartitionMode::Partitioned);
        let options = JoinReorderOptions::default();
        let swapped_cost =
            selected_cost(plan.clone(), 3, &options, &ConfigOnlyContext::new(&config))?.unwrap();
        config.optimizer.join_reordering = false;
        let preserved_cost =
            selected_cost(plan.clone(), 3, &options, &ConfigOnlyContext::new(&config))?.unwrap();
        assert!(swapped_cost > preserved_cost);

        #[derive(Debug)]
        struct CatalogStatistics;
        impl StatisticsProvider for CatalogStatistics {
            fn compute_statistics(
                &self,
                plan: &dyn ExecutionPlan,
                _: &[ExtendedStatistics],
            ) -> Result<StatisticsResult> {
                if plan.children().is_empty() && plan.schema().index_of("c_key").is_ok() {
                    let mut stats = Statistics::new_unknown(&plan.schema());
                    stats.num_rows = Precision::Inexact(9_000_000);
                    stats.total_byte_size = Precision::Inexact(72_000_000);
                    Ok(StatisticsResult::Computed(stats.into()))
                } else {
                    Ok(StatisticsResult::Delegate)
                }
            }
        }
        struct RegistryContext {
            config: ConfigOptions,
            registry: StatisticsRegistry,
        }
        impl PhysicalOptimizerContext for RegistryContext {
            fn config_options(&self) -> &ConfigOptions {
                &self.config
            }
            fn statistics_registry(&self) -> Option<&StatisticsRegistry> {
                Some(&self.registry)
            }
        }
        config.optimizer.join_reordering = true;
        config.optimizer.use_statistics_registry = true;
        let mut registry = StatisticsRegistry::new();
        registry.register(Arc::new(CatalogStatistics));
        let context = RegistryContext { config, registry };
        let registry_cost = selected_cost(plan, 3, &options, &context)?.unwrap();
        assert_eq!(registry_cost, preserved_cost);
        Ok(())
    }
}
