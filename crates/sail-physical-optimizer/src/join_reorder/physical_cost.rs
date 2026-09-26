use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::physical_optimizer::optimizer::PhysicalOptimizerContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;

use super::JoinReorderOptions;
use super::builder::GraphBuilder;
use super::cardinality_estimator::CardinalityEstimator;
use super::dp_plan::DPPlan;
use super::graph::{QueryGraph, RelationNode};
use super::physical_model::PhysicalModel;

/// Price the already selected input using the same statistics and physical model as DP.
pub fn estimate_cost(
    plan: Arc<dyn ExecutionPlan>,
    relations: &[RelationNode],
    options: &JoinReorderOptions,
    context: &dyn PhysicalOptimizerContext,
) -> Result<Option<f64>> {
    let Some((graph, target)) = GraphBuilder::new(options.clone())
        .statistics_context(context)
        .cache_relation_statistics(relations)
        .build(Arc::clone(&plan))?
    else {
        return Ok(None);
    };
    if graph.relation_count() != relations.len() {
        return Ok(None);
    }
    let model = PhysicalModel::new(&graph, options, context.config_options(), &target);
    let mut estimator = CardinalityEstimator::new(graph.clone());
    estimate_subtree(&plan, &graph, &mut estimator, &model).map(|p| Some(p.cost))
}

fn estimate_subtree(
    plan: &Arc<dyn ExecutionPlan>,
    graph: &QueryGraph,
    estimator: &mut CardinalityEstimator,
    model: &PhysicalModel,
) -> Result<DPPlan> {
    if let Some(relation) = graph.relations.iter().find(|r| Arc::ptr_eq(&r.plan, plan)) {
        return model.leaf(relation.relation_id);
    }
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        return estimate_subtree(projection.input(), graph, estimator, model);
    }
    let join = plan.downcast_ref::<HashJoinExec>().ok_or_else(|| {
        DataFusionError::Internal(format!("Unexpected join-cost node: {}", plan.name()))
    })?;
    let left = Arc::new(estimate_subtree(join.left(), graph, estimator, model)?);
    let right = Arc::new(estimate_subtree(join.right(), graph, estimator, model)?);
    let edges = graph.get_connecting_edge_indices(left.join_set, right.join_set);
    let rows = estimator.estimate_cardinality(left.join_set | right.join_set)?;
    Ok(model.join(left, right, &edges, rows, *join.partition_mode()))
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::stats::Precision;
    use datafusion::common::{JoinSide, NullEquality, ScalarValue, Statistics};
    use datafusion::config::ConfigOptions;
    use datafusion::logical_expr::{JoinType, Operator};
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_optimizer::join_selection::JoinSelection;
    use datafusion::physical_optimizer::optimizer::ConfigOnlyContext;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
    use datafusion::physical_plan::joins::{NestedLoopJoinExec, PartitionMode};
    use datafusion::physical_plan::operator_statistics::{
        ExtendedStatistics, StatisticsProvider, StatisticsRegistry, StatisticsResult,
    };
    use datafusion::physical_plan::test::exec::StatisticsExec;

    use super::*;
    use crate::join_reorder::reconstructor::PlanReconstructor;

    fn selected_cost(
        plan: Arc<dyn ExecutionPlan>,
        relation_count: usize,
        options: &JoinReorderOptions,
        context: &dyn PhysicalOptimizerContext,
    ) -> Result<Option<f64>> {
        let plan = JoinSelection::new().optimize_with_context(plan, context)?;
        let (graph, _) = GraphBuilder::new(options.clone())
            .statistics_context(context)
            .build(plan.clone())?
            .unwrap();
        assert_eq!(graph.relation_count(), relation_count);
        estimate_cost(plan, &graph.relations, options, context)
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
        assert!(estimate_cost(plan.clone(), &graph.relations, &options, &context)?.is_none());
        // An existing boundary is shared by both alternatives and may stay opaque.
        assert!(selected_cost(plan, 3, &options, &context)?.is_some());
        let optimized =
            crate::join_reorder::JoinReorder::new(options.clone()).optimize(original, &config)?;
        let (optimized_graph, _) = GraphBuilder::new(options).build(optimized)?.unwrap();
        assert_eq!(optimized_graph.relation_count(), 4);
        Ok(())
    }

    #[test]
    fn dp_cost_matches_selected_children_after_reconstruction() -> Result<()> {
        let sales = relation(&["s_key", "s_other"], 1_000_000, 100_000);
        let returns = relation(&["r_key", "r_other"], 100_000, 100_000);
        let catalog = relation(&["c_key"], 200_000, 200_000);
        let plan = join(
            join(
                returns,
                sales,
                &[("r_key", "s_key"), ("r_other", "s_other")],
            )?,
            catalog,
            &[("r_key", "c_key")],
        )?;
        let options = JoinReorderOptions::default();
        let mut config = ConfigOptions::new();
        config.execution.target_partitions = 10;
        let context = ConfigOnlyContext::new(&config);
        let (graph, target) = GraphBuilder::new(options.clone()).build(plan)?.unwrap();
        let mut enumerator = crate::join_reorder::enumerator::PlanEnumerator::new(
            graph,
            options.clone(),
            &config,
            &target,
        );
        let root = enumerator.solve()?.unwrap();
        let mut reconstructor = PlanReconstructor::new(&enumerator.query_graph);
        reconstructor.prepare_required_output_columns(&root, &target)?;
        let reconstructed = reconstructor.reconstruct(&root)?.0;
        let cost = estimate_cost(
            reconstructed,
            &enumerator.query_graph.relations,
            &options,
            &context,
        )?
        .unwrap();
        assert!(
            (cost - root.cost).abs() < root.cost * 1e-10,
            "{cost} != {}",
            root.cost
        );
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
        assert_ne!(swapped_cost, preserved_cost);

        #[derive(Debug)]
        struct CatalogStatistics;
        impl StatisticsProvider for CatalogStatistics {
            fn compute_statistics(
                &self,
                plan: &dyn ExecutionPlan,
                _: &[ExtendedStatistics],
            ) -> Result<StatisticsResult> {
                if plan.is::<FilterExec>() && plan.schema().index_of("c_key").is_ok() {
                    let mut stats = Statistics::new_unknown(&plan.schema());
                    stats.num_rows = Precision::Inexact(7);
                    stats.total_byte_size = Precision::Inexact(56);
                    Ok(StatisticsResult::Computed(stats.into()))
                } else if plan.children().is_empty() && plan.schema().index_of("c_key").is_ok() {
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
        assert!(registry_cost > preserved_cost);
        let filtered: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("c_key", 0)),
                Operator::Gt,
                Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
            )),
            relation(&["c_key"], 200_000, 200_000),
        )?);
        let plan = join(
            filtered,
            relation(&["other"], 1_000, 1_000),
            &[("c_key", "other")],
        )?;
        let (graph, _) = GraphBuilder::new(options)
            .statistics_context(&context)
            .build(plan)?
            .unwrap();
        assert_eq!(graph.relations[0].initial_cardinality, 7.0);
        assert_eq!(graph.relations[0].base_cardinality, 9_000_000.0);
        assert_eq!(
            graph.relations[0].statistics.total_byte_size,
            Precision::Inexact(56)
        );
        Ok(())
    }
}
