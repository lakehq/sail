use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_plan::joins::PartitionMode;

use super::JoinReorderOptions;
use super::builder::ColumnMapEntry;
use super::cost_model::CostModel;
use super::dp_plan::{DPPlan, JoinDistribution};
use super::graph::{QueryGraph, StableColumn};
use super::join_set::JoinSet;

/// Shared physical decisions and costing for enumeration and the input-plan comparison.
pub struct PhysicalModel {
    graph: QueryGraph,
    config: ConfigOptions,
    costs: CostModel,
    output_columns: HashSet<StableColumn>,
    row_widths: RefCell<HashMap<JoinSet, f64>>,
}

impl PhysicalModel {
    pub fn new(
        graph: &QueryGraph,
        options: &JoinReorderOptions,
        config: &ConfigOptions,
        target: &[ColumnMapEntry],
    ) -> Self {
        let mut output_columns = HashSet::new();
        collect_output_columns(target, &mut output_columns);
        if target.is_empty() {
            for relation in &graph.relations {
                for index in 0..relation.plan.schema().fields().len() {
                    output_columns.insert(column(relation.relation_id, index));
                }
            }
        }
        Self {
            graph: graph.clone(),
            config: config.clone(),
            costs: CostModel::new(options),
            output_columns,
            row_widths: RefCell::default(),
        }
    }

    pub fn leaf(&self, id: usize) -> Result<DPPlan> {
        let relation = &self.graph.relations[id];
        let mut plan = DPPlan::new_leaf(id, relation.initial_cardinality)?;
        plan.row_width = self.row_width(plan.join_set);
        plan.has_byte_statistics = relation.statistics.total_byte_size.get_value().is_some();
        let partitioning = relation.plan.properties().output_partitioning();
        plan.distribution = match partitioning {
            Partitioning::Hash(keys, count) => {
                let columns: Option<Vec<_>> = keys
                    .iter()
                    .map(|expr| expr.downcast_ref::<Column>().map(|c| column(id, c.index())))
                    .collect();
                columns.map_or(JoinDistribution::Unknown(*count), |keys| {
                    JoinDistribution::Hash(keys, *count)
                })
            }
            other => JoinDistribution::Unknown(other.partition_count()),
        };
        plan.distribution = self.project_distribution(plan.distribution, plan.join_set);
        Ok(plan)
    }

    pub fn join_candidates(
        &self,
        left: Arc<DPPlan>,
        right: Arc<DPPlan>,
        edges: &[usize],
        rows: f64,
    ) -> Vec<DPPlan> {
        let mut result = vec![];
        let orientations =
            if self.config.optimizer.join_reordering && self.graph.can_swap_physical_order(edges) {
                vec![(Arc::clone(&left), Arc::clone(&right)), (right, left)]
            } else {
                vec![(left, right)]
            };
        for (left, right) in orientations {
            let (keys, _) = self.join_keys(left.join_set, right.join_set, edges);
            let can_partition = !keys.is_empty()
                && self.config.optimizer.repartition_joins
                && self.config.execution.target_partitions > 1;
            let can_collect = if left.has_byte_statistics {
                left.cardinality * left.row_width
                    < self.config.optimizer.hash_join_single_partition_threshold as f64
            } else {
                left.cardinality
                    < self
                        .config
                        .optimizer
                        .hash_join_single_partition_threshold_rows as f64
            };
            if !can_partition || can_collect {
                result.push(self.join(
                    Arc::clone(&left),
                    Arc::clone(&right),
                    edges,
                    rows,
                    PartitionMode::CollectLeft,
                ));
            }
            if can_partition {
                result.push(self.join(left, right, edges, rows, PartitionMode::Partitioned));
            }
        }
        result
    }

    pub fn join(
        &self,
        left: Arc<DPPlan>,
        right: Arc<DPPlan>,
        edges: &[usize],
        rows: f64,
        mode: PartitionMode,
    ) -> DPPlan {
        let (left_keys, right_keys) = self.join_keys(left.join_set, right.join_set, edges);
        let key_count = left_keys.len();
        let count = self.config.execution.target_partitions;
        let mut redistribution = 0.0;
        let distribution = if mode == PartitionMode::Partitioned && key_count > 0 {
            for (input, required) in [(&left, &left_keys), (&right, &right_keys)] {
                if !self.satisfies(input, required, count) {
                    redistribution += self.costs.redistribution_cost(
                        input.cardinality,
                        input.row_width,
                        key_count,
                    );
                }
            }
            JoinDistribution::Hash(right_keys, count)
        } else {
            if left.distribution.partition_count() > 1 {
                redistribution +=
                    self.costs
                        .redistribution_cost(left.cardinality, left.row_width, 0);
            }
            right.distribution.clone()
        };
        let cost = self
            .costs
            .compute_cost(&left, &right, rows, key_count, redistribution);
        let mut result = DPPlan::new_join(left, right, edges.to_vec(), cost, rows, mode);
        result.row_width = self.row_width(result.join_set);
        result.distribution = self.project_distribution(distribution, result.join_set);
        result
    }

    fn join_keys(
        &self,
        left: JoinSet,
        right: JoinSet,
        edges: &[usize],
    ) -> (Vec<StableColumn>, Vec<StableColumn>) {
        let mut pairs = vec![];
        for &index in edges {
            for (a, b) in &self.graph.edges[index].equi_pairs {
                let pair = if contains(left, a) && contains(right, b) {
                    Some((a.clone(), b.clone()))
                } else if contains(left, b) && contains(right, a) {
                    Some((b.clone(), a.clone()))
                } else {
                    None
                };
                if let Some(pair) = pair {
                    pairs.push(pair);
                }
            }
        }
        pairs.into_iter().unzip()
    }

    fn satisfies(&self, plan: &DPPlan, required: &[StableColumn], partitions: usize) -> bool {
        match &plan.distribution {
            JoinDistribution::Hash(keys, count)
                if *count == partitions && keys.len() == required.len() =>
            {
                keys.iter()
                    .zip(required)
                    .all(|(a, b)| self.equivalent_columns(a, plan.join_set).contains(b))
            }
            _ => false,
        }
    }

    fn equivalent_columns(&self, start: &StableColumn, set: JoinSet) -> HashSet<StableColumn> {
        let mut columns = HashSet::from([start.clone()]);
        loop {
            let previous = columns.len();
            for edge in self
                .graph
                .edges
                .iter()
                .filter(|e| e.join_set.is_subset(&set))
            {
                for (a, b) in &edge.equi_pairs {
                    if columns.contains(a) || columns.contains(b) {
                        columns.insert(a.clone());
                        columns.insert(b.clone());
                    }
                }
            }
            if previous == columns.len() {
                return columns;
            }
        }
    }

    fn project_distribution(
        &self,
        distribution: JoinDistribution,
        set: JoinSet,
    ) -> JoinDistribution {
        let JoinDistribution::Hash(keys, count) = distribution else {
            return distribution;
        };
        let required = self.required_columns(set);
        let projected: Option<Vec<_>> = keys
            .iter()
            .map(|key| {
                self.equivalent_columns(key, set)
                    .into_iter()
                    .filter(|c| required.contains(c))
                    .min_by_key(|c| (c.relation_id, c.column_index))
            })
            .collect();
        projected.map_or(JoinDistribution::Unknown(count), |keys| {
            JoinDistribution::Hash(keys, count)
        })
    }

    pub fn required_columns(&self, set: JoinSet) -> HashSet<StableColumn> {
        let mut required: HashSet<_> = self
            .output_columns
            .iter()
            .filter(|c| contains(set, c))
            .cloned()
            .collect();
        for edge in self
            .graph
            .edges
            .iter()
            .filter(|e| !e.join_set.is_subset(&set))
        {
            for (a, b) in &edge.equi_pairs {
                for c in [a, b] {
                    if contains(set, c) {
                        required.insert(c.clone());
                    }
                }
            }
            for c in edge.residual_filter.iter().flat_map(collect_columns) {
                if let Some((id, index)) = StableColumn::parse_stable_name(c.name()) {
                    let c = column(id, index);
                    if contains(set, &c) {
                        required.insert(c);
                    }
                }
            }
        }
        // Reconstruction retains all fields when a zero-column projection is unavailable.
        if required.is_empty() {
            for id in set.iter() {
                for index in 0..self.graph.relations[id].plan.schema().fields().len() {
                    required.insert(column(id, index));
                }
            }
        }
        required
    }

    pub fn row_width(&self, set: JoinSet) -> f64 {
        if let Some(&width) = self.row_widths.borrow().get(&set) {
            return width;
        }
        let required = self.required_columns(set);
        let mut width = 0.0;
        for id in set.iter() {
            let relation = &self.graph.relations[id];
            let schema = relation.plan.schema();
            let full: f64 = schema
                .fields()
                .iter()
                .map(|f| type_width(f.data_type()))
                .sum();
            let selected: f64 = schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(i, _)| required.contains(&column(id, *i)))
                .map(|(_, f)| type_width(f.data_type()))
                .sum();
            let measured = relation
                .statistics
                .total_byte_size
                .get_value()
                .copied()
                .zip(relation.statistics.num_rows.get_value().copied())
                .filter(|(_, n)| *n > 0)
                .map(|(bytes, rows)| bytes as f64 / rows as f64);
            width += measured.map_or(selected, |bytes| bytes * selected / full.max(1.0));
        }
        let width = width.max(8.0);
        self.row_widths.borrow_mut().insert(set, width);
        width
    }
}

fn column(relation_id: usize, column_index: usize) -> StableColumn {
    StableColumn {
        relation_id,
        column_index,
        name: StableColumn::format_stable_name(relation_id, column_index),
    }
}

fn contains(set: JoinSet, column: &StableColumn) -> bool {
    set.bits() & (1 << column.relation_id) != 0
}

fn type_width(data_type: &DataType) -> f64 {
    match data_type {
        DataType::Null => 0.0,
        DataType::Boolean => 0.125,
        DataType::FixedSizeBinary(n) => (*n).max(0) as f64,
        other => other.primitive_width().unwrap_or(32) as f64,
    }
}

fn collect_output_columns(map: &[ColumnMapEntry], columns: &mut HashSet<StableColumn>) {
    for entry in map {
        match entry {
            ColumnMapEntry::Stable {
                relation_id,
                column_index,
            } => {
                columns.insert(column(*relation_id, *column_index));
            }
            ColumnMapEntry::Expression { expr, input_map } => {
                for c in collect_columns(expr) {
                    if let Some(entry) = input_map.get(c.index()) {
                        collect_output_columns(std::slice::from_ref(entry), columns);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::common::Statistics;
    use datafusion::common::stats::Precision;
    use datafusion::logical_expr::JoinType;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_optimizer::ensure_requirements::EnsureRequirements;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::test::exec::StatisticsExec;

    use super::*;
    use crate::join_reorder::dp_plan::PlanType;
    use crate::join_reorder::graph::{JoinEdge, RelationNode};
    use crate::join_reorder::reconstructor::PlanReconstructor;

    fn chain() -> Result<QueryGraph> {
        let mut graph = QueryGraph::new();
        for id in 0..3 {
            let schema = Schema::new(vec![Field::new(format!("k{id}"), DataType::Int64, false)]);
            let mut stats = Statistics::new_unknown(&schema);
            stats.num_rows = Precision::Exact(100_000);
            stats.total_byte_size = Precision::Exact(800_000);
            stats.column_statistics[0].distinct_count = Precision::Exact(100_000);
            let plan = Arc::new(StatisticsExec::new(stats.clone(), schema));
            graph.add_relation(RelationNode::new(plan, id, 100_000.0, 100_000.0, stats));
            if id > 0 {
                graph.add_edge(JoinEdge::new(
                    JoinSet::new_singleton(id - 1)?,
                    JoinSet::new_singleton(id)?,
                    None,
                    JoinType::Inner,
                    vec![(column(id - 1, 0), column(id, 0))],
                ))?;
            }
        }
        Ok(graph)
    }

    #[test]
    fn partitioned_child_reuses_distribution_after_enforcement() -> Result<()> {
        let graph = chain()?;
        let mut config = ConfigOptions::new();
        config.execution.target_partitions = 4;
        let model = PhysicalModel::new(&graph, &JoinReorderOptions::default(), &config, &[]);
        let ab = Arc::new(model.join(
            Arc::new(model.leaf(0)?),
            Arc::new(model.leaf(1)?),
            &[0],
            100_000.0,
            PartitionMode::Partitioned,
        ));
        let root = Arc::new(model.join(
            ab,
            Arc::new(model.leaf(2)?),
            &[1],
            100_000.0,
            PartitionMode::Partitioned,
        ));
        // Three source exchanges; AB's output already satisfies the parent's hash key.
        assert_eq!(root.cost, 2.0 * 210_000.0 + 3.0 * 110_000.0);
        assert!(
            matches!(&root.distribution, JoinDistribution::Hash(keys, 4) if keys == &vec![column(0, 0)])
        );
        let plan = PlanReconstructor::new(&graph).reconstruct(&root)?.0;
        let plan = EnsureRequirements::new().optimize(plan, &config)?;
        fn hash_exchanges(plan: &Arc<dyn ExecutionPlan>) -> usize {
            usize::from(
                plan.downcast_ref::<RepartitionExec>()
                    .is_some_and(|r| matches!(r.partitioning(), Partitioning::Hash(_, 4))),
            ) + plan
                .children()
                .into_iter()
                .map(hash_exchanges)
                .sum::<usize>()
        }
        assert_eq!(hash_exchanges(&plan), 3);
        Ok(())
    }

    #[test]
    fn collection_uses_bytes_and_respects_single_partition_configuration() -> Result<()> {
        let mut graph = chain()?;
        graph.relations[0].initial_cardinality = 10.0;
        graph.relations[0].statistics.num_rows = Precision::Exact(10);
        graph.relations[0].statistics.total_byte_size = Precision::Exact(100_000_000);
        let mut config = ConfigOptions::new();
        config.execution.target_partitions = 4;
        config.optimizer.join_reordering = false;
        for (partitions, repartition, expected) in [
            (4, true, PartitionMode::Partitioned),
            (1, true, PartitionMode::CollectLeft),
            (4, false, PartitionMode::CollectLeft),
        ] {
            config.execution.target_partitions = partitions;
            config.optimizer.repartition_joins = repartition;
            let model = PhysicalModel::new(&graph, &JoinReorderOptions::default(), &config, &[]);
            let candidates = model.join_candidates(
                Arc::new(model.leaf(0)?),
                Arc::new(model.leaf(1)?),
                &[0],
                10.0,
            );
            assert_eq!(candidates.len(), 1);
            assert!(
                matches!(candidates[0].plan_type, PlanType::Join { mode, .. } if mode == expected)
            );
        }
        Ok(())
    }

    #[test]
    fn absent_bytes_fall_back_to_the_configured_row_threshold() -> Result<()> {
        let mut graph = chain()?;
        graph.relations[0].statistics.total_byte_size = Precision::Absent;
        let mut config = ConfigOptions::new();
        config.execution.target_partitions = 4;
        config.optimizer.join_reordering = false;
        config.optimizer.hash_join_single_partition_threshold = 0;
        for (threshold, expected) in [(100_001, 2), (100_000, 1)] {
            config.optimizer.hash_join_single_partition_threshold_rows = threshold;
            let model = PhysicalModel::new(&graph, &JoinReorderOptions::default(), &config, &[]);
            assert_eq!(
                model
                    .join_candidates(
                        Arc::new(model.leaf(0)?),
                        Arc::new(model.leaf(1)?),
                        &[0],
                        100_000.0
                    )
                    .len(),
                expected
            );
        }
        Ok(())
    }

    #[test]
    fn eligible_small_build_keeps_both_distributions() -> Result<()> {
        let graph = chain()?;
        let mut config = ConfigOptions::new();
        config.execution.target_partitions = 4;
        config.optimizer.hash_join_single_partition_threshold = 2_000_000;
        config.optimizer.join_reordering = false;
        let model = PhysicalModel::new(&graph, &JoinReorderOptions::default(), &config, &[]);
        let candidates = model.join_candidates(
            Arc::new(model.leaf(0)?),
            Arc::new(model.leaf(1)?),
            &[0],
            100_000.0,
        );
        assert_eq!(candidates.len(), 2);
        assert_ne!(candidates[0].distribution, candidates[1].distribution);
        Ok(())
    }
}
