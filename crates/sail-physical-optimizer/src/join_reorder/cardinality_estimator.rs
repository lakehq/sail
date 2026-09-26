use std::collections::{HashMap, HashSet};

use datafusion::error::Result;

use crate::join_reorder::graph::{QueryGraph, StableColumn};
use crate::join_reorder::join_set::JoinSet;

/// Estimates each relation subset independently of its physical join tree.
pub struct CardinalityEstimator {
    graph: QueryGraph,
    cardinality_cache: HashMap<JoinSet, f64>,
}

impl CardinalityEstimator {
    pub fn new(graph: QueryGraph) -> Self {
        Self {
            graph,
            cardinality_cache: HashMap::new(),
        }
    }

    pub fn estimate_cardinality(&mut self, join_set: JoinSet) -> Result<f64> {
        if let Some(&rows) = self.cardinality_cache.get(&join_set) {
            return Ok(rows);
        }
        let rows = if join_set.cardinality() == 1 {
            join_set
                .iter()
                .next()
                .map_or(1.0, |id| self.graph.relations[id].initial_cardinality)
        } else {
            self.estimate_subset(join_set)
        };
        self.cardinality_cache.insert(join_set, rows);
        Ok(rows)
    }

    fn estimate_subset(&self, join_set: JoinSet) -> f64 {
        let mut log_rows = 0.0;
        for relation in join_set.iter().map(|id| &self.graph.relations[id]) {
            if relation.initial_cardinality == 0.0 {
                return 0.0;
            }
            log_rows += relation.initial_cardinality.ln();
        }

        let mut equalities: Vec<HashSet<StableColumn>> = vec![];
        for edge in self
            .graph
            .edges
            .iter()
            .filter(|edge| edge.join_set.is_subset(&join_set))
        {
            for (left, right) in &edge.equi_pairs {
                merge_columns(&mut equalities, left, right);
            }
            if edge.residual_filter.is_some() && !edge.equi_pairs.is_empty() {
                log_rows += 0.8_f64.ln();
            }
        }

        let mut constraints = vec![];
        for columns in equalities {
            let fallback = columns
                .iter()
                .map(|c| self.graph.relations[c.relation_id].base_cardinality)
                .fold(f64::INFINITY, f64::min)
                .max(1.0);
            let known_max = columns
                .iter()
                .filter_map(|c| self.distinct_count(c))
                .fold(1.0, f64::max);
            let mut domains: HashMap<usize, f64> = HashMap::new();
            for column in columns {
                let domain = self
                    .distinct_count(&column)
                    .unwrap_or(fallback.max(known_max))
                    .max(1.0);
                domains
                    .entry(column.relation_id)
                    .and_modify(|d| *d = d.max(domain))
                    .or_insert(domain);
            }
            let mut domains: Vec<_> = domains.into_iter().collect();
            domains.sort_by(|(a, ad), (b, bd)| ad.total_cmp(bd).then(a.cmp(b)));
            if let Some(&(anchor, _)) = domains.first() {
                // Uniform, contained domains: divide by every domain except the smallest.
                // Only equalities available in this subset contribute a domain.
                constraints.extend(
                    domains
                        .iter()
                        .skip(1)
                        .map(|&(id, domain)| (domain.ln(), anchor, id)),
                );
            }
        }
        constraints
            .sort_by(|(a, al, ar), (b, bl, br)| b.total_cmp(a).then(al.cmp(bl)).then(ar.cmp(br)));
        let mut components: Vec<JoinSet> = vec![];
        for (log_domain, left, right) in constraints {
            let left_bit = JoinSet::from_bits(1 << left);
            let right_bit = JoinSet::from_bits(1 << right);
            if components
                .iter()
                .any(|c| left_bit.is_subset(c) && right_bit.is_subset(c))
            {
                continue;
            }
            // Without joint NDV, additional keys between already connected relations may
            // be fully correlated. Retain the strongest available spanning constraints.
            let mut connected = left_bit | right_bit;
            components.retain(|component| {
                if !component.is_disjoint(&connected) {
                    connected |= *component;
                    false
                } else {
                    true
                }
            });
            components.push(connected);
            log_rows -= log_domain;
        }
        log_rows.exp().min(f64::MAX)
    }

    fn distinct_count(&self, column: &StableColumn) -> Option<f64> {
        self.graph.relations[column.relation_id]
            .statistics
            .column_statistics
            .get(column.column_index)?
            .distinct_count
            .get_value()
            .map(|&n| n as f64)
    }
}

fn merge_columns(sets: &mut Vec<HashSet<StableColumn>>, left: &StableColumn, right: &StableColumn) {
    let mut merged = HashSet::from([left.clone(), right.clone()]);
    let mut index = 0;
    while index < sets.len() {
        if sets[index].contains(left) || sets[index].contains(right) {
            merged.extend(sets.remove(index));
        } else {
            index += 1;
        }
    }
    sets.push(merged);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Statistics;
    use datafusion::common::stats::Precision;
    use datafusion::logical_expr::JoinType;
    use datafusion::physical_plan::empty::EmptyExec;

    use super::*;
    use crate::join_reorder::graph::{JoinEdge, RelationNode};

    fn graph(
        rows: &[usize],
        ndvs: &[Option<usize>],
        edges: &[(usize, usize)],
    ) -> Result<QueryGraph> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
        let mut graph = QueryGraph::new();
        for (id, &rows) in rows.iter().enumerate() {
            let mut stats = Statistics::new_unknown(&schema);
            stats.num_rows = Precision::Exact(rows);
            stats.column_statistics[0].distinct_count =
                ndvs[id].map_or(Precision::Absent, Precision::Exact);
            graph.add_relation(RelationNode::new(
                Arc::new(EmptyExec::new(schema.clone())),
                id,
                rows as f64,
                rows as f64,
                stats,
            ));
        }
        for &(left, right) in edges {
            let column = |id| StableColumn {
                relation_id: id,
                column_index: 0,
                name: StableColumn::format_stable_name(id, 0),
            };
            graph.add_edge(JoinEdge::new(
                JoinSet::new_singleton(left)?,
                JoinSet::new_singleton(right)?,
                None,
                JoinType::Inner,
                vec![(column(left), column(right))],
            ))?;
        }
        Ok(graph)
    }

    fn split_cardinality(graph: QueryGraph, left: u64, right: u64) -> Result<f64> {
        let left = JoinSet::from_bits(left);
        let right = JoinSet::from_bits(right);
        let mut estimator = CardinalityEstimator::new(graph);
        estimator.estimate_cardinality(left | right)
    }

    #[test]
    fn absent_relations_do_not_expand_a_subset_domain() -> Result<()> {
        let graph = graph(
            &[100, 100, 1_000_000],
            &[Some(100), Some(100), Some(1_000_000)],
            &[(0, 1), (1, 2)],
        )?;
        let mut estimator = CardinalityEstimator::new(graph);
        for bits in [3, 7, 3] {
            assert!(
                (estimator.estimate_cardinality(JoinSet::from_bits(bits))? - 100.0).abs() < 1e-8
            );
        }
        Ok(())
    }

    #[test]
    fn known_single_value_domain_preserves_cross_product() -> Result<()> {
        let graph = graph(&[100, 100], &[Some(1), Some(1)], &[(0, 1)])?;
        assert!((split_cardinality(graph, 1, 2)? - 10_000.0).abs() < 1e-8);
        Ok(())
    }

    #[test]
    fn smaller_relation_does_not_cap_known_join_domain() -> Result<()> {
        let graph = graph(&[1000, 10], &[Some(1000), Some(10)], &[(0, 1)])?;
        assert!((split_cardinality(graph, 1, 2)? - 10.0).abs() < 1e-9);
        Ok(())
    }

    #[test]
    fn equality_cycles_and_duplicate_keys_do_not_reduce_rows_again() -> Result<()> {
        for edges in [vec![(0, 1), (1, 2)], vec![(0, 1), (1, 2), (0, 2), (0, 1)]] {
            let graph = graph(&[100; 3], &[Some(100); 3], &edges)?;
            for (left, right) in [(1, 6), (3, 4), (5, 2)] {
                assert!((split_cardinality(graph.clone(), left, right)? - 100.0).abs() < 1e-9);
            }
        }
        Ok(())
    }

    #[test]
    fn disconnected_equality_components_need_separate_constraints() -> Result<()> {
        let graph = graph(&[100; 4], &[Some(100); 4], &[(0, 1), (2, 3), (1, 2)])?;
        assert!((split_cardinality(graph, 3, 12)? - 100.0).abs() < 1e-9);
        Ok(())
    }

    #[test]
    fn missing_statistics_and_local_filters_keep_base_domain() -> Result<()> {
        let mut filtered = graph(
            &[2_000_000, 60_000_000],
            &[Some(2_000_000), None],
            &[(0, 1)],
        )?;
        filtered.relations[0].initial_cardinality = 400_000.0;
        assert!((split_cardinality(filtered, 1, 2)? - 12_000_000.0).abs() < 1e-6);
        let graph = graph(&[1500, 90_000], &[None, None], &[(0, 1)])?;
        assert!((split_cardinality(graph, 1, 2)? - 90_000.0).abs() < 1e-9);
        Ok(())
    }

    #[test]
    fn partial_ndv_does_not_replace_the_missing_base_domain() -> Result<()> {
        let mut filtered = graph(&[10_000, 1000], &[Some(10), None], &[(0, 1)])?;
        filtered.relations[1].initial_cardinality = 100.0;
        assert!((split_cardinality(filtered, 1, 2)? - 1000.0).abs() < 1e-9);
        Ok(())
    }

    #[test]
    fn composite_keys_without_joint_statistics_use_strongest_domain() -> Result<()> {
        let mut graph = graph(&[1000, 1_000_000], &[Some(100), Some(100)], &[(0, 1)])?;
        for relation in &mut graph.relations {
            relation.plan = Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int32, false),
                Field::new("second", DataType::Int32, false),
            ]))));
            let mut second = relation.statistics.column_statistics[0].clone();
            second.distinct_count = Precision::Exact(200);
            relation.statistics.column_statistics.push(second);
        }
        let mut second = graph.edges[0].equi_pairs[0].clone();
        second.0.column_index = 1;
        second.1.column_index = 1;
        graph.edges[0].equi_pairs.push(second);
        assert!((split_cardinality(graph, 1, 2)? - 5_000_000.0).abs() < 1e-7);
        Ok(())
    }

    #[test]
    fn correlated_composite_keys_do_not_collapse_positive_output() -> Result<()> {
        let rows = 1_000_000_000_000_000_000;
        let mut graph = graph(&[rows; 2], &[Some(rows); 2], &[(0, 1)])?;
        let schema = Arc::new(Schema::new(
            (0..18)
                .map(|index| Field::new(format!("k{index}"), DataType::Int32, false))
                .collect::<Vec<_>>(),
        ));
        for relation in &mut graph.relations {
            relation.plan = Arc::new(EmptyExec::new(schema.clone()));
            relation.statistics.column_statistics =
                vec![relation.statistics.column_statistics[0].clone(); 18];
        }
        let pair = graph.edges[0].equi_pairs[0].clone();
        graph.edges[0].equi_pairs = (0..18)
            .map(|index| {
                let mut pair = pair.clone();
                pair.0.column_index = index;
                pair.1.column_index = index;
                pair
            })
            .collect();
        let estimate = split_cardinality(graph, 1, 2)?;
        assert!((estimate / rows as f64 - 1.0).abs() < 1e-10);
        Ok(())
    }

    #[test]
    fn large_row_products_and_domains_cancel_before_rounding() -> Result<()> {
        let rows = [1_000_000_000_000_000_000; 20];
        let ndvs = [Some(rows[0]); 20];
        let edges: Vec<_> = (1..20).map(|id| (id - 1, id)).collect();
        let graph = graph(&rows, &ndvs, &edges)?;
        let mut estimator = CardinalityEstimator::new(graph.clone());
        let direct = estimator.estimate_cardinality(JoinSet::from_bits((1 << 20) - 1))?;
        let split = split_cardinality(graph, (1 << 10) - 1, ((1 << 10) - 1) << 10)?;
        assert!((direct / rows[0] as f64 - 1.0).abs() < 1e-10);
        assert!((split / direct - 1.0).abs() < 1e-10);
        Ok(())
    }

    #[test]
    fn theta_join_and_empty_inputs_have_bounded_estimates() -> Result<()> {
        let mut graph = graph(&[1_000_000; 2], &[None; 2], &[(0, 1)])?;
        graph.edges[0].equi_pairs.clear();
        assert!((split_cardinality(graph.clone(), 1, 2)? / 1e12 - 1.0).abs() < 1e-10);
        graph.relations[0].initial_cardinality = 0.0;
        assert_eq!(split_cardinality(graph, 1, 2)?, 0.0);
        Ok(())
    }
}
