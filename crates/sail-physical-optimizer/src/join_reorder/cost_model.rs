use datafusion::config::ConfigOptions;

use crate::join_reorder::JoinReorderOptions;
use crate::join_reorder::dp_plan::DPPlan;

/// Cost model for evaluating the quality of a join plan.
///
/// `HashJoinExec` builds its hash table from the left input and probes with the right input.
/// Hashing scales with the number of keys. Large joins also redistribute their inputs;
/// charging only join outputs can otherwise favor filtering after a large fact join.
pub struct CostModel {
    build_side_weight: f64,
    probe_side_weight: f64,
    output_weight: f64,
    repartition_joins: bool,
    collect_threshold_rows: f64,
}

impl CostModel {
    pub fn new(options: &JoinReorderOptions, config: &ConfigOptions) -> Self {
        Self {
            build_side_weight: options.build_side_weight,
            probe_side_weight: options.probe_side_weight,
            output_weight: options.output_weight,
            repartition_joins: config.optimizer.repartition_joins
                && config.execution.target_partitions > 1,
            collect_threshold_rows: config.optimizer.hash_join_single_partition_threshold_rows
                as f64,
        }
    }

    /// Calculate the total cost of a new plan after joining two subplans.
    pub fn compute_cost(
        &self,
        left_plan: &DPPlan,
        right_plan: &DPPlan,
        new_cardinality: f64,
        key_count: usize,
    ) -> f64 {
        let keys = key_count.max(1) as f64;
        let redistribution = if key_count > 0
            && self.repartition_joins
            && left_plan.cardinality.min(right_plan.cardinality) >= self.collect_threshold_rows
        {
            (left_plan.cardinality + right_plan.cardinality)
                * (self.output_weight + keys * self.probe_side_weight)
        } else {
            0.0
        };
        self.compute_cost_for_distribution(
            left_plan,
            right_plan,
            new_cardinality,
            key_count,
            redistribution,
        )
    }

    /// Cost an already selected distribution instead of predicting CollectLeft from rows.
    pub fn compute_cost_for_distribution(
        &self,
        left_plan: &DPPlan,
        right_plan: &DPPlan,
        new_cardinality: f64,
        key_count: usize,
        redistribution: f64,
    ) -> f64 {
        let keys = key_count.max(1) as f64;
        let probe_rows = if key_count == 0 {
            // Residual predicates still require testing every input pair.
            left_plan.cardinality * right_plan.cardinality
        } else {
            right_plan.cardinality
        };
        (left_plan.cost
            + right_plan.cost
            + (new_cardinality * self.output_weight)
            + (left_plan.cardinality * self.build_side_weight * keys)
            + (probe_rows * self.probe_side_weight * keys)
            + redistribution)
            .min(f64::MAX)
    }

    pub fn redistribution_cost(&self, rows: f64, row_width: f64, key_count: usize) -> f64 {
        (rows * (row_width / 8.0 * self.output_weight + key_count as f64 * self.probe_side_weight))
            .min(f64::MAX)
    }
}

impl Default for CostModel {
    fn default() -> Self {
        Self::new(&JoinReorderOptions::default(), &ConfigOptions::new())
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn saturated_cardinalities_keep_cost_finite() {
        let model = CostModel::default();
        let left = DPPlan::new_leaf(0, f64::MAX).unwrap();
        let right = DPPlan::new_leaf(1, f64::MAX).unwrap();
        assert_eq!(model.compute_cost(&left, &right, f64::MAX, 3), f64::MAX);
    }

    #[test]
    fn test_cost_model_creation() {
        let _model = CostModel::new(&JoinReorderOptions::default(), &ConfigOptions::new());
        let _default_model = CostModel::default();
    }

    #[test]
    fn test_compute_cost() {
        let model = CostModel::default();

        let left_plan = DPPlan::new_leaf(0, 1000.0).unwrap();
        let right_plan = DPPlan::new_leaf(1, 2000.0).unwrap();

        let cost = model.compute_cost(&left_plan, &right_plan, 500.0, 1);

        // Cost = output + build + probe = 500 + 1000 + 200 = 1700
        assert_eq!(cost, 1700.0);
    }

    #[test]
    fn test_compute_cost_with_existing_costs() {
        let model = CostModel::default();

        let mut left_plan = DPPlan::new_leaf(0, 1000.0).unwrap();
        left_plan.cost = 100.0;

        let mut right_plan = DPPlan::new_leaf(1, 2000.0).unwrap();
        right_plan.cost = 200.0;

        let cost = model.compute_cost(&left_plan, &right_plan, 500.0, 1);

        // Cost = child costs + output + build + probe = 100 + 200 + 500 + 1000 + 200
        assert_eq!(cost, 2000.0);
    }

    #[test]
    fn keyless_join_charges_input_pairs_before_residual_filtering() {
        let model = CostModel::default();
        let left = DPPlan::new_leaf(0, 1000.0).unwrap();
        let right = DPPlan::new_leaf(1, 2000.0).unwrap();
        let hash_cost = model.compute_cost(&left, &right, 1.0, 1);
        let nested_loop_cost = model.compute_cost(&left, &right, 1.0, 0);
        assert_eq!(nested_loop_cost, 201001.0);
        assert!(nested_loop_cost > hash_cost * 100.0);
    }

    #[test]
    fn selective_dimension_precedes_large_multikey_join() {
        let model = CostModel::default();
        let fact = DPPlan::new_leaf(0, 28_000_000.0).unwrap();
        let returns = DPPlan::new_leaf(1, 400_000.0).unwrap();
        let dimension = DPPlan::new_leaf(2, 90.0).unwrap();

        let mut filtered_fact = fact.clone();
        filtered_fact.cardinality = 840_000.0;
        filtered_fact.cost = model.compute_cost(&dimension, &fact, 840_000.0, 1);
        let filter_first = model.compute_cost(&returns, &filtered_fact, 9_000.0, 3);

        let mut joined_facts = fact.clone();
        joined_facts.cardinality = 300_000.0;
        joined_facts.cost = model.compute_cost(&returns, &fact, 300_000.0, 3);
        let filter_last = model.compute_cost(&dimension, &joined_facts, 9_000.0, 1);

        assert!(filter_first < filter_last);
    }

    #[test]
    fn redistribution_respects_execution_configuration() {
        let left = DPPlan::new_leaf(0, 1_000_000.0).unwrap();
        let right = DPPlan::new_leaf(1, 2_000_000.0).unwrap();
        let options = JoinReorderOptions::default();
        let mut config = ConfigOptions::new();
        config.execution.target_partitions = 10;
        let partitioned = CostModel::new(&options, &config).compute_cost(&left, &right, 500.0, 2);
        config.execution.target_partitions = 1;
        let single = CostModel::new(&options, &config).compute_cost(&left, &right, 500.0, 2);
        config.execution.target_partitions = 10;
        config.optimizer.repartition_joins = false;
        let disabled = CostModel::new(&options, &config).compute_cost(&left, &right, 500.0, 2);
        assert!(partitioned > single);
        assert_eq!(single, disabled);
    }
}
