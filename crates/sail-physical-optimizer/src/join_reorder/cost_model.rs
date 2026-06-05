use crate::join_reorder::dp_plan::DPPlan;
use crate::join_reorder::JoinReorderOptions;

/// Cost model for evaluating the quality of a join plan.
///
/// `HashJoinExec` builds its hash table from the left input and probes with the right input.
/// The join-output term preserves the previous Cmout behavior, while build/probe terms let DP
/// choose the physical child order that best matches execution cost.
pub struct CostModel {
    build_side_weight: f64,
    probe_side_weight: f64,
    output_weight: f64,
}

impl CostModel {
    pub fn new(options: &JoinReorderOptions) -> Self {
        Self {
            build_side_weight: options.build_side_weight,
            probe_side_weight: options.probe_side_weight,
            output_weight: options.output_weight,
        }
    }

    /// Calculate the total cost of a new plan after joining two subplans.
    pub fn compute_cost(
        &self,
        left_plan: &DPPlan,
        right_plan: &DPPlan,
        new_cardinality: f64,
    ) -> f64 {
        left_plan.cost
            + right_plan.cost
            + (new_cardinality * self.output_weight)
            + (left_plan.cardinality * self.build_side_weight)
            + (right_plan.cardinality * self.probe_side_weight)
    }
}

impl Default for CostModel {
    fn default() -> Self {
        Self::new(&JoinReorderOptions::default())
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_cost_model_creation() {
        let _model = CostModel::new(&JoinReorderOptions::default());
        let _default_model = CostModel::default();
    }

    #[test]
    fn test_compute_cost() {
        let model = CostModel::default();

        let left_plan = DPPlan::new_leaf(0, 1000.0).unwrap();
        let right_plan = DPPlan::new_leaf(1, 2000.0).unwrap();

        let cost = model.compute_cost(&left_plan, &right_plan, 500.0);

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

        let cost = model.compute_cost(&left_plan, &right_plan, 500.0);

        // Cost = child costs + output + build + probe = 100 + 200 + 500 + 1000 + 200
        assert_eq!(cost, 2000.0);
    }
}
