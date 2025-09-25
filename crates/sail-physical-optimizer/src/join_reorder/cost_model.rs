use crate::join_reorder::dp_plan::DPPlan;

/// Cost model for evaluating the quality of a join plan.
/// Cost(AB) = Cost(A) + Cost(B) + Cardinality(AB)
pub struct CostModel;

impl CostModel {
    pub fn new() -> Self {
        Self
    }

    /// Calculate the total cost of a new plan after joining two subplans.
    pub fn compute_cost(
        &self,
        left_plan: &DPPlan,
        right_plan: &DPPlan,
        new_cardinality: f64,
    ) -> f64 {
        left_plan.cost + right_plan.cost + new_cardinality
    }
}

impl Default for CostModel {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_cost_model_creation() {
        let _model = CostModel::new();
        let _default_model = CostModel;
    }

    #[test]
    fn test_compute_cost() {
        let model = CostModel::new();

        let left_plan = DPPlan::new_leaf(0, 1000.0).unwrap();
        let right_plan = DPPlan::new_leaf(1, 2000.0).unwrap();

        let cost = model.compute_cost(&left_plan, &right_plan, 500.0);

        // Cost = left_cost + right_cost + new_cardinality = 0 + 0 + 500 = 500
        assert_eq!(cost, 500.0);
    }

    #[test]
    fn test_compute_cost_with_existing_costs() {
        let model = CostModel::new();

        let mut left_plan = DPPlan::new_leaf(0, 1000.0).unwrap();
        left_plan.cost = 100.0;

        let mut right_plan = DPPlan::new_leaf(1, 2000.0).unwrap();
        right_plan.cost = 200.0;

        let cost = model.compute_cost(&left_plan, &right_plan, 500.0);

        // Cost = 100 + 200 + 500 = 800
        assert_eq!(cost, 800.0);
    }
}
