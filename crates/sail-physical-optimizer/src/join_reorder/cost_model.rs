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
        // The cost of a join plan is the sum of its children's costs plus the
        // cardinality of its own output. This penalizes plans with large intermediate results.
        left_plan.cost + right_plan.cost + new_cardinality
    }

    /// Estimate access cost for a single relation.
    /// Returns the cardinality as the scan cost.
    pub fn compute_scan_cost(&self, cardinality: f64) -> f64 {
        cardinality
    }

    /// Compare costs of two plans and return the better one.
    pub fn choose_better_plan<'a>(&self, plan1: &'a DPPlan, plan2: &'a DPPlan) -> &'a DPPlan {
        if plan1.cost <= plan2.cost {
            plan1
        } else {
            plan2
        }
    }
}

impl Default for CostModel {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cost_model_creation() {
        let _model = CostModel::new();
        let _default_model = CostModel::default();
    }

    #[test]
    fn test_compute_cost() {
        let model = CostModel::new();

        let left_plan = DPPlan::new_leaf(0, 1000.0);
        let right_plan = DPPlan::new_leaf(1, 2000.0);

        let cost = model.compute_cost(&left_plan, &right_plan, 500.0);

        // Cost = left_cost + right_cost + new_cardinality = 1000 + 2000 + 500 = 3500
        assert_eq!(cost, 3500.0);
    }

    #[test]
    fn test_compute_cost_with_existing_costs() {
        let model = CostModel::new();

        let mut left_plan = DPPlan::new_leaf(0, 1000.0);
        left_plan.cost = 100.0;

        let mut right_plan = DPPlan::new_leaf(1, 2000.0);
        right_plan.cost = 200.0;

        let cost = model.compute_cost(&left_plan, &right_plan, 500.0);

        // Cost = 100 + 200 + 500 = 800
        assert_eq!(cost, 800.0);
    }

    #[test]
    fn test_scan_cost() {
        let model = CostModel::new();
        let cost = model.compute_scan_cost(1000.0);
        assert_eq!(cost, 1000.0);
    }

    #[test]
    fn test_choose_better_plan() {
        let model = CostModel::new();

        let mut plan1 = DPPlan::new_leaf(0, 1000.0);
        plan1.cost = 100.0;

        let mut plan2 = DPPlan::new_leaf(1, 2000.0);
        plan2.cost = 200.0;

        let better = model.choose_better_plan(&plan1, &plan2);
        assert_eq!(better.cost, 100.0);
    }
}
