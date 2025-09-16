use crate::join_reorder::dp_plan::DPPlan;

/// Join algorithm type, affects cost calculation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinAlgorithm {
    /// Hash Join - suitable for most equi-joins
    Hash,
    /// Nested Loop Join - suitable for small tables or non-equi joins
    NestedLoop,
    /// Sort-Merge Join - suitable for sorted data
    SortMerge,
}

impl JoinAlgorithm {
    /// Choose the best join algorithm based on input sizes.
    pub fn choose_best(left_cardinality: f64, right_cardinality: f64) -> Self {
        // Simplified algorithm selection logic
        if left_cardinality < 1000.0 || right_cardinality < 1000.0 {
            JoinAlgorithm::NestedLoop
        } else {
            JoinAlgorithm::Hash
        }
    }

    /// Get the relative cost factor of the algorithm.
    pub fn cost_factor(&self) -> f64 {
        match self {
            JoinAlgorithm::Hash => 1.0,
            JoinAlgorithm::NestedLoop => 2.0, // Usually more expensive than Hash Join
            JoinAlgorithm::SortMerge => 1.5,
        }
    }
}

/// Cost model for evaluating the quality of a join plan.
pub struct CostModel {
    /// CPU cost weight
    cpu_cost_weight: f64,
    /// I/O cost weight
    io_cost_weight: f64,
    /// Memory cost weight
    memory_cost_weight: f64,
}

impl CostModel {
    pub fn new() -> Self {
        Self {
            cpu_cost_weight: 1.0,
            io_cost_weight: 1.0,
            memory_cost_weight: 0.1,
        }
    }

    /// Create cost model with custom weights.
    pub fn with_weights(cpu_weight: f64, io_weight: f64, memory_weight: f64) -> Self {
        Self {
            cpu_cost_weight: cpu_weight,
            io_cost_weight: io_weight,
            memory_cost_weight: memory_weight,
        }
    }

    /// Calculate the total cost of a new plan after joining two subplans.
    pub fn compute_cost(
        &self,
        left_plan: &DPPlan,
        right_plan: &DPPlan,
        new_cardinality: f64,
    ) -> f64 {
        // Cost(AB) = Cost(A) + Cost(B) + JoinCost(A, B)
        let left_cost = left_plan.cost;
        let right_cost = right_plan.cost;

        let join_cost = self.compute_join_cost(
            left_plan.cardinality,
            right_plan.cardinality,
            new_cardinality,
        );

        left_cost + right_cost + join_cost
    }

    /// Calculate the cost of the join operation itself.
    fn compute_join_cost(
        &self,
        left_cardinality: f64,
        right_cardinality: f64,
        output_cardinality: f64,
    ) -> f64 {
        let algorithm = JoinAlgorithm::choose_best(left_cardinality, right_cardinality);

        let base_cost = match algorithm {
            JoinAlgorithm::Hash => {
                // Hash Join: O(M + N) where M, N are input sizes
                // Build phase: read smaller table to build hash table
                // Probe phase: read larger table for probing
                let build_cost = left_cardinality.min(right_cardinality);
                let probe_cost = left_cardinality.max(right_cardinality);
                build_cost + probe_cost
            }
            JoinAlgorithm::NestedLoop => {
                // Nested Loop Join: O(M * N)
                left_cardinality * right_cardinality
            }
            JoinAlgorithm::SortMerge => {
                // Sort-Merge Join: O(M log M + N log N + M + N)
                let left_sort_cost = if left_cardinality > 1.0 {
                    left_cardinality * left_cardinality.log2()
                } else {
                    left_cardinality
                };
                let right_sort_cost = if right_cardinality > 1.0 {
                    right_cardinality * right_cardinality.log2()
                } else {
                    right_cardinality
                };
                let merge_cost = left_cardinality + right_cardinality;
                left_sort_cost + right_sort_cost + merge_cost
            }
        };

        // Apply algorithm cost factor
        let adjusted_cost = base_cost * algorithm.cost_factor();

        // Add output cost (writing intermediate results)
        let output_cost = output_cardinality;

        // Combine various costs
        self.cpu_cost_weight * adjusted_cost
            + self.io_cost_weight * output_cost
            + self.memory_cost_weight * left_cardinality.max(right_cardinality)
    }

    /// Estimate access cost for a single relation.
    pub fn compute_scan_cost(&self, cardinality: f64) -> f64 {
        // Simplified scan cost model
        self.io_cost_weight * cardinality
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
    fn test_join_algorithm_selection() {
        // Small tables should choose Nested Loop
        let algorithm = JoinAlgorithm::choose_best(100.0, 200.0);
        assert_eq!(algorithm, JoinAlgorithm::NestedLoop);

        // Large tables should choose Hash Join
        let algorithm = JoinAlgorithm::choose_best(10000.0, 20000.0);
        assert_eq!(algorithm, JoinAlgorithm::Hash);
    }

    #[test]
    fn test_cost_model_creation() {
        let model = CostModel::new();
        assert_eq!(model.cpu_cost_weight, 1.0);
        assert_eq!(model.io_cost_weight, 1.0);
        assert_eq!(model.memory_cost_weight, 0.1);

        let custom_model = CostModel::with_weights(2.0, 1.5, 0.5);
        assert_eq!(custom_model.cpu_cost_weight, 2.0);
        assert_eq!(custom_model.io_cost_weight, 1.5);
        assert_eq!(custom_model.memory_cost_weight, 0.5);
    }

    #[test]
    fn test_compute_cost() {
        let model = CostModel::new();

        let left_plan = DPPlan::new_leaf(0, 1000.0);
        let right_plan = DPPlan::new_leaf(1, 2000.0);

        let cost = model.compute_cost(&left_plan, &right_plan, 500.0);

        // Cost should include both subplan costs (both are 0) plus join cost
        assert!(cost > 0.0);
    }

    #[test]
    fn test_scan_cost() {
        let model = CostModel::new();
        let cost = model.compute_scan_cost(1000.0);
        assert_eq!(cost, 1000.0); // Default I/O weight is 1.0
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
