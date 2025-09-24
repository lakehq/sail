use datafusion::error::Result;

use crate::join_reorder::join_set::JoinSet;

/// An entry in the DP table representing a join subplan.
#[derive(Debug, Clone)]
pub struct DPPlan {
    /// The set of relations contained in this plan.
    pub join_set: JoinSet,
    /// The estimated cost of this plan.
    pub cost: f64,
    /// The estimated output cardinality of this plan.
    pub cardinality: f64,
    /// How this plan was constructed.
    pub plan_type: PlanType,
}

impl DPPlan {
    /// Creates a leaf node plan (single relation).
    pub fn new_leaf(relation_id: usize, cardinality: f64) -> Result<Self> {
        Ok(Self {
            join_set: JoinSet::new_singleton(relation_id)?,
            cost: 0.0, // Leaf nodes have zero join cost
            cardinality,
            plan_type: PlanType::Leaf { relation_id },
        })
    }

    /// Creates a join node plan.
    pub fn new_join(
        left_set: JoinSet,
        right_set: JoinSet,
        edge_indices: Vec<usize>,
        cost: f64,
        cardinality: f64,
    ) -> Self {
        Self {
            join_set: left_set.union(&right_set),
            cost,
            cardinality,
            plan_type: PlanType::Join {
                left_set,
                right_set,
                edge_indices,
            },
        }
    }

    /// Returns true if this is a leaf plan (single relation).
    #[cfg(test)]
    pub fn is_leaf(&self) -> bool {
        matches!(self.plan_type, PlanType::Leaf { .. })
    }
}

#[derive(Debug, Clone)]
pub enum PlanType {
    /// Leaf node representing a single relation.
    Leaf { relation_id: usize },
    /// Internal node formed by joining two subplans.
    Join {
        /// The relation set of the left subplan.
        left_set: JoinSet,
        /// The relation set of the right subplan.
        right_set: JoinSet,
        /// Edges used to connect left and right subplans (indices in QueryGraph).
        edge_indices: Vec<usize>,
    },
}

impl PlanType {
    /// Returns the left set if this is a Join, None otherwise.
    #[cfg(test)]
    pub fn left_set(&self) -> Option<JoinSet> {
        match self {
            PlanType::Join { left_set, .. } => Some(*left_set),
            PlanType::Leaf { .. } => None,
        }
    }

    /// Returns the right set if this is a Join, None otherwise.
    #[cfg(test)]
    pub fn right_set(&self) -> Option<JoinSet> {
        match self {
            PlanType::Join { right_set, .. } => Some(*right_set),
            PlanType::Leaf { .. } => None,
        }
    }

    /// Returns the edge indices if this is a Join, None otherwise.
    #[cfg(test)]
    pub fn edge_indices(&self) -> Option<&[usize]> {
        match self {
            PlanType::Join { edge_indices, .. } => Some(edge_indices),
            PlanType::Leaf { .. } => None,
        }
    }

    /// Returns the relation ID if this is a Leaf, None otherwise.
    #[cfg(test)]
    pub fn relation_id(&self) -> Option<usize> {
        match self {
            PlanType::Leaf { relation_id } => Some(*relation_id),
            PlanType::Join { .. } => None,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_leaf_plan() {
        let plan = DPPlan::new_leaf(0, 1000.0).unwrap();
        assert!(plan.is_leaf());
        assert_eq!(plan.join_set.cardinality(), 1);
        assert_eq!(plan.cardinality, 1000.0);
        assert_eq!(plan.cost, 0.0); // Leaf nodes have zero join cost

        if let PlanType::Leaf { relation_id } = plan.plan_type {
            assert_eq!(relation_id, 0);
        } else {
            unreachable!("Expected leaf plan type");
        }
    }

    #[test]
    fn test_join_plan() {
        let left_set = JoinSet::new_singleton(0).unwrap();
        let right_set = JoinSet::new_singleton(1).unwrap();
        let edge_indices = vec![0];

        let plan = DPPlan::new_join(left_set, right_set, edge_indices.clone(), 2000.0, 500.0);

        assert!(!plan.is_leaf());
        assert_eq!(plan.join_set.cardinality(), 2);
        assert_eq!(plan.cardinality, 500.0);
        assert_eq!(plan.cost, 2000.0);

        if let PlanType::Join {
            left_set: l,
            right_set: r,
            edge_indices: e,
        } = plan.plan_type
        {
            assert_eq!(l, left_set);
            assert_eq!(r, right_set);
            assert_eq!(e, edge_indices);
        } else {
            unreachable!("Expected join plan type");
        }
    }

    #[test]
    fn test_plan_type_methods() {
        let left_set = JoinSet::new_singleton(0).unwrap();
        let right_set = JoinSet::new_singleton(1).unwrap();
        let join_type = PlanType::Join {
            left_set,
            right_set,
            edge_indices: vec![0],
        };

        assert_eq!(join_type.left_set(), Some(left_set));
        assert_eq!(join_type.right_set(), Some(right_set));
        assert_eq!(join_type.edge_indices(), Some(&[0][..]));
        assert_eq!(join_type.relation_id(), None);

        let leaf_type = PlanType::Leaf { relation_id: 5 };
        assert_eq!(leaf_type.left_set(), None);
        assert_eq!(leaf_type.right_set(), None);
        assert_eq!(leaf_type.edge_indices(), None);
        assert_eq!(leaf_type.relation_id(), Some(5));
    }
}
