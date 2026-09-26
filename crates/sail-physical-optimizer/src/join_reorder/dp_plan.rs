use std::sync::Arc;

use datafusion::error::Result;
use datafusion::physical_plan::joins::PartitionMode;

use crate::join_reorder::graph::StableColumn;
use crate::join_reorder::join_set::JoinSet;

/// Distribution after the selected operators' input requirements have been enforced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinDistribution {
    Unknown(usize),
    Hash(Vec<StableColumn>, usize),
}

impl JoinDistribution {
    pub fn partition_count(&self) -> usize {
        match self {
            Self::Unknown(n) | Self::Hash(_, n) => *n,
        }
    }
}

/// A candidate owns its exact children; another winner for a child subset cannot replace them.
#[derive(Debug, Clone)]
pub struct DPPlan {
    pub join_set: JoinSet,
    pub cost: f64,
    pub heuristic_penalty: f64,
    pub cardinality: f64,
    pub row_width: f64,
    pub has_byte_statistics: bool,
    pub distribution: JoinDistribution,
    pub plan_type: PlanType,
}

impl DPPlan {
    pub fn new_leaf(relation_id: usize, cardinality: f64) -> Result<Self> {
        Ok(Self {
            join_set: JoinSet::new_singleton(relation_id)?,
            cost: 0.0,
            heuristic_penalty: 0.0,
            cardinality,
            row_width: 8.0,
            has_byte_statistics: false,
            distribution: JoinDistribution::Unknown(1),
            plan_type: PlanType::Leaf { relation_id },
        })
    }

    pub fn new_join(
        left: Arc<DPPlan>,
        right: Arc<DPPlan>,
        edge_indices: Vec<usize>,
        cost: f64,
        cardinality: f64,
        mode: PartitionMode,
    ) -> Self {
        Self {
            join_set: left.join_set | right.join_set,
            cost,
            heuristic_penalty: (left.heuristic_penalty + right.heuristic_penalty).min(f64::MAX),
            has_byte_statistics: left.has_byte_statistics && right.has_byte_statistics,
            cardinality,
            row_width: left.row_width + right.row_width,
            distribution: JoinDistribution::Unknown(1),
            plan_type: PlanType::Join {
                left,
                right,
                edge_indices,
                mode,
            },
        }
    }

    pub fn score(&self) -> f64 {
        (self.cost + self.heuristic_penalty).min(f64::MAX)
    }

    #[cfg(test)]
    pub fn is_leaf(&self) -> bool {
        matches!(self.plan_type, PlanType::Leaf { .. })
    }
}

#[derive(Debug, Clone)]
pub enum PlanType {
    Leaf {
        relation_id: usize,
    },
    Join {
        left: Arc<DPPlan>,
        right: Arc<DPPlan>,
        edge_indices: Vec<usize>,
        mode: PartitionMode,
    },
}
