use std::sync::Arc;

use datafusion::optimizer::{Optimizer, OptimizerRule};

pub fn default_optimizer_rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let Optimizer { rules } = Optimizer::default();
    rules
}
