mod core;
mod options;
mod state;

use indexmap::IndexMap;
pub use options::WorkerScalerOptions;
pub(crate) use state::{WorkerDemandReason, WorkerLaunchRequest, WorkerRetryRequest};

use crate::driver::worker_scaler::state::WorkerDemand;
use crate::id::{IdGenerator, WorkerDemandId, WorkerId};

pub struct WorkerScaler {
    options: WorkerScalerOptions,
    demands: IndexMap<WorkerDemandId, WorkerDemand>,
    workers: IndexMap<WorkerId, WorkerDemandId>,
    worker_demand_id_generator: IdGenerator<WorkerDemandId>,
}

impl WorkerScaler {
    pub fn new(options: WorkerScalerOptions) -> Self {
        Self {
            options,
            demands: IndexMap::new(),
            workers: IndexMap::new(),
            worker_demand_id_generator: IdGenerator::new(),
        }
    }
}
