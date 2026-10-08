use std::time::Duration;

use sail_common::utils::retry::RetrySchedule;

use crate::id::{WorkerDemandId, WorkerId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerDemandReason {
    Initial,
    Task,
}

pub struct WorkerDemand {
    pub reason: WorkerDemandReason,
    pub state: WorkerDemandState,
    pub retries: RetrySchedule,
}

pub enum WorkerDemandState {
    Created { attempt: usize },
    Launching { worker_id: WorkerId, attempt: usize },
    WaitingForRetry { attempt: usize },
    Exhausted,
}

#[derive(Debug, Clone, Copy)]
pub struct WorkerLaunchRequest {
    pub demand_id: WorkerDemandId,
    pub attempt: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct WorkerRetryRequest {
    pub demand_id: WorkerDemandId,
    pub attempt: usize,
    pub delay: Duration,
}
