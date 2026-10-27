use log::warn;

use crate::driver::worker_scaler::state::{WorkerDemand, WorkerDemandState};
use crate::driver::worker_scaler::{
    WorkerDemandReason, WorkerLaunchRequest, WorkerRetryRequest, WorkerScaler,
};
use crate::error::ExecutionResult;
use crate::id::{WorkerDemandId, WorkerId};

impl WorkerScaler {
    pub fn request_initial_workers(
        &mut self,
        count: usize,
    ) -> ExecutionResult<Vec<WorkerLaunchRequest>> {
        self.create_demands(count, WorkerDemandReason::Initial)
    }

    pub fn reconcile(&mut self, target: usize) -> ExecutionResult<Vec<WorkerLaunchRequest>> {
        let initial = self
            .demands
            .values()
            .filter(|demand| matches!(demand.reason, WorkerDemandReason::Initial))
            .count();
        let target = target.saturating_sub(initial);
        self.remove_surplus_task_demands(target);

        let current = self
            .demands
            .values()
            .filter(|demand| matches!(demand.reason, WorkerDemandReason::Task))
            .count();
        self.create_demands(target.saturating_sub(current), WorkerDemandReason::Task)
    }

    pub fn bind_worker(&mut self, request: WorkerLaunchRequest, worker_id: WorkerId) -> bool {
        let Some(demand) = self.demands.get_mut(&request.demand_id) else {
            warn!("worker demand {} not found", request.demand_id);
            return false;
        };
        if !matches!(
            demand.state,
            WorkerDemandState::Created { attempt } if attempt == request.attempt
        ) {
            warn!("worker demand {} is not ready to launch", request.demand_id);
            return false;
        }
        demand.state = WorkerDemandState::Launching {
            worker_id,
            attempt: request.attempt,
        };
        self.workers.insert(worker_id, request.demand_id);
        true
    }

    pub fn worker_registered(&mut self, worker_id: WorkerId) -> bool {
        let Some(demand_id) = self.workers.swap_remove(&worker_id) else {
            warn!("worker {worker_id} is not associated with a demand");
            return false;
        };
        self.demands.swap_remove(&demand_id).is_some()
    }

    pub fn worker_failed(&mut self, worker_id: WorkerId) -> Option<WorkerRetryRequest> {
        let Some(demand_id) = self.workers.swap_remove(&worker_id) else {
            warn!("worker {worker_id} is not associated with a demand");
            return None;
        };
        let attempt = match self.demands.get(&demand_id).map(|demand| &demand.state) {
            Some(WorkerDemandState::Launching {
                worker_id: launching_worker_id,
                attempt,
            }) if *launching_worker_id == worker_id => *attempt,
            Some(_) => {
                warn!("worker demand {demand_id} is not launching worker {worker_id}");
                return None;
            }
            None => {
                warn!("worker demand {demand_id} not found");
                return None;
            }
        };
        self.fail_demand(demand_id, attempt)
    }

    pub fn retry(&mut self, request: WorkerRetryRequest) -> Option<WorkerLaunchRequest> {
        let demand = self.demands.get_mut(&request.demand_id)?;
        if !matches!(
            demand.state,
            WorkerDemandState::WaitingForRetry { attempt } if attempt == request.attempt
        ) {
            return None;
        }
        demand.state = WorkerDemandState::Created {
            attempt: request.attempt,
        };
        Some(WorkerLaunchRequest {
            demand_id: request.demand_id,
            attempt: request.attempt,
        })
    }

    pub fn has_pending_worker_demands(&self) -> bool {
        self.demands.values().any(|demand| {
            matches!(
                demand.state,
                WorkerDemandState::Created { .. }
                    | WorkerDemandState::Launching { .. }
                    | WorkerDemandState::WaitingForRetry { .. }
            )
        })
    }

    fn create_demands(
        &mut self,
        count: usize,
        reason: WorkerDemandReason,
    ) -> ExecutionResult<Vec<WorkerLaunchRequest>> {
        let mut requests = Vec::with_capacity(count);
        for _ in 0..count {
            let demand_id = self.worker_demand_id_generator.generate()?;
            let attempt = 0;
            self.demands.insert(
                demand_id,
                WorkerDemand {
                    reason,
                    state: WorkerDemandState::Created { attempt },
                    retries: self.options.worker_launch_retry_strategy.retries(),
                },
            );
            requests.push(WorkerLaunchRequest { demand_id, attempt });
        }
        Ok(requests)
    }

    fn fail_demand(
        &mut self,
        demand_id: WorkerDemandId,
        attempt: usize,
    ) -> Option<WorkerRetryRequest> {
        let demand = self.demands.get_mut(&demand_id)?;
        if let Some(step) = demand.retries.next() {
            demand.state = WorkerDemandState::WaitingForRetry {
                attempt: step.retry,
            };
            Some(WorkerRetryRequest {
                demand_id,
                attempt: step.retry,
                delay: step.delay,
            })
        } else {
            warn!("worker demand {demand_id} launch retries exhausted after attempt {attempt}");
            let reason = demand.reason;
            if matches!(reason, WorkerDemandReason::Task) {
                demand.state = WorkerDemandState::Exhausted;
            }
            if matches!(reason, WorkerDemandReason::Initial) {
                self.demands.swap_remove(&demand_id);
            }
            None
        }
    }

    fn remove_surplus_task_demands(&mut self, target: usize) {
        let mut surplus = self
            .demands
            .values()
            .filter(|demand| matches!(demand.reason, WorkerDemandReason::Task))
            .count()
            .saturating_sub(target);
        if surplus == 0 {
            return;
        }

        let predicates: [fn(&WorkerDemandState) -> bool; 3] = [
            |state: &WorkerDemandState| matches!(state, WorkerDemandState::Exhausted),
            |state: &WorkerDemandState| matches!(state, WorkerDemandState::WaitingForRetry { .. }),
            |state: &WorkerDemandState| matches!(state, WorkerDemandState::Created { .. }),
        ];
        for predicate in predicates {
            let removable = self
                .demands
                .iter()
                .filter_map(|(demand_id, demand)| {
                    (matches!(demand.reason, WorkerDemandReason::Task) && predicate(&demand.state))
                        .then_some(*demand_id)
                })
                .collect::<Vec<_>>();
            for demand_id in removable {
                if surplus == 0 {
                    return;
                }
                self.demands.swap_remove(&demand_id);
                surplus -= 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]

    use std::time::Duration;

    use sail_common::utils::retry::RetryStrategy;

    use crate::driver::worker_scaler::{WorkerScaler, WorkerScalerOptions};
    use crate::id::WorkerId;

    fn worker_scaler(max_count: usize) -> WorkerScaler {
        WorkerScaler::new(WorkerScalerOptions::new(RetryStrategy::Fixed {
            max_count,
            delay: Duration::ZERO,
        }))
    }

    #[test]
    fn exhausted_demand_is_not_recreated_until_target_decreases() {
        let mut scaler = worker_scaler(0);
        let request = scaler.reconcile(1).unwrap().pop().unwrap();
        assert!(scaler.bind_worker(request, WorkerId::from(1)));
        assert!(scaler.worker_failed(WorkerId::from(1)).is_none());

        assert!(scaler.reconcile(1).unwrap().is_empty());
        assert!(scaler.reconcile(0).unwrap().is_empty());
        let next = scaler.reconcile(1).unwrap().pop().unwrap();
        assert_ne!(next.demand_id, request.demand_id);
    }

    #[test]
    fn retry_preserves_worker_demand_id() {
        let mut scaler = worker_scaler(1);
        let first = scaler.reconcile(1).unwrap().pop().unwrap();
        assert!(scaler.bind_worker(first, WorkerId::from(1)));

        let retry = scaler.worker_failed(WorkerId::from(1)).unwrap();
        assert_eq!(retry.demand_id, first.demand_id);
        assert_eq!(retry.attempt, 1);

        let second = scaler.retry(retry).unwrap();
        assert_eq!(second.demand_id, first.demand_id);
        assert!(scaler.bind_worker(second, WorkerId::from(2)));
        assert!(scaler.worker_registered(WorkerId::from(2)));
    }

    #[test]
    fn initial_demand_counts_toward_task_target() {
        let mut scaler = worker_scaler(0);
        assert_eq!(scaler.request_initial_workers(2).unwrap().len(), 2);
        assert!(scaler.reconcile(2).unwrap().is_empty());
        assert_eq!(scaler.reconcile(3).unwrap().len(), 1);
    }
}
