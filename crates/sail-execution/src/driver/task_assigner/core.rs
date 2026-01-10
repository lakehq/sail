use std::collections::HashSet;

use indexmap::IndexSet;
use log::{error, warn};

use crate::driver::task_assigner::state::{TaskSlot, WorkerResource};
use crate::driver::task_assigner::{TaskAssigner, TaskRegion};
use crate::id::{JobId, TaskKey, WorkerId};
use crate::job_graph::TaskPlacement;
use crate::task::scheduling::{
    TaskAssignment, TaskAssignmentGetter, TaskSetAssignment, TaskStreamAssignment,
};

impl TaskAssigner {
    pub fn request_workers(&mut self) -> usize {
        let enqueued_slots = self
            .task_queue
            .iter()
            .map(|region| {
                region
                    .tasks
                    .iter()
                    .filter(|(placement, _)| matches!(placement, TaskPlacement::Worker))
                    .count()
            })
            .sum::<usize>();
        let vacant_slots = self
            .workers
            .values()
            .map(|worker| match worker {
                WorkerResource::Active { task_slots, .. } => {
                    task_slots.iter().filter(|x| x.is_vacant()).count()
                }
                WorkerResource::Inactive => 0,
            })
            .sum::<usize>();
        let required_slots = enqueued_slots.saturating_sub(vacant_slots);
        let active_workers = self
            .workers
            .values()
            .filter(|worker| matches!(worker, WorkerResource::Active { .. }))
            .count();
        let allowed_workers = if self.options.worker_max_count == 0 {
            usize::MAX
        } else {
            self.options
                .worker_max_count
                .saturating_sub(self.requested_worker_count)
                .saturating_sub(active_workers)
        };
        let required_workers = required_slots
            .div_ceil(self.options.worker_task_slots)
            .min(allowed_workers);
        self.requested_worker_count = self.requested_worker_count.saturating_add(required_workers);
        required_workers
    }

    pub fn track_worker_failed_to_start(&mut self) {
        self.requested_worker_count = self.requested_worker_count.saturating_sub(1);
    }

    pub fn activate_worker(&mut self, worker_id: WorkerId) {
        self.requested_worker_count = self.requested_worker_count.saturating_sub(1);
        if self.workers.contains_key(&worker_id) {
            warn!("worker {worker_id} is already active");
            return;
        }
        self.workers.insert(
            worker_id,
            WorkerResource::Active {
                task_slots: vec![TaskSlot::default(); self.options.worker_task_slots],
                local_streams: IndexSet::new(),
            },
        );
    }

    pub fn deactivate_worker(&mut self, worker_id: WorkerId) {
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            warn!("worker {worker_id} not found");
            return;
        };
        match worker {
            WorkerResource::Active { .. } => {
                *worker = WorkerResource::Inactive;
            }
            WorkerResource::Inactive => {
                warn!("worker {worker_id} is already inactive");
            }
        }
    }

    pub fn enqueue_tasks(&mut self, region: TaskRegion) {
        self.task_queue.push_back(region);
    }

    pub fn exclude_task(&mut self, key: &TaskKey) {
        self.task_queue.retain(|x| !x.contains(key));
    }

    pub fn assign_tasks(&mut self) -> Vec<TaskSetAssignment> {
        let mut assignments = vec![];
        let mut assigner = self.build_worker_task_slot_assigner();

        while let Some(region) = self.task_queue.pop_front() {
            match assigner.try_assign_task_region(region) {
                Ok(x) => assignments.extend(x),
                Err(region) => {
                    // The region cannot be successfully assigned as a whole
                    // due to insufficient worker task slots.
                    // Put the region back to the queue and try again later.
                    // We must put the region back to the front of the queue to
                    // avoid starvation.
                    // This does result in head-of-line blocking, but we would
                    // like the regions to be assigned in the same order as they
                    // are enqueued.
                    self.task_queue.push_front(region);
                    break;
                }
            }
        }

        // Update the driver and worker based on the assignments.
        for assignment in assignments.iter() {
            match assignment.assignment {
                TaskAssignment::Driver => {
                    self.driver.add_task_set(assignment.set.clone());
                    for key in assignment.set.tasks() {
                        self.task_assignments
                            .insert(key.clone(), TaskAssignment::Driver);
                    }
                }
                TaskAssignment::Worker { worker_id, slot } => {
                    if let Some(worker) = self.workers.get_mut(&worker_id) {
                        worker.add_task_set(slot, assignment.set.clone());
                        for key in assignment.set.tasks() {
                            self.task_assignments
                                .insert(key.clone(), TaskAssignment::Worker { worker_id, slot });
                        }
                    } else {
                        error!("worker {worker_id} not found");
                    }
                }
            }
        }

        assignments
    }

    pub fn unassign_task(&mut self, key: &TaskKey) -> Option<TaskAssignment> {
        let assignment = self.task_assignments.get(key)?;
        match assignment {
            TaskAssignment::Driver => {
                self.driver.remove_task(key);
            }
            TaskAssignment::Worker { worker_id, slot } => {
                let Some(worker) = self.workers.get_mut(worker_id) else {
                    warn!("worker {worker_id} not found");
                    return None;
                };
                worker.remove_task(key, *slot);
            }
        }
        Some(assignment.clone())
    }

    pub fn track_streams(&mut self, assignments: &[TaskSetAssignment]) {
        for assignment in assignments {
            self.driver.track_remote_streams(&assignment.set);
            match &assignment.assignment {
                TaskAssignment::Driver => {
                    self.driver.track_local_streams(&assignment.set);
                }
                TaskAssignment::Worker { worker_id, .. } => {
                    if let Some(worker) = self.workers.get_mut(worker_id) {
                        worker.track_local_streams(&assignment.set);
                    } else {
                        error!("worker {worker_id} not found");
                    }
                }
            }
        }
    }

    pub fn untrack_local_streams(
        &mut self,
        job_id: JobId,
        stage: Option<usize>,
    ) -> HashSet<TaskStreamAssignment> {
        let mut assignments = HashSet::new();
        if self.driver.untrack_local_streams(job_id, stage) {
            assignments.insert(TaskStreamAssignment::Driver);
        }
        for (worker_id, worker) in self.workers.iter_mut() {
            if matches!(worker, WorkerResource::Active { .. })
                && worker.untrack_local_streams(job_id, stage)
            {
                assignments.insert(TaskStreamAssignment::Worker {
                    worker_id: *worker_id,
                });
            }
        }
        assignments
    }

    pub fn untrack_remote_streams(&mut self, job_id: JobId, stage: Option<usize>) -> bool {
        self.driver.untrack_remote_streams(job_id, stage)
    }

    pub fn is_worker_idle(&self, worker_id: WorkerId) -> bool {
        let Some(worker) = self.workers.get(&worker_id) else {
            warn!("worker {worker_id} not found");
            return false;
        };
        match worker {
            WorkerResource::Active {
                task_slots: slots,
                local_streams: streams,
            } => slots.iter().all(|s| s.is_vacant()) && streams.is_empty(),
            WorkerResource::Inactive => false,
        }
    }

    pub fn find_worker_tasks(&self, worker_id: WorkerId) -> Vec<TaskKey> {
        let Some(worker) = self.workers.get(&worker_id) else {
            warn!("worker {worker_id} not found");
            return vec![];
        };
        match worker {
            WorkerResource::Active {
                task_slots: slots, ..
            } => slots
                .iter()
                .flat_map(|x| x.list_tasks().cloned().collect::<Vec<_>>())
                .collect(),
            WorkerResource::Inactive => vec![],
        }
    }

    fn build_worker_task_slot_assigner(&self) -> TaskSlotAssigner {
        let slots = self
            .workers
            .iter()
            .filter_map(|(id, worker)| {
                let slots = match worker {
                    WorkerResource::Inactive => vec![],
                    WorkerResource::Active {
                        task_slots: slots, ..
                    } => slots
                        .iter()
                        .enumerate()
                        .filter_map(|(i, s)| s.is_vacant().then_some(i))
                        .collect(),
                };
                if !slots.is_empty() {
                    Some((*id, slots))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        TaskSlotAssigner::new(slots)
    }
}

impl TaskAssignmentGetter for TaskAssigner {
    fn get(&self, key: &TaskKey) -> Option<&TaskAssignment> {
        self.task_assignments.get(key)
    }
}

struct TaskSlotAssigner {
    /// The available task slots on workers.
    slots: Vec<(WorkerId, Vec<usize>)>,
}

impl TaskSlotAssigner {
    fn new(slots: Vec<(WorkerId, Vec<usize>)>) -> Self {
        Self { slots }
    }

    fn next(&mut self) -> Option<(WorkerId, usize)> {
        self.slots
            .iter_mut()
            .find_map(|(worker_id, slots)| slots.pop().map(|slot| (*worker_id, slot)))
    }

    fn try_assign_task_region(
        &mut self,
        region: TaskRegion,
    ) -> Result<Vec<TaskSetAssignment>, TaskRegion> {
        let mut assignments = vec![];

        for (placement, set) in &region.tasks {
            match placement {
                TaskPlacement::Driver => {
                    assignments.push(TaskSetAssignment {
                        set: set.clone(),
                        assignment: TaskAssignment::Driver,
                    });
                }
                TaskPlacement::Worker => {
                    if let Some((worker_id, slot)) = self.next() {
                        assignments.push(TaskSetAssignment {
                            set: set.clone(),
                            assignment: TaskAssignment::Worker { worker_id, slot },
                        });
                    } else {
                        // The worker task slots are not enough for assigning all the
                        // worker tasks in this region. So we return the region back
                        // to indicate the error.
                        return Err(region);
                    }
                }
            }
        }
        Ok(assignments)
    }
}
