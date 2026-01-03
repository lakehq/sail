use indexmap::IndexSet;
use log::{error, warn};

use crate::driver::task::{TaskAssignment, TaskAssignmentGetter, TaskSetAssignment};
use crate::driver::task_assigner::state::{TaskSlot, WorkerResource};
use crate::driver::task_assigner::{TaskAssigner, TaskRegion};
use crate::id::{StageKey, TaskKey, TaskKeyDisplay, WorkerId};
use crate::job_graph::TaskPlacement;

impl TaskAssigner {
    pub fn activate_worker(&mut self, worker_id: WorkerId) {
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

    pub fn exclude_task(&mut self, key: TaskKey) {
        self.task_queue.retain(|x| !x.contains(&key));
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
                    self.task_queue.push_front(region);
                }
            }
        }

        // Update the driver and worker based on the assignments.
        for assignment in assignments.iter() {
            match assignment.assignment {
                TaskAssignment::Driver => {
                    self.driver.add_task_set(assignment.set.clone());
                }
                TaskAssignment::Worker { worker_id, slot } => {
                    if let Some(worker) = self.workers.get_mut(&worker_id) {
                        worker.add_task_set(slot, assignment.set.clone());
                    } else {
                        error!("worker {worker_id} not found");
                    }
                }
            }
        }

        assignments
    }

    pub fn unassign_task(&mut self, key: &TaskKey) -> Option<TaskAssignment> {
        let Some(assignment) = self.task_assignments.get(key) else {
            warn!("task {} not found in task assignments", TaskKeyDisplay(key));
            return None;
        };
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

    pub fn unassign_streams_by_stage(&mut self, stage: &StageKey) {
        self.driver.remove_streams_by_stage(stage);
        for worker in self.workers.values_mut() {
            if matches!(worker, WorkerResource::Active { .. }) {
                worker.remove_streams_by_stage(stage);
            }
        }
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
        self.slots.iter_mut().find_map(|(worker_id, slots)| {
            if let Some(slot) = slots.pop() {
                Some((*worker_id, slot))
            } else {
                None
            }
        })
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
