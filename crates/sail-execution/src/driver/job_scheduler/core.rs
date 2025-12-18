use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{exec_datafusion_err, DataFusionError};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::ExecutionPlan;
use log::{debug, warn};
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use sail_server::actor::ActorContext;

use crate::driver::job_scheduler::state::{JobDescriptor, TaskDescriptor, TaskState};
use crate::driver::job_scheduler::{
    JobOutputChannel, JobOutputMetadata, JobScheduler, TaskSchedule, TaskSchedulePlan, TaskTimeout,
};
use crate::driver::planner::JobGraph;
use crate::driver::{DriverActor, DriverEvent};
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskInstance, WorkerId};
use crate::plan::{ShuffleConsumption, ShuffleReadExec, ShuffleWriteExec};
use crate::stream::reader::TaskReadLocation;
use crate::stream::writer::{LocalStreamStorage, TaskWriteLocation};

impl JobScheduler {
    fn next_job_id(&mut self) -> ExecutionResult<JobId> {
        self.job_id_generator.next()
    }

    pub fn accept_job(&mut self, plan: Arc<dyn ExecutionPlan>) -> ExecutionResult<JobId> {
        let job_id = self.next_job_id()?;
        debug!(
            "job {} execution plan\n{}",
            job_id,
            DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
        );
        let graph = JobGraph::try_new(plan)?;
        debug!("job {job_id} job graph \n{graph}");
        let job = JobDescriptor::try_new(job_id, graph)?;
        for (task_id, task) in job.tasks.iter() {
            self.task_queue.push_back(TaskInstance {
                job_id,
                task_id: *task_id,
                attempt: task.attempt,
            });
        }
        self.jobs.insert(job_id, job);
        Ok(job_id)
    }

    pub fn count_active_tasks(&self) -> usize {
        self.jobs
            .values()
            .flat_map(|job| {
                job.tasks.values().filter(|task| {
                    matches!(
                        task.state,
                        TaskState::Scheduled { .. } | TaskState::Running { .. }
                    )
                })
            })
            .count()
    }

    pub fn count_pending_tasks(&self) -> usize {
        self.jobs
            .values()
            .flat_map(|job| {
                job.tasks
                    .values()
                    .filter(|task| matches!(task.state, TaskState::Created | TaskState::Pending))
            })
            .count()
    }

    fn get_task_mut(&mut self, instance: &TaskInstance) -> Option<&mut TaskDescriptor> {
        let Some(job) = self.jobs.get_mut(&instance.job_id) else {
            warn!("job {} not found", instance.job_id);
            return None;
        };
        let Some(task) = job.tasks.get_mut(&instance.task_id) else {
            warn!(
                "job {} task {} not found",
                instance.job_id, instance.task_id,
            );
            return None;
        };
        Some(task)
    }

    pub fn record_running_task(&mut self, instance: &TaskInstance, message: Option<String>) {
        let Some(task) = self.get_task_mut(instance) else {
            return;
        };
        if let Some(state) = task.state.run() {
            task.state = state;
            task.messages.extend(message);
        } else {
            warn!(
                "job {} task {} cannot be updated to the running state from its current state",
                instance.job_id, instance.task_id,
            );
        }
    }

    pub fn record_succeeded_task(&mut self, instance: &TaskInstance, message: Option<String>) {
        let Some(task) = self.get_task_mut(instance) else {
            return;
        };
        if let Some(state) = task.state.succeed() {
            task.state = state;
            task.messages.extend(message);
        } else {
            warn!(
                "job {} task {} for cannot be updated to the succeeded state from its current state",
                instance.job_id, instance.task_id
            );
        }
    }

    pub fn record_failed_task(&mut self, instance: &TaskInstance, message: Option<String>) {
        let Some(task) = self.get_task_mut(instance) else {
            return;
        };
        task.state = TaskState::Failed;
        task.messages.extend(message);
    }

    pub fn record_canceled_task(&mut self, instance: &TaskInstance, message: Option<String>) {
        let Some(task) = self.get_task_mut(instance) else {
            return;
        };
        task.state = TaskState::Canceled;
        task.messages.extend(message);
    }

    pub fn schedule_tasks(
        &mut self,
        ctx: &mut ActorContext<DriverActor>,
        slots: Vec<(WorkerId, usize)>,
    ) -> Vec<TaskSchedule> {
        let mut assigner = TaskSlotAssigner::new(slots);
        let mut skipped_tasks = vec![];
        let mut scheduled_tasks = vec![];
        while let Some(instance) = self.task_queue.pop_front() {
            let Some(job) = self.jobs.get_mut(&instance.job_id) else {
                warn!(
                    "job {} not found for task {}",
                    instance.job_id, instance.task_id
                );
                continue;
            };
            if !job.can_schedule_task(instance.task_id) {
                skipped_tasks.push(instance);
                continue;
            }
            let Some(task) = job.tasks.get_mut(&instance.task_id) else {
                warn!(
                    "job {} task {} not found",
                    instance.job_id, instance.task_id
                );
                continue;
            };
            match task.state {
                TaskState::Created => {
                    task.state = TaskState::Pending;
                    ctx.send_with_delay(
                        DriverEvent::ProbePendingTask {
                            instance: instance.clone(),
                        },
                        self.options.task_launch_timeout,
                    );
                }
                TaskState::Pending => {}
                _ => {
                    warn!(
                        "job {} task {} cannot be scheduled in its current state",
                        instance.job_id, instance.task_id
                    );
                    continue;
                }
            };
            let Some(worker_id) = assigner.next() else {
                skipped_tasks.push(instance);
                // We do not break the loop even if there are no more available task slots.
                // We want to examine all the tasks in the queue and mark eligible tasks as pending.
                continue;
            };
            task.state = TaskState::Scheduled { worker_id };
            let stage = task.stage;
            let partition = task.partition;
            let channel = task.channel.clone();
            let plan = match Self::rewrite_shuffle(
                instance.job_id,
                job,
                Arc::clone(&job.stages[stage].plan),
            ) {
                Ok(plan) => TaskSchedulePlan::Valid(plan),
                Err(e) => {
                    let message = format!("failed to rewrite shuffle: {e}");
                    let cause = CommonErrorCause::new::<PyErrExtractor>(&e);
                    TaskSchedulePlan::Invalid {
                        message,
                        cause: Some(cause),
                    }
                }
            };
            scheduled_tasks.push(TaskSchedule {
                instance,
                worker_id,
                plan,
                partition,
                channel,
            });
        }
        self.task_queue.extend(skipped_tasks);
        scheduled_tasks
    }

    pub fn probe_pending_task(&self, instance: &TaskInstance) -> TaskTimeout {
        let Some(job) = self.jobs.get(&instance.job_id) else {
            warn!("job {} not found", instance.job_id);
            return TaskTimeout::No;
        };
        let Some(task) = job.tasks.get(&instance.task_id) else {
            warn!(
                "job {} task {} not found",
                instance.job_id, instance.task_id
            );
            return TaskTimeout::No;
        };
        if task.attempt == instance.attempt && matches!(&task.state, TaskState::Pending) {
            TaskTimeout::Yes
        } else {
            TaskTimeout::No
        }
    }

    pub fn cancel_job(&mut self, job_id: JobId) -> Vec<TaskInstance> {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            warn!("job {job_id} not found");
            return vec![];
        };
        let reason = format!("task canceled for job {job_id}");
        // The tasks are canceled, but they may remain in the task queue.
        // This is OK, since they will be removed when the task scheduling logic runs next time.
        job.cancel_active_tasks(job_id, Some(reason))
    }

    pub fn get_job_output(&self, job_id: JobId) -> Option<ExecutionResult<JobOutputMetadata>> {
        let Some(job) = self.jobs.get(&job_id) else {
            return Some(Err(ExecutionError::InternalError(format!(
                "job {job_id} not found"
            ))));
        };
        let Some(last_stage) = job.stages.last() else {
            return Some(Err(ExecutionError::InternalError(format!(
                "last stage not found for job {job_id}"
            ))));
        };
        let schema = last_stage.plan.schema();
        let metadata = last_stage
            .tasks
            .iter()
            .map(|task_id| {
                job.tasks.get(task_id).and_then(|task| match task.state {
                    // We should not consider tasks in the "scheduled" state even if the
                    // worker ID is known at that time. This is because the task may not be
                    // running on the worker, and the shuffle stream may not be available.
                    TaskState::Running { worker_id } | TaskState::Succeeded { worker_id } => {
                        Some((*task_id, task.channel.clone(), worker_id))
                    }
                    _ => None,
                })
            })
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .map(|(task_id, channel, worker_id)| {
                let channel = channel.ok_or_else(|| {
                    ExecutionError::InternalError(format!(
                        "job {} task {} channel is not set",
                        job_id, task_id
                    ))
                })?;
                Ok(JobOutputChannel { worker_id, channel })
            })
            .collect::<ExecutionResult<Vec<_>>>()
            .map(|channels| JobOutputMetadata { schema, channels });
        Some(metadata)
    }

    fn rewrite_shuffle(
        job_id: JobId,
        job: &JobDescriptor,
        plan: Arc<dyn ExecutionPlan>,
    ) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
        // TODO: This can be expensive. We may want to cache the result
        //   when the task attempt is the same.
        let result = plan.transform(|node| {
            if let Some(shuffle) = node.as_any().downcast_ref::<ShuffleReadExec>() {
                let locations = (0..shuffle.locations().len())
                    .map(|partition| {
                        job.stages[shuffle.stage()]
                            .tasks
                            .iter()
                            .map(|task_id| {
                                let task = job
                                    .tasks
                                    .get(task_id)
                                    .ok_or_else(|| exec_datafusion_err!("job {} task {} not found", job_id, task_id))?;
                                let worker_id = task.state.worker_id().ok_or_else(
                                    || exec_datafusion_err!("job {} task {} is not bound to a worker", job_id, task_id),
                                )?;
                                let attempt = task.attempt;
                                Ok(TaskReadLocation::Worker {
                                    worker_id,
                                    channel: format!("job-{job_id}/task-{task_id}/attempt-{attempt}/partition-{partition}")
                                        .into(),
                                })
                            })
                            .collect::<Result<Vec<_>, DataFusionError>>()
                    })
                    .collect::<Result<Vec<_>, DataFusionError>>()?;
                let shuffle = shuffle.clone().with_locations(locations);
                Ok(Transformed::yes(Arc::new(shuffle)))
            } else if let Some(shuffle) = node.as_any().downcast_ref::<ShuffleWriteExec>() {
                let storage = match shuffle.consumption() {
                    ShuffleConsumption::Single => LocalStreamStorage::Ephemeral,
                    ShuffleConsumption::Multiple => LocalStreamStorage::Memory,
                };
                let locations = (0..shuffle.locations().len())
                    .map(|partition| {
                        let task_id = job.stages[shuffle.stage()].tasks[partition];
                        let task = job
                            .tasks
                            .get(&task_id)
                            .ok_or_else(|| exec_datafusion_err!("job {} task {} not found", job_id, task_id))?;
                        let attempt = task.attempt;
                        let locations = (0..shuffle.shuffle_partitioning().partition_count())
                            .map(|p| {
                                TaskWriteLocation::Local {
                                    channel: format!("job-{job_id}/task-{task_id}/attempt-{attempt}/partition-{p}")
                                        .into(),
                                    storage,
                                }
                            })
                            .collect();
                        Ok(locations)
                    })
                    .collect::<Result<Vec<_>, DataFusionError>>()?;
                let shuffle = shuffle.clone().with_locations(locations);
                Ok(Transformed::yes(Arc::new(shuffle)))
            } else {
                Ok(Transformed::no(node))
            }
        });
        Ok(result.data()?)
    }
}

struct TaskSlotAssigner {
    slots: Vec<(WorkerId, usize)>,
}

impl TaskSlotAssigner {
    fn new(slots: Vec<(WorkerId, usize)>) -> Self {
        Self { slots }
    }

    fn next(&mut self) -> Option<WorkerId> {
        self.slots.iter_mut().find_map(|(worker_id, slots)| {
            if *slots > 0 {
                *slots -= 1;
                Some(*worker_id)
            } else {
                None
            }
        })
    }
}
