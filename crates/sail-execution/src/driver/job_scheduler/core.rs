use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use log::{debug, warn};
use prost::Message;
use sail_server::actor::ActorContext;

use crate::driver::job_scheduler::state::{
    JobDescriptor, JobState, TaskAttemptDescriptor, TaskState,
};
use crate::driver::job_scheduler::{JobAction, JobScheduler, JobSchedulerOptions};
use crate::driver::output::build_job_output;
use crate::driver::DriverActor;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskKey, TaskKeyDisplay, TaskStreamKey};
use crate::job_graph::{InputMode, JobGraph, OutputDistribution, OutputMode, TaskPlacement};
use crate::task::definition::{
    TaskDefinition, TaskInput, TaskInputKey, TaskInputLocator, TaskOutput, TaskOutputDistribution,
    TaskOutputLocator,
};
use crate::task::scheduling::{
    TaskAssignment, TaskAssignmentGetter, TaskOutputKind, TaskRegion, TaskSet, TaskSetEntry,
};

impl JobScheduler {
    fn next_job_id(&mut self) -> ExecutionResult<JobId> {
        self.job_id_generator.next()
    }

    pub fn accept_job(
        &mut self,
        ctx: &mut ActorContext<DriverActor>,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> ExecutionResult<(JobId, SendableRecordBatchStream)> {
        let job_id = self.next_job_id()?;

        debug!(
            "job {job_id} execution plan\n{}",
            DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
        );
        let graph = JobGraph::try_new(plan)?;
        debug!("job {job_id} job graph \n{graph}");

        let (output, stream) = build_job_output(ctx, job_id, graph.schema().clone());
        let descriptor = JobDescriptor::try_new(graph, JobState::Running { output, context })?;
        self.jobs.insert(job_id, descriptor);

        Ok((job_id, stream))
    }

    fn get_task_attempt(&self, key: &TaskKey) -> Option<&TaskAttemptDescriptor> {
        let descriptor = self
            .jobs
            .get(&key.job_id)
            .and_then(|job| job.stages.get(key.stage))
            .and_then(|stage| stage.tasks.get(key.partition))
            .and_then(|task| task.attempts.get(key.attempt));
        if descriptor.is_none() {
            warn!("{} not found", TaskKeyDisplay(&key));
        };
        descriptor
    }

    fn get_task_attempt_mut(&mut self, key: &TaskKey) -> Option<&mut TaskAttemptDescriptor> {
        let descriptor = self
            .jobs
            .get_mut(&key.job_id)
            .and_then(|job| job.stages.get_mut(key.stage))
            .and_then(|stage| stage.tasks.get_mut(key.partition))
            .and_then(|task| task.attempts.get_mut(key.attempt));
        if descriptor.is_none() {
            warn!("{} not found", TaskKeyDisplay(&key));
        };
        descriptor
    }

    pub fn update_task(&mut self, key: &TaskKey, state: TaskState, message: Option<String>) {
        let Some(descriptor) = self.get_task_attempt_mut(key) else {
            return;
        };
        descriptor.state = descriptor.state.consolidate(state);
        descriptor.messages.extend(message);
    }

    pub fn get_task_state(&self, key: &TaskKey) -> Option<TaskState> {
        self.get_task_attempt(key).map(|x| x.state.clone())
    }

    /// Determine the actions needed in the driver for the job whose
    /// task states may have changed.
    ///
    /// The method first determines the task regions and then decides
    /// the actions to take.
    ///   1. For each task region, if any task attempt fails, all existing task attempts
    ///      in the region are canceled if not already.
    ///   2. If any task exceeds the maximum allowed attempts, the job is marked as failed.
    ///   3. If all the tasks in the final stage have succeeded, the job is marked as succeeded.
    ///   4. If any task in the final stage is running or has succeeded, all its channels are
    ///      added as job output streams.
    ///   5. For each task region, schedule the tasks of the region if all the dependency
    ///      regions have succeeded.
    ///   6. For each stage, if all the stages that depends on it have succeeded, remove
    ///      the output streams of the stage.
    pub fn refresh_job(&mut self, job_id: JobId) -> Vec<JobAction> {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            warn!("job {job_id} not found");
            return vec![];
        };
        if !matches!(job.state, JobState::Running { .. }) {
            return vec![];
        }

        let mut actions = vec![];

        actions.extend(Self::update_job_output(job_id, job));
        actions.extend(Self::cascade_cancel_task_attempts(job_id, job));
        actions.extend(Self::remove_streams_by_stage(job_id, job));

        let region_status = Self::get_task_region_status(job, &self.options);
        if region_status
            .iter()
            .any(|x| matches!(*x, TaskRegionStatus::Failed))
        {
            if let JobState::Running { output, .. } = &job.state {
                actions.push(JobAction::FailJobOutput {
                    notifier: output.fail(),
                })
            }
            job.state = JobState::Failed;
            return actions;
        }
        if region_status
            .iter()
            .all(|x| matches!(*x, TaskRegionStatus::Succeeded))
        {
            job.state = JobState::Draining;
            return actions;
        }
        actions.extend(Self::schedule_task_regions(job_id, job, &region_status));

        actions
    }

    fn get_task_region_status(
        job: &JobDescriptor,
        options: &JobSchedulerOptions,
    ) -> Vec<TaskRegionStatus> {
        job.topology
            .regions
            .iter()
            .map(|region| {
                let failed = region.tasks.iter().any(|t| {
                    let attempts = &job.stages[t.stage].tasks[t.partition].attempts;
                    if let Some(attempt) = attempts.last() {
                        if attempt.state.is_failure() && attempts.len() >= options.task_max_attempts
                        {
                            return true;
                        }
                    }
                    false
                });
                if failed {
                    return TaskRegionStatus::Failed;
                }
                let succeeded = region.tasks.iter().all(|t| {
                    job.stages[t.stage].tasks[t.partition]
                        .attempts
                        .last()
                        .is_some_and(|x| matches!(x.state, TaskState::Succeeded))
                });
                if succeeded {
                    TaskRegionStatus::Succeeded
                } else {
                    TaskRegionStatus::Running
                }
            })
            .collect::<Vec<_>>()
    }

    fn cascade_cancel_task_attempts(job_id: JobId, job: &mut JobDescriptor) -> Vec<JobAction> {
        let mut actions = vec![];

        for region in &job.topology.regions {
            let mut failed = false;

            for t in &region.tasks {
                let attempts = &job.stages[t.stage].tasks[t.partition].attempts;
                if let Some(attempt) = attempts.last() {
                    match attempt.state {
                        TaskState::Failed => {
                            failed = true;
                        }
                        _ => {}
                    }
                }
            }

            if failed {
                // cancel all other tasks in this region
                for t in &region.tasks {
                    let task = &mut job.stages[t.stage].tasks[t.partition];
                    for (a, attempt) in task.attempts.iter_mut().enumerate() {
                        if !attempt.state.is_terminal() {
                            attempt.state = TaskState::Canceled;
                            actions.push(JobAction::CancelTask {
                                key: TaskKey {
                                    job_id,
                                    stage: t.stage,
                                    partition: t.partition,
                                    attempt: a,
                                },
                            });
                        }
                    }
                }
            }
        }

        actions
    }

    fn remove_streams_by_stage(job_id: JobId, job: &JobDescriptor) -> Vec<JobAction> {
        let mut actions = vec![];

        for (s, stage) in job.topology.stages.iter().enumerate() {
            let all_consumers_succeeded = stage.consumers.iter().all(|&c| {
                let partitions = job.graph.stages()[c]
                    .plan
                    .output_partitioning()
                    .partition_count();
                (0..partitions).all(|p| {
                    job.stages[c].tasks[p]
                        .attempts
                        .last()
                        .map_or(false, |a| matches!(a.state, TaskState::Succeeded))
                })
            });

            if all_consumers_succeeded && !stage.consumers.is_empty() {
                actions.push(JobAction::RemoveStreams {
                    job_id,
                    stage: Some(s),
                });
            }
        }

        actions
    }

    fn schedule_task_regions(
        job_id: JobId,
        job: &mut JobDescriptor,
        region_status: &[TaskRegionStatus],
    ) -> Vec<JobAction> {
        let mut actions = vec![];

        'region: for (r, region) in job.topology.regions.iter().enumerate() {
            if matches!(region_status[r], TaskRegionStatus::Succeeded) {
                continue;
            }
            if !region
                .dependencies
                .iter()
                .all(|d| matches!(region_status[*d], TaskRegionStatus::Succeeded))
            {
                // The region has dependencies that are not yet succeeded.
                continue;
            }

            for t in &region.tasks {
                let task = &job.stages[t.stage].tasks[t.partition];
                if task.attempts.last().is_some_and(|x| !x.state.is_terminal()) {
                    // The latest task attempt is still active, so the region must have been
                    // scheduled already.
                    continue 'region;
                }
            }

            for t in &region.tasks {
                let task = &mut job.stages[t.stage].tasks[t.partition];
                task.attempts.push(TaskAttemptDescriptor {
                    state: TaskState::Created,
                    messages: vec![],
                });
            }

            // Proceed with scheduling (bucket logic)
            let mut stages_by_group: HashMap<(TaskPlacement, String), HashSet<usize>> =
                HashMap::new();
            for t in &region.tasks {
                let stage = &job.graph.stages()[t.stage];
                let key = (stage.placement, stage.group.clone());
                stages_by_group.entry(key).or_default().insert(t.stage);
            }

            let mut buckets_by_group: HashMap<
                (TaskPlacement, String),
                (usize, Vec<Vec<TaskSetEntry>>),
            > = HashMap::new();

            for (key, stages) in stages_by_group {
                let max_partitions = stages
                    .iter()
                    .map(|&s| {
                        job.graph.stages()[s]
                            .plan
                            .output_partitioning()
                            .partition_count()
                    })
                    .max()
                    .unwrap_or(1);
                let buckets = vec![Vec::new(); max_partitions];
                buckets_by_group.insert(key, (max_partitions, buckets));
            }

            for t in &region.tasks {
                if let Some(attempt) = Self::get_latest_task_attempt(job, t.stage, t.partition) {
                    let stage = &job.graph.stages()[t.stage];
                    let output = match stage.mode {
                        OutputMode::Pipelined => TaskOutputKind::Local,
                        OutputMode::Blocking => TaskOutputKind::Remote,
                    };
                    let key = (stage.placement, stage.group.clone());
                    if let Some((max_partitions, buckets)) = buckets_by_group.get_mut(&key) {
                        let p_count = stage.plan.output_partitioning().partition_count();
                        let b = t.partition * *max_partitions / p_count;
                        if b < buckets.len() {
                            buckets[b].push(TaskSetEntry {
                                key: TaskKey {
                                    job_id,
                                    stage: t.stage,
                                    partition: t.partition,
                                    attempt,
                                },
                                output,
                            });
                        }
                    }
                }
            }

            let mut tasks: Vec<(TaskPlacement, TaskSet)> = vec![];
            for ((placement, _), (_, buckets)) in buckets_by_group {
                for entries in buckets {
                    if !entries.is_empty() {
                        tasks.push((placement, TaskSet { entries }));
                    }
                }
            }

            actions.push(JobAction::ScheduleTaskRegion {
                region: TaskRegion { tasks },
            });
        }

        actions
    }

    fn update_job_output(job_id: JobId, job: &mut JobDescriptor) -> Vec<JobAction> {
        let JobState::Running { output, .. } = &mut job.state else {
            return vec![];
        };
        let s = job.graph.stages().len() - 1;
        let stage = &job.graph.stages()[s];
        let partitions = stage.plan.output_partitioning().partition_count();
        let channels = stage.distribution.channels();
        let schema = job.graph.schema().clone();
        let mut actions = vec![];
        for p in 0..partitions {
            if let Some(attempt) = job.stages[s].tasks[p].attempts.last_mut() {
                if matches!(attempt.state, TaskState::Running | TaskState::Succeeded) {
                    let a = job.stages[s].tasks[p].attempts.len() - 1;
                    for c in 0..channels {
                        let key = TaskStreamKey {
                            job_id,
                            stage: s,
                            partition: p,
                            attempt: a,
                            channel: c,
                        };
                        if !output.has_stream(&key) {
                            let sender = output.send_stream(key.clone());
                            actions.push(JobAction::FetchJobOutputStream {
                                key,
                                schema: schema.clone(),
                                sender,
                            });
                        }
                    }
                }
            }
        }
        actions
    }

    /// Determine the actions needed in the driver to cancel the job.
    /// The method cancels all the task attempts that are not in terminal states
    /// and removes all the job output streams.
    pub fn clean_up_job(&mut self, job_id: JobId) -> Vec<JobAction> {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            warn!("job {job_id} not found");
            return vec![];
        };
        let mut actions = vec![];
        for (s, stage) in job.stages.iter().enumerate() {
            for (t, task) in stage.tasks.iter().enumerate() {
                for (a, attempt) in task.attempts.iter().enumerate() {
                    if !attempt.state.is_terminal() {
                        actions.push(JobAction::CancelTask {
                            key: TaskKey {
                                job_id,
                                stage: s,
                                partition: t,
                                attempt: a,
                            },
                        });
                    }
                }
            }
        }
        actions.push(JobAction::RemoveStreams {
            job_id,
            stage: None,
        });
        if matches!(job.state, JobState::Draining) {
            job.state = JobState::Succeeded;
        } else {
            job.state = JobState::Canceled;
        }
        actions
    }

    pub fn get_task_definition(
        &self,
        key: &TaskKey,
        assignments: &dyn TaskAssignmentGetter,
    ) -> ExecutionResult<(TaskDefinition, Arc<TaskContext>)> {
        let Some(job) = self.jobs.get(&key.job_id) else {
            return Err(ExecutionError::InvalidArgument(format!(
                "job {} not found",
                key.job_id
            )));
        };
        let JobState::Running { context, .. } = &job.state else {
            return Err(ExecutionError::InvalidArgument(format!(
                "job {} is not running",
                key.job_id
            )));
        };
        let Some(stage) = job.graph.stages().get(key.stage) else {
            return Err(ExecutionError::InvalidArgument(format!(
                "stage {} not found in job {}",
                key.stage, key.job_id
            )));
        };

        let latest_attempt = |stage: usize, partition: usize| -> ExecutionResult<usize> {
            Self::get_latest_task_attempt(job, stage, partition).ok_or_else(|| {
                ExecutionError::InvalidArgument(format!(
                    "no latest task attempt found for job {} stage {} partition {}",
                    key.job_id, stage, partition
                ))
            })
        };

        let inputs = stage
            .inputs
            .iter()
            .map(|input| {
                let Some(stage) = job.graph.stages().get(input.stage) else {
                    return Err(ExecutionError::InvalidArgument(format!(
                        "job {} input stage {} not found",
                        key.job_id, input.stage
                    )));
                };
                let partitions = stage.plan.output_partitioning().partition_count();
                let channels = stage.distribution.channels();
                let keys = match input.mode {
                    InputMode::Forward => {
                        let mut keys = vec![vec![]; partitions];
                        let Some(k) = keys.get_mut(key.partition) else {
                            return Err(ExecutionError::InvalidArgument(format!(
                                "job {} input stage {} has no partition {}",
                                key.job_id, input.stage, key.partition
                            )));
                        };
                        *k = (0..channels)
                            .map(|channel| {
                                Ok(TaskInputKey {
                                    partition: key.partition,
                                    attempt: latest_attempt(input.stage, key.partition)?,
                                    channel,
                                })
                            })
                            .collect::<ExecutionResult<Vec<_>>>()?;
                        keys
                    }
                    InputMode::Shuffle => {
                        let mut keys = vec![vec![]; channels];
                        let Some(k) = keys.get_mut(key.partition) else {
                            return Err(ExecutionError::InvalidArgument(format!(
                                "job {} input stage {} has no channel {}",
                                key.job_id, input.stage, key.partition
                            )));
                        };
                        *k = (0..partitions)
                            .map(|partition| {
                                Ok(TaskInputKey {
                                    partition,
                                    attempt: latest_attempt(input.stage, partition)?,
                                    channel: key.partition,
                                })
                            })
                            .collect::<ExecutionResult<Vec<_>>>()?;
                        keys
                    }
                    InputMode::Broadcast => {
                        (0..partitions).map(|partition| {
                            (0..channels)
                                .map(|channel| {
                                    Ok(TaskInputKey {
                                        partition,
                                        attempt: latest_attempt(input.stage, partition)?,
                                        channel,
                                    })
                                })
                                .collect::<ExecutionResult<Vec<_>>>()
                        }).collect::<ExecutionResult<Vec<Vec<_>>>>()?
                    }
                };
                let locator = match stage.mode {
                    OutputMode::Pipelined => match stage.placement {
                        TaskPlacement::Driver => {
                            keys.iter().flatten().try_for_each(|k| {
                                match assignments.get(
                                    &TaskKey {
                                        job_id: key.job_id,
                                        stage: input.stage,
                                        partition: k.partition,
                                        attempt: k.attempt,
                                    }
                                ) {
                                    Some(TaskAssignment::Driver) => Ok(()),
                                    _ => Err(ExecutionError::InternalError(format!(
                                        "job {} input stage {} partition {} attempt {} is not assigned to driver",
                                        key.job_id, input.stage, k.partition, k.attempt
                                    ))),
                                }
                            })?;
                            TaskInputLocator::Driver {
                                stage: input.stage,
                                keys,
                            }
                        },
                        TaskPlacement::Worker => {
                            let keys = keys.into_iter().map(|keys| keys.into_iter().map(|k|{
                                let Some(TaskAssignment::Worker { worker_id, slot: _ }) = assignments.get(
                                    &TaskKey {
                                        job_id: key.job_id,
                                        stage: input.stage,
                                        partition: k.partition,
                                        attempt: k.attempt,
                                    }
                                ) else {
                                    return Err(ExecutionError::InternalError(format!(
                                        "job {} input stage {} partition {} attempt {} is not assigned to worker",
                                        key.job_id, input.stage, k.partition, k.attempt
                                    )));
                                };
                                Ok((*worker_id, k))
                            }).collect::<ExecutionResult<Vec<_>>>()).collect::<ExecutionResult<Vec<Vec<_>>>>()?;
                            TaskInputLocator::Worker {
                                stage: input.stage,
                                keys,
                            }
                        },
                    },
                    OutputMode::Blocking => {
                        let uri =
                            Err(ExecutionError::InternalError("not implemented".to_string()))?;
                        TaskInputLocator::Remote {
                            uri,
                            stage: input.stage,
                            keys,
                        }
                    }
                };
                Ok(TaskInput { locator })
            })
            .collect::<ExecutionResult<Vec<_>>>()?;
        let plan =
            PhysicalPlanNode::try_from_physical_plan(stage.plan.clone(), self.codec.as_ref())?
                .encode_to_vec();
        let replicas = job.graph.replicas(key.stage);
        let distribution = match &stage.distribution {
            OutputDistribution::Hash { keys, channels } => {
                let keys = keys
                    .iter()
                    .map(|expr| {
                        let expr =
                            serialize_physical_expr(expr, self.codec.as_ref())?.encode_to_vec();
                        Ok(Arc::from(expr))
                    })
                    .collect::<ExecutionResult<Vec<Arc<[u8]>>>>()?;
                TaskOutputDistribution::Hash {
                    keys,
                    channels: *channels,
                }
            }
            OutputDistribution::RoundRobin { channels } => TaskOutputDistribution::RoundRobin {
                channels: *channels,
            },
        };
        let locator = match stage.mode {
            OutputMode::Pipelined => TaskOutputLocator::Local { replicas },
            OutputMode::Blocking => {
                let uri = Err(ExecutionError::InternalError("not implemented".to_string()))?;
                TaskOutputLocator::Remote { uri }
            }
        };
        let output = TaskOutput {
            distribution,
            locator,
        };
        let definition = TaskDefinition {
            plan: Arc::from(plan),
            inputs,
            output,
        };
        Ok((definition, context.clone()))
    }

    fn get_latest_task_attempt(
        job: &JobDescriptor,
        stage: usize,
        partition: usize,
    ) -> Option<usize> {
        let descriptor = job
            .stages
            .get(stage)
            .and_then(|stage| stage.tasks.get(partition));
        descriptor.and_then(|task| {
            let n = task.attempts.len();
            if n == 0 {
                None
            } else {
                Some(n - 1)
            }
        })
    }
}

enum TaskRegionStatus {
    Running,
    Failed,
    Succeeded,
}
