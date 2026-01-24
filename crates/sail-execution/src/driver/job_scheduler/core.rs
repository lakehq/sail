use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use indexmap::{IndexMap, IndexSet};
use log::{debug, warn};
use prost::Message;
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use sail_server::actor::ActorContext;

use crate::driver::job_scheduler::state::{
    JobDescriptor, JobState, StageState, TaskAttemptDescriptor, TaskRegionState, TaskState,
};
use crate::driver::job_scheduler::topology::TaskRegionTopology;
use crate::driver::job_scheduler::{JobAction, JobScheduler, JobSchedulerOptions};
use crate::driver::output::build_job_output;
use crate::driver::DriverActor;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskKey, TaskKeyDisplay, TaskStreamKey};
use crate::job_graph::{
    InputMode, JobGraph, OutputDistribution, OutputMode, Stage, StageInput, TaskPlacement,
};
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

    pub fn update_task(
        &mut self,
        key: &TaskKey,
        state: TaskState,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
    ) {
        let Some(attempt) = self
            .jobs
            .get_mut(&key.job_id)
            .and_then(|job| job.stages.get_mut(key.stage))
            .and_then(|stage| stage.tasks.get_mut(key.partition))
            .and_then(|task| task.attempts.get_mut(key.attempt))
        else {
            warn!("{} not found", TaskKeyDisplay(key));
            return;
        };
        attempt.state = attempt.state.consolidate(state);
        if attempt.state.is_terminal() && attempt.stopped_at.is_none() {
            attempt.stopped_at = Some(Utc::now());
        }
        attempt.messages.extend(message);
        if let Some(cause) = cause {
            attempt.cause = Some(cause);
        }
    }

    pub fn get_task_state(&self, key: &TaskKey) -> Option<TaskState> {
        let attempt = self
            .jobs
            .get(&key.job_id)
            .and_then(|job| job.stages.get(key.stage))
            .and_then(|stage| stage.tasks.get(key.partition))
            .and_then(|task| task.attempts.get(key.attempt));
        if attempt.is_none() {
            warn!("{} not found", TaskKeyDisplay(key));
        };
        attempt.map(|x| x.state)
    }

    /// Determine the actions needed in the driver for the job whose
    /// task states may have changed.
    ///
    /// The method first determines the task regions and then decides
    /// the actions to take.
    ///   1. For each task region, if any task attempt fails, all existing task attempts
    ///      in the region are canceled if not already.
    ///   2. If any task in the final stages is running or has succeeded, all its channels are
    ///      added as job output streams if not already.
    ///   3. For each stage, if all the stages that consume it have succeeded, remove
    ///      the output streams of the stage if not already.
    ///   4. If any task exceeds the maximum allowed attempts, the task region and the job
    ///      are marked as failed.
    ///   5. If all the tasks in the final stages have succeeded, the job is marked as succeeded.
    ///   6. For each task region, schedule the tasks of the region if all the dependency
    ///      regions have succeeded.
    pub fn refresh_job(&mut self, job_id: JobId) -> Vec<JobAction> {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            warn!("job {job_id} not found");
            return vec![];
        };
        if !matches!(job.state, JobState::Running { .. }) {
            return vec![];
        }

        let mut actions = vec![];

        actions.extend(Self::cascade_cancel_task_attempts(job_id, job));
        actions.extend(Self::extend_job_output(job_id, job));
        actions.extend(Self::clean_up_job_by_stage(job_id, job));

        Self::update_task_regions(job, &self.options);

        if job
            .regions
            .iter()
            .any(|x| matches!(x.state, TaskRegionState::Failed))
        {
            let cause = Self::infer_job_failure_cause(job);
            if let JobState::Running { output, .. } = &job.state {
                actions.push(JobAction::FailJobOutput {
                    handle: output.handle(),
                    cause,
                })
            }
            job.state = JobState::Failed;
            job.stopped_at = Some(Utc::now());
            return actions;
        }

        if job
            .regions
            .iter()
            .all(|x| matches!(x.state, TaskRegionState::Succeeded))
        {
            // This drops `JobOutputManager` in the job state,
            // so that `JobOutputStream` turns to the draining state as well.
            job.state = JobState::Draining;
            return actions;
        }

        actions.extend(Self::schedule_task_regions(job_id, job));

        actions
    }

    fn update_task_regions(job: &mut JobDescriptor, options: &JobSchedulerOptions) {
        for (r, region) in job.topology.regions.iter().enumerate() {
            let failed = region.tasks.iter().any(|t| {
                let attempts = &job.stages[t.stage].tasks[t.partition].attempts;
                if let Some(attempt) = attempts.last() {
                    if matches!(attempt.state, TaskState::Failed | TaskState::Canceled)
                        && attempts.len() >= options.task_max_attempts
                    {
                        return true;
                    }
                }
                false
            });
            if failed {
                job.regions[r].state = TaskRegionState::Failed;
                continue;
            }
            let succeeded = region.tasks.iter().all(|t| {
                job.stages[t.stage].tasks[t.partition]
                    .attempts
                    .last()
                    .is_some_and(|x| matches!(x.state, TaskState::Succeeded))
            });
            if succeeded {
                job.regions[r].state = TaskRegionState::Succeeded;
            } else {
                job.regions[r].state = TaskRegionState::Running;
            }
        }
    }

    fn cascade_cancel_task_attempts(job_id: JobId, job: &mut JobDescriptor) -> Vec<JobAction> {
        let mut actions = vec![];

        for region in &job.topology.regions {
            let mut failed = false;

            for t in &region.tasks {
                let attempts = &job.stages[t.stage].tasks[t.partition].attempts;
                if let Some(attempt) = attempts.last() {
                    if matches!(attempt.state, TaskState::Failed) {
                        failed = true;
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
                            attempt.stopped_at = Some(Utc::now());
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

    fn clean_up_job_by_stage(job_id: JobId, job: &mut JobDescriptor) -> Vec<JobAction> {
        let mut actions = vec![];

        for (s, stage) in job.topology.stages.iter().enumerate() {
            if matches!(job.stages[s].state, StageState::Inactive) {
                continue;
            }
            let all_consumers_succeeded = stage.consumers.iter().all(|&c| {
                let partitions = job.graph.stages()[c]
                    .plan
                    .output_partitioning()
                    .partition_count();
                (0..partitions).all(|p| {
                    job.stages[c].tasks[p]
                        .attempts
                        .last()
                        .is_some_and(|a| matches!(a.state, TaskState::Succeeded))
                })
            });

            if all_consumers_succeeded && !stage.consumers.is_empty() {
                job.stages[s].state = StageState::Inactive;
                job.stages[s].stopped_at = Some(Utc::now());
                actions.push(JobAction::CleanUpJob {
                    job_id,
                    stage: Some(s),
                });
            }
        }

        actions
    }

    fn schedule_task_regions(job_id: JobId, job: &mut JobDescriptor) -> Vec<JobAction> {
        let mut actions = vec![];

        for (r, region) in job.topology.regions.iter().enumerate() {
            if matches!(job.regions[r].state, TaskRegionState::Succeeded) {
                continue;
            }

            if !region
                .dependencies
                .iter()
                .all(|d| matches!(job.regions[*d].state, TaskRegionState::Succeeded))
            {
                // The region has dependencies that are not yet succeeded.
                continue;
            }

            if region.tasks.iter().any(|t| {
                job.stages[t.stage].tasks[t.partition]
                    .attempts
                    .last()
                    .is_some_and(|x| !x.state.is_terminal())
            }) {
                // The latest task attempt is still active, so the region must have been
                // scheduled already.
                continue;
            }

            for t in &region.tasks {
                job.stages[t.stage].tasks[t.partition]
                    .attempts
                    .push(TaskAttemptDescriptor {
                        state: TaskState::Created,
                        messages: vec![],
                        cause: None,
                        job_output_fetched: false,
                        created_at: Utc::now(),
                        stopped_at: None,
                    });
            }

            actions.push(JobAction::ScheduleTaskRegion {
                region: Self::build_task_region(job_id, job, region),
            });
        }

        actions
    }

    fn build_task_region(
        job_id: JobId,
        job: &JobDescriptor,
        region: &TaskRegionTopology,
    ) -> TaskRegion {
        let stages = region
            .tasks
            .iter()
            .map(|t| t.stage)
            .collect::<IndexSet<_>>();
        let mut stage_groups: IndexMap<StageGroupKey, StageGroup> = IndexMap::new();
        for s in stages {
            let stage = &job.graph.stages()[s];
            let key = StageGroupKey {
                placement: stage.placement,
                group: stage.group.clone(),
            };
            let group: &mut StageGroup = stage_groups.entry(key).or_default();
            group.stages.insert(s);
            let n = group.buckets.len();
            let p = stage.plan.output_partitioning().partition_count();
            // ensure that the number of buckets is the maximum partitions among the stages
            if p > n {
                group.buckets.resize(p, Vec::new());
            }
        }

        for t in &region.tasks {
            if let Some(attempt) = Self::get_latest_task_attempt(job, t.stage, t.partition) {
                let stage = &job.graph.stages()[t.stage];
                let output = match stage.mode {
                    OutputMode::Pipelined => TaskOutputKind::Local,
                    OutputMode::Blocking => TaskOutputKind::Remote,
                };
                let key = StageGroupKey {
                    placement: stage.placement,
                    group: stage.group.clone(),
                };
                if let Some(group) = stage_groups.get_mut(&key) {
                    let partitions = stage.plan.output_partitioning().partition_count();
                    let b = t.partition * group.buckets.len() / partitions;
                    group.buckets[b].push(TaskSetEntry {
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

        let mut tasks: Vec<(TaskPlacement, TaskSet)> = vec![];
        for (key, value) in stage_groups {
            for entries in value.buckets {
                tasks.push((key.placement, TaskSet { entries }));
            }
        }

        TaskRegion { tasks }
    }

    fn extend_job_output(job_id: JobId, job: &mut JobDescriptor) -> Vec<JobAction> {
        let JobState::Running { output, .. } = &mut job.state else {
            return vec![];
        };
        let mut actions = vec![];
        let schema = job.graph.schema().clone();
        for (s, stage) in job.graph.stages().iter().enumerate() {
            if !job.topology.stages[s].consumers.is_empty() {
                // The stage is not a final stage.
                continue;
            }
            let partitions = stage.plan.output_partitioning().partition_count();
            let channels = stage.distribution.channels();
            for p in 0..partitions {
                let Some((attempt, head)) = job.stages[s].tasks[p].attempts.split_last_mut() else {
                    continue;
                };
                if !matches!(attempt.state, TaskState::Running | TaskState::Succeeded)
                    || attempt.job_output_fetched
                {
                    continue;
                }
                attempt.job_output_fetched = true;
                for c in 0..channels {
                    let key = TaskStreamKey {
                        job_id,
                        stage: s,
                        partition: p,
                        attempt: head.len(),
                        channel: c,
                    };
                    actions.push(JobAction::ExtendJobOutput {
                        handle: output.handle(),
                        key,
                        schema: schema.clone(),
                    });
                }
            }
        }

        actions
    }

    fn infer_job_failure_cause(job: &JobDescriptor) -> CommonErrorCause {
        // a map from (stage, partition) to a list of causes
        let mut causes: HashMap<(usize, usize), Vec<&CommonErrorCause>> = HashMap::new();
        for (s, stage) in job.stages.iter().enumerate() {
            for (t, task) in stage.tasks.iter().enumerate() {
                for attempt in task.attempts.iter() {
                    if matches!(attempt.state, TaskState::Failed) {
                        if let Some(cause) = &attempt.cause {
                            causes.entry((s, t)).or_default().push(cause);
                        }
                    }
                }
            }
        }
        // Get the most recent cause from the most likely failed task.
        if let Some((_, &[.., cause])) = causes.iter().map(|(k, v)| (k, v.as_slice())).min_by(
            |((s1, p1), v1), ((s2, p2), v2)| {
                // Find the task that fails the most number of times,
                // and if there is a tie, choose the one with the smallest
                // stage and partition.
                v2.len().cmp(&v1.len()).then(s1.cmp(s2)).then(p1.cmp(p2))
            },
        ) {
            cause.clone()
        } else {
            CommonErrorCause::new::<PyErrExtractor>(&ExecutionError::InternalError(
                "job failed for unknown reason".to_string(),
            ))
        }
    }

    /// Determine the actions needed in the driver to clean up the job.
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
        for stage in job.stages.iter_mut() {
            stage.state = StageState::Inactive;
            if stage.stopped_at.is_none() {
                stage.stopped_at = Some(Utc::now());
            }
        }
        actions.push(JobAction::CleanUpJob {
            job_id,
            stage: None,
        });
        if matches!(job.state, JobState::Draining) {
            job.state = JobState::Succeeded;
        } else {
            job.state = JobState::Canceled;
        }
        job.stopped_at = Some(Utc::now());
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

        let plan =
            PhysicalPlanNode::try_from_physical_plan(stage.plan.clone(), self.codec.as_ref())?
                .encode_to_vec();
        let inputs = stage
            .inputs
            .iter()
            .map(|input| self.get_task_input(job, key, input, assignments))
            .collect::<ExecutionResult<Vec<_>>>()?;
        let output = self.get_task_output(job, key, stage)?;
        let definition = TaskDefinition {
            plan: Arc::from(plan),
            inputs,
            output,
        };
        Ok((definition, context.clone()))
    }

    fn get_task_input(
        &self,
        job: &JobDescriptor,
        key: &TaskKey,
        input: &StageInput,
        assignments: &dyn TaskAssignmentGetter,
    ) -> ExecutionResult<TaskInput> {
        let latest_attempt = |stage: usize, partition: usize| -> ExecutionResult<usize> {
            Self::get_latest_task_attempt(job, stage, partition).ok_or_else(|| {
                ExecutionError::InvalidArgument(format!(
                    "no latest task attempt found for job {} stage {} partition {}",
                    key.job_id, stage, partition
                ))
            })
        };

        let Some(producer) = job.graph.stages().get(input.stage) else {
            return Err(ExecutionError::InvalidArgument(format!(
                "job {} input stage {} not found",
                key.job_id, input.stage
            )));
        };
        let partitions = producer.plan.output_partitioning().partition_count();
        let channels = producer.distribution.channels();
        let keys = match input.mode {
            InputMode::Forward | InputMode::Merge => (0..partitions)
                .map(|partition| {
                    (0..channels)
                        .map(|channel| {
                            Ok(TaskInputKey {
                                partition,
                                attempt: latest_attempt(input.stage, partition)?,
                                channel,
                            })
                        })
                        .collect::<ExecutionResult<Vec<_>>>()
                })
                .collect::<ExecutionResult<Vec<Vec<_>>>>()?,
            // Enumerate channels in the outer loop and partitions in the inner loop.
            // This is the whole point of shuffle!
            InputMode::Shuffle => (0..channels)
                .map(|channel| {
                    (0..partitions)
                        .map(|partition| {
                            Ok(TaskInputKey {
                                partition,
                                attempt: latest_attempt(input.stage, partition)?,
                                channel,
                            })
                        })
                        .collect::<ExecutionResult<Vec<_>>>()
                })
                .collect::<ExecutionResult<Vec<Vec<_>>>>()?,
            InputMode::Broadcast => {
                let keys = (0..partitions)
                    .flat_map(|partition| {
                        (0..channels).map(move |channel| {
                            Ok(TaskInputKey {
                                partition,
                                attempt: latest_attempt(input.stage, partition)?,
                                channel,
                            })
                        })
                    })
                    .collect::<ExecutionResult<Vec<_>>>()?;
                vec![keys]
            }
        };
        let locator = match producer.mode {
            OutputMode::Pipelined => match producer.placement {
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
                }
                TaskPlacement::Worker => {
                    let keys = keys.into_iter().map(|keys| {
                        keys.into_iter().map(|k| {
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
                        }).collect::<ExecutionResult<Vec<_>>>()
                    }).collect::<ExecutionResult<Vec<Vec<_>>>>()?;
                    TaskInputLocator::Worker {
                        stage: input.stage,
                        keys,
                    }
                }
            },
            OutputMode::Blocking => {
                let uri = Err(ExecutionError::InternalError("not implemented".to_string()))?;
                TaskInputLocator::Remote {
                    uri,
                    stage: input.stage,
                    keys,
                }
            }
        };
        Ok(TaskInput { locator })
    }

    fn get_task_output(
        &self,
        job: &JobDescriptor,
        key: &TaskKey,
        stage: &Stage,
    ) -> ExecutionResult<TaskOutput> {
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
        Ok(TaskOutput {
            distribution,
            locator,
        })
    }

    fn get_latest_task_attempt(
        job: &JobDescriptor,
        stage: usize,
        partition: usize,
    ) -> Option<usize> {
        job.stages
            .get(stage)
            .and_then(|stage| stage.tasks.get(partition))
            .and_then(|task| task.attempts.split_last().map(|(_, head)| head.len()))
    }
}

#[derive(PartialEq, Eq, Hash)]
struct StageGroupKey {
    placement: TaskPlacement,
    group: String,
}

#[derive(Default)]
struct StageGroup {
    stages: IndexSet<usize>,
    buckets: Vec<Vec<TaskSetEntry>>,
}
