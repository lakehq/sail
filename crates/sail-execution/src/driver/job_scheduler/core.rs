use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use indexmap::{IndexMap, IndexSet};
use log::{debug, warn};
use sail_common::actor::ActorContext;
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use sail_system_store::SystemEvent;
use sail_telemetry::events::SystemEventReporter;

use crate::driver::DriverActor;
use crate::driver::job_scheduler::state::{
    JobDescriptor, StageState, TaskAttemptDescriptor, TaskRegionState, TaskState,
};
use crate::driver::job_scheduler::topology::TaskRegionTopology;
use crate::driver::job_scheduler::{JobAction, JobScheduler, JobState};
use crate::driver::output::{JobOutputOutcome, build_job_output};
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskKey, TaskKeyDisplay, TaskStreamKey};
use crate::job_graph::{
    InputMode, JobGraph, OutputDistribution, OutputMode, Stage, StageInput, TaskPlacement,
};
use crate::proto::{
    encode_remote_partitioning, encode_remote_physical_expr, encode_remote_physical_plan,
};
use crate::shuffle::ShuffleBackendKind;
use crate::task::definition::{
    TaskDefinition, TaskInput, TaskInputKey, TaskInputLocator, TaskOutput, TaskOutputDistribution,
    TaskOutputLocator,
};
use crate::task::scheduling::{
    TaskAssignment, TaskAssignmentGetter, TaskOutputKind, TaskRegion, TaskSet, TaskSetEntry,
};

impl JobScheduler {
    fn next_job_id(&mut self) -> ExecutionResult<JobId> {
        self.job_id_generator.generate()
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
        let graph = JobGraph::try_new(
            plan,
            crate::job_graph::JobGraphOptions {
                shuffle_backend: self.options.shuffle_backend.clone(),
            },
        )?;
        debug!("job {job_id} job graph \n{graph}");

        let (output, stream) = build_job_output(ctx, job_id, graph.schema().clone());
        let descriptor = JobDescriptor::try_new(graph, JobState::Running { output }, context)?;
        self.jobs.insert(job_id, descriptor);

        if let Some(job) = self.jobs.get(&job_id) {
            let event_reporter = &self.event_reporter;
            let session_id = &self.options.session_id;
            event_reporter.report(SystemEvent::JobCreated {
                session_id: session_id.clone(),
                job_id: u64::from(job_id),
                status: job.state.status().to_string(),
                created_at: Utc::now(),
            });
            for (s, (stage, descriptor)) in job.graph.stages().iter().zip(&job.stages).enumerate() {
                event_reporter.report(SystemEvent::StageCreated {
                    session_id: session_id.clone(),
                    job_id: u64::from(job_id),
                    stage: s as u64,
                    partitions: stage.plan.output_partitioning().partition_count() as u64,
                    inputs: stage
                        .inputs
                        .iter()
                        .map(|input| sail_system_store::types::StageInput {
                            stage: input.stage as u64,
                            mode: input.mode.to_string(),
                        })
                        .collect(),
                    group: stage.group.clone(),
                    mode: stage.mode.to_string(),
                    distribution: stage.distribution.to_string(),
                    placement: stage.placement.to_string(),
                    status: descriptor.state.status().to_string(),
                    created_at: Utc::now(),
                });
            }
        }

        Ok((job_id, stream))
    }

    pub fn update_task(
        &mut self,
        key: &TaskKey,
        state: TaskState,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
    ) {
        let event_reporter = self.event_reporter.clone();
        let session_id = self.options.session_id.clone();
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
        attempt.messages.extend(message);
        if let Some(cause) = cause {
            attempt.cause = Some(cause);
        }
        event_reporter.report(SystemEvent::TaskUpdated {
            session_id,
            job_id: u64::from(key.job_id),
            stage: key.stage as u64,
            partition: key.partition as u64,
            attempt: key.attempt as u64,
            status: attempt.state.status().to_string(),
            updated_at: Utc::now(),
        });
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
    ///   4. If any task fails without allowing a retry or exceeds the maximum allowed
    ///      attempts, or the task region needs a retry after its output has reached the
    ///      client, the task region and the job are marked as failed.
    ///   5. If all the tasks in the final stages have succeeded, the job is marked as succeeded.
    ///   6. For each task region, schedule the tasks of the region if all the dependency
    ///      regions have succeeded.
    pub fn refresh_job(&mut self, job_id: JobId) -> Vec<JobAction> {
        let event_reporter = self.event_reporter.clone();
        let session_id = self.options.session_id.clone();
        let Some(job) = self.jobs.get_mut(&job_id) else {
            warn!("job {job_id} not found");
            return vec![];
        };
        if !matches!(job.state, JobState::Running { .. }) {
            return vec![];
        }

        let mut actions = vec![];

        actions.extend(Self::cascade_cancel_task_attempts(
            job_id,
            job,
            &event_reporter,
            &session_id,
        ));
        actions.extend(Self::extend_job_output(job_id, job));
        actions.extend(Self::clean_up_job_by_stage(
            job_id,
            job,
            &event_reporter,
            &session_id,
        ));

        Self::update_task_regions(job, self.options.task_max_attempts);
        let regions = Self::start_task_regions(job);

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
            event_reporter.report(SystemEvent::JobUpdated {
                session_id,
                job_id: u64::from(job_id),
                status: job.state.status().to_string(),
                updated_at: Utc::now(),
            });
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
            event_reporter.report(SystemEvent::JobUpdated {
                session_id,
                job_id: u64::from(job_id),
                status: job.state.status().to_string(),
                updated_at: Utc::now(),
            });
            return actions;
        }

        actions.extend(Self::schedule_task_regions(
            job_id,
            job,
            regions,
            &event_reporter,
            &session_id,
        ));

        actions
    }

    fn update_task_regions(job: &mut JobDescriptor, task_max_attempts: usize) {
        for (r, region) in job.topology.regions.iter().enumerate() {
            let failed = region.tasks.iter().any(|t| {
                let attempts = &job.stages[t.stage].tasks[t.partition].attempts;
                // A terminal cause can arrive after the attempt was canceled or replaced.
                if attempts.iter().any(|attempt| {
                    attempt
                        .cause
                        .as_ref()
                        .is_some_and(|cause| !cause.is_retryable())
                }) {
                    return true;
                }
                if let Some(attempt) = attempts.last()
                    && matches!(attempt.state, TaskState::Failed | TaskState::Canceled)
                    && attempts.len() >= task_max_attempts
                {
                    return true;
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

    fn cascade_cancel_task_attempts(
        job_id: JobId,
        job: &mut JobDescriptor,
        event_reporter: &SystemEventReporter,
        session_id: &str,
    ) -> Vec<JobAction> {
        let mut actions = vec![];

        for region in &job.topology.regions {
            let failed = region.tasks.iter().any(|t| {
                let attempts = &job.stages[t.stage].tasks[t.partition].attempts;
                attempts.iter().any(|attempt| {
                    attempt
                        .cause
                        .as_ref()
                        .is_some_and(|cause| !cause.is_retryable())
                }) || attempts
                    .last()
                    .is_some_and(|attempt| matches!(attempt.state, TaskState::Failed))
            });

            if failed {
                // cancel all other tasks in this region
                for t in &region.tasks {
                    let task = &mut job.stages[t.stage].tasks[t.partition];
                    for (a, attempt) in task.attempts.iter_mut().enumerate() {
                        if !attempt.state.is_terminal() {
                            attempt.state = TaskState::Canceled;
                            event_reporter.report(SystemEvent::TaskUpdated {
                                session_id: session_id.to_string(),
                                job_id: u64::from(job_id),
                                stage: t.stage as u64,
                                partition: t.partition as u64,
                                attempt: a as u64,
                                status: attempt.state.status().to_string(),
                                updated_at: Utc::now(),
                            });
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

    fn clean_up_job_by_stage(
        job_id: JobId,
        job: &mut JobDescriptor,
        event_reporter: &SystemEventReporter,
        session_id: &str,
    ) -> Vec<JobAction> {
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
                event_reporter.report(SystemEvent::StageUpdated {
                    session_id: session_id.to_string(),
                    job_id: u64::from(job_id),
                    stage: s as u64,
                    status: job.stages[s].state.status().to_string(),
                    updated_at: Utc::now(),
                });
                actions.push(JobAction::CleanUpJob {
                    job_id,
                    stage: Some(s),
                    context: job.context.clone(),
                });
            }
        }

        actions
    }

    /// Finds the task regions that need new task attempts and starts their output.
    /// A region whose output has reached the client cannot be started again,
    /// so it is marked as failed instead.
    fn start_task_regions(job: &mut JobDescriptor) -> Vec<usize> {
        let mut regions = vec![];

        for (r, region) in job.topology.regions.iter().enumerate() {
            if !matches!(job.regions[r].state, TaskRegionState::Running) {
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

            // All the tasks in a region have the same number of attempts.
            let attempt = region
                .tasks
                .first()
                .map_or(0, |t| job.stages[t.stage].tasks[t.partition].attempts.len());
            if !job.regions[r].output.start(attempt) {
                job.regions[r].state = TaskRegionState::Failed;
                continue;
            }

            regions.push(r);
        }

        regions
    }

    fn schedule_task_regions(
        job_id: JobId,
        job: &mut JobDescriptor,
        regions: Vec<usize>,
        event_reporter: &SystemEventReporter,
        session_id: &str,
    ) -> Vec<JobAction> {
        let mut actions = vec![];

        for r in regions {
            let region = &job.topology.regions[r];
            for t in &region.tasks {
                let attempts = &mut job.stages[t.stage].tasks[t.partition].attempts;
                attempts.push(TaskAttemptDescriptor {
                    state: TaskState::Created,
                    messages: vec![],
                    cause: None,
                    job_output_fetched: false,
                });
                event_reporter.report(SystemEvent::TaskCreated {
                    session_id: session_id.to_string(),
                    job_id: u64::from(job_id),
                    stage: t.stage as u64,
                    partition: t.partition as u64,
                    attempt: (attempts.len() - 1) as u64,
                    status: TaskState::Created.status().to_string(),
                    created_at: Utc::now(),
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
        let mut stage_partitions: IndexMap<StageGroupKey, IndexMap<usize, usize>> = IndexMap::new();
        for s in stages {
            let stage = &job.graph.stages()[s];
            let key = StageGroupKey {
                placement: stage.placement,
                group: stage.group.clone(),
            };
            let p = stage.plan.output_partitioning().partition_count();
            stage_partitions.entry(key).or_default().insert(s, p);
        }
        let mut stage_groups = stage_partitions
            .into_iter()
            .map(|(key, partitions)| (key, StageGroup::new(partitions)))
            .collect::<IndexMap<_, _>>();

        for t in &region.tasks {
            if let Some(attempt) = Self::get_latest_task_attempt(job, t.stage, t.partition) {
                let stage = &job.graph.stages()[t.stage];
                let output = match stage.mode {
                    OutputMode::Pipelined => TaskOutputKind::Local,
                    OutputMode::Blocking => match job.graph.shuffle_backend() {
                        ShuffleBackendKind::Storage { .. } => TaskOutputKind::Storage,
                        ShuffleBackendKind::Celeborn { .. } => TaskOutputKind::External,
                        ShuffleBackendKind::Flight { .. } => unreachable!(),
                    },
                };
                let key = StageGroupKey {
                    placement: stage.placement,
                    group: stage.group.clone(),
                };
                if let Some(group) = stage_groups.get_mut(&key) {
                    group.add_task(TaskSetEntry {
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
            // A region may contain only some stage partitions. Empty buckets need no task slots.
            for entries in value
                .buckets
                .into_iter()
                .filter(|entries| !entries.is_empty())
            {
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
        for (r, region) in job.topology.regions.iter().enumerate() {
            for t in &region.tasks {
                if !job.topology.stages[t.stage].consumers.is_empty() {
                    // The stage is not a final stage.
                    continue;
                }
                let channels = job.graph.stages()[t.stage].distribution.channels();
                let task = &mut job.stages[t.stage].tasks[t.partition];
                let Some((attempt, head)) = task.attempts.split_last_mut() else {
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
                        stage: t.stage,
                        partition: t.partition,
                        attempt: head.len(),
                        channel: c,
                    };
                    actions.push(JobAction::ExtendJobOutput {
                        handle: output.handle(),
                        key,
                        schema: schema.clone(),
                        region: job.regions[r].output.clone(),
                    });
                }
            }
        }

        actions
    }

    fn infer_job_failure_cause(job: &JobDescriptor) -> CommonErrorCause {
        for attempt in job
            .stages
            .iter()
            .flat_map(|stage| &stage.tasks)
            .flat_map(|task| &task.attempts)
        {
            if let Some(cause) = &attempt.cause
                && !cause.is_retryable()
            {
                return cause.clone();
            }
        }
        // Report the failed region. Among its failed task attempts, take the most recent
        // attempt index, and then the smallest stage and partition.
        if let Some(r) = job
            .regions
            .iter()
            .position(|x| matches!(x.state, TaskRegionState::Failed))
        {
            let cause = job.topology.regions[r]
                .tasks
                .iter()
                .flat_map(|t| {
                    job.stages[t.stage].tasks[t.partition]
                        .attempts
                        .iter()
                        .enumerate()
                        .filter(|(_, attempt)| matches!(attempt.state, TaskState::Failed))
                        .filter_map(move |(a, attempt)| {
                            let cause = attempt.cause.as_ref()?;
                            Some(((std::cmp::Reverse(a), t.stage, t.partition), cause))
                        })
                })
                .min_by_key(|(key, _)| *key);
            if let Some((_, cause)) = cause {
                return cause.clone();
            }
        }
        CommonErrorCause::new::<PyErrExtractor>(&ExecutionError::InternalError(
            "job failed for unknown reason".to_string(),
        ))
    }

    /// Determine the actions needed in the driver to clean up the job.
    /// The method cancels all the task attempts that are not in terminal states
    /// and removes all the job output streams.
    pub fn clean_up_job(&mut self, job_id: JobId, outcome: JobOutputOutcome) -> Vec<JobAction> {
        let event_reporter = self.event_reporter.clone();
        let session_id = self.options.session_id.clone();
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
        for (s, stage) in job.stages.iter_mut().enumerate() {
            match stage.state {
                StageState::Active => {
                    stage.state = StageState::Inactive;
                    event_reporter.report(SystemEvent::StageUpdated {
                        session_id: session_id.clone(),
                        job_id: u64::from(job_id),
                        stage: s as u64,
                        status: stage.state.status().to_string(),
                        updated_at: Utc::now(),
                    });
                }
                StageState::Inactive => {}
            }
        }
        actions.push(JobAction::CleanUpJob {
            job_id,
            stage: None,
            context: job.context.clone(),
        });
        job.state.finish_output(outcome);
        event_reporter.report(SystemEvent::JobUpdated {
            session_id,
            job_id: u64::from(job_id),
            status: job.state.status().to_string(),
            updated_at: Utc::now(),
        });
        actions
    }

    /// Returns the retry region containing a task partition.
    pub fn task_region(&self, key: &TaskKey) -> Option<usize> {
        let job = self.jobs.get(&key.job_id)?;
        job.topology
            .task_regions
            .get(&super::topology::TaskTopology {
                stage: key.stage,
                partition: key.partition,
            })
            .copied()
    }

    /// Builds the stage definition against the current assignment snapshot.
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
        let Some(stage) = job.graph.stages().get(key.stage) else {
            return Err(ExecutionError::InvalidArgument(format!(
                "stage {} not found in job {}",
                key.stage, key.job_id
            )));
        };

        let plan = encode_remote_physical_plan(self.codec.as_ref(), stage.plan.clone())?;
        let inputs = stage
            .inputs
            .iter()
            .map(|input| TaskInputBuilder::try_new(job, key, input, assignments)?.build())
            .collect::<ExecutionResult<Vec<_>>>()?;
        let output = TaskOutputBuilder::new(job, key, stage, self.codec.as_ref()).build()?;
        let definition = TaskDefinition {
            plan: Arc::from(plan),
            inputs,
            output,
        };
        Ok((definition, job.context.clone()))
    }

    pub fn stop(&mut self) {
        let event_reporter = self.event_reporter.clone();
        let session_id = self.options.session_id.clone();
        for (job_id, job) in self.jobs.iter_mut() {
            let job_id = u64::from(*job_id);
            if matches!(job.state, JobState::Running { .. } | JobState::Draining) {
                // For running jobs, the job output is dropped here.
                // Internally, the job output manages the receiving end of the output stream.
                // So once the stream receiver is no longer available, all the running tasks
                // will ultimately find that the sink (owned by the shuffle write node) is closed.
                // The task will stop running even if the task produces an infinite stream.
                // Once the tasks are stopped, the inputs (gRPC streaming responses owned by
                // shuffle read nodes) are dropped. So the worker gRPC server will have no active
                // clients subscribing to local streams, and the server can proceed with shutdown.
                job.state = JobState::Canceled;
                event_reporter.report(SystemEvent::JobUpdated {
                    session_id: session_id.to_string(),
                    job_id,
                    status: job.state.status().to_string(),
                    updated_at: Utc::now(),
                });
            }
            for (s, stage) in job.stages.iter_mut().enumerate() {
                if matches!(stage.state, StageState::Active) {
                    stage.state = StageState::Inactive;
                    event_reporter.report(SystemEvent::StageUpdated {
                        session_id: session_id.to_string(),
                        job_id,
                        stage: s as u64,
                        status: stage.state.status().to_string(),
                        updated_at: Utc::now(),
                    });
                }
                for (partition, task) in stage.tasks.iter_mut().enumerate() {
                    for (a, attempt) in task.attempts.iter_mut().enumerate() {
                        if !attempt.state.is_terminal() {
                            attempt.state = TaskState::Canceled;
                            event_reporter.report(SystemEvent::TaskUpdated {
                                session_id: session_id.to_string(),
                                job_id,
                                stage: s as u64,
                                partition: partition as u64,
                                attempt: a as u64,
                                status: attempt.state.status().to_string(),
                                updated_at: Utc::now(),
                            });
                        }
                    }
                }
            }
        }
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

struct TaskInputBuilder<'a> {
    job: &'a JobDescriptor,
    key: &'a TaskKey,
    input: &'a StageInput,
    producer: &'a Stage,
    consumer: &'a Stage,
    assignments: &'a dyn TaskAssignmentGetter,
}

impl<'a> TaskInputBuilder<'a> {
    fn try_new(
        job: &'a JobDescriptor,
        key: &'a TaskKey,
        input: &'a StageInput,
        assignments: &'a dyn TaskAssignmentGetter,
    ) -> ExecutionResult<Self> {
        let producer = job.graph.stages().get(input.stage).ok_or_else(|| {
            ExecutionError::InvalidArgument(format!(
                "job {} input stage {} not found",
                key.job_id, input.stage
            ))
        })?;
        let consumer = job.graph.stages().get(key.stage).ok_or_else(|| {
            ExecutionError::InvalidArgument(format!(
                "stage {} not found in job {}",
                key.stage, key.job_id
            ))
        })?;
        Ok(Self {
            job,
            key,
            input,
            producer,
            consumer,
            assignments,
        })
    }

    fn build(&self) -> ExecutionResult<TaskInput> {
        let locator = match self.producer.mode {
            OutputMode::Pipelined => match self.producer.placement {
                TaskPlacement::Driver => self.build_driver_locator()?,
                TaskPlacement::Worker => self.build_worker_locator()?,
            },
            OutputMode::Blocking => match self.job.graph.shuffle_backend() {
                ShuffleBackendKind::Storage { .. } => self.build_storage_locator()?,
                ShuffleBackendKind::Celeborn { .. } => self.build_shuffle_service_locator()?,
                ShuffleBackendKind::Flight { .. } => self.build_storage_locator()?,
            },
        };
        Ok(TaskInput {
            stage: self.input.stage,
            locator: Arc::new(locator),
        })
    }

    fn build_driver_locator(&self) -> ExecutionResult<TaskInputLocator> {
        let keys = self.build_task_input_keys()?;
        keys.iter()
            .flatten()
            .try_for_each(|key| match self.assignment(key) {
                Some(TaskAssignment::Driver) => Ok(()),
                _ => Err(ExecutionError::InternalError(format!(
                    "job {} input stage {} partition {} attempt {} is not assigned to driver",
                    self.key.job_id, self.input.stage, key.partition, key.attempt
                ))),
            })?;
        Ok(TaskInputLocator::Driver { keys })
    }

    fn build_worker_locator(&self) -> ExecutionResult<TaskInputLocator> {
        let keys = self
            .build_task_input_keys()?
            .into_iter()
            .map(|keys| {
                keys.into_iter()
                    .map(|key| {
                        let Some(TaskAssignment::Worker { worker_id, slot: _ }) =
                            self.assignment(&key)
                        else {
                            return Err(ExecutionError::InternalError(format!(
                                "job {} input stage {} partition {} attempt {} is not assigned to worker",
                                self.key.job_id,
                                self.input.stage,
                                key.partition,
                                key.attempt
                            )));
                        };
                        Ok((*worker_id, key))
                    })
                    .collect::<ExecutionResult<Vec<_>>>()
            })
            .collect::<ExecutionResult<Vec<Vec<_>>>>()?;
        Ok(TaskInputLocator::Worker { keys })
    }

    fn build_storage_locator(&self) -> ExecutionResult<TaskInputLocator> {
        Ok(TaskInputLocator::Storage {
            keys: self.build_task_input_keys()?,
        })
    }

    fn build_shuffle_service_locator(&self) -> ExecutionResult<TaskInputLocator> {
        let input_channels = self.producer.distribution.channels();
        let output_partitions = self.consumer.plan.output_partitioning().partition_count();
        let channels = match self.input.mode {
            InputMode::Shuffle => (0..output_partitions)
                .map(|partition| vec![partition])
                .collect(),
            InputMode::Broadcast => {
                let channels = (0..input_channels).collect::<Vec<_>>();
                vec![channels; output_partitions]
            }
            mode => {
                return Err(ExecutionError::InvalidArgument(format!(
                    "Celeborn shuffle service does not support {mode} input"
                )));
            }
        };
        Ok(TaskInputLocator::ShuffleService { channels })
    }

    fn assignment(&self, key: &TaskInputKey) -> Option<&TaskAssignment> {
        self.assignments.get(&TaskKey {
            job_id: self.key.job_id,
            stage: self.input.stage,
            partition: key.partition,
            attempt: key.attempt,
        })
    }

    fn build_task_input_keys(&self) -> ExecutionResult<Vec<Vec<TaskInputKey>>> {
        let input_partitions = self.producer.plan.output_partitioning().partition_count();
        let input_channels = self.producer.distribution.channels();

        match self.input.mode {
            InputMode::Forward | InputMode::Merge => {
                let mut groups = Vec::with_capacity(input_partitions);
                for partition in 0..input_partitions {
                    // Forward regions are assigned independently. Preserve input-local indices,
                    // but do not resolve producers for consumer partitions outside this snapshot.
                    if matches!(self.input.mode, InputMode::Forward)
                        && input_partitions
                            == self.consumer.plan.output_partitioning().partition_count()
                        && !JobScheduler::get_latest_task_attempt(
                            self.job,
                            self.key.stage,
                            partition,
                        )
                        .is_some_and(|attempt| {
                            self.assignments
                                .get(&TaskKey {
                                    job_id: self.key.job_id,
                                    stage: self.key.stage,
                                    partition,
                                    attempt,
                                })
                                .is_some()
                        })
                    {
                        groups.push(vec![]);
                        continue;
                    }
                    let attempt = self.latest_attempt(partition)?;
                    let mut group = Vec::with_capacity(input_channels);
                    for channel in 0..input_channels {
                        group.push(TaskInputKey {
                            partition,
                            attempt,
                            channel,
                        });
                    }
                    groups.push(group);
                }
                Ok(groups)
            }
            // Enumerate channels in the outer loop and partitions in the inner loop.
            // This is the whole point of shuffle!
            InputMode::Shuffle => {
                let mut groups = Vec::with_capacity(input_channels);
                for channel in 0..input_channels {
                    let mut group = Vec::with_capacity(input_partitions);
                    for partition in 0..input_partitions {
                        group.push(TaskInputKey {
                            partition,
                            attempt: self.latest_attempt(partition)?,
                            channel,
                        });
                    }
                    groups.push(group);
                }
                Ok(groups)
            }
            InputMode::Broadcast => {
                let mut keys = Vec::with_capacity(input_partitions * input_channels);
                for partition in 0..input_partitions {
                    let attempt = self.latest_attempt(partition)?;
                    for channel in 0..input_channels {
                        keys.push(TaskInputKey {
                            partition,
                            attempt,
                            channel,
                        });
                    }
                }
                Ok(vec![keys])
            }
            InputMode::Rescale {
                partitions: output_partitions,
            } => {
                // Keep rescale input expansion aligned with CoalesceExec's contiguous partition
                // grouping. A parent such as UnionExec can change the containing stage's
                // partition count, so use the target count recorded on this input edge.
                let mut groups = Vec::with_capacity(output_partitions);
                for output_partition in 0..output_partitions {
                    let start = output_partition * input_partitions / output_partitions;
                    let end = (output_partition + 1) * input_partitions / output_partitions;
                    let mut group =
                        Vec::with_capacity((end.saturating_sub(start)) * input_channels);
                    for partition in start..end {
                        let attempt = self.latest_attempt(partition)?;
                        for channel in 0..input_channels {
                            group.push(TaskInputKey {
                                partition,
                                attempt,
                                channel,
                            });
                        }
                    }
                    groups.push(group);
                }
                Ok(groups)
            }
        }
    }

    fn latest_attempt(&self, partition: usize) -> ExecutionResult<usize> {
        if let Some(attempt) =
            JobScheduler::get_latest_task_attempt(self.job, self.input.stage, partition)
        {
            Ok(attempt)
        } else {
            Err(ExecutionError::InvalidArgument(format!(
                "no latest task attempt found for job {} stage {} partition {}",
                self.key.job_id, self.input.stage, partition
            )))
        }
    }
}

struct TaskOutputBuilder<'a> {
    job: &'a JobDescriptor,
    key: &'a TaskKey,
    stage: &'a Stage,
    codec: &'a dyn PhysicalExtensionCodec,
}

impl<'a> TaskOutputBuilder<'a> {
    fn new(
        job: &'a JobDescriptor,
        key: &'a TaskKey,
        stage: &'a Stage,
        codec: &'a dyn PhysicalExtensionCodec,
    ) -> Self {
        Self {
            job,
            key,
            stage,
            codec,
        }
    }

    fn build(&self) -> ExecutionResult<TaskOutput> {
        let distribution = match &self.stage.distribution {
            OutputDistribution::Hash { keys, channels } => {
                let keys = keys
                    .iter()
                    .map(|expr| {
                        let expr = encode_remote_physical_expr(self.codec, expr)?;
                        Ok(Arc::from(expr))
                    })
                    .collect::<ExecutionResult<Vec<Arc<[u8]>>>>()?;
                TaskOutputDistribution::Hash {
                    keys,
                    channels: *channels,
                }
            }
            OutputDistribution::Range { partitioning } => {
                let channels = partitioning.partition_count();
                let partitioning = encode_remote_partitioning(
                    self.codec,
                    &Partitioning::Range(partitioning.clone()),
                )?;
                TaskOutputDistribution::Range {
                    partitioning: Arc::from(partitioning),
                    channels,
                }
            }
            OutputDistribution::RoundRobinBatch { channels } => {
                TaskOutputDistribution::RoundRobinBatch {
                    channels: *channels,
                }
            }
            OutputDistribution::RoundRobinRow { channels } => {
                TaskOutputDistribution::RoundRobinRow {
                    channels: *channels,
                }
            }
        };
        let locator = match self.stage.mode {
            OutputMode::Pipelined => TaskOutputLocator::Pipelined {
                replicas: self.job.graph.replicas(self.key.stage),
            },
            OutputMode::Blocking => TaskOutputLocator::Blocking,
        };
        Ok(TaskOutput {
            distribution,
            locator,
        })
    }
}

#[derive(PartialEq, Eq, Hash)]
struct StageGroupKey {
    placement: TaskPlacement,
    group: String,
}

struct StageGroup {
    partition_offsets: HashMap<usize, usize>,
    buckets: Vec<Vec<TaskSetEntry>>,
}

impl StageGroup {
    fn new(stage_partitions: IndexMap<usize, usize>) -> Self {
        let count = stage_partitions.values().copied().max().unwrap_or(0);
        let mut partition_offsets = HashMap::new();
        let mut offset = 0;
        for (stage, partitions) in stage_partitions {
            if partitions > 0 {
                partition_offsets.insert(stage, offset);
                offset = (offset + partitions) % count;
            }
        }
        Self {
            partition_offsets,
            buckets: vec![vec![]; count],
        }
    }

    fn add_task(&mut self, entry: TaskSetEntry) {
        // Place each stage consecutively, continuing from the preceding stage's offset.
        // Full regions have bucket sizes differing by at most one, with no two partitions
        // of a stage sharing a bucket. For partition-sliced forward regions, all stages
        // have the same partition count, so offsets are zero and matching partitions
        // continue to share a bucket.
        let bucket =
            (self.partition_offsets[&entry.key.stage] + entry.key.partition) % self.buckets.len();
        self.buckets[bucket].push(entry);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::time::Duration;

    use datafusion::arrow::array::{ArrayRef, Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::{SendableRecordBatchStream, TaskContext};
    use datafusion::physical_expr::Partitioning;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;
    use sail_common::actor::{ActorContext, ActorSystem};
    use sail_common::config::{AppConfig, SystemCatalogStore};
    use sail_common::runtime::RuntimeHandle;
    use sail_common_datafusion::error::CommonErrorCause;
    use sail_telemetry::events::SystemEventReporter;
    use sail_telemetry::telemetry::{global_system_event_reporter, init_telemetry};
    use sail_telemetry::{ResourceKind, ResourceOptions};
    use tokio::sync::mpsc;
    use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

    use super::{JobAction, JobScheduler, JobState, StageGroup, TaskState};
    use crate::driver::job_scheduler::JobSchedulerOptions;
    use crate::driver::output::JobOutputItem;
    use crate::driver::{DriverActor, DriverComponents, DriverOptions};
    use crate::id::{DriverId, JobId, TaskKey, TaskStreamKey};
    use crate::stream::error::TaskStreamResult;
    use crate::task::scheduling::{TaskOutputKind, TaskSetEntry};
    use crate::worker_manager::LocalWorkerManager;

    #[tokio::test]
    async fn late_unknown_commit_cancels_queued_retry() -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::Arc;

        use datafusion::arrow::datatypes::Schema;
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::execution::TaskContext;
        use datafusion::prelude::SessionContext;
        use sail_common::actor::{ActorContext, ActorSystem};
        use sail_common::config::{AppConfig, SystemCatalogStore};
        use sail_common::runtime::RuntimeHandle;
        use sail_common_datafusion::error::CommonErrorCause;
        use sail_iceberg::SnapshotUpdateKind;
        use sail_iceberg::physical_plan::IcebergCommitExec;
        use url::Url;

        use super::{JobAction, JobScheduler, JobState, TaskState};
        use crate::driver::job_scheduler::JobSchedulerOptions;
        use crate::driver::task_assigner::{TaskAssigner, TaskAssignerOptions};
        use crate::driver::{DriverActor, DriverComponents, DriverOptions};
        use crate::id::{DriverId, WorkerId};
        use crate::worker_manager::LocalWorkerManager;

        let mut config = AppConfig::load()?;
        config.cluster.task_max_attempts = 4;
        config.cluster.worker_task_slots = 1;
        config.cluster.worker_max_count = 1;
        config.catalog.system.store = SystemCatalogStore::Memory;
        config.telemetry.export_traces = false;
        config.telemetry.export_metrics = false;
        config.telemetry.export_logs = false;
        let reporter = test_event_reporter(&config)?;
        let handle = tokio::runtime::Handle::current();
        let runtime = RuntimeHandle::new(handle.clone(), handle);
        let options = DriverOptions::try_new(
            &config,
            runtime.clone(),
            "late-unknown-commit".into(),
            DriverId::from(1),
            0,
        )?;
        let mut scheduler =
            JobScheduler::new(JobSchedulerOptions::from(&options), reporter.clone());
        let mut assigner = TaskAssigner::new(TaskAssignerOptions::from(&options));
        let mut system = ActorSystem::new();
        let driver = system.spawn::<DriverActor>((
            options,
            DriverComponents {
                worker_manager: Box::new(LocalWorkerManager::new(runtime, SessionContext::new())),
                event_reporter: reporter,
            },
        ));
        let mut ctx = ActorContext::new(&driver);
        let input = MemorySourceConfig::try_new_exec(&[vec![]], Arc::new(Schema::empty()), None)?;
        let plan = Arc::new(IcebergCommitExec::new(
            input,
            Url::parse("memory://bucket/late-unknown-commit")?,
            None,
            SnapshotUpdateKind::FastAppend,
        ));
        let (job_id, _output) =
            scheduler.accept_job(&mut ctx, plan, Arc::new(TaskContext::default()))?;
        scheduler.refresh_job(job_id);
        let original = TaskKey {
            job_id,
            stage: 0,
            partition: 0,
            attempt: 0,
        };
        scheduler.update_task(&original, TaskState::Running, None, None);
        // Losing the output worker cancels the driver commit in the same region.
        let output_task = TaskKey {
            stage: 1,
            ..original.clone()
        };
        assert_eq!(
            scheduler.task_region(&original),
            scheduler.task_region(&output_task)
        );
        scheduler.update_task(
            &output_task,
            TaskState::Failed,
            None,
            Some(CommonErrorCause::Execution(
                "worker heartbeat timeout".into(),
            )),
        );
        for action in scheduler.refresh_job(job_id) {
            if let JobAction::ScheduleTaskRegion { region } = action {
                assigner.enqueue_tasks(&region)?;
            }
        }
        assert!(matches!(
            scheduler.get_task_state(&original),
            Some(TaskState::Canceled)
        ));
        assert!(assigner.assign_tasks().is_empty());
        let replacement = TaskKey {
            attempt: 1,
            ..original.clone()
        };
        assert!(matches!(
            scheduler.get_task_state(&replacement),
            Some(TaskState::Created)
        ));

        scheduler.update_task(
            &original,
            TaskState::Failed,
            None,
            Some(CommonErrorCause::CommitStateUnknown("response lost".into())),
        );
        let actions = scheduler.refresh_job(job_id);
        assert!(matches!(scheduler.jobs[&job_id].state, JobState::Failed));
        assert!(actions.iter().any(|action| matches!(
            action,
            JobAction::FailJobOutput {
                cause: CommonErrorCause::CommitStateUnknown(_),
                ..
            }
        )));
        for action in &actions {
            if let JobAction::CancelTask { key } = action {
                assigner.exclude_task(key);
            }
        }
        // A worker can register before the driver processes asynchronous output cleanup.
        assigner.activate_worker(WorkerId::from(1));
        let assignments = assigner.assign_tasks();
        assert!(
            actions.iter().any(
                |action| matches!(action, JobAction::CancelTask { key } if key == &replacement)
            ) && assignments.is_empty(),
            "late unknown commit must cancel its queued retry; actions: {actions:?}; assignments: {assignments:?}"
        );
        Ok(())
    }

    #[test]
    fn unknown_commit_stops_retries_and_takes_precedence_over_other_failures()
    -> crate::error::ExecutionResult<()> {
        use std::sync::Arc;

        use datafusion::arrow::datatypes::Schema;
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::execution::TaskContext;
        use sail_common_datafusion::error::CommonErrorCause;

        use super::{
            JobDescriptor, JobScheduler, JobState, TaskAttemptDescriptor, TaskRegionState,
            TaskState,
        };
        use crate::job_graph::{JobGraph, JobGraphOptions};
        use crate::shuffle::{ShuffleBackendKind, ShuffleCompression};

        let graph = JobGraph::try_new(
            MemorySourceConfig::try_new_exec(&[vec![], vec![]], Arc::new(Schema::empty()), None)?,
            JobGraphOptions {
                shuffle_backend: ShuffleBackendKind::Flight {
                    compression: ShuffleCompression::None,
                    connection_count: std::num::NonZeroUsize::MIN,
                    initial_stream_window_size: None,
                    initial_connection_window_size: None,
                },
            },
        )?;
        let mut job =
            JobDescriptor::try_new(graph, JobState::Draining, Arc::new(TaskContext::default()))?;
        let attempt = |state, cause| TaskAttemptDescriptor {
            state,
            messages: vec![],
            cause,
            job_output_fetched: false,
        };
        for _ in 0..2 {
            job.stages[0].tasks[0].attempts.push(attempt(
                TaskState::Failed,
                Some(CommonErrorCause::Io("transient failure".into())),
            ));
        }
        JobScheduler::update_task_regions(&mut job, 4);
        assert!(
            job.regions
                .iter()
                .all(|r| matches!(r.state, TaskRegionState::Running))
        );

        // A late error from a canceled attempt must still stop a replacement attempt.
        job.stages[0].tasks[1].attempts.push(attempt(
            TaskState::Canceled,
            Some(CommonErrorCause::CommitStateUnknown(
                "ambiguous commit".into(),
            )),
        ));
        job.stages[0].tasks[1]
            .attempts
            .push(attempt(TaskState::Scheduled, None));
        JobScheduler::update_task_regions(&mut job, 4);
        assert!(
            job.regions
                .iter()
                .any(|r| matches!(r.state, TaskRegionState::Failed))
        );
        assert!(matches!(
            JobScheduler::infer_job_failure_cause(&job),
            CommonErrorCause::CommitStateUnknown(message) if message == "ambiguous commit"
        ));
        Ok(())
    }

    #[test]
    fn forward_routing_uses_only_assigned_consumers_and_refreshes_producer_attempts()
    -> crate::error::ExecutionResult<()> {
        use std::collections::HashMap;
        use std::sync::Arc;

        use datafusion::arrow::datatypes::Schema;
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::execution::TaskContext;
        use datafusion::physical_expr::Partitioning;
        use datafusion::physical_plan::repartition::RepartitionExec;

        use super::{JobDescriptor, JobState, TaskAttemptDescriptor, TaskInputBuilder, TaskState};
        use crate::id::WorkerId;
        use crate::job_graph::{InputMode, JobGraph, JobGraphOptions, StageInput};
        use crate::shuffle::{ShuffleBackendKind, ShuffleCompression};
        use crate::task::definition::TaskInputLocator;
        use crate::task::scheduling::{TaskAssignment, TaskAssignmentGetter};

        struct Assignments(HashMap<TaskKey, TaskAssignment>);
        impl TaskAssignmentGetter for Assignments {
            fn get(&self, key: &TaskKey) -> Option<&TaskAssignment> {
                self.0.get(key)
            }
        }
        let input =
            MemorySourceConfig::try_new_exec(&vec![vec![]; 4], Arc::new(Schema::empty()), None)?;
        let graph = JobGraph::try_new(
            Arc::new(RepartitionExec::try_new(
                input,
                Partitioning::RoundRobinBatch(4),
            )?),
            JobGraphOptions {
                shuffle_backend: ShuffleBackendKind::Flight {
                    compression: ShuffleCompression::None,
                    connection_count: std::num::NonZeroUsize::MIN,
                    initial_stream_window_size: None,
                    initial_connection_window_size: None,
                },
            },
        )?;
        let mut job =
            JobDescriptor::try_new(graph, JobState::Draining, Arc::new(TaskContext::default()))?;
        let attempt = || TaskAttemptDescriptor {
            state: TaskState::Created,
            messages: vec![],
            cause: None,
            job_output_fetched: false,
        };
        for stage in &mut job.stages {
            for task in &mut stage.tasks {
                task.attempts.push(attempt());
            }
        }
        let key = |stage, partition, attempt| TaskKey {
            job_id: JobId::from(1),
            stage,
            partition,
            attempt,
        };
        let worker = |id| TaskAssignment::Worker {
            worker_id: WorkerId::from(id),
            slot: 0,
        };
        let mut assignments = Assignments(HashMap::new());
        for stage in 0..2 {
            for partition in 0..2 {
                assignments.0.insert(key(stage, partition, 0), worker(1));
            }
        }
        // Exercise the routing builder with a forward edge of the same partition shape.
        // Partitions 2 and 3 have attempts, but their regions have no assignments yet.
        let edge = StageInput {
            stage: 0,
            mode: InputMode::Forward,
        };
        let consumer = key(1, 0, 0);
        let original = TaskInputBuilder::try_new(&job, &consumer, &edge, &assignments)?.build()?;
        let TaskInputLocator::Worker { keys } = original.locator.as_ref() else {
            return Err(crate::error::ExecutionError::InternalError(
                "expected worker routing".into(),
            ));
        };
        assert_eq!(keys.len(), 4);
        assert!(!keys[0].is_empty());
        assert!(!keys[1].is_empty());
        assert!(keys[2].is_empty());
        assert!(keys[3].is_empty());

        job.stages[0].tasks[0].attempts.push(attempt());
        assignments.0.insert(key(0, 0, 1), worker(2));
        let retried = TaskInputBuilder::try_new(&job, &consumer, &edge, &assignments)?.build()?;
        let TaskInputLocator::Worker { keys: updated } = retried.locator.as_ref() else {
            return Err(crate::error::ExecutionError::InternalError(
                "expected worker routing".into(),
            ));
        };
        assert!(
            updated[0]
                .iter()
                .all(|(worker, key)| *worker == WorkerId::from(2) && key.attempt == 1)
        );
        assert!(
            keys[0]
                .iter()
                .all(|(worker, key)| *worker == WorkerId::from(1) && key.attempt == 0)
        );

        // Missing routing for an assigned consumer is still an error, not an empty input.
        assignments.0.remove(&key(0, 0, 1));
        assert!(
            TaskInputBuilder::try_new(&job, &consumer, &edge, &assignments)?
                .build()
                .is_err()
        );
        Ok(())
    }

    fn task(stage: usize, partition: usize) -> TaskSetEntry {
        TaskSetEntry {
            key: TaskKey {
                job_id: JobId::from(1),
                stage,
                partition,
                attempt: 0,
            },
            output: TaskOutputKind::Local,
        }
    }

    #[test]
    fn stage_group_balances_buckets_without_sharing_stage_partitions() {
        for partitions in [
            vec![4, 4, 8],
            vec![256, 256, 256, 768],
            vec![5, 3, 7],
            vec![8, 4, 4],
            vec![4, 8, 4],
        ] {
            let mut group = StageGroup::new(partitions.iter().copied().enumerate().collect());
            // Task insertion order must not affect the bucket offsets.
            for (stage, count) in partitions.iter().copied().enumerate().rev() {
                for partition in (0..count).rev() {
                    group.add_task(task(stage, partition));
                }
            }

            let count = partitions.iter().copied().max().unwrap_or(0);
            let total = partitions.iter().sum::<usize>();
            assert_eq!(group.buckets.len(), count);
            let mut tasks = HashSet::new();
            for bucket in &group.buckets {
                assert!((total / count..=total.div_ceil(count)).contains(&bucket.len()));
                let stages = bucket
                    .iter()
                    .map(|entry| entry.key.stage)
                    .collect::<HashSet<_>>();
                assert_eq!(stages.len(), bucket.len());
                for entry in bucket {
                    assert!(tasks.insert((entry.key.stage, entry.key.partition)));
                }
            }
            assert_eq!(tasks.len(), total);
        }
    }

    #[test]
    fn stage_group_preserves_partition_sliced_forward_regions() {
        for partition in 0..4 {
            let mut group = StageGroup::new([(0, 4), (1, 4), (2, 4)].into());
            for stage in 0..3 {
                group.add_task(task(stage, partition));
            }
            assert_eq!(group.buckets[partition].len(), 3);
            assert_eq!(
                group
                    .buckets
                    .iter()
                    .filter(|bucket| !bucket.is_empty())
                    .count(),
                1
            );
        }
    }

    type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

    /// Telemetry can be initialized only once in a process, and the tests of
    /// this module run in one process under `cargo test`.
    fn test_event_reporter(config: &AppConfig) -> TestResult<SystemEventReporter> {
        let _ = init_telemetry(
            &config.telemetry,
            &config.catalog.system,
            ResourceOptions {
                kind: ResourceKind::Server,
            },
        );
        Ok(global_system_event_reporter().ok_or("missing event reporter")?)
    }

    struct TestDriver {
        scheduler: JobScheduler,
        ctx: ActorContext<DriverActor>,
        _system: ActorSystem,
    }

    impl TestDriver {
        fn try_new(session_id: &str, task_max_attempts: usize) -> TestResult<Self> {
            let mut config = AppConfig::load()?;
            config.cluster.task_max_attempts = task_max_attempts;
            config.cluster.worker_task_slots = 1;
            config.cluster.worker_max_count = 1;
            config.catalog.system.store = SystemCatalogStore::Memory;
            config.telemetry.export_traces = false;
            config.telemetry.export_metrics = false;
            config.telemetry.export_logs = false;
            let reporter = test_event_reporter(&config)?;
            let handle = tokio::runtime::Handle::current();
            let runtime = RuntimeHandle::new(handle.clone(), handle);
            let options = DriverOptions::try_new(
                &config,
                runtime.clone(),
                session_id.into(),
                DriverId::from(1),
                0,
            )?;
            let scheduler =
                JobScheduler::new(JobSchedulerOptions::from(&options), reporter.clone());
            let mut system = ActorSystem::new();
            let driver = system.spawn::<DriverActor>((
                options,
                DriverComponents {
                    worker_manager: Box::new(LocalWorkerManager::new(
                        runtime,
                        SessionContext::new(),
                    )),
                    event_reporter: reporter,
                },
            ));
            Ok(Self {
                scheduler,
                ctx: ActorContext::new(&driver),
                _system: system,
            })
        }
    }

    /// A job whose output streams are fed by the test instead of by tasks.
    struct TestJob {
        driver: TestDriver,
        job_id: JobId,
        output: SendableRecordBatchStream,
        streams: HashMap<TaskStreamKey, mpsc::Sender<TaskStreamResult<RecordBatch>>>,
    }

    #[derive(Default)]
    struct TestRefresh {
        /// The number of task regions scheduled.
        scheduled: usize,
        failure: Option<CommonErrorCause>,
    }

    impl TestJob {
        /// Creates a job with a single stage, or with a shuffle to `output` partitions.
        fn try_new(partitions: usize, output: Option<usize>) -> TestResult<Self> {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int32,
                false,
            )]));
            let mut plan: Arc<dyn ExecutionPlan> =
                MemorySourceConfig::try_new_exec(&vec![vec![]; partitions], schema, None)?;
            if let Some(output) = output {
                plan = Arc::new(RepartitionExec::try_new(
                    plan,
                    Partitioning::RoundRobinBatch(output),
                )?);
            }
            let mut driver = TestDriver::try_new("job-output", 3)?;
            let (job_id, output) = driver.scheduler.accept_job(
                &mut driver.ctx,
                plan,
                Arc::new(TaskContext::default()),
            )?;
            Ok(Self {
                driver,
                job_id,
                output,
                streams: HashMap::new(),
            })
        }

        /// Refreshes the job and applies the job output actions.
        async fn refresh(&mut self) -> TestRefresh {
            let mut refresh = TestRefresh::default();
            for action in self.driver.scheduler.refresh_job(self.job_id) {
                match action {
                    JobAction::ScheduleTaskRegion { .. } => refresh.scheduled += 1,
                    JobAction::ExtendJobOutput {
                        handle,
                        key,
                        region,
                        ..
                    } => {
                        let (tx, rx) = mpsc::channel(1);
                        self.streams.insert(key.clone(), tx);
                        let stream = Box::pin(ReceiverStream::new(rx));
                        let item = JobOutputItem::Stream {
                            key,
                            stream,
                            region,
                        };
                        handle.send(item).await;
                    }
                    JobAction::FailJobOutput { handle, cause } => {
                        refresh.failure = Some(cause.clone());
                        handle.send(JobOutputItem::Error { cause }).await;
                    }
                    JobAction::CancelTask { .. } | JobAction::CleanUpJob { .. } => {}
                }
            }
            refresh
        }

        fn update(
            &mut self,
            stage: usize,
            partition: usize,
            attempt: usize,
            state: TaskState,
            failure: Option<&str>,
        ) {
            let key = TaskKey {
                job_id: self.job_id,
                stage,
                partition,
                attempt,
            };
            let cause = failure.map(|x| CommonErrorCause::Execution(x.to_string()));
            self.driver.scheduler.update_task(&key, state, None, cause);
        }

        /// Updates the latest attempt of all the tasks.
        fn update_all(&mut self, state: TaskState) {
            let job = &self.driver.scheduler.jobs[&self.job_id];
            let tasks = job
                .stages
                .iter()
                .enumerate()
                .flat_map(|(s, stage)| {
                    let tasks = stage.tasks.iter().enumerate();
                    tasks.map(move |(p, task)| (s, p, task.attempts.len() - 1))
                })
                .collect::<Vec<_>>();
            for (stage, partition, attempt) in tasks {
                self.update(stage, partition, attempt, state, None);
            }
        }

        fn stream_key(&self, stage: usize, partition: usize, attempt: usize) -> TaskStreamKey {
            TaskStreamKey {
                job_id: self.job_id,
                stage,
                partition,
                attempt,
                channel: 0,
            }
        }

        /// Writes a batch to a job output stream that has been registered.
        async fn write(
            &mut self,
            stage: usize,
            partition: usize,
            attempt: usize,
            rows: i32,
        ) -> TestResult<()> {
            let batch = RecordBatch::try_from_iter([(
                "value",
                Arc::new(Int32Array::from_iter_values(0..rows)) as ArrayRef,
            )])?;
            let key = self.stream_key(stage, partition, attempt);
            let stream = self.streams.get(&key).ok_or("missing job output stream")?;
            // The job output may have dropped the stream already.
            let _ = stream.send(Ok(batch)).await;
            Ok(())
        }

        /// Ends a job output stream that has been registered.
        fn end(&mut self, stage: usize, partition: usize, attempt: usize) -> TestResult<()> {
            let key = self.stream_key(stage, partition, attempt);
            self.streams
                .remove(&key)
                .ok_or("missing job output stream")?;
            Ok(())
        }

        /// Reads the number of rows in the next batch that the client receives.
        async fn read(&mut self) -> TestResult<Option<usize>> {
            let batch = tokio::time::timeout(Duration::from_secs(10), self.output.next()).await?;
            Ok(batch.transpose()?.map(|batch| batch.num_rows()))
        }

        fn state(&self) -> &JobState {
            &self.driver.scheduler.jobs[&self.job_id].state
        }
    }

    #[tokio::test]
    async fn region_with_delivered_output_fails_with_task_cause() -> TestResult<()> {
        let mut job = TestJob::try_new(2, Some(2))?;
        assert_eq!(job.refresh().await.scheduled, 1);
        job.update_all(TaskState::Running);
        job.refresh().await;
        job.write(1, 0, 0, 1).await?;
        assert_eq!(job.read().await?, Some(1));

        // An upstream task fails after rows of the region have reached the client.
        job.update(0, 1, 0, TaskState::Failed, Some("upstream failure"));
        let refresh = job.refresh().await;
        assert_eq!(refresh.scheduled, 0);
        assert!(matches!(
            refresh.failure,
            Some(CommonErrorCause::Execution(message)) if message == "upstream failure"
        ));
        assert!(matches!(job.state(), JobState::Failed));
        assert!(matches!(
            job.read().await,
            Err(error) if error.to_string().contains("upstream failure")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn region_without_delivered_output_restarts_and_drains() -> TestResult<()> {
        let mut job = TestJob::try_new(2, Some(2))?;
        assert_eq!(job.refresh().await.scheduled, 1);
        job.update_all(TaskState::Running);
        job.refresh().await;
        // The job output reads the streams of the first attempt. Empty batches are not rows.
        for partition in 0..2 {
            job.write(1, partition, 0, 0).await?;
            assert_eq!(job.read().await?, Some(0));
        }

        job.update(0, 1, 0, TaskState::Failed, Some("upstream failure"));
        let refresh = job.refresh().await;
        assert_eq!(refresh.scheduled, 1);
        assert!(refresh.failure.is_none());
        // The first attempt has been canceled. Its rows must not reach the client,
        // and its output streams are left open.
        job.write(1, 0, 0, 7).await?;

        job.update_all(TaskState::Running);
        job.refresh().await;
        job.write(1, 0, 1, 1).await?;
        job.write(1, 1, 1, 2).await?;
        job.end(1, 0, 1)?;
        job.end(1, 1, 1)?;
        job.update_all(TaskState::Succeeded);
        let refresh = job.refresh().await;
        assert_eq!(refresh.scheduled, 0);
        assert!(refresh.failure.is_none());
        assert!(matches!(job.state(), JobState::Draining));

        let mut rows = vec![];
        while let Some(count) = job.read().await? {
            rows.push(count);
        }
        rows.sort();
        assert_eq!(rows, vec![1, 2]);
        Ok(())
    }

    #[tokio::test]
    async fn job_failure_reports_cause_of_failed_region() -> TestResult<()> {
        // Each partition of a single-stage job is a task region.
        let mut job = TestJob::try_new(2, None)?;
        assert_eq!(job.refresh().await.scheduled, 2);

        // The first region fails once and then succeeds.
        job.update(0, 0, 0, TaskState::Failed, Some("recovered failure"));
        assert_eq!(job.refresh().await.scheduled, 1);
        job.update_all(TaskState::Running);
        job.refresh().await;
        job.end(0, 0, 1)?;
        job.update(0, 0, 1, TaskState::Succeeded, None);
        job.refresh().await;

        // The second region fails after its rows have reached the client.
        job.write(0, 1, 0, 1).await?;
        assert_eq!(job.read().await?, Some(1));
        job.update(0, 1, 0, TaskState::Failed, Some("final failure"));
        let refresh = job.refresh().await;
        assert_eq!(refresh.scheduled, 0);
        assert!(matches!(
            refresh.failure,
            Some(CommonErrorCause::Execution(message)) if message == "final failure"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn region_with_many_output_partitions_restarts_and_drains() -> TestResult<()> {
        let partitions = 40;
        let mut job = TestJob::try_new(1, Some(partitions))?;
        assert_eq!(job.refresh().await.scheduled, 1);
        job.update_all(TaskState::Running);
        job.refresh().await;
        assert_eq!(job.streams.len(), partitions);

        job.update(0, 0, 0, TaskState::Failed, Some("upstream failure"));
        assert_eq!(job.refresh().await.scheduled, 1);
        job.update_all(TaskState::Running);
        job.refresh().await;
        assert_eq!(job.streams.len(), 2 * partitions);
        for partition in 0..partitions {
            job.write(1, partition, 1, 1).await?;
            job.end(1, partition, 1)?;
        }
        job.update_all(TaskState::Succeeded);
        job.refresh().await;
        assert!(matches!(job.state(), JobState::Draining));

        let mut rows = 0;
        while let Some(count) = job.read().await? {
            rows += count;
        }
        assert_eq!(rows, partitions);
        Ok(())
    }
}
