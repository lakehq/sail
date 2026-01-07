use std::collections::{HashMap, HashSet, VecDeque};
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
use crate::driver::job_scheduler::{JobAction, JobScheduler};
use crate::driver::output::{build_job_output, JobOutputCommand};
use crate::driver::DriverActor;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskKey, TaskKeyDisplay, TaskStreamKey};
use crate::job_graph::{InputMode, JobGraph, OutputDistribution, OutputMode, TaskPlacement};
use crate::stream::reader::TaskStreamSource;
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
        let descriptor = JobDescriptor::new(graph, JobState::Running { output, context });
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
        let topology = {
            let Some(job) = self.jobs.get(&job_id) else {
                warn!("job {job_id} not found");
                return vec![];
            };
            if job.state.is_terminal() {
                return vec![];
            }
            JobTopology::new(&job.graph)
        };

        let Some(job) = self.jobs.get_mut(&job_id) else {
            return vec![];
        };

        const MAX_ATTEMPTS: usize = 3;
        let mut actions = vec![];
        let mut job_failure: Option<String> = None;

        // 1. Check for failures and cancel regions.
        for region in &topology.regions {
            let mut region_failed = false;
            let mut failure_reason = None;

            for &(stage_idx, partition_idx) in &region.tasks {
                let attempts = &job.stages[stage_idx].tasks[partition_idx].attempts;
                if let Some(attempt) = attempts.last() {
                    match attempt.state {
                        TaskState::Failed => {
                            region_failed = true;
                            failure_reason = attempt.messages.first().cloned();
                        }
                        _ => {}
                    }
                }
            }

            if region_failed && job_failure.is_none() {
                // Cancel all other tasks in this region
                for &(stage_idx, partition_idx) in &region.tasks {
                    let task = &job.stages[stage_idx].tasks[partition_idx];
                    for (attempt_idx, attempt) in task.attempts.iter().enumerate() {
                        if !attempt.state.is_terminal() {
                            actions.push(JobAction::CancelTask {
                                key: TaskKey {
                                    job_id,
                                    stage: stage_idx,
                                    partition: partition_idx,
                                    attempt: attempt_idx,
                                },
                            });
                        }
                    }
                }
            }
        }

        if let Some(_reason) = job_failure {
            let mut cancel_actions = vec![];
            for (s, stage) in job.stages.iter().enumerate() {
                for (t, task) in stage.tasks.iter().enumerate() {
                    for (a, attempt) in task.attempts.iter().enumerate() {
                        if !attempt.state.is_terminal() {
                            cancel_actions.push(JobAction::CancelTask {
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
            cancel_actions.push(JobAction::RemoveStreams {
                job_id,
                stage: None,
            });
            job.state = JobState::Failed;
            return cancel_actions;
        }

        // 4. Job Output Streams from Final Stage
        // Note: This step requires mutable access to job.stages (to update stream_started),
        // so it must run before we borrow job.stages immutably for other checks.
        // We explicitly split the borrow on job to avoid conflicts.
        let final_stage_indices = topology.final_stages.clone();
        let graph = &job.graph;
        let stages = &mut job.stages;

        let step4_info: Vec<_> = final_stage_indices
            .iter()
            .map(|&idx| {
                let s = &graph.stages()[idx];
                (
                    idx,
                    s.plan.output_partitioning().partition_count(),
                    s.distribution.channels(),
                )
            })
            .collect();
        let schema = graph.schema().clone();

        for (stage_idx, partitions, channels) in step4_info {
            let mut keys = vec![];
            for p in 0..partitions {
                if let Some(task) = stages[stage_idx].tasks[p].attempts.last_mut() {
                    if matches!(task.state, TaskState::Running | TaskState::Succeeded)
                        && !task.stream_started
                    {
                        task.stream_started = true;
                        let attempt = stages[stage_idx].tasks[p].attempts.len() - 1;
                        for c in 0..channels {
                            keys.push(TaskStreamKey {
                                job_id,
                                stage: stage_idx,
                                partition: p,
                                attempt,
                                channel: c,
                            });
                        }
                    }
                }
            }
            if !keys.is_empty() {
                actions.push(JobAction::FetchJobOutputStreams {
                    keys,
                    schema: schema.clone(),
                });
            }
        }

        // Scope for immutable borrow of job.stages
        let (all_final_succeeded, region_success) = {
            let stages = &job.stages;
            let is_task_succeeded = |stage: usize, partition: usize| -> bool {
                let task = &stages[stage].tasks[partition];
                task.attempts
                    .last()
                    .map_or(false, |a| matches!(a.state, TaskState::Succeeded))
            };

            // 3. Check Job Success (Final stage succeeded)
            let mut all_succeeded = true;
            for &stage_idx in &final_stage_indices {
                let partitions = job.graph.stages()[stage_idx]
                    .plan
                    .output_partitioning()
                    .partition_count();
                for p in 0..partitions {
                    if !is_task_succeeded(stage_idx, p) {
                        all_succeeded = false;
                        break;
                    }
                }
            }

            // 6. Garbage Collect Streams
            for (stage_idx, consumers) in &topology.stage_consumers {
                let all_consumers_succeeded = consumers.iter().all(|&c_idx| {
                    // Check if stage c_idx succeeded
                    let partitions = job.graph.stages()[c_idx]
                        .plan
                        .output_partitioning()
                        .partition_count();
                    (0..partitions).all(|p| is_task_succeeded(c_idx, p))
                });

                if all_consumers_succeeded && !consumers.is_empty() {
                    actions.push(JobAction::RemoveStreams {
                        job_id,
                        stage: Some(*stage_idx),
                    });
                }
            }

            // Pre-compute region success
            let r_success: Vec<bool> = (0..topology.regions.len())
                .map(|r_idx| {
                    let r = &topology.regions[r_idx];
                    r.tasks.iter().all(|(s, p)| is_task_succeeded(*s, *p))
                })
                .collect();
            (all_succeeded, r_success)
        };

        if all_final_succeeded {
            let (output, context) = match &job.state {
                JobState::Running { output, context } => (output.clone(), context.clone()),
                _ => unreachable!("job must be running"),
            };
            job.state = JobState::Succeeded {
                output: Some(output),
                context,
            };
            actions.push(JobAction::CompleteJobOutput { job_id });
            return actions;
        }

        // 5. Schedule Regions
        for (r_idx, region) in topology.regions.iter().enumerate() {
            // Check dependencies using pre-computed success status
            let deps = &topology.dependencies[r_idx];
            let deps_met = deps.iter().all(|&d| region_success[d]);

            if deps_met {
                // Check if region needs scheduling
                // The region needs scheduling if it is not *already* being scheduled or running.
                // We determine this by checking the state of its tasks.

                let mut active_run = false;
                let mut all_succeeded = true;
                let mut has_attempts = false;

                for &(s, p) in &region.tasks {
                    let task = &job.stages[s].tasks[p];
                    if let Some(attempt) = task.attempts.last() {
                        has_attempts = true;
                        if !matches!(attempt.state, TaskState::Succeeded) {
                            all_succeeded = false;
                        }
                        if !attempt.state.is_terminal()
                            && !matches!(attempt.state, TaskState::Created)
                        {
                            active_run = true;
                        }
                    } else {
                        all_succeeded = false;
                    }
                }

                if active_run || (has_attempts && all_succeeded) {
                    continue;
                }

                let mut all_created = true;
                let mut task_attempts: HashMap<(usize, usize), usize> = HashMap::new();

                for &(s, p) in &region.tasks {
                    let task_desc = &mut job.stages[s].tasks[p];

                    // Logic to ensure non-terminal state "Created" if currently terminal
                    let need_new_attempt = match task_desc.attempts.last() {
                        None => true,
                        Some(a) if a.state.is_terminal() => true,
                        _ => false,
                    };

                    if need_new_attempt {
                        if task_desc.attempts.len() >= MAX_ATTEMPTS {
                            job_failure = Some("Max attempts exceeded".to_string());
                            break;
                        }
                        task_desc.attempts.push(TaskAttemptDescriptor {
                            state: TaskState::Created,
                            messages: vec![],
                            stream_started: false,
                        });
                    }

                    // Now check state of the latest attempt
                    let latest_idx = task_desc.attempts.len() - 1;
                    let latest_attempt = &task_desc.attempts[latest_idx];

                    if !matches!(latest_attempt.state, TaskState::Created) {
                        all_created = false;
                    }

                    task_attempts.insert((s, p), latest_idx);
                }

                if job_failure.is_some() {
                    break;
                }

                if all_created {
                    // Update state to Pending for all tasks in the region
                    for &(s, p) in &region.tasks {
                        let task_desc = &mut job.stages[s].tasks[p];
                        if let Some(last) = task_desc.attempts.last_mut() {
                            last.state = TaskState::Pending;
                        }
                    }

                    // Proceed with scheduling (bucket logic)
                    let mut stages_by_group: HashMap<(TaskPlacement, String), HashSet<usize>> =
                        HashMap::new();
                    for &(s, _) in &region.tasks {
                        let stage = &job.graph.stages()[s];
                        let key = (stage.placement, stage.group.clone());
                        stages_by_group.entry(key).or_default().insert(s);
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

                    for &(s, p) in &region.tasks {
                        if let Some(&attempt) = task_attempts.get(&(s, p)) {
                            let stage = &job.graph.stages()[s];
                            let key = (stage.placement, stage.group.clone());
                            if let Some((max_partitions, buckets)) = buckets_by_group.get_mut(&key)
                            {
                                let p_count = stage.plan.output_partitioning().partition_count();
                                let bucket_idx = p * *max_partitions / p_count;
                                if bucket_idx < buckets.len() {
                                    buckets[bucket_idx].push(TaskSetEntry {
                                        key: TaskKey {
                                            job_id,
                                            stage: s,
                                            partition: p,
                                            attempt,
                                        },
                                        output: TaskOutputKind::Local,
                                    });
                                }
                            }
                        }
                    }

                    let mut task_sets: Vec<(TaskPlacement, TaskSet)> = vec![];
                    for ((placement, _), (_, buckets)) in buckets_by_group {
                        for entries in buckets {
                            if !entries.is_empty() {
                                task_sets.push((placement, TaskSet { entries }));
                            }
                        }
                    }

                    actions.push(JobAction::ScheduleTasks {
                        region: TaskRegion { tasks: task_sets },
                    });
                }
            }
        }

        actions
    }

    /// Determine the actions needed in the driver to cancel the job.
    /// The method cancels all the task attempts that are not in terminal states
    /// and removes all the job output streams.
    pub fn cancel_job(&mut self, job_id: JobId) -> Vec<JobAction> {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            warn!("job {job_id} not found");
            return vec![];
        };
        let mut actions = vec![];
        for (s, stage) in job.stages.iter().enumerate() {
            for (t, task) in stage.tasks.iter().enumerate() {
                for (a, attempt) in task.attempts.iter().enumerate() {
                    if !attempt.state.is_terminal() {}
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
        actions.push(JobAction::RemoveStreams {
            job_id,
            stage: None,
        });
        job.state = JobState::Canceled;
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
            self.get_latest_task_attempt(key.job_id, stage, partition)
                .ok_or_else(|| {
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
                    InputMode::Shuffle => {
                        (0..channels).map(|channel| {
                            (0..partitions)
                                .map(|partition| {
                                    Ok(TaskInputKey {
                                        partition,
                                        attempt: latest_attempt(input.stage, partition)?,
                                        channel,
                                    })
                                })
                                .collect::<ExecutionResult<Vec<_>>>()
                        }).collect::<ExecutionResult<Vec<Vec<_>>>>()?
                    }
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
        &self,
        job_id: JobId,
        stage: usize,
        partition: usize,
    ) -> Option<usize> {
        let descriptor = self
            .jobs
            .get(&job_id)
            .and_then(|job| job.stages.get(stage))
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

    pub fn add_job_output_stream(
        &mut self,
        ctx: &mut ActorContext<DriverActor>,
        key: TaskStreamKey,
        stream: TaskStreamSource,
    ) {
        let Some(job) = self.jobs.get_mut(&key.job_id) else {
            warn!("job {} not found", key.job_id);
            return;
        };
        match &mut job.state {
            JobState::Running { output, .. } => {
                let output = output.clone();
                ctx.spawn(async move {
                    // We ignore the error here because it indicates that the job output
                    // consumer has been dropped.
                    let _ = output
                        .send(JobOutputCommand::AddStream { key, stream })
                        .await;
                });
            }
            JobState::Succeeded { output, .. } => {
                if let Some(output) = output {
                    let output = output.clone();
                    ctx.spawn(async move {
                        // We ignore the error here because it indicates that the job output
                        // consumer has been dropped.
                        let _ = output
                            .send(JobOutputCommand::AddStream { key, stream })
                            .await;
                    });
                } else {
                    warn!(
                        "cannot add output stream to succeeded and closed job {}",
                        key.job_id
                    );
                }
            }
            _ => {
                warn!(
                    "cannot add output stream to job {} that is not running",
                    key.job_id
                );
            }
        }
    }

    pub fn complete_job_output(&mut self, job_id: JobId) {
        if let Some(job) = self.jobs.get_mut(&job_id) {
            if let JobState::Succeeded { output, .. } = &mut job.state {
                *output = None;
            }
        }
    }
}

#[derive(Debug)]
struct JobTopology {
    regions: Vec<LogicalRegion>,
    dependencies: Vec<HashSet<usize>>, // region_idx -> set of dependency region_indices
    final_stages: Vec<usize>,
    stage_consumers: HashMap<usize, Vec<usize>>, // stage -> list of stages that consume it
}

#[derive(Debug)]
struct LogicalRegion {
    tasks: Vec<(usize, usize)>,
}

impl JobTopology {
    fn new(graph: &JobGraph) -> Self {
        let n_stages = graph.stages().len();
        let mut adj_pipelined: HashMap<usize, Vec<usize>> = HashMap::new();
        let mut stage_consumers: HashMap<usize, Vec<usize>> = HashMap::new();

        for (i, stage) in graph.stages().iter().enumerate() {
            for input in &stage.inputs {
                stage_consumers.entry(input.stage).or_default().push(i);

                let source_stage = &graph.stages()[input.stage];
                if let OutputMode::Pipelined = source_stage.mode {
                    adj_pipelined.entry(i).or_default().push(input.stage);
                    adj_pipelined.entry(input.stage).or_default().push(i);
                }
            }
        }

        // Final stages are those that are not consumed by any other stage
        let final_stages: Vec<usize> = (0..n_stages)
            .filter(|i| !stage_consumers.contains_key(i))
            .collect();

        // 1. Find Pipelined Components
        let mut visited = vec![false; n_stages];
        let mut components = vec![];
        for i in 0..n_stages {
            if !visited[i] {
                let mut component = vec![];
                let mut queue = VecDeque::new();
                queue.push_back(i);
                visited[i] = true;
                while let Some(u) = queue.pop_front() {
                    component.push(u);
                    if let Some(neighbors) = adj_pipelined.get(&u) {
                        for &v in neighbors {
                            if !visited[v] {
                                visited[v] = true;
                                queue.push_back(v);
                            }
                        }
                    }
                }
                components.push(component);
            }
        }

        // 2. Generate Logical Regions
        let mut regions = vec![];

        for comp in components {
            // Check if all edges within component are Forward
            let mut all_forward = true;
            for &u in &comp {
                for input in &graph.stages()[u].inputs {
                    if comp.contains(&input.stage) {
                        if !matches!(input.mode, InputMode::Forward) {
                            all_forward = false;
                            break;
                        }
                    }
                }
                if !all_forward {
                    break;
                }
            }

            if all_forward {
                // Way 2: Sliced by partition
                let partitions = graph.stages()[comp[0]]
                    .plan
                    .output_partitioning()
                    .partition_count();
                for p in 0..partitions {
                    let mut task_list = vec![];
                    for &s in &comp {
                        task_list.push((s, p));
                    }
                    regions.push(LogicalRegion { tasks: task_list });
                }
            } else {
                // Way 1: All partitions
                let mut task_list = vec![];
                for &s in &comp {
                    let partitions = graph.stages()[s]
                        .plan
                        .output_partitioning()
                        .partition_count();
                    for p in 0..partitions {
                        task_list.push((s, p));
                    }
                }
                regions.push(LogicalRegion { tasks: task_list });
            }
        }

        // 3. Build Region Dependencies
        let mut dependencies = vec![HashSet::new(); regions.len()];

        // Map (stage, partition) to region_idx for quick lookup
        let mut task_to_region = HashMap::new();
        for (r_idx, r) in regions.iter().enumerate() {
            for &t in &r.tasks {
                task_to_region.insert(t, r_idx);
            }
        }

        for (r_idx, r) in regions.iter().enumerate() {
            for &(s, p) in &r.tasks {
                // Check inputs of stage s
                for input in &graph.stages()[s].inputs {
                    let input_partitions = graph.stages()[input.stage]
                        .plan
                        .output_partitioning()
                        .partition_count();
                    for ip in 0..input_partitions {
                        if let Some(&dep_r_idx) = task_to_region.get(&(input.stage, ip)) {
                            if dep_r_idx != r_idx {
                                dependencies[r_idx].insert(dep_r_idx);
                            }
                        }
                    }
                }
            }
        }

        Self {
            regions,
            dependencies,
            final_stages,
            stage_consumers,
        }
    }
}
