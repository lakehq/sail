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
use crate::task::scheduling::{TaskAssignment, TaskAssignmentGetter};

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

    pub fn refresh_job(&mut self, job_id: JobId) -> Vec<JobAction> {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            warn!("job {job_id} not found");
            return vec![];
        };
        todo!()
    }

    pub fn cancel_job(&mut self, job_id: JobId) -> Vec<JobAction> {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            warn!("job {job_id} not found");
            return vec![];
        };
        let reason = format!("task canceled for job {job_id}");
        todo!()
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
                let keys = match input.mode {
                    InputMode::Forward => {
                        let channels = stage.distribution.channels();
                        (0..channels)
                            .map(|channel| {
                                Ok(TaskInputKey {
                                    partition: key.partition,
                                    attempt: latest_attempt(input.stage, key.partition)?,
                                    channel,
                                })
                            })
                            .collect::<ExecutionResult<Vec<_>>>()?
                    }
                    InputMode::Shuffle => {
                        let partitions = stage.plan.output_partitioning().partition_count();
                        (0..partitions)
                            .map(|partition| {
                                Ok(TaskInputKey {
                                    partition,
                                    attempt: latest_attempt(input.stage, partition)?,
                                    channel: key.partition,
                                })
                            })
                            .collect::<ExecutionResult<Vec<_>>>()?
                    }
                    InputMode::Broadcast => {
                        let channels = stage.distribution.channels();
                        let partitions = stage.plan.output_partitioning().partition_count();
                        (0..partitions)
                            .flat_map(|partition| {
                                (0..channels).map(move |channel| {
                                    Ok(TaskInputKey {
                                        partition,
                                        attempt: latest_attempt(input.stage, partition)?,
                                        channel,
                                    })
                                })
                            })
                            .collect::<ExecutionResult<Vec<_>>>()?
                    }
                };
                let locator = match stage.mode {
                    OutputMode::Pipelined => match stage.placement {
                        TaskPlacement::Driver => {
                            keys.iter().try_for_each(|k| {
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
                            let keys = keys.into_iter().map(|k| {
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
                            }).collect::<ExecutionResult<Vec<_>>>()?;
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
            _ => {
                warn!(
                    "cannot add output stream to job {} that is not running",
                    key.job_id
                );
            }
        }
    }
}
