use std::sync::Arc;

use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::ExecutionPlan;
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
use crate::error::ExecutionResult;
use crate::id::{JobId, TaskKey, TaskKeyDisplay, TaskStreamKey};
use crate::job_graph::JobGraph;
use crate::stream::reader::TaskStreamSource;
use crate::task::definition::TaskDefinition;
use crate::task::scheduling::TaskAssignmentGetter;

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
        todo!()
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

    fn encode_plan(&self, plan: Arc<dyn ExecutionPlan>) -> ExecutionResult<Vec<u8>> {
        let plan = PhysicalPlanNode::try_from_physical_plan(plan, self.codec.as_ref())?;
        Ok(plan.encode_to_vec())
    }
}
