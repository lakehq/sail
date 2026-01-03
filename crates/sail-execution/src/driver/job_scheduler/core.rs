use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use log::{debug, warn};
use prost::bytes::BytesMut;
use prost::Message;

use crate::driver::job_scheduler::state::{JobDescriptor, TaskAttemptDescriptor, TaskState};
use crate::driver::job_scheduler::{JobScheduler, JobSchedulerAction};
use crate::driver::output::JobOutputHandle;
use crate::driver::task::TaskAssignmentGetter;
use crate::error::ExecutionResult;
use crate::id::{JobId, TaskKey, TaskKeyDisplay};
use crate::job_graph::JobGraph;
use crate::worker::task::TaskDefinition;

impl JobScheduler {
    fn next_job_id(&mut self) -> ExecutionResult<JobId> {
        self.job_id_generator.next()
    }

    pub fn accept_job(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> ExecutionResult<(JobId, SendableRecordBatchStream)> {
        let job_id = self.next_job_id()?;
        debug!(
            "job {job_id} execution plan\n{}",
            DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
        );
        let schema = plan.schema();
        let graph = JobGraph::try_new(plan)?;
        debug!("job {job_id} job graph \n{graph}");
        let (handle, stream) = JobOutputHandle::create(schema);
        let job = JobDescriptor::try_new(graph, handle)?;
        self.jobs.insert(job_id, job);
        Ok((job_id, Box::pin(stream)))
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

    pub fn schedule_job(&mut self, job_id: JobId) -> Vec<JobSchedulerAction> {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            warn!("job {job_id} not found");
            return vec![];
        };
        todo!()
    }

    pub fn cancel_job(&mut self, job_id: JobId) -> Vec<JobSchedulerAction> {
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
    ) -> ExecutionResult<TaskDefinition> {
        todo!()
    }

    fn encode_plan(&self, plan: Arc<dyn ExecutionPlan>) -> ExecutionResult<Vec<u8>> {
        let plan =
            PhysicalPlanNode::try_from_physical_plan(plan, self.physical_plan_codec.as_ref())?;
        let mut buffer = BytesMut::new();
        plan.encode(&mut buffer)?;
        Ok(buffer.freeze().into())
    }
}
