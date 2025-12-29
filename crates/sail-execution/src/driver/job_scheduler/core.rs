use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use log::{debug, warn};
use prost::bytes::BytesMut;
use prost::Message;
use sail_server::actor::ActorContext;

use crate::driver::job_scheduler::state::{JobDescriptor, TaskAttemptDescriptor, TaskState};
use crate::driver::job_scheduler::{JobScheduler, JobSchedulerAction};
use crate::driver::output::JobOutputHandle;
use crate::driver::DriverActor;
use crate::error::ExecutionResult;
use crate::id::{JobId, TaskKey, TaskKeyDisplay, WorkerId};
use crate::job_graph::JobGraph;
use crate::worker::task::TaskDefinition;

impl JobScheduler {
    fn next_job_id(&mut self) -> ExecutionResult<JobId> {
        self.job_id_generator.next()
    }

    pub fn accept_job(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> ExecutionResult<SendableRecordBatchStream> {
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
        self.job_queue.push_back(job_id);
        Ok(Box::pin(stream))
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

    pub fn record_pending_task(&mut self, key: &TaskKey, message: Option<String>) {
        let Some(descriptor) = self.get_task_attempt_mut(key) else {
            return;
        };
        if let Some(state) = descriptor.state.pending() {
            descriptor.state = state;
            descriptor.messages.extend(message);
        } else {
            warn!(
                "{} cannot be updated to the pending state from its current state",
                TaskKeyDisplay(&key)
            );
        }
    }

    pub fn record_running_task(&mut self, key: &TaskKey, message: Option<String>) {
        let Some(descriptor) = self.get_task_attempt_mut(key) else {
            return;
        };
        if let Some(state) = descriptor.state.run() {
            descriptor.state = state;
            descriptor.messages.extend(message);
        } else {
            warn!(
                "{} cannot be updated to the running state from its current state",
                TaskKeyDisplay(&key)
            );
        }
    }

    pub fn record_succeeded_task(&mut self, key: &TaskKey, message: Option<String>) {
        let Some(descriptor) = self.get_task_attempt_mut(key) else {
            return;
        };
        if let Some(state) = descriptor.state.succeed() {
            descriptor.state = state;
            descriptor.messages.extend(message);
        } else {
            warn!(
                "{} cannot be updated to the succeeded state from its current state",
                TaskKeyDisplay(&key)
            );
        }
    }

    pub fn record_failed_task(&mut self, key: &TaskKey, message: Option<String>) {
        let Some(descriptor) = self.get_task_attempt_mut(key) else {
            return;
        };
        descriptor.state = TaskState::Failed;
        descriptor.messages.extend(message);
    }

    pub fn record_canceled_task(&mut self, key: &TaskKey, message: Option<String>) {
        let Some(descriptor) = self.get_task_attempt_mut(key) else {
            return;
        };
        descriptor.state = TaskState::Canceled;
        descriptor.messages.extend(message);
    }

    pub fn next(&mut self) -> Vec<JobSchedulerAction> {
        todo!()
    }

    pub fn run_tasks(
        &mut self,
        _ctx: &mut ActorContext<DriverActor>,
        slots: Vec<(WorkerId, usize)>,
    ) -> Vec<(WorkerId, TaskKey, TaskDefinition)> {
        let mut _assigner = TaskSlotAssigner::new(slots);
        todo!()
    }

    pub fn is_pending_task(&self, key: &TaskKey) -> bool {
        self.get_task_attempt(key)
            .is_some_and(|x| matches!(x.state, TaskState::Pending))
    }

    pub fn cancel_job(&mut self, job_id: JobId) -> Vec<TaskKey> {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            warn!("job {job_id} not found");
            return vec![];
        };
        let reason = format!("task canceled for job {job_id}");
        // The tasks are canceled, but they may remain in the task queue.
        // This is OK, since they will be removed when the task scheduling logic runs next time.
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
