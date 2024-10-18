use std::collections::HashMap;

use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use log::error;
use sail_server::actor::{Actor, ActorAction, ActorHandle};

use crate::codec::RemoteExecutionCodec;
use crate::driver::event::TaskChannel;
use crate::driver::state::DriverState;
use crate::driver::{DriverEvent, DriverOptions};
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{IdGenerator, JobId, TaskId, WorkerId};
use crate::rpc::ServerMonitor;
use crate::worker::WorkerClient;
use crate::worker_manager::{LocalWorkerManager, WorkerManager};

pub struct DriverActor {
    options: DriverOptions,
    pub(super) state: DriverState,
    pub(super) server: ServerMonitor,
    pub(super) server_listen_port: Option<u16>,
    pub(super) job_id_generator: IdGenerator<JobId>,
    pub(super) task_id_generator: IdGenerator<TaskId>,
    pub(super) worker_id_generator: IdGenerator<WorkerId>,
    pub(super) worker_manager: Box<dyn WorkerManager>,
    pub(super) worker_client_cache: HashMap<WorkerId, WorkerClient>,
    pub(super) physical_plan_codec: Box<dyn PhysicalExtensionCodec>,
    pub(super) task_channels: HashMap<TaskId, TaskChannel>,
}

impl Actor for DriverActor {
    type Message = DriverEvent;
    type Options = DriverOptions;
    type Error = ExecutionError;

    fn new(options: DriverOptions) -> Self {
        let worker_manager = Box::new(LocalWorkerManager::new());
        Self {
            options,
            state: DriverState::new(),
            server: ServerMonitor::idle(),
            server_listen_port: None,
            job_id_generator: IdGenerator::new(),
            task_id_generator: IdGenerator::new(),
            worker_id_generator: IdGenerator::new(),
            worker_manager,
            worker_client_cache: HashMap::new(),
            physical_plan_codec: Box::new(RemoteExecutionCodec::new()),
            task_channels: HashMap::new(),
        }
    }

    fn start(&mut self, handle: &ActorHandle<Self>) -> ExecutionResult<()> {
        self.start_server(handle)
    }

    fn receive(
        &mut self,
        message: DriverEvent,
        _handle: &ActorHandle<Self>,
    ) -> ExecutionResult<ActorAction> {
        let action = match &message {
            DriverEvent::Shutdown => ActorAction::Stop,
            _ => ActorAction::Continue,
        };
        let out = match message {
            DriverEvent::ServerReady { port, signal } => self.handle_server_ready(port, signal),
            DriverEvent::RegisterWorker {
                worker_id,
                host,
                port,
            } => self.handle_register_worker(worker_id, host, port),
            DriverEvent::ExecuteJob { job, channel } => self.handle_execute_job(job, channel),
            DriverEvent::TaskUpdated {
                task_id,
                partition,
                status,
            } => self.handle_task_updated(task_id, partition, status),
            DriverEvent::Shutdown => Ok(()),
        };
        if let Err(e) = out {
            error!("error processing driver event: {e}");
        }
        Ok(action)
    }

    fn stop(mut self) -> ExecutionResult<()> {
        self.stop_server()
    }
}

impl DriverActor {
    pub(super) fn options(&self) -> &DriverOptions {
        &self.options
    }
}
