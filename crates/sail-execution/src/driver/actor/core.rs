use std::collections::{HashMap, VecDeque};
use std::mem;

use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use sail_server::actor::{Actor, ActorAction, ActorContext};
use tokio::sync::oneshot;

use crate::codec::RemoteExecutionCodec;
use crate::driver::state::DriverState;
use crate::driver::{DriverEvent, DriverOptions};
use crate::error::ExecutionResult;
use crate::id::{JobId, WorkerId};
use crate::rpc::ServerMonitor;
use crate::worker::WorkerClient;
use crate::worker_manager::{LocalWorkerManager, WorkerManager};

pub struct DriverActor {
    options: DriverOptions,
    pub(super) state: DriverState,
    pub(super) server: ServerMonitor,
    pub(super) worker_manager: Box<dyn WorkerManager>,
    pub(super) worker_clients: HashMap<WorkerId, WorkerClient>,
    pub(super) physical_plan_codec: Box<dyn PhysicalExtensionCodec>,
    pub(super) incoming_job_queue: VecDeque<(
        JobId,
        oneshot::Sender<ExecutionResult<SendableRecordBatchStream>>,
    )>,
    pub(super) pending_jobs:
        HashMap<JobId, oneshot::Sender<ExecutionResult<SendableRecordBatchStream>>>,
}

impl Actor for DriverActor {
    type Message = DriverEvent;
    type Options = DriverOptions;

    fn new(options: DriverOptions) -> Self {
        let worker_manager = Box::new(LocalWorkerManager::new());
        Self {
            options,
            state: DriverState::new(),
            server: ServerMonitor::new(),
            worker_manager,
            worker_clients: HashMap::new(),
            physical_plan_codec: Box::new(RemoteExecutionCodec::new(SessionContext::default())),
            incoming_job_queue: VecDeque::new(),
            pending_jobs: HashMap::new(),
        }
    }

    fn start(&mut self, ctx: &mut ActorContext<Self>) {
        let addr = (
            self.options().driver_listen_host.clone(),
            self.options().driver_listen_port,
        );
        let server = mem::take(&mut self.server);
        self.server = server.start(Self::serve(ctx.handle().clone(), addr));
    }

    fn receive(&mut self, ctx: &mut ActorContext<Self>, message: DriverEvent) -> ActorAction {
        match message {
            DriverEvent::ServerReady { port, signal } => {
                self.handle_server_ready(ctx, port, signal)
            }
            DriverEvent::RegisterWorker {
                worker_id,
                host,
                port,
            } => self.handle_register_worker(ctx, worker_id, host, port),
            DriverEvent::ExecuteJob { plan, result } => self.handle_execute_job(ctx, plan, result),
            DriverEvent::TaskUpdated {
                worker_id,
                task_id,
                status,
            } => self.handle_task_updated(ctx, worker_id, task_id, status),
            DriverEvent::Shutdown => ActorAction::Stop,
        }
    }

    fn stop(self) {
        self.server.stop();
    }
}

impl DriverActor {
    pub(super) fn options(&self) -> &DriverOptions {
        &self.options
    }
}
