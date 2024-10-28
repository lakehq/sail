use std::collections::{HashMap, VecDeque};
use std::mem;
use std::sync::Arc;

use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use log::error;
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
    pub(super) worker_manager: Arc<dyn WorkerManager>,
    pub(super) worker_clients: HashMap<WorkerId, WorkerClient>,
    pub(super) physical_plan_codec: Box<dyn PhysicalExtensionCodec>,
    pub(super) incoming_jobs: VecDeque<JobId>,
    pub(super) job_subscribers: HashMap<JobId, JobSubscriber>,
}

impl Actor for DriverActor {
    type Message = DriverEvent;
    type Options = DriverOptions;

    fn new(options: DriverOptions) -> Self {
        Self {
            options,
            state: DriverState::new(),
            server: ServerMonitor::new(),
            worker_manager: Arc::new(LocalWorkerManager::new()),
            worker_clients: HashMap::new(),
            physical_plan_codec: Box::new(RemoteExecutionCodec::new(SessionContext::default())),
            incoming_jobs: VecDeque::new(),
            job_subscribers: HashMap::new(),
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
            DriverEvent::StartWorker { worker_id } => self.handle_start_worker(ctx, worker_id),
            DriverEvent::RegisterWorker {
                worker_id,
                host,
                port,
            } => self.handle_register_worker(ctx, worker_id, host, port),
            DriverEvent::StopWorker { worker_id } => self.handle_stop_worker(ctx, worker_id),
            DriverEvent::ExecuteJob { plan, result } => self.handle_execute_job(ctx, plan, result),
            DriverEvent::UpdateTask {
                worker_id,
                task_id,
                status,
                message,
            } => self.handle_update_task(ctx, worker_id, task_id, status, message),
            DriverEvent::Shutdown => ActorAction::Stop,
        }
    }

    fn stop(self) {
        self.server.stop();
        tokio::runtime::Handle::current().block_on(async {
            if let Err(e) = self.worker_manager.stop_all_workers().await {
                error!("encountered error while stopping workers: {e}");
            }
        });
    }
}

impl DriverActor {
    pub(super) fn options(&self) -> &DriverOptions {
        &self.options
    }
}

pub(super) struct JobSubscriber {
    pub result: oneshot::Sender<ExecutionResult<SendableRecordBatchStream>>,
}
