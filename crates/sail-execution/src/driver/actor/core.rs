use std::collections::HashMap;
use std::mem;
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use log::{debug, error};
use sail_server::actor::{Actor, ActorAction, ActorContext};

use crate::codec::RemoteExecutionCodec;
use crate::driver::actor::output::JobOutput;
use crate::driver::state::DriverState;
use crate::driver::{DriverEvent, DriverOptions};
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
    pub(super) job_outputs: HashMap<JobId, JobOutput>,
}

#[tonic::async_trait]
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
            job_outputs: HashMap::new(),
        }
    }

    async fn start(&mut self, ctx: &mut ActorContext<Self>) {
        let addr = (
            self.options().driver_listen_host.clone(),
            self.options().driver_listen_port,
        );
        let server = mem::take(&mut self.server);
        self.server = server.start(Self::serve(ctx.handle().clone(), addr)).await;
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
            DriverEvent::RemoveJobOutput { job_id } => self.handle_remove_job_output(ctx, job_id),
            DriverEvent::UpdateTask {
                task_id,
                attempt,
                status,
                message,
                sequence,
            } => self.handle_update_task(ctx, task_id, attempt, status, message, sequence),
            DriverEvent::Shutdown => ActorAction::Stop,
        }
    }

    async fn stop(self) {
        self.server.stop().await;
        debug!("driver server has stopped");
        if let Err(e) = self.worker_manager.stop_all_workers().await {
            error!("encountered error while stopping workers: {e}");
        }
    }
}

impl DriverActor {
    pub(super) fn options(&self) -> &DriverOptions {
        &self.options
    }
}
