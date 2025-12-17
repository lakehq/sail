use std::collections::{HashMap, VecDeque};
use std::mem;
use std::sync::Arc;

use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use fastrace::future::FutureExt;
use fastrace::Span;
use log::{error, info};
use sail_server::actor::{Actor, ActorAction, ActorContext};

use crate::codec::RemoteExecutionCodec;
use crate::driver::actor::output::JobOutput;
use crate::driver::state::DriverState;
use crate::driver::{DriverEvent, DriverOptions, WorkerManagerOptions};
use crate::error::ExecutionResult;
use crate::id::{JobId, TaskId, WorkerId};
use crate::rpc::ServerMonitor;
use crate::worker::WorkerClient;
use crate::worker_manager::{KubernetesWorkerManager, LocalWorkerManager, WorkerManager};

pub struct DriverActor {
    options: DriverOptions,
    pub(super) state: DriverState,
    pub(super) server: ServerMonitor,
    pub(super) worker_manager: Arc<dyn WorkerManager>,
    worker_clients: HashMap<WorkerId, WorkerClient>,
    pub(super) physical_plan_codec: Box<dyn PhysicalExtensionCodec>,
    /// The queue of tasks that need to be scheduled.
    /// A task is enqueued after all its dependencies in the previous job stage.
    pub(super) task_queue: VecDeque<TaskId>,
    /// The sequence number corresponding to the last task status update from the worker.
    pub(super) task_sequences: HashMap<TaskId, u64>,
    pub(super) job_outputs: HashMap<JobId, JobOutput>,
}

#[tonic::async_trait]
impl Actor for DriverActor {
    type Message = DriverEvent;
    type Options = DriverOptions;

    fn name() -> &'static str {
        "DriverActor"
    }

    fn new(options: DriverOptions) -> Self {
        let worker_manager: Arc<dyn WorkerManager> = match &options.worker_manager {
            WorkerManagerOptions::Local => {
                Arc::new(LocalWorkerManager::new(options.runtime.clone()))
            }
            WorkerManagerOptions::Kubernetes(options) => {
                Arc::new(KubernetesWorkerManager::new(options.clone()))
            }
        };
        Self {
            options,
            state: DriverState::new(),
            server: ServerMonitor::new(),
            worker_manager,
            worker_clients: HashMap::new(),
            physical_plan_codec: Box::new(RemoteExecutionCodec),
            task_queue: VecDeque::new(),
            task_sequences: HashMap::new(),
            job_outputs: HashMap::new(),
        }
    }

    async fn start(&mut self, ctx: &mut ActorContext<Self>) {
        let addr = (
            self.options().driver_listen_host.clone(),
            self.options().driver_listen_port,
        );
        let server = mem::take(&mut self.server);
        let span = Span::enter_with_local_parent("DriverActor::serve");
        self.server = server
            .start(Self::serve(ctx.handle().clone(), addr).in_span(span))
            .await;
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
                result,
            } => self.handle_register_worker(ctx, worker_id, host, port, result),
            DriverEvent::WorkerHeartbeat { worker_id } => {
                self.handle_worker_heartbeat(ctx, worker_id)
            }
            DriverEvent::ProbePendingWorker { worker_id } => {
                self.handle_probe_pending_worker(ctx, worker_id)
            }
            DriverEvent::ProbeIdleWorker { worker_id, instant } => {
                self.handle_probe_idle_worker(ctx, worker_id, instant)
            }
            DriverEvent::ProbeLostWorker { worker_id, instant } => {
                self.handle_probe_lost_worker(ctx, worker_id, instant)
            }
            DriverEvent::ExecuteJob { plan, result } => self.handle_execute_job(ctx, plan, result),
            DriverEvent::CleanUpJob { job_id } => self.handle_clean_up_job(ctx, job_id),
            DriverEvent::UpdateTask {
                task_id,
                attempt,
                status,
                message,
                cause,
                sequence,
            } => self.handle_update_task(ctx, task_id, attempt, status, message, cause, sequence),
            DriverEvent::ProbePendingTask { task_id, attempt } => {
                self.handle_probe_pending_task(ctx, task_id, attempt)
            }
            DriverEvent::Shutdown => ActorAction::Stop,
        }
    }

    async fn stop(mut self, ctx: &mut ActorContext<Self>) {
        self.stop_all_workers(ctx);
        info!("stopping driver server");
        self.server.stop().await;
        info!("driver server has stopped");
        // TODO: support timeout for worker manager stop
        if let Err(e) = self.worker_manager.stop().await {
            error!("encountered error while stopping workers: {e}");
        }
    }
}

impl DriverActor {
    pub(super) fn options(&self) -> &DriverOptions {
        &self.options
    }

    pub(super) fn worker_client(&mut self, id: WorkerId) -> ExecutionResult<WorkerClient> {
        let options = self.worker_client_options(id)?;
        let client = self
            .worker_clients
            .entry(id)
            .or_insert_with(|| WorkerClient::new(options));
        Ok(client.clone())
    }
}
