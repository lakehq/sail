use std::collections::HashMap;
use std::mem;

use fastrace::future::FutureExt;
use fastrace::Span;
use log::{error, info};
use sail_server::actor::{Actor, ActorAction, ActorContext};

use crate::driver::job_scheduler::{JobScheduler, JobSchedulerOptions};
use crate::driver::task_assigner::{TaskAssigner, TaskAssignerOptions};
use crate::driver::worker_pool::{WorkerPool, WorkerPoolOptions};
use crate::driver::{DriverActor, DriverEvent, DriverOptions};
use crate::rpc::ServerMonitor;
use crate::stream_manager::{StreamManager, StreamManagerOptions};
use crate::task_runner::TaskRunner;

#[tonic::async_trait]
impl Actor for DriverActor {
    type Message = DriverEvent;
    type Options = DriverOptions;

    fn name() -> &'static str {
        "DriverActor"
    }

    fn new(options: DriverOptions) -> Self {
        let worker_pool = WorkerPool::new(
            options.worker_manager.clone(),
            WorkerPoolOptions::from(&options),
        );
        let job_scheduler = JobScheduler::new(JobSchedulerOptions::from(&options));
        let task_assigner = TaskAssigner::new(TaskAssignerOptions::from(&options));
        let stream_manager = StreamManager::new(StreamManagerOptions::from(&options));
        Self {
            options,
            server: ServerMonitor::new(),
            worker_pool,
            job_scheduler,
            task_assigner,
            task_runner: TaskRunner::new(),
            stream_manager,
            task_sequences: HashMap::new(),
            history: None,
        }
    }

    async fn start(&mut self, ctx: &mut ActorContext<Self>) {
        let addr = (
            self.options.driver_listen_host.clone(),
            self.options.driver_listen_port,
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
            DriverEvent::WorkerKnownPeers {
                worker_id,
                peer_worker_ids,
            } => self.handle_worker_known_peers(ctx, worker_id, peer_worker_ids),
            DriverEvent::ProbePendingWorker { worker_id } => {
                self.handle_probe_pending_worker(ctx, worker_id)
            }
            DriverEvent::ProbeIdleWorker { worker_id, instant } => {
                self.handle_probe_idle_worker(ctx, worker_id, instant)
            }
            DriverEvent::ProbeLostWorker { worker_id, instant } => {
                self.handle_probe_lost_worker(ctx, worker_id, instant)
            }
            DriverEvent::ExecuteJob {
                plan,
                context,
                result,
            } => self.handle_execute_job(ctx, plan, context, result),
            DriverEvent::CleanUpJob { job_id } => self.handle_clean_up_job(ctx, job_id),
            DriverEvent::UpdateTask {
                key,
                status,
                message,
                cause,
                sequence,
            } => self.handle_update_task(ctx, key, status, message, cause, sequence),
            DriverEvent::ProbePendingTask { key } => self.handle_probe_pending_task(ctx, key),
            DriverEvent::ProbePendingLocalStream { key } => {
                self.handle_probe_pending_local_stream(ctx, key)
            }
            DriverEvent::CreateLocalStream {
                key,
                storage,
                schema,
                result,
            } => self.handle_create_local_stream(ctx, key, storage, schema, result),
            DriverEvent::CreateRemoteStream {
                uri,
                key,
                schema,
                result,
            } => self.handle_create_remote_stream(ctx, uri, key, schema, result),
            DriverEvent::FetchDriverStream { key, result } => {
                self.handle_fetch_driver_stream(ctx, key, result)
            }
            DriverEvent::FetchWorkerStream {
                worker_id,
                key,
                schema,
                result,
            } => self.handle_fetch_worker_stream(ctx, worker_id, key, schema, result),
            DriverEvent::FetchRemoteStream {
                uri,
                key,
                schema,
                result,
            } => self.handle_fetch_remote_stream(ctx, uri, key, schema, result),
            DriverEvent::ObserveState { observer } => self.handle_observe_state(ctx, observer),
            DriverEvent::Shutdown { history } => self.handle_shutdown(ctx, history),
        }
    }

    async fn stop(mut self, ctx: &mut ActorContext<Self>) {
        if let Err(e) = self.worker_pool.close(ctx).await {
            error!("encountered error while stopping workers: {e}");
        }
        if let Some(history) = self.history.take() {
            let _ = history.send(self.build_history());
        }
        info!("stopping driver server");
        self.server.stop().await;
        info!("driver server has stopped");
    }
}
