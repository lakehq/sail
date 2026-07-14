use std::collections::HashMap;
use std::mem;

use fastrace::Span;
use fastrace::future::FutureExt;
use log::info;
use sail_server::actor::{Actor, ActorAction, ActorContext};

use crate::driver::DriverClientSet;
use crate::rpc::{ClientOptions, ServerMonitor};
use crate::stream_manager::{StreamManager, StreamManagerOptions};
use crate::task_runner::TaskRunner;
use crate::worker::WorkerActor;
use crate::worker::event::WorkerEvent;
use crate::worker::options::WorkerOptions;
use crate::worker::peer_tracker::{PeerTracker, PeerTrackerOptions};

#[tonic::async_trait]
impl Actor for WorkerActor {
    type Message = WorkerEvent;
    type Options = WorkerOptions;

    fn name() -> &'static str {
        "WorkerActor"
    }

    fn new(options: WorkerOptions) -> Self {
        let driver_client_set = DriverClientSet::new(ClientOptions {
            enable_tls: options.enable_tls,
            host: options.driver_host.clone(),
            port: options.driver_port,
        });
        let peer_tracker = PeerTracker::new(PeerTrackerOptions::from(&options));
        let stream_manager = StreamManager::new(StreamManagerOptions::from(&options));
        Self {
            options,
            server: ServerMonitor::new(),
            driver_client_set,
            peer_tracker,
            task_runner: TaskRunner::new(),
            preparing_tasks: HashMap::new(),
            current_preparations: HashMap::new(),
            pending_job_cleanups: vec![],
            pending_worker_stops: vec![],
            stopping: false,
            job_cleanup_generations: HashMap::new(),
            next_job_cleanup_generation: 0,
            canceled_tasks: Default::default(),
            next_resource_preparation_id: 0,
            stream_manager,
            sequence: 42,
        }
    }

    async fn start(&mut self, ctx: &mut ActorContext<Self>) {
        let addr = (
            self.options.worker_listen_host.clone(),
            self.options.worker_listen_port,
        );
        let server = mem::take(&mut self.server);
        let span = Span::enter_with_local_parent("WorkerActor::serve");
        self.server = server
            .start(Self::serve(ctx.handle().clone(), addr).in_span(span))
            .await;
    }

    fn receive(&mut self, ctx: &mut ActorContext<Self>, message: Self::Message) -> ActorAction {
        match message {
            WorkerEvent::ServerReady { port, signal } => {
                self.handle_server_ready(ctx, port, signal)
            }
            WorkerEvent::StartHeartbeat => self.handle_start_heartbeat(ctx),
            WorkerEvent::ReportKnownPeers { peer_worker_ids } => {
                self.handle_report_known_peers(ctx, peer_worker_ids)
            }
            WorkerEvent::RunTask {
                key,
                definition,
                launch_context,
                peers,
            } => self.handle_run_task(ctx, key, definition, launch_context, peers),
            WorkerEvent::TaskResourcesMaterialized {
                preparation_id,
                key,
                definition,
                result,
                peers,
            } => self.handle_task_resources_materialized(
                ctx,
                preparation_id,
                key,
                definition,
                result,
                peers,
            ),
            WorkerEvent::StopTask { key } => self.handle_stop_task(ctx, key),
            WorkerEvent::ReportTaskStatus {
                key,
                status,
                message,
                cause,
            } => self.handle_report_task_status(ctx, key, status, message, cause),
            WorkerEvent::ProbePendingLocalStream { key } => {
                self.handle_probe_pending_local_stream(ctx, key)
            }
            WorkerEvent::CreateLocalStream {
                key,
                storage,
                schema,
                result,
            } => self.handle_create_local_stream(ctx, key, storage, schema, result),
            WorkerEvent::CreateRemoteStream {
                uri,
                key,
                schema,
                result,
            } => self.handle_create_remote_stream(ctx, uri, key, schema, result),
            WorkerEvent::FetchDriverStream {
                key,
                schema,
                result,
            } => self.handle_fetch_driver_stream(ctx, key, schema, result),
            WorkerEvent::FetchWorkerStream { owner, key, result } => {
                self.handle_fetch_worker_stream(ctx, owner, key, result)
            }
            WorkerEvent::FetchRemoteStream {
                uri,
                key,
                schema,
                result,
            } => self.handle_fetch_remote_stream(ctx, uri, key, schema, result),
            WorkerEvent::CleanUpJob {
                job_id,
                stage,
                result,
            } => self.handle_clean_up_job(ctx, job_id, stage, result),
            WorkerEvent::StopWorker { result } => self.handle_stop_worker(ctx, result),
            WorkerEvent::ExpireJobCancellation { job_id, generation } => {
                self.handle_expire_job_cancellation(job_id, generation)
            }
            WorkerEvent::Shutdown => ActorAction::Stop,
        }
    }

    async fn stop(mut self, ctx: &mut ActorContext<Self>) {
        self.stopping = true;
        for preparing in self.preparing_tasks.values_mut() {
            if let Some(cancel) = preparing.cancel.take() {
                let _ = cancel.send(());
            }
        }
        self.task_runner.stop_all();
        self.stream_manager.stop().await;
        ctx.shutdown_tasks().await;
        mem::take(&mut self.server).stop().await;
        info!("worker {} server has stopped", self.options.worker_id);
    }
}
