use std::mem;

use fastrace::Span;
use fastrace::future::FutureExt;
use log::info;
use sail_common::actor::{Actor, ActorAction, ActorContext};

use crate::driver::DriverClientSet;
use crate::rpc::{ClientOptions, ServerMonitor};
use crate::task_runner::TaskRunner;
use crate::worker::actor::extensions::WorkerExtensions;
use crate::worker::peer_tracker::{PeerTracker, PeerTrackerOptions};
use crate::worker::{WorkerActor, WorkerMessage, WorkerOptions};

#[tonic::async_trait]
impl Actor for WorkerActor {
    type Message = WorkerMessage;
    type Options = WorkerOptions;

    fn name() -> &'static str {
        "WorkerActor"
    }

    fn new(options: WorkerOptions) -> Self {
        let driver_client_set = DriverClientSet::new(
            options.driver_id,
            ClientOptions {
                enable_tls: options.enable_tls,
                host: options.driver_host.clone(),
                port: options.driver_port,
            },
        );
        let peer_tracker = PeerTracker::new(PeerTrackerOptions::from(&options));
        let extensions = WorkerExtensions::new(&options);
        Self {
            options,
            server: ServerMonitor::new(),
            driver_client_set,
            peer_tracker,
            task_runner: TaskRunner::new(),
            extensions,
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
            WorkerMessage::ServerReady { port, signal } => {
                self.handle_server_ready(ctx, port, signal)
            }
            WorkerMessage::StartHeartbeat => self.handle_start_heartbeat(ctx),
            WorkerMessage::ReportKnownPeers { peer_worker_ids } => {
                self.handle_report_known_peers(ctx, peer_worker_ids)
            }
            WorkerMessage::RunTask {
                key,
                definition,
                peers,
            } => self.handle_run_task(ctx, key, definition, peers),
            WorkerMessage::StopTask { key } => self.handle_stop_task(ctx, key),
            WorkerMessage::ReportTaskStatus {
                key,
                status,
                message,
                cause,
            } => self.handle_report_task_status(ctx, key, status, message, cause),
            WorkerMessage::ProbePendingLocalStream { key } => {
                self.handle_probe_pending_local_stream(ctx, key)
            }
            WorkerMessage::CreateLocalStream {
                key,
                replicas,
                schema,
                result,
            } => self.handle_create_local_stream(ctx, key, replicas, schema, result),
            WorkerMessage::CreateStorageStream {
                key,
                schema,
                context,
                result,
            } => self.handle_create_storage_stream(ctx, key, schema, context, result),
            WorkerMessage::FetchDriverStream {
                key,
                schema,
                result,
            } => self.handle_fetch_driver_stream(ctx, key, schema, result),
            WorkerMessage::FetchWorkerStream { owner, key, result } => {
                self.handle_fetch_worker_stream(ctx, owner, key, result)
            }
            WorkerMessage::FetchStorageStream {
                key,
                schema,
                context,
                result,
            } => self.handle_fetch_storage_stream(ctx, key, schema, context, result),
            WorkerMessage::CleanUpJob { job_id, stage } => {
                self.handle_clean_up_job(ctx, job_id, stage)
            }
            WorkerMessage::Shutdown => ActorAction::Stop,
        }
    }

    async fn stop(self, _ctx: &mut ActorContext<Self>) {
        self.server.stop().await;
        info!("worker {} server has stopped", self.options.worker_id);
    }
}
