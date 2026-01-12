use std::mem;

use fastrace::future::FutureExt;
use fastrace::Span;
use log::info;
use sail_server::actor::{Actor, ActorAction, ActorContext};

use crate::driver::DriverClientSet;
use crate::rpc::{ClientOptions, ServerMonitor};
use crate::stream_manager::{StreamManager, StreamManagerOptions};
use crate::task_runner::TaskRunner;
use crate::worker::event::WorkerEvent;
use crate::worker::options::WorkerOptions;
use crate::worker::peer_tracker::{PeerTracker, PeerTrackerOptions};
use crate::worker::WorkerActor;

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
                peers,
            } => self.handle_run_task(ctx, key, definition, peers),
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
            WorkerEvent::CleanUpJob { job_id, stage } => {
                self.handle_clean_up_job(ctx, job_id, stage)
            }
            WorkerEvent::Shutdown => ActorAction::Stop,
        }
    }

    async fn stop(self, _ctx: &mut ActorContext<Self>) {
        self.server.stop().await;
        info!("worker {} server has stopped", self.options.worker_id);
    }
}
