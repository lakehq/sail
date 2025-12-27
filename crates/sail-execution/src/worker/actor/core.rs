use std::collections::HashMap;
use std::mem;

use fastrace::future::FutureExt;
use fastrace::Span;
use log::info;
use sail_server::actor::{Actor, ActorAction, ActorContext};

use crate::codec::RemoteExecutionCodec;
use crate::driver::DriverClient;
use crate::rpc::{ClientOptions, ServerMonitor};
use crate::worker::actor::peer_tracker::{PeerTracker, PeerTrackerOptions};
use crate::worker::event::WorkerEvent;
use crate::worker::options::WorkerOptions;
use crate::worker::WorkerActor;

#[tonic::async_trait]
impl Actor for WorkerActor {
    type Message = WorkerEvent;
    type Options = WorkerOptions;

    fn name() -> &'static str {
        "WorkerActor"
    }

    fn new(options: WorkerOptions) -> Self {
        let driver_client = DriverClient::new(ClientOptions {
            enable_tls: options.enable_tls,
            host: options.driver_host.clone(),
            port: options.driver_port,
        });
        let peer_tracker = PeerTracker::new(PeerTrackerOptions::new(&options));
        Self {
            options,
            server: ServerMonitor::new(),
            driver_client,
            peer_tracker,
            task_signals: HashMap::new(),
            local_streams: HashMap::new(),
            physical_plan_codec: Box::new(RemoteExecutionCodec),
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
                instance,
                plan,
                partition,
                channel,
                peers,
            } => self.handle_run_task(ctx, instance, plan, partition, channel, peers),
            WorkerEvent::StopTask { instance } => self.handle_stop_task(ctx, instance),
            WorkerEvent::ReportTaskStatus {
                instance,
                status,
                message,
                cause,
            } => self.handle_report_task_status(ctx, instance, status, message, cause),
            WorkerEvent::CreateLocalStream {
                channel,
                storage,
                schema,
                result,
            } => self.handle_create_local_stream(ctx, channel, storage, schema, result),
            WorkerEvent::CreateRemoteStream {
                uri,
                schema,
                result,
            } => self.handle_create_remote_stream(ctx, uri, schema, result),
            WorkerEvent::FetchThisWorkerStream { channel, result } => {
                self.handle_fetch_this_worker_stream(ctx, channel, result)
            }
            WorkerEvent::FetchOtherWorkerStream {
                worker_id,
                channel,
                schema,
                result,
            } => self.handle_fetch_other_worker_stream(ctx, worker_id, channel, schema, result),
            WorkerEvent::FetchRemoteStream {
                uri,
                schema,
                result,
            } => self.handle_fetch_remote_stream(ctx, uri, schema, result),
            WorkerEvent::RemoveLocalStream { channel_prefix } => {
                self.handle_remove_local_stream(ctx, channel_prefix)
            }
            WorkerEvent::Shutdown => ActorAction::Stop,
        }
    }

    async fn stop(self, _ctx: &mut ActorContext<Self>) {
        self.server.stop().await;
        info!("worker {} server has stopped", self.options.worker_id);
    }
}
