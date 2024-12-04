use std::collections::HashMap;
use std::mem;

use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use log::info;
use sail_server::actor::{Actor, ActorAction, ActorContext};
use tokio::sync::oneshot;

use crate::codec::RemoteExecutionCodec;
use crate::driver::DriverClient;
use crate::error::ExecutionResult;
use crate::id::{TaskAttempt, WorkerId};
use crate::rpc::{ClientOptions, ServerMonitor};
use crate::stream::ChannelName;
use crate::worker::actor::local_stream::LocalStream;
use crate::worker::event::WorkerEvent;
use crate::worker::options::WorkerOptions;
use crate::worker::WorkerClient;

pub struct WorkerActor {
    options: WorkerOptions,
    pub(super) server: ServerMonitor,
    driver_client: DriverClient,
    worker_clients: HashMap<WorkerId, WorkerClient>,
    pub(super) task_signals: HashMap<TaskAttempt, oneshot::Sender<()>>,
    pub(super) local_streams: HashMap<ChannelName, Box<dyn LocalStream>>,
    pub(super) physical_plan_codec: Box<dyn PhysicalExtensionCodec>,
    /// A monotonically increasing sequence number for ordered events.
    pub(super) sequence: u64,
}

#[tonic::async_trait]
impl Actor for WorkerActor {
    type Message = WorkerEvent;
    type Options = WorkerOptions;

    fn new(options: WorkerOptions) -> Self {
        let driver_client = DriverClient::new(ClientOptions {
            enable_tls: options.enable_tls,
            host: options.driver_host.clone(),
            port: options.driver_port,
        });
        Self {
            options,
            server: ServerMonitor::new(),
            driver_client,
            worker_clients: HashMap::new(),
            task_signals: HashMap::new(),
            local_streams: HashMap::new(),
            physical_plan_codec: Box::new(RemoteExecutionCodec::new(SessionContext::default())),
            sequence: 42,
        }
    }

    async fn start(&mut self, ctx: &mut ActorContext<Self>) {
        let addr = (
            self.options().worker_listen_host.clone(),
            self.options().worker_listen_port,
        );
        let server = mem::take(&mut self.server);
        self.server = server.start(Self::serve(ctx.handle().clone(), addr)).await;
    }

    fn receive(&mut self, ctx: &mut ActorContext<Self>, message: Self::Message) -> ActorAction {
        match message {
            WorkerEvent::ServerReady { port, signal } => {
                self.handle_server_ready(ctx, port, signal)
            }
            WorkerEvent::StartHeartbeat => self.handle_start_heartbeat(ctx),
            WorkerEvent::RunTask {
                task_id,
                attempt,
                plan,
                partition,
                channel,
            } => self.handle_run_task(ctx, task_id, attempt, plan, partition, channel),
            WorkerEvent::StopTask { task_id, attempt } => {
                self.handle_stop_task(ctx, task_id, attempt)
            }
            WorkerEvent::ReportTaskStatus {
                task_id,
                attempt,
                status,
                message,
            } => self.handle_report_task_status(ctx, task_id, attempt, status, message),
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
                host,
                port,
                channel,
                schema,
                result,
            } => self.handle_fetch_other_worker_stream(
                ctx, worker_id, host, port, channel, schema, result,
            ),
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

impl WorkerActor {
    pub(super) fn options(&self) -> &WorkerOptions {
        &self.options
    }

    pub(super) fn driver_client(&mut self) -> DriverClient {
        self.driver_client.clone()
    }

    pub(super) fn worker_client(
        &mut self,
        id: WorkerId,
        host: String,
        port: u16,
    ) -> ExecutionResult<WorkerClient> {
        let enable_tls = self.options().enable_tls;
        let options = ClientOptions {
            enable_tls,
            host,
            port,
        };
        let client = self
            .worker_clients
            .entry(id)
            .or_insert_with(|| WorkerClient::new(options));
        Ok(client.clone())
    }
}
