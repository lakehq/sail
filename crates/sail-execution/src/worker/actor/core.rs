use std::collections::HashMap;
use std::mem;

use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use log::debug;
use sail_server::actor::{Actor, ActorAction, ActorContext};
use tokio::sync::oneshot;

use crate::codec::RemoteExecutionCodec;
use crate::driver::DriverClient;
use crate::id::{TaskAttempt, WorkerId};
use crate::rpc::{ClientOptions, ServerMonitor};
use crate::stream::ChannelName;
use crate::worker::event::WorkerEvent;
use crate::worker::options::WorkerOptions;
use crate::worker::WorkerClient;

pub struct WorkerActor {
    options: WorkerOptions,
    pub(super) server: ServerMonitor,
    pub(super) driver_client: DriverClient,
    pub(super) worker_clients: HashMap<WorkerId, WorkerClient>,
    pub(super) task_signals: HashMap<TaskAttempt, oneshot::Sender<()>>,
    pub(super) memory_streams: HashMap<ChannelName, SendableRecordBatchStream>,
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
            memory_streams: HashMap::new(),
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
            WorkerEvent::CreateMemoryTaskStream {
                channel,
                schema,
                result,
            } => self.handle_create_memory_task_stream(ctx, channel, schema, result),
            WorkerEvent::FetchThisWorkerTaskStream { channel, result } => {
                self.handle_fetch_this_worker_task_stream(ctx, channel, result)
            }
            WorkerEvent::FetchOtherWorkerTaskStream {
                worker_id,
                host,
                port,
                channel,
                schema,
                result,
            } => self.handle_fetch_other_worker_task_stream(
                ctx, worker_id, host, port, channel, schema, result,
            ),
            WorkerEvent::Shutdown => ActorAction::Stop,
        }
    }

    async fn stop(self, _ctx: &mut ActorContext<Self>) {
        self.server.stop().await;
        debug!("worker {} server has stopped", self.options.worker_id);
    }
}

impl WorkerActor {
    pub(super) fn options(&self) -> &WorkerOptions {
        &self.options
    }
}
