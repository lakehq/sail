use std::collections::HashMap;
use std::mem;

use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use sail_server::actor::{Actor, ActorAction, ActorContext};

use crate::codec::RemoteExecutionCodec;
use crate::driver::DriverClient;
use crate::id::WorkerId;
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
    pub(super) memory_streams: HashMap<ChannelName, SendableRecordBatchStream>,
    pub(super) physical_plan_codec: Box<dyn PhysicalExtensionCodec>,
}

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
            memory_streams: HashMap::new(),
            physical_plan_codec: Box::new(RemoteExecutionCodec::new(SessionContext::default())),
        }
    }

    fn start(&mut self, ctx: &mut ActorContext<Self>) {
        let addr = (
            self.options().worker_listen_host.clone(),
            self.options().worker_listen_port,
        );
        let server = mem::take(&mut self.server);
        self.server = server.start(Self::serve(ctx.handle().clone(), addr));
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
                channel,
                schema,
                result,
            } => {
                self.handle_fetch_other_worker_task_stream(ctx, worker_id, channel, schema, result)
            }
            WorkerEvent::Shutdown => ActorAction::Stop,
        }
    }

    fn stop(self) {
        self.server.stop();
    }
}

impl WorkerActor {
    pub(super) fn options(&self) -> &WorkerOptions {
        &self.options
    }
}
