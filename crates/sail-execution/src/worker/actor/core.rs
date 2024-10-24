use std::collections::HashMap;
use std::mem;

use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use log::error;
use sail_server::actor::{Actor, ActorAction, ActorContext};

use crate::codec::RemoteExecutionCodec;
use crate::driver::DriverClient;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::TaskId;
use crate::rpc::{ClientOptions, ServerMonitor};
use crate::worker::event::WorkerEvent;
use crate::worker::options::WorkerOptions;

#[derive(Eq, Hash, PartialEq)]
pub struct TaskAttempt(TaskId, usize);

impl TaskAttempt {
    pub fn new(task_id: TaskId, attempt: usize) -> Self {
        Self(task_id, attempt)
    }
}

pub struct WorkerActor {
    options: WorkerOptions,
    pub(super) server: ServerMonitor,
    pub(super) driver_client: DriverClient,
    pub(super) task_streams: HashMap<TaskAttempt, SendableRecordBatchStream>,
    pub(super) physical_plan_codec: Box<dyn PhysicalExtensionCodec>,
}

impl Actor for WorkerActor {
    type Message = WorkerEvent;
    type Options = WorkerOptions;
    type Error = ExecutionError;

    fn new(options: WorkerOptions) -> Self {
        let driver_client = DriverClient::new(
            options.worker_id,
            ClientOptions {
                enable_tls: options.enable_tls,
                host: options.driver_host.clone(),
                port: options.driver_port,
            },
        );
        Self {
            options,
            server: ServerMonitor::new(),
            driver_client,
            task_streams: HashMap::new(),
            physical_plan_codec: Box::new(RemoteExecutionCodec::new(SessionContext::default())),
        }
    }

    fn start(&mut self, ctx: &mut ActorContext<Self>) -> ExecutionResult<()> {
        let addr = (
            self.options().worker_listen_host.clone(),
            self.options().worker_listen_port,
        );
        let server = mem::take(&mut self.server);
        self.server = server.start(Self::serve(ctx.handle().clone(), addr));
        Ok(())
    }

    fn receive(
        &mut self,
        ctx: &mut ActorContext<Self>,
        message: Self::Message,
    ) -> ExecutionResult<ActorAction> {
        let action = match &message {
            WorkerEvent::Shutdown => ActorAction::Stop,
            _ => ActorAction::Continue,
        };
        let out = match message {
            WorkerEvent::ServerReady { port, signal } => {
                self.handle_server_ready(ctx, port, signal)
            }
            WorkerEvent::RunTask {
                task_id,
                attempt,
                plan,
                partition,
            } => self.handle_run_task(ctx, task_id, attempt, plan, partition),
            WorkerEvent::StopTask { task_id, attempt } => {
                self.handle_stop_task(ctx, task_id, attempt)
            }
            WorkerEvent::FetchTaskStream {
                task_id,
                attempt,
                result,
            } => self.handle_fetch_task_stream(ctx, task_id, attempt, result),
            WorkerEvent::Shutdown => Ok(()),
        };
        if let Err(e) = out {
            let worker_id = self.options().worker_id;
            error!("error processing worker {worker_id} event: {e}");
        }
        Ok(action)
    }

    fn stop(self) -> ExecutionResult<()> {
        self.server.stop();
        Ok(())
    }
}

impl WorkerActor {
    pub(super) fn options(&self) -> &WorkerOptions {
        &self.options
    }
}
