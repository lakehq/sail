use std::collections::HashMap;

use datafusion::execution::SendableRecordBatchStream;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use log::error;
use sail_server::actor::{Actor, ActorAction, ActorHandle};

use crate::codec::RemoteExecutionCodec;
use crate::driver::DriverClient;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::TaskId;
use crate::rpc::ServerMonitor;
use crate::worker::event::WorkerEvent;
use crate::worker::options::WorkerOptions;

pub struct WorkerActor {
    options: WorkerOptions,
    pub(super) server: ServerMonitor,
    pub(super) driver_client_cache: Option<DriverClient>,
    pub(super) task_streams: HashMap<(TaskId, usize), SendableRecordBatchStream>,
    pub(super) physical_plan_codec: Box<dyn PhysicalExtensionCodec>,
}

impl Actor for WorkerActor {
    type Message = WorkerEvent;
    type Options = WorkerOptions;
    type Error = ExecutionError;

    fn new(options: WorkerOptions) -> Self {
        Self {
            options,
            server: ServerMonitor::idle(),
            driver_client_cache: None,
            task_streams: HashMap::new(),
            physical_plan_codec: Box::new(RemoteExecutionCodec::new()),
        }
    }

    fn start(&mut self, handle: &ActorHandle<Self>) -> ExecutionResult<()> {
        self.start_server(handle)
    }

    fn receive(
        &mut self,
        message: Self::Message,
        _handle: &ActorHandle<Self>,
    ) -> ExecutionResult<ActorAction> {
        let action = match &message {
            WorkerEvent::Shutdown => ActorAction::Stop,
            _ => ActorAction::Continue,
        };
        let out = match message {
            WorkerEvent::ServerReady { port, signal } => self.handle_server_ready(port, signal),
            WorkerEvent::RunTask {
                task_id,
                partition,
                plan,
            } => self.handle_run_task(task_id, partition, plan),
            WorkerEvent::StopTask { task_id, partition } => {
                self.handle_stop_task(task_id, partition)
            }
            WorkerEvent::Shutdown => Ok(()),
        };
        if let Err(e) = out {
            let worker_id = self.options().worker_id;
            error!("error processing worker {worker_id} event: {e}");
        }
        Ok(action)
    }

    fn stop(mut self) -> ExecutionResult<()> {
        self.stop_server()
    }
}

impl WorkerActor {
    pub(super) fn options(&self) -> &WorkerOptions {
        &self.options
    }
}
