use std::mem;
use std::sync::Arc;

use fastrace::Span;
use fastrace::future::FutureExt;
use log::info;
use sail_celeborn::shuffle::{ShuffleClient, ShuffleClientActor, ShuffleClientOptions};
use sail_common::actor::{Actor, ActorAction, ActorContext};

use crate::driver::DriverClientSet;
use crate::rpc::{ClientOptions, ServerMonitor};
use crate::shuffle::{ShuffleBackendKind, celeborn_application_id};
use crate::stream::celeborn::{CelebornStreamManager, RemoteLifecycleManager};
use crate::stream::local::LocalStreamManager;
use crate::stream::storage::StorageStreamManager;
use crate::task_runner::{
    TaskRunnerActor, TaskRunnerComponents, TaskRunnerExtensions, TaskRunnerPlacement,
};
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
        Self {
            options,
            server: ServerMonitor::new(),
            driver_client_set,
            task_runner: None,
        }
    }

    async fn start(&mut self, ctx: &mut ActorContext<Self>) {
        let worker = ctx.handle().clone();
        let local_streams = LocalStreamManager::new((&self.options).into());
        let storage_streams = match &self.options.shuffle_backend {
            ShuffleBackendKind::Storage {
                path,
                max_file_size,
                compression,
            } => Some(StorageStreamManager::new(
                path.clone(),
                self.options.session_id.clone(),
                *max_file_size,
                *compression,
            )),
            ShuffleBackendKind::Flight | ShuffleBackendKind::Celeborn { .. } => None,
        };
        let celeborn_streams = match &self.options.shuffle_backend {
            ShuffleBackendKind::Celeborn { compression, .. } => {
                let application_id = celeborn_application_id(&self.options.session_id);
                let lifecycle_manager = Arc::new(RemoteLifecycleManager::new(
                    self.driver_client_set.celeborn.clone(),
                ));
                let client = ShuffleClient::new(ctx.children_mut().spawn::<ShuffleClientActor>(
                    ShuffleClientOptions::new(
                        application_id,
                        lifecycle_manager,
                        self.options.shuffle_backend.celeborn_endpoint_resolver(),
                        *compression,
                    ),
                ));
                Some(CelebornStreamManager::new(client))
            }
            ShuffleBackendKind::Flight | ShuffleBackendKind::Storage { .. } => None,
        };
        let task_runner = ctx
            .children_mut()
            .spawn::<TaskRunnerActor>(TaskRunnerComponents {
                extensions: TaskRunnerExtensions {
                    local_streams,
                    storage_streams,
                    celeborn_streams,
                },
                placement: TaskRunnerPlacement::Worker {
                    worker_id: self.options.worker_id,
                    sequence: 42,
                    driver: self.driver_client_set.clone(),
                    worker,
                    peers: PeerTracker::new(PeerTrackerOptions::from(&self.options)),
                    retry_strategy: self.options.rpc_retry_strategy.clone(),
                },
            });
        self.task_runner = Some(task_runner.clone());
        let addr = (
            self.options.worker_listen_host.clone(),
            self.options.worker_listen_port,
        );
        let server = mem::take(&mut self.server);
        let span = Span::enter_with_local_parent("WorkerActor::serve");
        let task_context = self.options.session.task_ctx();
        self.server = server
            .start(Self::serve(ctx.handle().clone(), task_runner, task_context, addr).in_span(span))
            .await;
    }

    fn receive(&mut self, ctx: &mut ActorContext<Self>, message: Self::Message) -> ActorAction {
        match message {
            WorkerMessage::ServerReady { port, signal } => {
                self.handle_server_ready(ctx, port, signal)
            }
            WorkerMessage::StartHeartbeat => self.handle_start_heartbeat(ctx),
            WorkerMessage::Shutdown => ActorAction::Stop,
        }
    }

    async fn stop(mut self, ctx: &mut ActorContext<Self>) {
        if let Some(task_runner) = self.task_runner.take() {
            let _ = task_runner
                .send(crate::task_runner::TaskRunnerMessage::Shutdown)
                .await;
        }
        self.server.stop().await;
        ctx.children_mut().join().await;
        info!("worker {} server has stopped", self.options.worker_id);
    }
}
