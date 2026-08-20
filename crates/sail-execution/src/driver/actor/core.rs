use std::collections::HashMap;
use std::sync::Arc;

use log::{error, info};
use sail_celeborn::lifecycle::{
    LifecycleManager, LifecycleManagerActor, LifecycleManagerOptions, LocalLifecycleManager,
};
use sail_celeborn::master::MasterClientOptions;
use sail_celeborn::shuffle::{ShuffleClient, ShuffleClientActor, ShuffleClientOptions};
use sail_common::actor::{Actor, ActorAction, ActorContext};

use crate::driver::job_scheduler::{JobScheduler, JobSchedulerOptions};
use crate::driver::task_assigner::{TaskAssigner, TaskAssignerOptions};
use crate::driver::worker_pool::{WorkerPool, WorkerPoolOptions};
use crate::driver::{DriverActor, DriverComponents, DriverMessage, DriverOptions};
use crate::shuffle::{ShuffleBackendKind, celeborn_application_id};
use crate::stream::celeborn::CelebornStreamManager;
use crate::stream::local::LocalStreamManager;
use crate::stream::storage::StorageStreamManager;
use crate::task_runner::{
    TaskRunnerActor, TaskRunnerComponents, TaskRunnerExtensions, TaskRunnerMessage,
    TaskRunnerPlacement,
};

#[tonic::async_trait]
impl Actor for DriverActor {
    type Message = DriverMessage;
    type Options = (DriverOptions, DriverComponents);

    fn name() -> &'static str {
        "DriverActor"
    }

    fn new(options: Self::Options) -> Self {
        let (options, components) = options;
        let DriverComponents {
            worker_manager,
            history_reporter,
        } = components;
        let worker_pool = WorkerPool::new(worker_manager, WorkerPoolOptions::from(&options));
        let job_scheduler = JobScheduler::new(JobSchedulerOptions::from(&options));
        let task_assigner = TaskAssigner::new(TaskAssignerOptions::from(&options));
        Self {
            options,
            history_reporter,
            worker_pool,
            job_scheduler,
            task_assigner,
            task_runner: None,
            extensions: Default::default(),
            task_sequences: HashMap::new(),
            shutdown_notifier: None,
        }
    }

    async fn start(&mut self, ctx: &mut ActorContext<Self>) {
        let driver = ctx.handle().clone();
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
            ShuffleBackendKind::Celeborn {
                master_host,
                master_port,
                compression,
                heartbeat_interval_secs,
                partition_split_threshold,
                partition_split_mode,
                ..
            } => {
                let application_id = celeborn_application_id(&self.options.session_id);
                let options = LifecycleManagerOptions::new(
                    application_id.clone(),
                    MasterClientOptions::new(master_host.clone(), *master_port),
                );
                let options = match self.options.shuffle_backend.celeborn_endpoint_resolver() {
                    Some(endpoint_resolver) => options.with_endpoint_resolver(endpoint_resolver),
                    None => options,
                };
                let options =
                    options.with_partition_split(*partition_split_threshold, *partition_split_mode);
                let options = options.with_heartbeat_interval(std::time::Duration::from_secs(
                    *heartbeat_interval_secs,
                ));
                let handle = ctx.children_mut().spawn::<LifecycleManagerActor>(options);
                let lifecycle_manager = LocalLifecycleManager::new(handle);
                self.extensions.lifecycle_manager = Some(lifecycle_manager.clone());
                let client = ShuffleClient::new(ctx.children_mut().spawn::<ShuffleClientActor>(
                    ShuffleClientOptions::new(
                        application_id,
                        Arc::new(lifecycle_manager),
                        self.options.shuffle_backend.celeborn_endpoint_resolver(),
                        *compression,
                    ),
                ));
                let streams = CelebornStreamManager::new(client);
                Some(streams)
            }
            ShuffleBackendKind::Flight | ShuffleBackendKind::Storage { .. } => None,
        };
        self.task_runner = Some(ctx.children_mut().spawn::<TaskRunnerActor>(
            TaskRunnerComponents {
                extensions: TaskRunnerExtensions {
                    local_streams,
                    storage_streams,
                    celeborn_streams,
                },
                placement: TaskRunnerPlacement::Driver { driver },
            },
        ));
    }

    fn receive(&mut self, ctx: &mut ActorContext<Self>, message: DriverMessage) -> ActorAction {
        match message {
            DriverMessage::Activate => self.handle_activate(ctx),
            DriverMessage::RegisterWorker {
                worker_id,
                host,
                port,
                result,
            } => self.handle_register_worker(ctx, worker_id, host, port, result),
            DriverMessage::WorkerHeartbeat { worker_id } => {
                self.handle_worker_heartbeat(ctx, worker_id)
            }
            DriverMessage::WorkerKnownPeers {
                worker_id,
                peer_worker_ids,
            } => self.handle_worker_known_peers(ctx, worker_id, peer_worker_ids),
            DriverMessage::ProbePendingWorker { worker_id } => {
                self.handle_probe_pending_worker(ctx, worker_id)
            }
            DriverMessage::ProbeIdleWorker { worker_id, instant } => {
                self.handle_probe_idle_worker(ctx, worker_id, instant)
            }
            DriverMessage::ProbeLostWorker { worker_id, instant } => {
                self.handle_probe_lost_worker(ctx, worker_id, instant)
            }
            DriverMessage::ExecuteJob {
                plan,
                context,
                result,
            } => self.handle_execute_job(ctx, plan, context, result),
            DriverMessage::CleanUpJob { job_id } => self.handle_clean_up_job(ctx, job_id),
            DriverMessage::UpdateTask {
                key,
                status,
                message,
                cause,
                sequence,
            } => self.handle_update_task(ctx, key, status, message, cause, sequence),
            DriverMessage::ProbePendingTask { key } => self.handle_probe_pending_task(ctx, key),
            DriverMessage::FetchDriverStream { key, result } => {
                self.handle_fetch_driver_stream(ctx, key, result)
            }
            DriverMessage::FetchWorkerStream {
                worker_id,
                key,
                schema,
                result,
            } => self.handle_fetch_worker_stream(ctx, worker_id, key, schema, result),
            DriverMessage::CelebornGetLifecycleManager { result } => {
                self.handle_celeborn_get_lifecycle_manager(result)
            }
            DriverMessage::ObserveState { observer } => self.handle_observe_state(ctx, observer),
            DriverMessage::Shutdown { result } => self.handle_shutdown(ctx, result),
        }
    }

    async fn stop(mut self, ctx: &mut ActorContext<Self>) {
        self.job_scheduler.stop();
        if let Some(task_runner) = self.task_runner.take() {
            let _ = task_runner.send(TaskRunnerMessage::Shutdown).await;
        }
        if let Err(e) = self.worker_pool.close(ctx).await {
            error!("encountered error while stopping workers: {e}");
        }
        if let Some(lifecycle_manager) = self.extensions.lifecycle_manager.take() {
            let _ = lifecycle_manager.stop().await;
        }
        ctx.children_mut().join().await;
        let history = self.build_history();
        self.history_reporter.report(history).await;
        if let Some(result) = self.shutdown_notifier.take() {
            let _ = result.send(());
        }
        info!("driver {} has stopped", self.options.driver_id);
    }
}
