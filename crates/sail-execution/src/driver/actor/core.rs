use std::collections::HashMap;

use log::{error, info};
use sail_common::actor::{Actor, ActorAction, ActorContext};

use crate::driver::actor::extensions::DriverExtensions;
use crate::driver::job_scheduler::{JobScheduler, JobSchedulerOptions};
use crate::driver::task_assigner::{TaskAssigner, TaskAssignerOptions};
use crate::driver::worker_pool::{WorkerPool, WorkerPoolOptions};
use crate::driver::{DriverActor, DriverComponents, DriverMessage, DriverOptions};
use crate::task_runner::TaskRunner;

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
        let extensions = DriverExtensions::new(&options);
        Self {
            options,
            history_reporter,
            worker_pool,
            job_scheduler,
            task_assigner,
            task_runner: TaskRunner::new(),
            extensions,
            task_sequences: HashMap::new(),
            shutdown_notifier: None,
        }
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
            DriverMessage::ProbePendingLocalStream { key } => {
                self.handle_probe_pending_local_stream(ctx, key)
            }
            DriverMessage::CreateLocalStream {
                key,
                replicas,
                schema,
                result,
            } => self.handle_create_local_stream(ctx, key, replicas, schema, result),
            DriverMessage::CreateStorageStream {
                key,
                schema,
                context,
                result,
            } => self.handle_create_storage_stream(ctx, key, schema, context, result),
            DriverMessage::FetchDriverStream { key, result } => {
                self.handle_fetch_driver_stream(ctx, key, result)
            }
            DriverMessage::FetchWorkerStream {
                worker_id,
                key,
                schema,
                result,
            } => self.handle_fetch_worker_stream(ctx, worker_id, key, schema, result),
            DriverMessage::FetchStorageStream {
                key,
                schema,
                context,
                result,
            } => self.handle_fetch_storage_stream(ctx, key, schema, context, result),
            DriverMessage::ObserveState { observer } => self.handle_observe_state(ctx, observer),
            DriverMessage::Shutdown { result } => self.handle_shutdown(ctx, result),
        }
    }

    async fn stop(mut self, ctx: &mut ActorContext<Self>) {
        self.job_scheduler.stop();
        if let Err(e) = self.worker_pool.close(ctx).await {
            error!("encountered error while stopping workers: {e}");
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
