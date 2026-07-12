use std::collections::HashSet;
use std::mem;

use datafusion::arrow::datatypes::SchemaRef;
use log::{error, info, warn};
use sail_common_datafusion::error::CommonErrorCause;
use sail_server::actor::{ActorAction, ActorContext};
use tokio::sync::oneshot;

use crate::driver::TaskStatus;
use crate::error::ExecutionResult;
use crate::id::{JobId, TaskKey, TaskStreamKey, WorkerId};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{LocalStreamStorage, TaskStreamSink};
use crate::task::definition::{TaskDefinition, TaskLaunchContext};
use crate::worker::WorkerEvent;
use crate::worker::actor::{
    PendingJobCleanup, PendingWorkerStop, PreparingTaskResources, WorkerActor,
    should_run_materialized_task,
};
use crate::worker::event::{WorkerLocation, WorkerStreamOwner};
use crate::worker::launch_context::materialize_task_launch_context;

impl WorkerActor {
    pub(super) fn handle_server_ready(
        &mut self,
        ctx: &mut ActorContext<Self>,
        port: u16,
        signal: oneshot::Sender<()>,
    ) -> ActorAction {
        let worker_id = self.options.worker_id;
        info!("worker {worker_id} server is ready on port {port}");
        let server = mem::take(&mut self.server);
        self.server = match server.ready(signal) {
            Ok(x) => x,
            Err(e) => {
                error!("{e}");
                return ActorAction::Stop;
            }
        };
        let host = self.options.worker_external_host.clone();
        let port = if self.options.worker_external_port > 0 {
            self.options.worker_external_port
        } else {
            port
        };
        let retry_strategy = self.options.rpc_retry_strategy.clone();
        let client = self.driver_client_set.core.clone();
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            if let Err(e) = retry_strategy
                .run(|| {
                    let client = client.clone();
                    let host = host.clone();
                    async move { client.register_worker(worker_id, host, port).await }
                })
                .await
            {
                error!("failed to register worker with retries: {e}");
                let _ = handle.send(WorkerEvent::Shutdown).await;
            }
            if let Err(e) = handle.send(WorkerEvent::StartHeartbeat).await {
                error!("failed to start worker heartbeat: {e}");
                let _ = handle.send(WorkerEvent::Shutdown).await;
            }
        });
        ActorAction::Continue
    }

    pub(super) fn handle_start_heartbeat(&mut self, ctx: &mut ActorContext<Self>) -> ActorAction {
        let worker_id = self.options.worker_id;
        let client = self.driver_client_set.core.clone();
        let interval = self.options.worker_heartbeat_interval;
        ctx.spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if let Err(e) = client.report_worker_heartbeat(worker_id).await {
                    warn!("failed to report worker heartbeat: {e}");
                    // We do not retry heartbeat since we will report heartbeat again after
                    // the configured interval.
                }
            }
        });
        ActorAction::Continue
    }

    pub(super) fn handle_report_known_peers(
        &mut self,
        ctx: &mut ActorContext<Self>,
        peer_worker_ids: Vec<WorkerId>,
    ) -> ActorAction {
        let worker_id = self.options.worker_id;
        let client = self.driver_client_set.core.clone();
        ctx.spawn(async move {
            if let Err(e) = client
                .report_worker_known_peers(worker_id, peer_worker_ids)
                .await
            {
                warn!("failed to report worker known peers: {e}");
            }
        });
        ActorAction::Continue
    }

    pub(super) fn handle_run_task(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: TaskKey,
        definition: TaskDefinition,
        launch_context: TaskLaunchContext,
        peers: Vec<WorkerLocation>,
    ) -> ActorAction {
        if self.stopping || self.canceled_tasks.contains(&key) {
            return ActorAction::Continue;
        }
        let preparation_id = self.next_resource_preparation_id;
        self.next_resource_preparation_id = match preparation_id.checked_add(1) {
            Some(id) => id,
            None => {
                error!("task resource preparation ID overflow");
                return ActorAction::Stop;
            }
        };
        let (cancel, mut canceled) = oneshot::channel();
        self.preparing_tasks.insert(
            preparation_id,
            PreparingTaskResources {
                key: key.clone(),
                cancel: Some(cancel),
            },
        );
        if let Some(previous_id) = self
            .current_preparations
            .insert(key.clone(), preparation_id)
            && let Some(previous) = self.preparing_tasks.get_mut(&previous_id)
            && let Some(cancel) = previous.cancel.take()
        {
            let _ = cancel.send(());
        }
        let handle = ctx.handle().clone();
        let transfer_timeout = self.options.artifact_transfer_timeout;
        ctx.spawn(async move {
            let result = tokio::select! {
                _ = &mut canceled => None,
                result = tokio::time::timeout(
                    transfer_timeout,
                    materialize_task_launch_context(launch_context),
                ) => {
                    let result = result.unwrap_or_else(|_| {
                        Err(crate::error::ExecutionError::InvalidArgument(
                            "timed out materializing task resources".to_string(),
                        ))
                    });
                    Some(result)
                }
            };
            let _ = handle
                .send(WorkerEvent::TaskResourcesMaterialized {
                    preparation_id,
                    key,
                    definition,
                    result,
                    peers,
                })
                .await;
        });
        ActorAction::Continue
    }

    pub(super) fn handle_task_resources_materialized(
        &mut self,
        ctx: &mut ActorContext<Self>,
        preparation_id: u64,
        key: TaskKey,
        definition: TaskDefinition,
        result: Option<ExecutionResult<TaskLaunchContext>>,
        peers: Vec<WorkerLocation>,
    ) -> ActorAction {
        let is_current = self
            .current_preparations
            .get(&key)
            .is_some_and(|id| *id == preparation_id);
        let Some(preparing) = self.preparing_tasks.remove(&preparation_id) else {
            return ActorAction::Continue;
        };
        if preparing.key != key {
            error!("task resource preparation key mismatch");
            return ActorAction::Stop;
        }
        if is_current {
            self.current_preparations.remove(&key);
        }
        let run_task = should_run_materialized_task(
            is_current,
            self.stopping,
            self.canceled_tasks.contains(&key),
        );
        match result {
            Some(Ok(launch_context)) if run_task => {
                self.peer_tracker.track(ctx, peers);
                self.task_runner.run_task(
                    ctx,
                    key,
                    definition,
                    launch_context,
                    self.options.session.task_ctx(),
                );
            }
            Some(Err(error)) if run_task => {
                ctx.send(WorkerEvent::ReportTaskStatus {
                    key,
                    status: TaskStatus::Failed,
                    message: Some(format!("failed to materialize task resources: {error}")),
                    cause: Some(CommonErrorCause::new::<
                        sail_python_udf::error::PyErrExtractor,
                    >(&error)),
                });
            }
            Some(Ok(_)) | Some(Err(_)) | None => {}
        }
        self.finish_preparation_cleanup(ctx, preparation_id);
        ActorAction::Continue
    }

    pub(super) fn handle_stop_task(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        key: TaskKey,
    ) -> ActorAction {
        self.canceled_tasks.cancel(key.clone());
        if let Some(preparation_id) = self.current_preparations.get(&key).copied()
            && let Some(preparing) = self.preparing_tasks.get_mut(&preparation_id)
            && let Some(cancel) = preparing.cancel.take()
        {
            let _ = cancel.send(());
        } else {
            self.task_runner.stop_task(&key);
        }
        ActorAction::Continue
    }

    pub(super) fn handle_report_task_status(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: TaskKey,
        status: TaskStatus,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
    ) -> ActorAction {
        let sequence = self.sequence;
        self.sequence = match self.sequence.checked_add(1) {
            Some(x) => x,
            None => {
                error!("sequence number overflow");
                return ActorAction::Stop;
            }
        };
        let client = self.driver_client_set.core.clone();
        let handle = ctx.handle().clone();
        let retry_strategy = self.options.rpc_retry_strategy.clone();
        ctx.spawn(async move {
            if let Err(e) = retry_strategy
                .run(|| {
                    let client = client.clone();
                    let key = key.clone();
                    let message = message.clone();
                    let cause = cause.clone();
                    async move {
                        client
                            .report_task_status(key, status, message, cause, sequence)
                            .await
                    }
                })
                .await
            {
                // Failure to report task status is considered fatal so that
                // the driver can eventually detect this lost worker due to
                // missing worker heartbeats and mark all the task attempts
                // on this worker as failed.
                error!("failed to report task status with retries: {e}");
                let _ = handle.send(WorkerEvent::Shutdown).await;
            }
        });
        ActorAction::Continue
    }

    pub(super) fn handle_probe_pending_local_stream(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        key: TaskStreamKey,
    ) -> ActorAction {
        self.stream_manager.fail_local_stream_if_pending(&key);
        ActorAction::Continue
    }

    pub(super) fn handle_create_local_stream(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        key: TaskStreamKey,
        storage: LocalStreamStorage,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamSink>>>,
    ) -> ActorAction {
        let _ = result.send(
            self.stream_manager
                .create_local_stream(key, storage, schema),
        );
        ActorAction::Continue
    }

    pub(super) fn handle_create_remote_stream(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        uri: String,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamSink>>>,
    ) -> ActorAction {
        let _ = result.send(self.stream_manager.create_remote_stream(uri, key, schema));
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_driver_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        let client = self.driver_client_set.flight.clone();
        ctx.spawn(async move {
            let out = client.fetch_task_stream(key, schema).await;
            let _ = result.send(out);
        });
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_worker_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        owner: WorkerStreamOwner,
        key: TaskStreamKey,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        match owner {
            WorkerStreamOwner::This => {
                let _ = result.send(self.stream_manager.fetch_local_stream(ctx, &key));
            }
            WorkerStreamOwner::Worker {
                worker_id,
                schema: _,
            } if worker_id == self.options.worker_id => {
                let _ = result.send(self.stream_manager.fetch_local_stream(ctx, &key));
            }
            WorkerStreamOwner::Worker { worker_id, schema } => {
                let client = match self.peer_tracker.get_client_set(worker_id) {
                    Ok(x) => x.flight,
                    Err(e) => {
                        let _ = result.send(Err(e));
                        return ActorAction::Continue;
                    }
                };
                ctx.spawn(async move {
                    let out = client.fetch_task_stream(key, schema).await;
                    let _ = result.send(out);
                });
            }
        }
        ActorAction::Continue
    }

    pub(super) fn handle_fetch_remote_stream(
        &mut self,
        ctx: &mut ActorContext<Self>,
        uri: String,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    ) -> ActorAction {
        let _ = result.send(
            self.stream_manager
                .fetch_remote_stream(ctx, uri, &key, schema),
        );
        ActorAction::Continue
    }

    pub(super) fn handle_clean_up_job(
        &mut self,
        ctx: &mut ActorContext<Self>,
        job_id: JobId,
        stage: Option<usize>,
        result: oneshot::Sender<()>,
    ) -> ActorAction {
        let generation = if stage.is_none() {
            let generation = self.next_job_cleanup_generation;
            let Some(next) = generation.checked_add(1) else {
                error!("job cleanup generation overflow");
                return ActorAction::Stop;
            };
            self.next_job_cleanup_generation = next;
            self.job_cleanup_generations.insert(job_id, generation);
            Some(generation)
        } else {
            None
        };
        let preparation_ids = self
            .preparing_tasks
            .iter()
            .filter_map(|(id, preparing)| {
                (preparing.key.job_id == job_id
                    && stage.is_none_or(|stage| preparing.key.stage == stage))
                .then_some(*id)
            })
            .collect::<std::collections::HashSet<_>>();
        for preparation_id in &preparation_ids {
            if let Some(preparing) = self.preparing_tasks.get_mut(preparation_id)
                && let Some(cancel) = preparing.cancel.take()
            {
                let _ = cancel.send(());
            }
        }
        self.canceled_tasks.clean_up_job(job_id, stage);
        self.task_runner.stop_job(job_id, stage);
        self.stream_manager.remove_local_streams(job_id, stage);
        if preparation_ids.is_empty() {
            self.acknowledge_job_cleanup(ctx, job_id, generation, result);
        } else {
            self.pending_job_cleanups.push(PendingJobCleanup {
                job_id,
                preparation_ids,
                generation,
                result,
            });
        }
        ActorAction::Continue
    }

    pub(super) fn handle_stop_worker(
        &mut self,
        ctx: &mut ActorContext<Self>,
        result: oneshot::Sender<()>,
    ) -> ActorAction {
        self.stopping = true;
        let preparation_ids = self.preparing_tasks.keys().copied().collect::<HashSet<_>>();
        for preparation_id in &preparation_ids {
            if let Some(preparing) = self.preparing_tasks.get_mut(preparation_id)
                && let Some(cancel) = preparing.cancel.take()
            {
                let _ = cancel.send(());
            }
        }
        self.task_runner.stop_all();
        if preparation_ids.is_empty() {
            let _ = result.send(());
            ctx.send(WorkerEvent::Shutdown);
        } else {
            self.pending_worker_stops.push(PendingWorkerStop {
                preparation_ids,
                result,
            });
        }
        ActorAction::Continue
    }

    fn finish_preparation_cleanup(&mut self, ctx: &mut ActorContext<Self>, preparation_id: u64) {
        let mut index = 0;
        while index < self.pending_job_cleanups.len() {
            if self.pending_job_cleanups[index].finish_preparation(preparation_id) {
                let cleanup = self.pending_job_cleanups.swap_remove(index);
                self.acknowledge_job_cleanup(
                    ctx,
                    cleanup.job_id,
                    cleanup.generation,
                    cleanup.result,
                );
            } else {
                index += 1;
            }
        }

        let mut stop_index = 0;
        let mut stop_completed = false;
        while stop_index < self.pending_worker_stops.len() {
            if self.pending_worker_stops[stop_index].finish_preparation(preparation_id) {
                let stop = self.pending_worker_stops.swap_remove(stop_index);
                let _ = stop.result.send(());
                stop_completed = true;
            } else {
                stop_index += 1;
            }
        }
        if stop_completed {
            ctx.send(WorkerEvent::Shutdown);
        }
    }

    fn acknowledge_job_cleanup(
        &mut self,
        ctx: &mut ActorContext<Self>,
        job_id: JobId,
        generation: Option<u64>,
        result: oneshot::Sender<()>,
    ) {
        let _ = result.send(());
        if let Some(generation) = generation {
            ctx.send_with_delay(
                WorkerEvent::ExpireJobCancellation { job_id, generation },
                self.options.artifact_transfer_timeout,
            );
        }
    }

    pub(super) fn handle_expire_job_cancellation(
        &mut self,
        job_id: JobId,
        generation: u64,
    ) -> ActorAction {
        if self
            .job_cleanup_generations
            .get(&job_id)
            .is_some_and(|current| *current == generation)
        {
            self.job_cleanup_generations.remove(&job_id);
            self.canceled_tasks.finish_job(job_id);
        }
        ActorAction::Continue
    }
}
