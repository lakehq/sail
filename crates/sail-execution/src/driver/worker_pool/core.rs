use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::sync::Arc;

use chrono::Utc;
use datafusion::arrow::datatypes::SchemaRef;
use fastrace::Span;
use fastrace::collector::SpanContext;
use futures::{StreamExt, TryStreamExt};
use log::{error, info, warn};
use object_store::{ObjectStoreExt, ObjectStoreScheme};
use prost::Message;
use sail_common::config::GRPC_MAX_MESSAGE_LENGTH_DEFAULT;
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use sail_server::actor::ActorContext;
use sail_telemetry::common::SpanAttribute;
use tokio::time::Instant;
use url::Url;

use crate::driver::worker_pool::state::WorkerState;
use crate::driver::worker_pool::task_resource_staging::{
    PendingTaskResourceDispatch, TaskResourceStagingCompletion, TaskResourceStagingKey,
    TaskResourceStagingRequest,
};
use crate::driver::worker_pool::{
    CompletedTaskResourceStaging, JobExecutionResources, TaskResourceStagingId, WorkerDescriptor,
    WorkerPool, WorkerPoolOptions,
};
use crate::driver::{DriverActor, DriverEvent, TaskStatus};
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskKey, TaskKeyDisplay, TaskStreamKey, WorkerId};
use crate::rpc::ClientOptions;
use crate::stream::error::TaskStreamError;
use crate::stream::reader::TaskStreamSource;
use crate::task::definition::{TaskDefinition, TaskLaunchContext};
use crate::worker::launch_context::{
    TaskResourceCleanupLease, TaskResourceStagingScope, handoff_task_resource_cleanup_blocking,
    stage_task_launch_context, task_launch_context_descriptor_size,
};
use crate::worker::{WorkerClientSet, WorkerLocation};
use crate::worker_manager::WorkerLaunchOptions;

impl WorkerPool {
    pub fn stage_task_resources(
        &mut self,
        ctx: &mut ActorContext<DriverActor>,
        worker_id: WorkerId,
        key: TaskKey,
        definition: TaskDefinition,
        launch_context: TaskLaunchContext,
    ) {
        let job_id = key.job_id;
        let stage = key.stage;
        let namespace = self.task_resource_namespace.clone();
        let inline_max_bytes = self.options.artifact_inline_max_bytes;
        let store_uri = self.options.artifact_store_uri.clone();
        let transfer_timeout = self.options.artifact_transfer_timeout;
        const WORKER_REQUEST_ENVELOPE_RESERVE: usize = 1024 * 1024;
        let definition_bytes =
            crate::task::r#gen::TaskDefinition::from(definition.clone()).encoded_len();
        let resource_descriptor_bytes = task_launch_context_descriptor_size(launch_context.clone());
        let inline_total_max_bytes = GRPC_MAX_MESSAGE_LENGTH_DEFAULT
            .saturating_sub(definition_bytes)
            .saturating_sub(resource_descriptor_bytes)
            .saturating_sub(WORKER_REQUEST_ENVELOPE_RESERVE);
        let staging_key = TaskResourceStagingKey::new(
            key.job_id,
            key.stage,
            inline_total_max_bytes,
            &launch_context,
        );
        let staging_id = match self.allocate_task_resource_staging_id() {
            Ok(staging_id) => staging_id,
            Err(error) => {
                ctx.send(DriverEvent::UpdateTask {
                    key,
                    status: TaskStatus::Failed,
                    message: Some(error.to_string()),
                    cause: Some(CommonErrorCause::new::<PyErrExtractor>(&error)),
                    sequence: None,
                });
                return;
            }
        };
        if !self.begin_job_request(job_id, worker_id) {
            return;
        }
        let request = self
            .job_resources
            .entry(job_id)
            .or_default()
            .task_resource_staging
            .request(
                staging_id,
                staging_key,
                PendingTaskResourceDispatch {
                    worker_id,
                    key,
                    definition,
                },
            );
        match request {
            TaskResourceStagingRequest::Wait => {
                self.finish_job_request(ctx, job_id);
                return;
            }
            TaskResourceStagingRequest::Reuse {
                launch_context,
                pending,
            } => {
                let pending = *pending;
                self.run_task(
                    ctx,
                    pending.worker_id,
                    pending.key,
                    pending.definition,
                    launch_context,
                );
                self.finish_job_request(ctx, job_id);
                return;
            }
            TaskResourceStagingRequest::Load => {}
        }
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            let result = tokio::time::timeout(
                transfer_timeout,
                stage_task_launch_context(
                    launch_context,
                    TaskResourceStagingScope {
                        job_id,
                        stage,
                        staging_id: staging_id.0,
                    },
                    &namespace,
                    inline_max_bytes,
                    inline_total_max_bytes,
                    store_uri.as_deref(),
                    transfer_timeout,
                ),
            )
            .await
            .unwrap_or_else(|_| {
                Err(ExecutionError::InvalidArgument(
                    "timed out staging task resources".to_string(),
                ))
            });
            let _ = handle
                .send(DriverEvent::TaskResourcesStaged {
                    job_id,
                    stage,
                    staging_id,
                    result,
                })
                .await;
            let _ = handle
                .send(DriverEvent::WorkerJobRequestFinished { job_id })
                .await;
        });
    }

    fn allocate_task_resource_staging_id(&mut self) -> ExecutionResult<TaskResourceStagingId> {
        let staging_id = self.next_task_resource_staging_id;
        self.next_task_resource_staging_id = staging_id.checked_add(1).ok_or_else(|| {
            ExecutionError::InternalError("task resource staging ID overflow".to_string())
        })?;
        Ok(TaskResourceStagingId(staging_id))
    }

    pub fn finish_task_resource_staging(
        &mut self,
        job_id: JobId,
        staging_id: TaskResourceStagingId,
        result: ExecutionResult<crate::worker::launch_context::StagedTaskLaunchContext>,
    ) -> Option<CompletedTaskResourceStaging> {
        let resources = self.job_resources.get_mut(&job_id)?;
        if !resources.accepts_requests() {
            let _ = resources.task_resource_staging.fail(staging_id);
            return None;
        }
        match result {
            Ok(staged) => match staged.accept() {
                Ok((launch_context, cleanup_leases)) => {
                    let TaskResourceStagingCompletion::Pending(pending) = resources
                        .task_resource_staging
                        .complete(staging_id, launch_context.clone())
                    else {
                        return None;
                    };
                    for lease in cleanup_leases {
                        resources
                            .cleanup_leases
                            .entry(lease.uri().to_string())
                            .or_default()
                            .push(Arc::new(lease));
                    }
                    Some(CompletedTaskResourceStaging::Ready {
                        pending,
                        launch_context,
                    })
                }
                Err(error) => {
                    let TaskResourceStagingCompletion::Pending(pending) =
                        resources.task_resource_staging.fail(staging_id)
                    else {
                        return None;
                    };
                    Some(CompletedTaskResourceStaging::Failed { pending, error })
                }
            },
            Err(error) => {
                let TaskResourceStagingCompletion::Pending(pending) =
                    resources.task_resource_staging.fail(staging_id)
                else {
                    return None;
                };
                Some(CompletedTaskResourceStaging::Failed { pending, error })
            }
        }
    }

    fn begin_job_request(&mut self, job_id: JobId, worker_id: WorkerId) -> bool {
        let resources = self.job_resources.entry(job_id).or_default();
        if !resources.accepts_requests() {
            warn!("job {job_id} cleanup has already started");
            return false;
        }
        let Some(pending_requests) = resources.pending_requests.checked_add(1) else {
            error!("job {job_id} request count overflow");
            return false;
        };
        resources.pending_requests = pending_requests;
        resources.workers.insert(worker_id);
        true
    }

    pub fn accepts_job_requests(&self, job_id: JobId) -> bool {
        self.job_resources
            .get(&job_id)
            .is_none_or(JobExecutionResources::accepts_requests)
    }

    pub fn finish_job_request(&mut self, ctx: &mut ActorContext<DriverActor>, job_id: JobId) {
        let Some(resources) = self.job_resources.get_mut(&job_id) else {
            warn!("job {job_id} request finished without tracked resources");
            return;
        };
        let Some(pending_requests) = resources.pending_requests.checked_sub(1) else {
            error!("job {job_id} request count underflow");
            return;
        };
        resources.pending_requests = pending_requests;
        self.try_start_job_resource_cleanup(ctx, job_id);
    }

    pub fn request_job_resource_cleanup(
        &mut self,
        ctx: &mut ActorContext<DriverActor>,
        job_id: JobId,
    ) {
        let cleanup_deadline = Instant::now() + self.options.artifact_transfer_timeout;
        let resources = self.job_resources.entry(job_id).or_default();
        resources.cleanup_requested = true;
        resources.task_resource_staging.clear();
        resources.cleanup_deadline.get_or_insert(cleanup_deadline);
        self.try_start_job_resource_cleanup(ctx, job_id);
    }

    fn try_start_job_resource_cleanup(
        &mut self,
        ctx: &mut ActorContext<DriverActor>,
        job_id: JobId,
    ) {
        let Some(resources) = self.job_resources.get_mut(&job_id) else {
            return;
        };
        if !resources.ready_for_cleanup() {
            return;
        }
        resources.cleanup_in_progress = true;
        let worker_ids = resources.workers.iter().copied().collect::<Vec<_>>();
        let cleanup_leases = resources
            .cleanup_leases
            .values()
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        let uris = resources.cleanup_leases.keys().cloned().collect::<Vec<_>>();
        let cleanup_deadline = resources
            .cleanup_deadline
            .unwrap_or_else(|| Instant::now() + self.options.artifact_transfer_timeout);
        let requires_worker_barrier = resources.requires_worker_barrier(Instant::now());
        let transfer_timeout = self.options.artifact_transfer_timeout;
        let cleanup_rpc_timeout =
            transfer_timeout.min(cleanup_deadline.saturating_duration_since(Instant::now()));

        let mut clients = Vec::with_capacity(worker_ids.len());
        let mut unavailable_worker = None;
        if requires_worker_barrier {
            for worker_id in worker_ids {
                match self.workers.get_mut(&worker_id) {
                    Some(worker) if worker_is_resource_quiesced(worker, Instant::now()) => {}
                    Some(worker) => match Self::get_client_set(worker_id, worker, &self.options) {
                        Ok(client) => clients.push((worker_id, client.core)),
                        Err(error) => unavailable_worker = Some((worker_id, error.to_string())),
                    },
                    None if cleanup_deadline <= Instant::now() => {}
                    None => {
                        unavailable_worker =
                            Some((worker_id, "worker descriptor not found".to_string()));
                    }
                }
            }
        }

        let retry_strategy = self.options.rpc_retry_strategy.clone();
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            let cleanup_results =
                futures::future::join_all(clients.into_iter().map(|(worker_id, client)| {
                    let retry_strategy = retry_strategy.clone();
                    async move {
                        await_worker_control_rpc(
                            retry_strategy.run(|| {
                                let client = client.clone();
                                async move { client.clean_up_job(job_id, None).await }
                            }),
                            cleanup_rpc_timeout,
                            format!("timed out cleaning up job {job_id} in worker {worker_id}"),
                        )
                        .await
                        .map_err(|error| {
                            ExecutionError::InternalError(format!(
                                "failed to clean up job {job_id} in worker {worker_id}: {error}"
                            ))
                        })
                    }
                }))
                .await;
            let barrier_result = unavailable_worker.map_or(Ok(()), |(worker_id, error)| {
                Err(ExecutionError::InternalError(format!(
                    "worker {worker_id} unavailable during job {job_id} cleanup: {error}"
                )))
            });
            let barrier_result = cleanup_results
                .into_iter()
                .fold(barrier_result, |result, cleanup| result.and(cleanup));
            let result = match barrier_result {
                Ok(()) => {
                    cleanup_leases.iter().for_each(|lease| lease.activate());
                    let failed =
                        delete_task_resource_uris(uris, transfer_timeout, cleanup_deadline).await;
                    if failed.is_empty() || Instant::now() < cleanup_deadline {
                        Ok(failed)
                    } else {
                        let handoff = failed.clone();
                        match tokio::task::spawn_blocking(move || {
                            handoff_task_resource_cleanup_blocking(
                                handoff,
                                transfer_timeout,
                                transfer_timeout,
                            )
                        })
                        .await
                        {
                            Ok(Ok(())) => Ok(vec![]),
                            Ok(Err(error)) => {
                                warn!("failed to hand off job {job_id} resource cleanup: {error}");
                                Ok(failed)
                            }
                            Err(error) => {
                                warn!("failed to join job {job_id} cleanup handoff: {error}");
                                Ok(failed)
                            }
                        }
                    }
                }
                Err(error) => Err(error),
            };
            let _ = handle
                .send(DriverEvent::JobResourceCleanupFinished { job_id, result })
                .await;
        });
    }

    pub fn finish_job_resource_cleanup(
        &mut self,
        ctx: &mut ActorContext<DriverActor>,
        job_id: JobId,
        result: ExecutionResult<Vec<String>>,
    ) {
        match result {
            Ok(failed_uris) if failed_uris.is_empty() => {
                self.job_resources.remove(&job_id);
            }
            Ok(failed_uris) => {
                if let Some(resources) = self.job_resources.get_mut(&job_id) {
                    let failed_uris = failed_uris.into_iter().collect::<HashSet<_>>();
                    resources
                        .cleanup_leases
                        .retain(|uri, _| failed_uris.contains(uri));
                    resources.cleanup_barrier_complete = true;
                    resources.cleanup_in_progress = false;
                }
                warn!("failed to delete one or more job {job_id} task resources");
                self.schedule_job_resource_cleanup_retry(ctx, job_id);
            }
            Err(error) => {
                if let Some(resources) = self.job_resources.get_mut(&job_id) {
                    resources.cleanup_in_progress = false;
                }
                warn!("job {job_id} resource cleanup barrier failed: {error}");
                self.schedule_job_resource_cleanup_retry(ctx, job_id);
            }
        }
    }

    fn schedule_job_resource_cleanup_retry(
        &mut self,
        ctx: &mut ActorContext<DriverActor>,
        job_id: JobId,
    ) {
        let Some(resources) = self.job_resources.get_mut(&job_id) else {
            return;
        };
        let delay = resources.next_cleanup_retry_delay();
        ctx.send_with_delay(DriverEvent::RetryJobResourceCleanup { job_id }, delay);
    }

    pub fn retry_job_resource_cleanup(
        &mut self,
        ctx: &mut ActorContext<DriverActor>,
        job_id: JobId,
    ) {
        self.try_start_job_resource_cleanup(ctx, job_id);
    }

    pub async fn close(&mut self, ctx: &mut ActorContext<DriverActor>) -> ExecutionResult<()> {
        let worker_ids = self.workers.keys().cloned().collect::<Vec<_>>();
        for worker_id in worker_ids.into_iter() {
            // TODO: Should we wait for the spawned tasks for stopping the workers?
            self.stop_worker(ctx, worker_id, Some("closing worker pool".to_string()));
        }
        // TODO: support timeout for worker manager stop
        let cleanup_leases = self
            .job_resources
            .values()
            .flat_map(|resources| resources.cleanup_leases.values().flatten().cloned())
            .collect::<Vec<_>>();
        let (stop_result, cleanup_result) = stop_worker_manager_and_cleanup_task_resources(
            self.worker_manager.as_ref(),
            self.options.artifact_transfer_timeout,
            move || activate_task_resource_cleanup_leases(cleanup_leases),
        )
        .await;
        if cleanup_result.is_ok() {
            self.job_resources.clear();
        }
        match (stop_result, cleanup_result) {
            (_, Err(error)) => Err(error),
            (Err(error), Ok(())) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    pub fn set_driver_server_port(&mut self, port: u16) {
        self.driver_server_port = Some(port);
    }

    pub fn start_worker(&mut self, ctx: &mut ActorContext<DriverActor>) {
        let Ok(worker_id) = self.worker_id_generator.next() else {
            error!("failed to generate worker ID");
            ctx.send(DriverEvent::Shutdown { history: None });
            return;
        };
        let descriptor = WorkerDescriptor {
            state: WorkerState::Pending,
            messages: vec![],
            peers: HashSet::new(),
            created_at: Utc::now(),
            stopped_at: None,
            resource_access_deadline: None,
        };
        self.workers.insert(worker_id, descriptor);
        ctx.send_with_delay(
            DriverEvent::ProbePendingWorker { worker_id },
            self.options.worker_launch_timeout,
        );
        // We create a placeholder span when starting the worker before creating the new trace.
        let span = Span::enter_with_local_parent("WorkerPool::start_worker")
            .with_property(|| (SpanAttribute::CLUSTER_WORKER_ID, worker_id.to_string()));
        let _guard = span.set_local_parent();
        // Create a new trace when starting the worker. Otherwise, the spans for the worker
        // may be nested in a query execution trace, which makes the trace harder to understand.
        // Note: We could have linked the span to the current trace,
        // but Fastrace currently does not support span links yet.
        let span = Span::root("WorkerPool::start_worker", SpanContext::random())
            .with_property(|| (SpanAttribute::CLUSTER_WORKER_ID, worker_id.to_string()));
        let _guard = span.set_local_parent();
        let Some(port) = self.driver_server_port else {
            error!("the driver server is not ready");
            return;
        };
        let options = WorkerLaunchOptions {
            enable_tls: self.options.enable_tls,
            driver_external_host: self.options.driver_external_host.to_string(),
            driver_external_port: if self.options.driver_external_port > 0 {
                self.options.driver_external_port
            } else {
                port
            },
            worker_heartbeat_interval: self.options.worker_heartbeat_interval,
            task_stream_buffer: self.options.task_stream_buffer,
            task_stream_creation_timeout: self.options.task_stream_creation_timeout,
            artifact_transfer_timeout: self.options.artifact_transfer_timeout,
            rpc_retry_strategy: self.options.rpc_retry_strategy.clone(),
        };
        let worker_manager = Arc::clone(&self.worker_manager);
        ctx.spawn(async move {
            if let Err(e) = worker_manager.launch_worker(worker_id, options).await {
                error!("failed to start worker {worker_id}: {e}");
            }
        });
    }

    pub fn register_worker(
        &mut self,
        ctx: &mut ActorContext<DriverActor>,
        worker_id: WorkerId,
        host: String,
        port: u16,
    ) -> ExecutionResult<()> {
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            return Err(ExecutionError::InvalidArgument(format!(
                "worker {worker_id} not found"
            )));
        };
        match worker.state {
            WorkerState::Pending => {
                worker.state = WorkerState::Running {
                    host,
                    port,
                    updated_at: Instant::now(),
                    heartbeat_at: Instant::now(),
                    client: None,
                };
                Self::schedule_lost_worker_probe(ctx, worker_id, worker, &self.options);
                Self::schedule_idle_worker_probe(ctx, worker_id, worker, &self.options);
                Ok(())
            }
            WorkerState::Running { .. } => Err(ExecutionError::InternalError(format!(
                "worker {worker_id} is already running"
            ))),
            WorkerState::Stopping { .. } => Err(ExecutionError::InternalError(format!(
                "worker {worker_id} is stopping"
            ))),
            WorkerState::Completed => Err(ExecutionError::InternalError(format!(
                "worker {worker_id} has completed"
            ))),
            WorkerState::Failed => Err(ExecutionError::InternalError(format!(
                "worker {worker_id} has failed"
            ))),
        }
    }

    pub fn stop_worker(
        &mut self,
        ctx: &mut ActorContext<DriverActor>,
        worker_id: WorkerId,
        reason: Option<String>,
    ) {
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            warn!("worker {worker_id} not found");
            return;
        };
        match worker.state {
            WorkerState::Pending => {
                warn!("trying to stop pending worker {worker_id}");
                worker.state = WorkerState::Completed;
                worker.stopped_at = Some(Utc::now());
                worker.resource_access_deadline = None;
                worker.messages.extend(reason);
            }
            WorkerState::Running { .. } => {
                info!("stopping worker {worker_id}");
                let client_set = match Self::get_client_set(worker_id, worker, &self.options) {
                    Ok(client) => client,
                    Err(e) => {
                        error!("failed to stop worker {worker_id}: {e}");
                        worker.state = WorkerState::Failed;
                        worker.stopped_at = Some(Utc::now());
                        worker.resource_access_deadline =
                            Some(Instant::now() + self.options.artifact_transfer_timeout);
                        return;
                    }
                };
                let (host, port) = match &worker.state {
                    WorkerState::Running { host, port, .. } => (host.clone(), *port),
                    _ => return,
                };
                worker.state = WorkerState::Stopping { host, port };
                worker.messages.extend(reason);
                let timeout = self.options.artifact_transfer_timeout;
                let retry_strategy = self.options.rpc_retry_strategy.clone();
                let client = client_set.core;
                let handle = ctx.handle().clone();
                ctx.spawn(async move {
                    let result = tokio::time::timeout(
                        timeout,
                        retry_strategy.run(|| {
                            let client = client.clone();
                            async move { client.stop_worker().await }
                        }),
                    )
                    .await
                    .unwrap_or_else(|_| {
                        Err(ExecutionError::InternalError(format!(
                            "timed out stopping worker {worker_id}"
                        )))
                    });
                    let _ = handle
                        .send(DriverEvent::WorkerStopFinished { worker_id, result })
                        .await;
                });
            }
            WorkerState::Stopping { .. } | WorkerState::Completed | WorkerState::Failed => {}
        }
    }

    pub fn finish_worker_stop(
        &mut self,
        ctx: &mut ActorContext<DriverActor>,
        worker_id: WorkerId,
        result: ExecutionResult<()>,
    ) {
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            warn!("worker {worker_id} not found after stop request");
            return;
        };
        if !matches!(worker.state, WorkerState::Stopping { .. }) {
            warn!("worker {worker_id} stop result is stale");
            return;
        }
        worker.stopped_at = Some(Utc::now());
        match result {
            Ok(()) => {
                worker.state = WorkerState::Completed;
                worker.resource_access_deadline = None;
            }
            Err(error) => {
                warn!("failed to stop worker {worker_id}: {error}");
                worker.state = WorkerState::Failed;
                worker.resource_access_deadline =
                    Some(Instant::now() + self.options.artifact_transfer_timeout);
            }
        }
        let jobs = self
            .job_resources
            .iter()
            .filter_map(|(job_id, resources)| {
                (resources.cleanup_requested && resources.workers.contains(&worker_id))
                    .then_some(*job_id)
            })
            .collect::<Vec<_>>();
        for job_id in jobs {
            self.try_start_job_resource_cleanup(ctx, job_id);
        }
    }

    /// Returns true if any worker is still launching (pending registration).
    ///
    /// A task stuck in `Created` should wait for such a worker rather than
    /// failing with a scheduling timeout: once the worker registers,
    /// `handle_register_worker` runs the pending tasks and can assign it. A
    /// worker that never registers is bounded by `worker_launch_timeout`
    /// (`fail_worker_if_pending`), after which it leaves the `Pending` state, so
    /// this cannot keep a task alive forever.
    pub fn has_pending_workers(&self) -> bool {
        self.workers
            .values()
            .any(|worker| matches!(worker.state, WorkerState::Pending))
    }

    fn list_running_workers(&self) -> Vec<WorkerLocation> {
        self.workers
            .iter()
            .filter_map(|(&worker_id, worker)| {
                if let WorkerState::Running { host, port, .. } = &worker.state {
                    Some(WorkerLocation {
                        worker_id,
                        host: host.clone(),
                        port: *port,
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn update_worker_heartbeat(
        &mut self,
        ctx: &mut ActorContext<DriverActor>,
        worker_id: WorkerId,
    ) {
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            warn!("worker {worker_id} not found");
            return;
        };
        if let WorkerState::Running { heartbeat_at, .. } = &mut worker.state {
            *heartbeat_at = Instant::now();
            Self::schedule_lost_worker_probe(ctx, worker_id, worker, &self.options);
        }
    }

    pub fn update_worker_known_peers(
        &mut self,
        worker_id: WorkerId,
        peer_worker_ids: Vec<WorkerId>,
    ) {
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            warn!("worker {worker_id} not found");
            return;
        };
        worker.peers.extend(peer_worker_ids);
    }

    pub fn fail_worker_if_pending(&mut self, worker_id: WorkerId) -> bool {
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            warn!("worker {worker_id} not found");
            return false;
        };
        if matches!(&worker.state, WorkerState::Pending) {
            warn!("worker {worker_id} registration timeout");
            let message = "worker registration timeout".to_string();
            worker.state = WorkerState::Failed;
            worker.resource_access_deadline = Some(Instant::now());
            worker.messages.push(message);
            true
        } else {
            false
        }
    }

    pub fn get_worker_last_update(&mut self, worker_id: WorkerId) -> Option<Instant> {
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            warn!("worker {worker_id} not found");
            return None;
        };
        match &worker.state {
            WorkerState::Running { updated_at, .. } => Some(*updated_at),
            _ => None,
        }
    }

    pub fn get_worker_last_heartbeat(&mut self, worker_id: WorkerId) -> Option<Instant> {
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            warn!("worker {worker_id} not found");
            return None;
        };
        match &worker.state {
            WorkerState::Running { heartbeat_at, .. } => Some(*heartbeat_at),
            _ => None,
        }
    }

    /// Dispatches a task to a specific worker by sending the task definition over gRPC.
    pub fn run_task(
        &mut self,
        ctx: &mut ActorContext<DriverActor>,
        worker_id: WorkerId,
        key: TaskKey,
        definition: TaskDefinition,
        launch_context: TaskLaunchContext,
    ) {
        let running_workers = self.list_running_workers();
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            let message = format!("worker {} not found", worker_id);
            let cause = CommonErrorCause::Internal(message.clone());
            ctx.send(DriverEvent::UpdateTask {
                key,
                status: TaskStatus::Failed,
                message: Some(message),
                cause: Some(cause),
                sequence: None,
            });
            return;
        };
        Self::track_worker_activity(ctx, worker_id, worker, &self.options);
        let client = match Self::get_client_set(worker_id, worker, &self.options) {
            Ok(client) => client.core,
            Err(e) => {
                let message = format!("failed to get worker {} client: {e}", worker_id);
                let cause = CommonErrorCause::new::<PyErrExtractor>(&e);
                ctx.send(DriverEvent::UpdateTask {
                    key,
                    status: TaskStatus::Failed,
                    message: Some(message),
                    cause: Some(cause),
                    sequence: None,
                });
                return;
            }
        };
        match &mut worker.state {
            WorkerState::Running { .. } => {}
            _ => {
                let message = format!(
                    "cannot assign {} to worker {} that is not running",
                    TaskKeyDisplay(&key),
                    worker_id
                );
                let cause = CommonErrorCause::Internal(message.clone());
                ctx.send(DriverEvent::UpdateTask {
                    key,
                    status: TaskStatus::Failed,
                    message: Some(message),
                    cause: Some(cause),
                    sequence: None,
                });
                return;
            }
        }
        let peers = running_workers
            .into_iter()
            .filter(|x| !worker.peers.contains(&x.worker_id))
            .collect();
        if !self.begin_job_request(key.job_id, worker_id) {
            return;
        }
        let job_id = key.job_id;
        let rpc_timeout = self.options.artifact_transfer_timeout;
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            let result = tokio::time::timeout(
                rpc_timeout,
                client.run_task(key.clone(), definition, launch_context, peers),
            )
            .await
            .unwrap_or_else(|_| {
                Err(ExecutionError::InternalError(format!(
                    "timed out dispatching {}",
                    TaskKeyDisplay(&key)
                )))
            });
            if let Err(e) = result {
                let _ = handle
                    .send(DriverEvent::UpdateTask {
                        key,
                        status: TaskStatus::Failed,
                        message: Some(format!("failed to run task via the worker client: {e}")),
                        cause: Some(CommonErrorCause::new::<PyErrExtractor>(&e)),
                        sequence: None,
                    })
                    .await;
            }
            let _ = handle
                .send(DriverEvent::WorkerJobRequestFinished { job_id })
                .await;
        });
    }

    pub fn stop_task(
        &mut self,
        ctx: &mut ActorContext<DriverActor>,
        worker_id: WorkerId,
        key: &TaskKey,
    ) {
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            warn!("worker {worker_id} not found");
            return;
        };
        Self::track_worker_activity(ctx, worker_id, worker, &self.options);
        let client = match Self::get_client_set(worker_id, worker, &self.options) {
            Ok(x) => x.core,
            Err(e) => {
                error!(
                    "failed to stop {} in worker {worker_id}: {e}",
                    TaskKeyDisplay(key)
                );
                return;
            }
        };
        let key = key.clone();
        if !self.begin_job_request(key.job_id, worker_id) {
            return;
        }
        let rpc_timeout = self.options.artifact_transfer_timeout;
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            let result = tokio::time::timeout(rpc_timeout, client.stop_task(key.clone()))
                .await
                .unwrap_or_else(|_| {
                    Err(ExecutionError::InternalError(format!(
                        "timed out stopping {}",
                        TaskKeyDisplay(&key)
                    )))
                });
            if let Err(e) = result {
                error!("failed to stop {}: {e}", TaskKeyDisplay(&key));
            }
            let _ = handle
                .send(DriverEvent::WorkerJobRequestFinished { job_id: key.job_id })
                .await;
        });
    }

    pub fn fetch_task_stream(
        &mut self,
        ctx: &mut ActorContext<DriverActor>,
        worker_id: WorkerId,
        key: &TaskStreamKey,
        schema: SchemaRef,
    ) -> ExecutionResult<TaskStreamSource> {
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            return Err(ExecutionError::InvalidArgument(format!(
                "worker {worker_id} not found"
            )));
        };
        Self::track_worker_activity(ctx, worker_id, worker, &self.options);
        let client = match Self::get_client_set(worker_id, worker, &self.options) {
            Ok(x) => x.flight,
            Err(e) => {
                return Err(ExecutionError::InternalError(format!(
                    "failed to get client for worker {worker_id}: {e}"
                )));
            }
        };
        let key = key.clone();
        let stream = futures::stream::once(async move {
            client
                .fetch_task_stream(key, schema.clone())
                .await
                .map_err(|e| TaskStreamError::External(Arc::new(e)))
        })
        .try_flatten();
        Ok(Box::pin(stream))
    }

    pub fn clean_up_job(
        &mut self,
        ctx: &mut ActorContext<DriverActor>,
        worker_id: WorkerId,
        job_id: JobId,
        stage: Option<usize>,
    ) {
        if stage.is_none() {
            self.request_job_resource_cleanup(ctx, job_id);
            return;
        }
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            warn!("worker {worker_id} not found");
            return;
        };
        Self::track_worker_activity(ctx, worker_id, worker, &self.options);
        let client = match Self::get_client_set(worker_id, worker, &self.options) {
            Ok(client) => client.core,
            Err(error) => {
                error!("failed to clean up job in worker {worker_id}: {error}");
                return;
            }
        };
        if !self.begin_job_request(job_id, worker_id) {
            return;
        }
        let handle = ctx.handle().clone();
        let retry_strategy = self.options.rpc_retry_strategy.clone();
        let rpc_timeout = self.options.artifact_transfer_timeout;
        ctx.spawn(async move {
            let result = tokio::time::timeout(
                rpc_timeout,
                retry_strategy.run(|| {
                    let client = client.clone();
                    async move { client.clean_up_job(job_id, stage).await }
                }),
            )
            .await
            .unwrap_or_else(|_| {
                Err(ExecutionError::InternalError(format!(
                    "timed out cleaning up job {job_id} stage {stage:?} in worker {worker_id}"
                )))
            });
            if let Err(error) = result {
                error!("failed to clean up job in worker {worker_id}: {error}");
            }
            let _ = handle
                .send(DriverEvent::WorkerJobRequestFinished { job_id })
                .await;
        });
    }

    fn get_client_set(
        worker_id: WorkerId,
        worker: &mut WorkerDescriptor,
        options: &WorkerPoolOptions,
    ) -> ExecutionResult<WorkerClientSet> {
        match &mut worker.state {
            WorkerState::Running {
                host, port, client, ..
            } => {
                let client = client.get_or_insert_with(|| {
                    let options = ClientOptions {
                        enable_tls: options.enable_tls,
                        host: host.clone(),
                        port: *port,
                    };
                    WorkerClientSet::new(options)
                });
                Ok(client.clone())
            }
            _ => Err(ExecutionError::InternalError(format!(
                "cannot get client for inactive worker: {worker_id}"
            ))),
        }
    }

    fn schedule_idle_worker_probe(
        ctx: &mut ActorContext<DriverActor>,
        worker_id: WorkerId,
        worker: &mut WorkerDescriptor,
        options: &WorkerPoolOptions,
    ) {
        let WorkerState::Running { updated_at, .. } = &worker.state else {
            warn!("worker {worker_id} is not running");
            return;
        };
        ctx.send_with_delay(
            DriverEvent::ProbeIdleWorker {
                worker_id,
                instant: *updated_at,
            },
            options.worker_max_idle_time,
        );
    }

    fn schedule_lost_worker_probe(
        ctx: &mut ActorContext<DriverActor>,
        worker_id: WorkerId,
        worker: &mut WorkerDescriptor,
        options: &WorkerPoolOptions,
    ) {
        let WorkerState::Running { heartbeat_at, .. } = &worker.state else {
            warn!("worker {worker_id} is not running");
            return;
        };
        ctx.send_with_delay(
            DriverEvent::ProbeLostWorker {
                worker_id,
                instant: *heartbeat_at,
            },
            options.worker_heartbeat_timeout,
        );
    }

    fn track_worker_activity(
        ctx: &mut ActorContext<DriverActor>,
        worker_id: WorkerId,
        worker: &mut WorkerDescriptor,
        options: &WorkerPoolOptions,
    ) {
        if let WorkerState::Running { updated_at, .. } = &mut worker.state {
            *updated_at = Instant::now();
            Self::schedule_idle_worker_probe(ctx, worker_id, worker, options);
        }
    }
}

fn worker_is_resource_quiesced(worker: &WorkerDescriptor, now: Instant) -> bool {
    matches!(worker.state, WorkerState::Completed)
        || matches!(worker.state, WorkerState::Failed)
            && worker
                .resource_access_deadline
                .is_some_and(|deadline| deadline <= now)
}

async fn await_worker_control_rpc<T>(
    operation: impl Future<Output = ExecutionResult<T>>,
    timeout: std::time::Duration,
    timeout_message: String,
) -> ExecutionResult<T> {
    tokio::time::timeout(timeout, operation)
        .await
        .unwrap_or_else(|_| Err(ExecutionError::InternalError(timeout_message)))
}

async fn stop_worker_manager_and_cleanup_task_resources(
    worker_manager: &dyn crate::worker_manager::WorkerManager,
    transfer_timeout: std::time::Duration,
    task_resources_ready: impl FnOnce() -> Vec<String>,
) -> (ExecutionResult<()>, ExecutionResult<()>) {
    let stop_margin = transfer_timeout.min(std::time::Duration::from_millis(250));
    let stop_timeout = transfer_timeout
        .saturating_mul(3)
        .saturating_add(stop_margin.saturating_mul(3));
    let stop_result =
        stop_worker_manager_with_retries(worker_manager, Instant::now() + stop_timeout).await;
    if stop_result.is_err() {
        tokio::time::sleep(transfer_timeout).await;
    }
    let task_resource_uris = task_resources_ready();
    let failed_uris = retry_task_resource_deletions(
        task_resource_uris,
        transfer_timeout,
        Instant::now() + transfer_timeout,
    )
    .await;
    let cleanup_result = if failed_uris.is_empty() {
        Ok(())
    } else {
        let retry_for = transfer_timeout;
        match tokio::task::spawn_blocking(move || {
            handoff_task_resource_cleanup_blocking(failed_uris, retry_for, transfer_timeout)
        })
        .await
        {
            Ok(result) => result,
            Err(error) => Err(ExecutionError::InternalError(format!(
                "failed to join task resource cleanup handoff: {error}"
            ))),
        }
    };
    (stop_result, cleanup_result)
}

fn activate_task_resource_cleanup_leases(
    cleanup_leases: Vec<Arc<TaskResourceCleanupLease>>,
) -> Vec<String> {
    let mut leases_by_uri = HashMap::<String, Vec<Arc<TaskResourceCleanupLease>>>::new();
    for lease in cleanup_leases {
        leases_by_uri
            .entry(lease.uri().to_string())
            .or_default()
            .push(lease);
    }
    for leases in leases_by_uri.values() {
        leases.iter().for_each(|lease| lease.activate());
    }
    leases_by_uri.into_keys().collect()
}

async fn stop_worker_manager_with_retries(
    worker_manager: &dyn crate::worker_manager::WorkerManager,
    deadline: Instant,
) -> ExecutionResult<()> {
    let mut retry_delay = std::time::Duration::from_millis(10);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(ExecutionError::InternalError(
                "timed out stopping worker manager".to_string(),
            ));
        }
        match tokio::time::timeout(remaining, worker_manager.stop()).await {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(error)) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(error);
                }
                tokio::time::sleep(retry_delay.min(remaining)).await;
                retry_delay = retry_delay
                    .saturating_mul(2)
                    .min(std::time::Duration::from_millis(250));
            }
            Err(_) => {
                return Err(ExecutionError::InternalError(
                    "timed out stopping worker manager".to_string(),
                ));
            }
        }
    }
}

async fn retry_task_resource_deletions(
    mut uris: Vec<String>,
    transfer_timeout: std::time::Duration,
    deadline: Instant,
) -> Vec<String> {
    let mut retry_delay = std::time::Duration::from_millis(10);
    loop {
        uris = delete_task_resource_uris(uris, transfer_timeout, deadline).await;
        if uris.is_empty() || Instant::now() >= deadline {
            return uris;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        tokio::time::sleep(retry_delay.min(remaining)).await;
        retry_delay = retry_delay
            .saturating_mul(2)
            .min(std::time::Duration::from_secs(1));
    }
}

async fn delete_task_resource_uris(
    uris: Vec<String>,
    operation_timeout: std::time::Duration,
    deadline: Instant,
) -> Vec<String> {
    delete_task_resource_uris_with(uris, operation_timeout, deadline, |uri| async move {
        delete_task_resource_uri(&uri).await
    })
    .await
}

async fn delete_task_resource_uris_with<F, Fut>(
    uris: Vec<String>,
    operation_timeout: std::time::Duration,
    deadline: Instant,
    delete: F,
) -> Vec<String>
where
    F: Fn(String) -> Fut + Clone,
    Fut: Future<Output = Result<(), String>>,
{
    const DELETE_CONCURRENCY: usize = 16;

    let mut pending = uris.into_iter().collect::<VecDeque<_>>();
    let mut active = futures::stream::FuturesUnordered::new();
    let mut active_uris = HashSet::new();
    let mut failed = Vec::new();
    loop {
        while active.len() < DELETE_CONCURRENCY && Instant::now() < deadline {
            let Some(uri) = pending.pop_front() else {
                break;
            };
            active_uris.insert(uri.clone());
            let operation = delete.clone()(uri.clone());
            let timeout = operation_timeout.min(deadline.saturating_duration_since(Instant::now()));
            active.push(async move {
                let deleted = await_task_resource_delete(operation, timeout).await;
                (uri, deleted)
            });
        }
        if active.is_empty() {
            failed.extend(pending);
            return failed;
        }
        match tokio::time::timeout_at(deadline, active.next()).await {
            Ok(Some((uri, deleted))) => {
                active_uris.remove(&uri);
                if !deleted {
                    failed.push(uri);
                }
            }
            Ok(None) => {}
            Err(_) => {
                failed.extend(active_uris);
                failed.extend(pending);
                return failed;
            }
        }
    }
}

async fn delete_task_resource_uri(uri: &str) -> Result<(), String> {
    let url = Url::parse(uri).map_err(|error| error.to_string())?;
    let (_scheme, path) = ObjectStoreScheme::parse(&url).map_err(|error| error.to_string())?;
    let store =
        sail_object_store::get_dynamic_object_store(&url).map_err(|error| error.to_string())?;
    match store.delete(&path).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(error) => Err(error.to_string()),
    }
}

async fn await_task_resource_delete(
    operation: impl Future<Output = Result<(), String>>,
    timeout: std::time::Duration,
) -> bool {
    matches!(tokio::time::timeout(timeout, operation).await, Ok(Ok(())))
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::worker_manager::WorkerManager;

    struct StopFailingWorkerManager;
    struct HangingWorkerManager;

    #[tonic::async_trait]
    impl WorkerManager for StopFailingWorkerManager {
        async fn launch_worker(
            &self,
            _id: WorkerId,
            _options: WorkerLaunchOptions,
        ) -> ExecutionResult<()> {
            Ok(())
        }

        async fn stop(&self) -> ExecutionResult<()> {
            Err(ExecutionError::InternalError(
                "worker manager stop failed".to_string(),
            ))
        }
    }

    #[tonic::async_trait]
    impl WorkerManager for HangingWorkerManager {
        async fn launch_worker(
            &self,
            _id: WorkerId,
            _options: WorkerLaunchOptions,
        ) -> ExecutionResult<()> {
            Ok(())
        }

        async fn stop(&self) -> ExecutionResult<()> {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn task_resource_cleanup_runs_when_worker_manager_stop_fails() {
        let temp = tempfile::tempdir().unwrap();
        let resource = temp.path().join("resource");
        std::fs::write(&resource, b"test").unwrap();
        let uri = url::Url::from_file_path(&resource).unwrap().to_string();

        let (stop_result, cleanup_result) = stop_worker_manager_and_cleanup_task_resources(
            &StopFailingWorkerManager,
            std::time::Duration::from_millis(20),
            || vec![uri],
        )
        .await;

        assert!(matches!(stop_result, Err(ExecutionError::InternalError(_))));
        assert!(cleanup_result.is_ok());
        assert!(!resource.exists());
    }

    #[test]
    fn job_cleanup_waits_for_staging_completion_and_rejects_late_dispatch() {
        let mut resources = JobExecutionResources {
            pending_requests: 1,
            cleanup_requested: true,
            ..Default::default()
        };

        assert!(!resources.accepts_requests());
        assert!(!resources.ready_for_cleanup());

        resources.pending_requests = 0;
        assert!(resources.ready_for_cleanup());

        let delays = (0..8)
            .map(|_| resources.next_cleanup_retry_delay())
            .collect::<Vec<_>>();
        assert_eq!(delays[0], std::time::Duration::from_secs(1));
        assert_eq!(delays[7], std::time::Duration::from_secs(64));
    }

    #[test]
    fn expired_job_cleanup_deadline_releases_running_worker_barrier() {
        let now = Instant::now();
        let mut resources = JobExecutionResources {
            cleanup_requested: true,
            cleanup_deadline: Some(now + std::time::Duration::from_secs(1)),
            ..Default::default()
        };

        assert!(resources.requires_worker_barrier(now));
        resources.cleanup_deadline = Some(now);
        assert!(!resources.requires_worker_barrier(now));
        assert!(resources.ready_for_cleanup());
    }

    #[tokio::test]
    async fn failed_task_resource_deletions_are_returned_for_retry() {
        let uri = "not-a-task-resource-uri".to_string();
        let failed = delete_task_resource_uris(
            vec![uri.clone()],
            std::time::Duration::from_secs(1),
            Instant::now() + std::time::Duration::from_secs(1),
        )
        .await;

        assert_eq!(failed, vec![uri]);
    }

    #[tokio::test]
    async fn hanging_task_resource_delete_is_bounded() {
        let deleted = await_task_resource_delete(
            std::future::pending::<Result<(), String>>(),
            std::time::Duration::from_millis(10),
        )
        .await;

        assert!(!deleted);
    }

    #[tokio::test]
    async fn aggregate_delete_deadline_retains_more_than_one_batch() {
        let uris = (0..33)
            .map(|index| format!("uri-{index}"))
            .collect::<Vec<_>>();
        let started = Instant::now();
        let deadline = started + std::time::Duration::from_millis(30);
        let failed = delete_task_resource_uris_with(
            uris.clone(),
            std::time::Duration::from_secs(10),
            deadline,
            |_| std::future::pending::<Result<(), String>>(),
        )
        .await;

        assert_eq!(failed.len(), uris.len());
        assert!(started.elapsed() < std::time::Duration::from_millis(250));
    }

    #[tokio::test]
    async fn hanging_worker_manager_stop_is_bounded() {
        let (stop_result, cleanup_result) = stop_worker_manager_and_cleanup_task_resources(
            &HangingWorkerManager,
            std::time::Duration::from_millis(10),
            Vec::new,
        )
        .await;

        assert!(stop_result.is_err());
        assert!(stop_result.unwrap_err().to_string().contains("timed out"));
        assert!(cleanup_result.is_ok());
    }

    #[tokio::test]
    async fn hanging_worker_cleanup_rpc_is_bounded() {
        let result = await_worker_control_rpc(
            std::future::pending::<ExecutionResult<()>>(),
            std::time::Duration::from_millis(10),
            "cleanup RPC timed out".to_string(),
        )
        .await;

        assert!(result.unwrap_err().to_string().contains("timed out"));
    }

    #[test]
    fn completed_and_expired_failed_workers_are_resource_quiesced() {
        let now = Instant::now();
        let descriptor = |state, resource_access_deadline| WorkerDescriptor {
            state,
            messages: vec![],
            peers: HashSet::new(),
            created_at: Utc::now(),
            stopped_at: None,
            resource_access_deadline,
        };

        assert!(worker_is_resource_quiesced(
            &descriptor(WorkerState::Completed, None),
            now,
        ));
        assert!(!worker_is_resource_quiesced(
            &descriptor(
                WorkerState::Stopping {
                    host: "localhost".to_string(),
                    port: 1,
                },
                None,
            ),
            now,
        ));
        assert!(!worker_is_resource_quiesced(
            &descriptor(
                WorkerState::Failed,
                Some(now + std::time::Duration::from_secs(1)),
            ),
            now,
        ));
        assert!(worker_is_resource_quiesced(
            &descriptor(WorkerState::Failed, Some(now)),
            now,
        ));
    }
}
