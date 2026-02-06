use std::collections::HashSet;
use std::sync::Arc;

use chrono::Utc;
use datafusion::arrow::datatypes::SchemaRef;
use fastrace::collector::SpanContext;
use fastrace::Span;
use futures::TryStreamExt;
use log::{error, info, warn};
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use sail_server::actor::ActorContext;
use sail_telemetry::common::SpanAttribute;
use tokio::time::Instant;

use crate::driver::worker_pool::state::WorkerState;
use crate::driver::worker_pool::{WorkerDescriptor, WorkerPool, WorkerPoolOptions};
use crate::driver::{DriverActor, DriverEvent, TaskStatus};
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskKey, TaskKeyDisplay, TaskStreamKey, WorkerId};
use crate::rpc::ClientOptions;
use crate::stream::error::TaskStreamError;
use crate::stream::reader::TaskStreamSource;
use crate::task::definition::TaskDefinition;
use crate::worker::{WorkerClientSet, WorkerLocation};
use crate::worker_manager::WorkerLaunchOptions;

impl WorkerPool {
    pub async fn close(&mut self, ctx: &mut ActorContext<DriverActor>) -> ExecutionResult<()> {
        let worker_ids = self.workers.keys().cloned().collect::<Vec<_>>();
        for worker_id in worker_ids.into_iter() {
            // TODO: Should we wait for the spawned tasks for stopping the workers?
            self.stop_worker(ctx, worker_id, Some("closing worker pool".to_string()));
        }
        // TODO: support timeout for worker manager stop
        self.worker_manager.stop().await?;
        Ok(())
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
                worker.messages.extend(reason);
            }
            WorkerState::Running { .. } => {
                info!("stopping worker {worker_id}");
                let client = match Self::get_client_set(worker_id, worker, &self.options) {
                    Ok(x) => x.core,
                    Err(e) => {
                        error!("failed to stop worker {worker_id}: {e}");
                        worker.state = WorkerState::Failed;
                        worker.stopped_at = Some(Utc::now());
                        return;
                    }
                };
                ctx.spawn(async move {
                    if let Err(e) = client.stop_worker().await {
                        error!("failed to stop worker {worker_id}: {e}");
                    }
                });
                worker.state = WorkerState::Completed;
                worker.stopped_at = Some(Utc::now());
                worker.messages.extend(reason);
            }
            WorkerState::Completed | WorkerState::Failed => {}
        }
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

    pub fn run_task(
        &mut self,
        ctx: &mut ActorContext<DriverActor>,
        worker_id: WorkerId,
        key: TaskKey,
        definition: TaskDefinition,
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
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            if let Err(e) = client.run_task(key.clone(), definition, peers).await {
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
        ctx.spawn(async move {
            if let Err(e) = client.stop_task(key.clone()).await {
                error!("failed to stop {}: {e}", TaskKeyDisplay(&key));
            }
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
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            warn!("worker {worker_id} not found");
            return;
        };
        Self::track_worker_activity(ctx, worker_id, worker, &self.options);
        Self::clean_up_job_for_worker(ctx, job_id, stage, worker_id, worker, &self.options);
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

    fn clean_up_job_for_worker(
        ctx: &mut ActorContext<DriverActor>,
        job_id: JobId,
        stage: Option<usize>,
        worker_id: WorkerId,
        worker: &mut WorkerDescriptor,
        options: &WorkerPoolOptions,
    ) {
        let client = match Self::get_client_set(worker_id, worker, options) {
            Ok(x) => x.core,
            Err(e) => {
                error!("failed to clean up job in worker {worker_id}: {e}");
                return;
            }
        };
        ctx.spawn(async move {
            if let Err(e) = client.clean_up_job(job_id, stage).await {
                error!("failed to clean up job in worker {worker_id}: {e}");
            }
        });
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
