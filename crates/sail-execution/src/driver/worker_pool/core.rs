use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use fastrace::collector::SpanContext;
use fastrace::Span;
use futures::future::try_join_all;
use futures::TryStreamExt;
use log::{error, info, warn};
use prost::bytes::BytesMut;
use prost::Message;
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use sail_server::actor::ActorContext;
use sail_telemetry::common::SpanAttribute;
use tokio::time::Instant;

use crate::driver::job_scheduler::{JobOutputMetadata, TaskSchedule, TaskSchedulePlan};
use crate::driver::worker_pool::state::WorkerState;
use crate::driver::worker_pool::{
    WorkerDescriptor, WorkerIdle, WorkerLost, WorkerPool, WorkerPoolOptions, WorkerTimeout,
};
use crate::driver::{DriverActor, DriverEvent, TaskStatus};
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{JobId, TaskInstance, WorkerId};
use crate::rpc::ClientOptions;
use crate::stream::merge::MergedRecordBatchStream;
use crate::worker::{WorkerClient, WorkerLocation};
use crate::worker_manager::WorkerLaunchOptions;

impl WorkerPool {
    pub async fn close(mut self, ctx: &mut ActorContext<DriverActor>) -> ExecutionResult<()> {
        self.stop_all_workers(ctx);
        // TODO: support timeout for worker manager stop
        self.worker_manager.stop().await?;
        Ok(())
    }

    pub fn set_driver_server_port(&mut self, port: u16) {
        self.driver_server_port = Some(port);
    }

    pub fn start_worker(&mut self, ctx: &mut ActorContext<DriverActor>) {
        let Ok(worker_id) = self.worker_id_generator.next() else {
            error!("failed to generate worker IDs");
            ctx.send(DriverEvent::Shutdown);
            return;
        };
        let descriptor = WorkerDescriptor {
            state: WorkerState::Pending,
            messages: vec![],
            peers: HashSet::new(),
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
            worker_stream_buffer: self.options.worker_stream_buffer,
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
                    tasks: Default::default(),
                    jobs: Default::default(),
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
            WorkerState::Stopped => Err(ExecutionError::InternalError(format!(
                "worker {worker_id} has stopped"
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
                worker.state = WorkerState::Stopped;
                worker.messages.extend(reason);
            }
            WorkerState::Running { .. } => {
                info!("stopping worker {worker_id}");
                let client = match Self::get_worker_client(worker_id, worker, &self.options) {
                    Ok(x) => x,
                    Err(e) => {
                        warn!("failed to stop worker {worker_id}: {e}");
                        return;
                    }
                };
                ctx.spawn(async move {
                    if let Err(e) = client.stop_worker().await {
                        error!("failed to stop worker {worker_id}: {e}");
                    }
                });
                worker.state = WorkerState::Stopped;
                worker.messages.extend(reason);
            }
            WorkerState::Stopped | WorkerState::Failed => {}
        }
    }

    fn stop_all_workers(&mut self, ctx: &mut ActorContext<DriverActor>) {
        let worker_ids = self.workers.keys().cloned().collect::<Vec<_>>();
        let reason = "stopping all workers".to_string();
        for worker_id in worker_ids.into_iter() {
            self.stop_worker(ctx, worker_id, Some(reason.clone()));
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

    pub fn count_active_workers(&self) -> usize {
        self.workers
            .values()
            .filter(|worker| {
                matches!(
                    worker.state,
                    WorkerState::Pending | WorkerState::Running { .. }
                )
            })
            .count()
    }

    pub fn find_idle_task_slots(&self) -> Vec<(WorkerId, usize)> {
        self.workers
            .iter()
            .filter_map(|(id, worker)| {
                let count = match &worker.state {
                    WorkerState::Running { tasks, .. } => {
                        self.options.worker_task_slots.saturating_sub(tasks.len())
                    }
                    _ => 0,
                };
                if count > 0 {
                    Some((*id, count))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }

    pub fn record_worker_heartbeat(
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

    pub fn record_worker_known_peers(
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

    pub fn probe_pending_worker(&mut self, worker_id: WorkerId) -> WorkerTimeout {
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            warn!("worker {worker_id} not found");
            return WorkerTimeout::No;
        };
        if matches!(&worker.state, WorkerState::Pending) {
            warn!("worker {worker_id} registration timeout");
            let message = "worker registration timeout".to_string();
            worker.state = WorkerState::Failed;
            worker.messages.push(message);
            WorkerTimeout::Yes
        } else {
            WorkerTimeout::No
        }
    }

    pub fn probe_idle_worker(&mut self, worker_id: WorkerId, instant: Instant) -> WorkerIdle {
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            warn!("worker {worker_id} not found");
            return WorkerIdle::No;
        };
        if let WorkerState::Running {
            tasks,
            jobs,
            updated_at,
            ..
        } = &worker.state
        {
            if tasks.is_empty() && jobs.is_empty() && *updated_at <= instant {
                return WorkerIdle::Yes;
            }
        }
        WorkerIdle::No
    }

    pub fn probe_lost_worker(&mut self, worker_id: WorkerId, instant: Instant) -> WorkerLost {
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            warn!("worker {worker_id} not found");
            return WorkerLost::No;
        };
        if let WorkerState::Running { heartbeat_at, .. } = &worker.state {
            if *heartbeat_at <= instant {
                return WorkerLost::Yes;
            }
        }
        WorkerLost::No
    }

    pub fn find_tasks_for_worker(&self, worker_id: WorkerId) -> Vec<TaskInstance> {
        let Some(worker) = self.workers.get(&worker_id) else {
            warn!("worker {worker_id} not found");
            return vec![];
        };
        match &worker.state {
            WorkerState::Running { tasks, .. } => tasks.iter().cloned().collect(),
            _ => vec![],
        }
    }

    pub fn run_task(&mut self, ctx: &mut ActorContext<DriverActor>, schedule: TaskSchedule) {
        let running_worker_locations = self.list_running_workers();
        let plan = match schedule.plan {
            TaskSchedulePlan::Valid(plan) => plan,
            TaskSchedulePlan::Invalid { message, cause } => {
                ctx.send(DriverEvent::UpdateTask {
                    instance: schedule.instance,
                    status: TaskStatus::Failed,
                    message: Some(message),
                    cause,
                    sequence: None,
                });
                return;
            }
        };
        let Some(worker) = self.workers.get_mut(&schedule.worker_id) else {
            let message = format!("worker {} not found", schedule.worker_id);
            ctx.send(DriverEvent::UpdateTask {
                instance: schedule.instance,
                status: TaskStatus::Failed,
                message: Some(message),
                cause: None,
                sequence: None,
            });
            return;
        };
        let client = match Self::get_worker_client(schedule.worker_id, worker, &self.options) {
            Ok(client) => client,
            Err(e) => {
                let message = format!("failed to get worker {} client: {e}", schedule.worker_id);
                let cause = CommonErrorCause::new::<PyErrExtractor>(&e);
                ctx.send(DriverEvent::UpdateTask {
                    instance: schedule.instance,
                    status: TaskStatus::Failed,
                    message: Some(message),
                    cause: Some(cause),
                    sequence: None,
                });
                return;
            }
        };
        match &mut worker.state {
            WorkerState::Running {
                tasks,
                jobs,
                updated_at,
                ..
            } => {
                tasks.insert(schedule.instance.clone());
                jobs.insert(schedule.instance.job_id);
                *updated_at = Instant::now();
            }
            _ => {
                let message = format!(
                    "cannot assign job {} task {} attempt {} to worker {} that is not running",
                    schedule.instance.job_id,
                    schedule.instance.task_id,
                    schedule.instance.attempt,
                    schedule.worker_id
                );
                ctx.send(DriverEvent::UpdateTask {
                    instance: schedule.instance,
                    status: TaskStatus::Failed,
                    message: Some(message),
                    cause: None,
                    sequence: None,
                });
                return;
            }
        }
        let peers = running_worker_locations
            .into_iter()
            .filter(|x| !worker.peers.contains(&x.worker_id))
            .collect();
        let plan = match self.encode_plan(plan) {
            Ok(plan) => plan,
            Err(e) => {
                let message = format!(
                    "failed to encode plan for job {} task {}: {e}",
                    schedule.instance.job_id, schedule.instance.task_id
                );
                let cause = CommonErrorCause::new::<PyErrExtractor>(&e);
                ctx.send(DriverEvent::UpdateTask {
                    instance: schedule.instance,
                    status: TaskStatus::Failed,
                    message: Some(message),
                    cause: Some(cause),
                    sequence: None,
                });
                return;
            }
        };
        let handle = ctx.handle().clone();
        ctx.spawn(async move {
            if let Err(e) = client
                .run_task(
                    schedule.instance.job_id,
                    schedule.instance.task_id,
                    schedule.instance.attempt,
                    plan,
                    schedule.partition,
                    schedule.channel,
                    peers,
                )
                .await
            {
                let _ = handle
                    .send(DriverEvent::UpdateTask {
                        instance: schedule.instance,
                        status: TaskStatus::Failed,
                        message: Some(format!("failed to run task via the worker client: {e}")),
                        cause: None,
                        sequence: None,
                    })
                    .await;
            }
        });
    }

    pub fn cancel_task(&mut self, ctx: &mut ActorContext<DriverActor>, instance: &TaskInstance) {
        let Some(worker_id) = self.detach_task(ctx, instance) else {
            return;
        };
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            warn!("worker {worker_id} not found");
            return;
        };
        let client = match Self::get_worker_client(worker_id, worker, &self.options) {
            Ok(x) => x,
            Err(e) => {
                warn!(
                    "failed to cancel job {} task {} attempt {} in worker {worker_id}: {e}",
                    instance.job_id, instance.task_id, instance.attempt
                );
                return;
            }
        };
        let instance = instance.clone();
        ctx.spawn(async move {
            if let Err(e) = client
                .stop_task(instance.job_id, instance.task_id, instance.attempt)
                .await
            {
                warn!(
                    "failed to stop job {} task {} attempt {}: {e}",
                    instance.job_id, instance.task_id, instance.attempt
                );
            }
        });
    }

    pub fn detach_task(
        &mut self,
        ctx: &mut ActorContext<DriverActor>,
        instance: &TaskInstance,
    ) -> Option<WorkerId> {
        for (&worker_id, worker) in self.workers.iter_mut() {
            if let WorkerState::Running {
                tasks, updated_at, ..
            } = &mut worker.state
            {
                if tasks.remove(instance) {
                    *updated_at = Instant::now();
                    Self::schedule_idle_worker_probe(ctx, worker_id, worker, &self.options);
                    return Some(worker_id);
                }
            }
        }
        None
    }

    pub fn detach_job(&mut self, ctx: &mut ActorContext<DriverActor>, job_id: JobId) {
        for (&worker_id, worker) in self.workers.iter_mut() {
            if let WorkerState::Running {
                jobs, updated_at, ..
            } = &mut worker.state
            {
                if !jobs.remove(&job_id) {
                    continue;
                }
                *updated_at = Instant::now();
                Self::schedule_idle_worker_probe(ctx, worker_id, worker, &self.options);
                Self::remove_worker_streams(ctx, job_id, worker_id, worker, &self.options);
            }
        }
    }

    fn encode_plan(&self, plan: Arc<dyn ExecutionPlan>) -> ExecutionResult<Vec<u8>> {
        let plan =
            PhysicalPlanNode::try_from_physical_plan(plan, self.physical_plan_codec.as_ref())?;
        let mut buffer = BytesMut::new();
        plan.encode(&mut buffer)?;
        Ok(buffer.freeze().into())
    }

    pub fn build_job_output_stream(
        &mut self,
        metadata: JobOutputMetadata,
    ) -> ExecutionResult<SendableRecordBatchStream> {
        let channels = metadata
            .channels
            .into_iter()
            .map(|channel| {
                let Some(worker) = self.workers.get_mut(&channel.worker_id) else {
                    return Err(ExecutionError::InternalError(format!(
                        "worker {} not found for job output stream",
                        channel.worker_id
                    )));
                };
                let client = Self::get_worker_client(channel.worker_id, worker, &self.options)?;
                Ok((channel.channel, client))
            })
            .collect::<ExecutionResult<Vec<_>>>()?;
        let schema = metadata.schema.clone();
        let output = futures::stream::once(async move {
            let futures = channels.into_iter().map(|(channel, client)| {
                let channel_schema = schema.clone();
                async move { client.fetch_task_stream(channel, channel_schema).await }
            });
            let streams = try_join_all(futures)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            Ok(Box::pin(MergedRecordBatchStream::new(schema, streams)))
                as Result<_, DataFusionError>
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            metadata.schema,
            output,
        )))
    }

    fn get_worker_client(
        worker_id: WorkerId,
        worker: &mut WorkerDescriptor,
        options: &WorkerPoolOptions,
    ) -> ExecutionResult<WorkerClient> {
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
                    WorkerClient::new(options)
                });
                Ok(client.clone())
            }
            _ => Err(ExecutionError::InternalError(format!(
                "cannot get client for inactive worker: {worker_id}"
            ))),
        }
    }

    fn remove_worker_streams(
        ctx: &mut ActorContext<DriverActor>,
        job_id: JobId,
        worker_id: WorkerId,
        worker: &mut WorkerDescriptor,
        options: &WorkerPoolOptions,
    ) {
        let client = match Self::get_worker_client(worker_id, worker, options) {
            Ok(x) => x,
            Err(e) => {
                warn!("failed to remove streams in worker {worker_id}: {e}");
                return;
            }
        };
        ctx.spawn(async move {
            let prefix = format!("job-{job_id}/");
            if let Err(e) = client.remove_stream(prefix).await {
                error!("failed to remove streams in worker {worker_id}: {e}");
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
}
