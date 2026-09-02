use std::borrow::Cow;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::TaskContext;
use sail_common::telemetry::{SpanAssociation, SpanAttribute};
use sail_common_datafusion::error::CommonErrorCause;
use tokio::sync::oneshot;

use crate::driver::TaskStatus;
use crate::error::ExecutionResult;
use crate::id::{JobId, TaskKey, TaskStreamKey, WorkerId};
use crate::stream::reader::TaskStreamSource;
use crate::stream::writer::{TaskStreamChannelSink, TaskStreamSink};
use crate::task::definition::TaskDefinition;
use crate::worker::WorkerLocation;

pub enum TaskRunnerMessage {
    RunTask {
        key: TaskKey,
        definition: TaskDefinition,
        context: Arc<TaskContext>,
        peers: Vec<WorkerLocation>,
    },
    StopTask {
        key: TaskKey,
    },
    ReportTaskStatus {
        key: TaskKey,
        status: TaskStatus,
        message: Option<String>,
        cause: Option<CommonErrorCause>,
    },
    ProbePendingLocalStream {
        key: TaskStreamKey,
    },
    CreateLocalStream {
        key: TaskStreamKey,
        replicas: usize,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamChannelSink>>>,
    },
    CreateStorageStream {
        key: TaskStreamKey,
        schema: SchemaRef,
        context: Arc<TaskContext>,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamChannelSink>>>,
    },
    CreateCelebornStream {
        key: TaskKey,
        mappers: usize,
        channels: usize,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<Box<dyn TaskStreamSink>>>,
    },
    FetchDriverStream {
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    },
    FetchWorkerStream {
        worker_id: WorkerId,
        key: TaskStreamKey,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    },
    FetchLocalStream {
        key: TaskStreamKey,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    },
    FetchStorageStream {
        key: TaskStreamKey,
        schema: SchemaRef,
        context: Arc<TaskContext>,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    },
    FetchCelebornStream {
        job_id: JobId,
        stage: usize,
        channels: Vec<usize>,
        schema: SchemaRef,
        result: oneshot::Sender<ExecutionResult<TaskStreamSource>>,
    },
    CleanUpLocalStreams {
        job_id: JobId,
        stage: Option<usize>,
    },
    CleanUpStorageStreams {
        job_id: JobId,
        stage: Option<usize>,
        context: Arc<TaskContext>,
    },
    CleanUpCelebornStreams {
        job_id: JobId,
        stage: Option<usize>,
    },
    Shutdown,
}

impl SpanAssociation for TaskRunnerMessage {
    fn name(&self) -> Cow<'static, str> {
        match self {
            Self::RunTask { .. } => "RunTask",
            Self::StopTask { .. } => "StopTask",
            Self::ReportTaskStatus { .. } => "ReportTaskStatus",
            Self::ProbePendingLocalStream { .. } => "ProbePendingLocalStream",
            Self::CreateLocalStream { .. } => "CreateLocalStream",
            Self::CreateStorageStream { .. } => "CreateStorageStream",
            Self::CreateCelebornStream { .. } => "CreateCelebornStream",
            Self::FetchDriverStream { .. } => "FetchDriverStream",
            Self::FetchWorkerStream { .. } => "FetchWorkerStream",
            Self::FetchLocalStream { .. } => "FetchLocalStream",
            Self::FetchStorageStream { .. } => "FetchStorageStream",
            Self::FetchCelebornStream { .. } => "FetchCelebornStream",
            Self::CleanUpLocalStreams { .. } => "CleanUpLocalStreams",
            Self::CleanUpStorageStreams { .. } => "CleanUpStorageStreams",
            Self::CleanUpCelebornStreams { .. } => "CleanUpCelebornStreams",
            Self::Shutdown => "Shutdown",
        }
        .into()
    }

    fn properties(&self) -> impl IntoIterator<Item = (Cow<'static, str>, Cow<'static, str>)> {
        let mut properties: Vec<(&'static str, String)> = vec![];
        match self {
            Self::RunTask {
                key:
                    TaskKey {
                        job_id,
                        stage,
                        partition,
                        attempt,
                    },
                ..
            }
            | Self::StopTask {
                key:
                    TaskKey {
                        job_id,
                        stage,
                        partition,
                        attempt,
                    },
            } => {
                properties.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                properties.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
                properties.push((SpanAttribute::EXECUTION_PARTITION, partition.to_string()));
                properties.push((SpanAttribute::EXECUTION_ATTEMPT, attempt.to_string()));
            }
            Self::ReportTaskStatus {
                key:
                    TaskKey {
                        job_id,
                        stage,
                        partition,
                        attempt,
                    },
                status,
                message,
                cause,
            } => {
                properties.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                properties.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
                properties.push((SpanAttribute::EXECUTION_PARTITION, partition.to_string()));
                properties.push((SpanAttribute::EXECUTION_ATTEMPT, attempt.to_string()));
                properties.push((SpanAttribute::EXECUTION_TASK_STATUS, status.to_string()));
                if let Some(message) = message {
                    properties.push((SpanAttribute::EXECUTION_TASK_MESSAGE, message.clone()));
                }
                if let Some(cause) = cause {
                    properties.push((
                        SpanAttribute::EXECUTION_TASK_ERROR_CAUSE,
                        format!("{cause:?}"),
                    ));
                }
            }
            Self::ProbePendingLocalStream { key }
            | Self::CreateLocalStream { key, .. }
            | Self::CreateStorageStream { key, .. } => {
                let TaskStreamKey {
                    job_id,
                    stage,
                    partition,
                    attempt,
                    channel,
                } = key;
                properties.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                properties.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
                properties.push((SpanAttribute::EXECUTION_PARTITION, partition.to_string()));
                properties.push((SpanAttribute::EXECUTION_ATTEMPT, attempt.to_string()));
                properties.push((SpanAttribute::EXECUTION_CHANNEL, channel.to_string()));
            }
            Self::CreateCelebornStream {
                key:
                    TaskKey {
                        job_id,
                        stage,
                        partition,
                        attempt,
                    },
                ..
            } => {
                properties.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                properties.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
                properties.push((SpanAttribute::EXECUTION_PARTITION, partition.to_string()));
                properties.push((SpanAttribute::EXECUTION_ATTEMPT, attempt.to_string()));
            }
            Self::FetchDriverStream {
                key:
                    TaskStreamKey {
                        job_id,
                        stage,
                        partition,
                        attempt,
                        channel,
                    },
                ..
            } => {
                properties.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                properties.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
                properties.push((SpanAttribute::EXECUTION_PARTITION, partition.to_string()));
                properties.push((SpanAttribute::EXECUTION_ATTEMPT, attempt.to_string()));
                properties.push((SpanAttribute::EXECUTION_CHANNEL, channel.to_string()));
            }
            Self::FetchWorkerStream {
                worker_id,
                key:
                    TaskStreamKey {
                        job_id,
                        stage,
                        partition,
                        attempt,
                        channel,
                    },
                ..
            } => {
                properties.push((SpanAttribute::CLUSTER_WORKER_ID, worker_id.to_string()));
                properties.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                properties.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
                properties.push((SpanAttribute::EXECUTION_PARTITION, partition.to_string()));
                properties.push((SpanAttribute::EXECUTION_ATTEMPT, attempt.to_string()));
                properties.push((SpanAttribute::EXECUTION_CHANNEL, channel.to_string()));
            }
            Self::FetchLocalStream {
                key:
                    TaskStreamKey {
                        job_id,
                        stage,
                        partition,
                        attempt,
                        channel,
                    },
                ..
            } => {
                properties.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                properties.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
                properties.push((SpanAttribute::EXECUTION_PARTITION, partition.to_string()));
                properties.push((SpanAttribute::EXECUTION_ATTEMPT, attempt.to_string()));
                properties.push((SpanAttribute::EXECUTION_CHANNEL, channel.to_string()));
            }
            Self::FetchStorageStream {
                key:
                    TaskStreamKey {
                        job_id,
                        stage,
                        partition,
                        attempt,
                        channel,
                    },
                ..
            } => {
                properties.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                properties.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
                properties.push((SpanAttribute::EXECUTION_PARTITION, partition.to_string()));
                properties.push((SpanAttribute::EXECUTION_ATTEMPT, attempt.to_string()));
                properties.push((SpanAttribute::EXECUTION_CHANNEL, channel.to_string()));
            }
            Self::FetchCelebornStream { job_id, stage, .. } => {
                properties.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                properties.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
            }
            Self::CleanUpLocalStreams { job_id, stage }
            | Self::CleanUpStorageStreams { job_id, stage, .. }
            | Self::CleanUpCelebornStreams { job_id, stage } => {
                properties.push((SpanAttribute::EXECUTION_JOB_ID, job_id.to_string()));
                if let Some(stage) = stage {
                    properties.push((SpanAttribute::EXECUTION_STAGE, stage.to_string()));
                }
            }
            Self::Shutdown => {}
        }
        properties
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
    }
}
