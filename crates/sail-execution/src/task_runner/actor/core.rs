use sail_common::actor::{Actor, ActorAction, ActorContext};

use crate::task_runner::{TaskRunnerActor, TaskRunnerComponents, TaskRunnerMessage};

impl Actor for TaskRunnerActor {
    type Message = TaskRunnerMessage;
    type Options = TaskRunnerComponents;

    fn name() -> &'static str {
        "TaskRunnerActor"
    }

    fn new(options: Self::Options) -> Self {
        let TaskRunnerComponents {
            session_id,
            enable_shuffle_read_coalescing,
            extensions,
            placement,
        } = options;
        Self {
            session_id,
            enable_shuffle_read_coalescing,
            signals: Default::default(),
            tasks: Default::default(),
            broadcasts: Default::default(),
            extensions,
            placement,
        }
    }

    async fn receive(
        &mut self,
        ctx: &mut ActorContext<Self>,
        message: Self::Message,
    ) -> ActorAction {
        match message {
            TaskRunnerMessage::RunTaskBatch {
                job_id,
                stage,
                tasks,
                definition,
                context,
                peers,
                result,
            } => {
                let output = self
                    .handle_run_task_batch(ctx, job_id, stage, tasks, definition, context, peers);
                let _ = result.send(output);
                ActorAction::Continue
            }
            TaskRunnerMessage::StopTask { key } => self.handle_stop_task(key),
            TaskRunnerMessage::ReportTaskStatus {
                key,
                status,
                message,
                cause,
            } => self.handle_report_task_status(ctx, key, status, message, cause),
            TaskRunnerMessage::ProbePendingLocalStream { key } => {
                self.handle_probe_pending_local_stream(key)
            }
            TaskRunnerMessage::CreateLocalStream {
                key,
                replayable,
                context,
                result,
            } => self.handle_create_local_stream(key, replayable, context, result),
            TaskRunnerMessage::CreateStorageStream {
                key,
                schema,
                context,
                result,
            } => self.handle_create_storage_stream(key, schema, context, result),
            TaskRunnerMessage::CreateCelebornStream {
                key,
                mappers,
                channels,
                schema,
                result,
            } => self.handle_create_celeborn_stream(ctx, key, mappers, channels, schema, result),
            TaskRunnerMessage::FetchBroadcastStream {
                key,
                fetch,
                context,
                result,
            } => self.handle_fetch_broadcast_stream(ctx, key, fetch, context, result),
            TaskRunnerMessage::FetchDriverStream {
                key,
                schema,
                result,
            } => self.handle_fetch_driver_stream(ctx, key, schema, result),
            TaskRunnerMessage::FetchWorkerStream {
                worker_id,
                key,
                schema,
                result,
            } => self.handle_fetch_worker_stream(ctx, worker_id, key, schema, result),
            TaskRunnerMessage::FetchLocalStream { key, result } => {
                self.handle_fetch_local_stream(ctx, key, result)
            }
            TaskRunnerMessage::FetchStorageStream {
                key,
                schema,
                context,
                result,
            } => self.handle_fetch_storage_stream(key, schema, context, result),
            TaskRunnerMessage::FetchCelebornStream {
                job_id,
                stage,
                channels,
                schema,
                result,
            } => self.handle_fetch_celeborn_stream(ctx, job_id, stage, channels, schema, result),
            TaskRunnerMessage::CleanUpLocalStreams { job_id, stage } => {
                self.handle_clean_up_local_streams(job_id, stage)
            }
            TaskRunnerMessage::CleanUpStorageStreams {
                job_id,
                stage,
                context,
            } => self.handle_clean_up_storage_streams(ctx, job_id, stage, context),
            TaskRunnerMessage::CleanUpCelebornStreams { job_id, stage } => {
                self.handle_clean_up_celeborn_streams(ctx, job_id, stage)
            }
            TaskRunnerMessage::CloseJob { job_id } => self.handle_close_job(job_id),
            TaskRunnerMessage::Shutdown => self.handle_shutdown(),
        }
    }

    async fn stop(self, _ctx: &mut ActorContext<Self>) {
        if let Some(streams) = self.extensions.celeborn_streams {
            streams.stop().await;
        }
    }
}
