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
            extensions,
            placement,
        } = options;
        Self {
            session_id,
            signals: Default::default(),
            completions: Default::default(),
            closing: Default::default(),
            tasks: Default::default(),
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
                replicas,
                schema,
                result,
            } => self.handle_create_local_stream(key, replicas, schema, result),
            TaskRunnerMessage::CreateReplayStream {
                key,
                id,
                storage,
                schema,
                context,
                result,
            } => self.handle_create_replay_stream(key, id, storage, schema, context, result),
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
                replay,
                schema,
                context,
                result,
            } => self.handle_fetch_storage_stream(key, replay, schema, context, result),
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
            TaskRunnerMessage::CloseJob { job_id, result } => {
                self.handle_close_job(ctx, job_id, result)
            }
            TaskRunnerMessage::FinishCloseJob { job_id, done } => {
                self.extensions.local_streams.remove_streams(job_id, None);
                let _ = done.send(true);
                ActorAction::Continue
            }
            TaskRunnerMessage::Shutdown => self.handle_shutdown(),
        }
    }

    async fn stop(self, _ctx: &mut ActorContext<Self>) {
        if let Some(streams) = self.extensions.celeborn_streams {
            streams.stop().await;
        }
    }
}
