use std::sync::Arc;

use chrono::Utc;
use datafusion::prelude::SessionContext;
use fastrace::Span;
use fastrace::collector::SpanContext;
use log::{info, warn};
use sail_cache::remote_checkpoint::RemoteCheckpointRegistry;
use sail_common::actor::{ActorAction, ActorContext};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::activity::ActivityTracker;
use sail_common_datafusion::session::job::{
    JobRunnerHistory, JobRunnerHistoryReporter, JobService,
};
use sail_common_datafusion::system::catalog::{OptionRow, SessionRow};
use sail_common_datafusion::system::observable::{JobRunnerObserver, SessionManagerObserver};
use sail_common_datafusion::system::predicate::PredicateExt;
use sail_execution::DriverId;
use sail_execution::driver::DriverHandle;
use sail_execution::error::ExecutionResult;
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::error::{SessionError, SessionResult};
use crate::session_factory::{ServerSessionInfo, SessionJobRunnerInfo};
use crate::session_manager::actor::SessionManagerActor;
use crate::session_manager::session::{ServerSession, ServerSessionState};
use crate::session_manager::{SessionHistory, SessionManagerMessage};

struct SessionJobRunnerHistoryReporter {
    session_id: String,
    session_manager: sail_common::actor::ActorHandle<SessionManagerActor>,
}

#[tonic::async_trait]
impl JobRunnerHistoryReporter for SessionJobRunnerHistoryReporter {
    async fn report(self: Box<Self>, history: JobRunnerHistory) {
        let _ = self
            .session_manager
            .send(SessionManagerMessage::SetSessionHistory {
                session_id: self.session_id,
                history: SessionHistory {
                    job_runner: history,
                },
            })
            .await;
    }
}

impl SessionManagerActor {
    pub(super) fn handle_get_driver(
        &self,
        driver_id: DriverId,
        result: oneshot::Sender<ExecutionResult<DriverHandle>>,
    ) -> ActorAction {
        let _ = result.send(self.drivers.get(driver_id));
        ActorAction::Continue
    }

    pub(super) fn handle_get_or_create_session(
        &mut self,
        ctx: &mut ActorContext<Self>,
        session_id: String,
        user_id: String,
        result: oneshot::Sender<SessionResult<SessionContext>>,
    ) -> ActorAction {
        let context = if let Some(session) = self.sessions.get(&session_id) {
            if let ServerSessionState::Running { context, .. } = &session.state {
                Ok(context.clone())
            } else {
                Err(SessionError::invalid(format!(
                    "session {session_id} is not running"
                )))
            }
        } else {
            // TODO: The session ID is used in various storage paths, so it is assumed to be unique
            //   across all session managers, and it should contain only valid characters for a
            //   path segment. Right now the session ID is generated as a UUID by the Spark client,
            //   so this is true in practice, but we may still want some validation here.
            let session_id = session_id.clone();
            info!("creating session {session_id}");
            let span = Span::root(
                "SessionManagerActor::create_session_context",
                SpanContext::random(),
            );
            let _guard = span.set_local_parent();
            let driver_id = match self.driver_id_generator.generate() {
                Ok(driver_id) => driver_id,
                Err(e) => {
                    let output = Err(SessionError::internal(e.to_string()));
                    let _ = result.send(output);
                    return ActorAction::Continue;
                }
            };
            let session_manager = ctx.handle().clone();
            let runner = self.job_runner_factory.create(
                ctx.children_mut(),
                SessionJobRunnerInfo {
                    session_id: session_id.clone(),
                    driver_id,
                    driver_server_port: self.driver_gateway.as_ref().map(|x| x.port()),
                    history_reporter: Box::new(SessionJobRunnerHistoryReporter {
                        session_id: session_id.clone(),
                        session_manager,
                    }),
                },
            );
            match runner {
                Ok(runner) => {
                    let (runner, driver) = runner.into_parts();
                    let registered_driver_id = driver.as_ref().map(|_| driver_id);
                    if let Some(driver) = &driver
                        && let Err(e) = self.drivers.insert(driver_id, driver.clone())
                    {
                        let session = ServerSession {
                            user_id,
                            created_at: Utc::now(),
                            deleted_at: None,
                            state: ServerSessionState::Failed,
                        };
                        self.sessions.insert(session_id, session);
                        let driver = driver.clone();
                        ctx.spawn(async move {
                            if let Err(e) = driver.shutdown().await {
                                warn!("failed to shut down driver {driver_id}: {e}");
                            }
                        });
                        let output = Err(e.into());
                        let _ = result.send(output);
                        return ActorAction::Continue;
                    }
                    let info = ServerSessionInfo {
                        session_id: session_id.clone(),
                        user_id: user_id.clone(),
                        session_manager: ctx.handle().clone(),
                        job_runner: Some(runner),
                    };
                    match self.session_factory.create(info) {
                        Ok(context) => {
                            if let Some(driver) = driver {
                                ctx.spawn(async move {
                                    if let Err(e) = driver.activate().await {
                                        warn!("failed to activate driver {driver_id}: {e}");
                                    }
                                });
                            }
                            let session = ServerSession {
                                user_id,
                                created_at: Utc::now(),
                                deleted_at: None,
                                state: ServerSessionState::Running {
                                    context: context.clone(),
                                    driver_id: registered_driver_id,
                                },
                            };
                            self.sessions.insert(session_id, session);
                            Ok(context)
                        }
                        Err(e) => {
                            if let Some(driver_id) = registered_driver_id
                                && let Some(driver) = self.drivers.remove(driver_id)
                            {
                                ctx.spawn(async move {
                                    if let Err(e) = driver.shutdown().await {
                                        warn!("failed to shut down driver {driver_id}: {e}");
                                    }
                                });
                            }
                            let session = ServerSession {
                                user_id,
                                created_at: Utc::now(),
                                deleted_at: None,
                                state: ServerSessionState::Failed,
                            };
                            self.sessions.insert(session_id, session);
                            Err(e.into())
                        }
                    }
                }
                Err(e) => Err(e.into()),
            }
        };
        if let Ok(context) = &context
            && let Ok(active_at) = context
                .extension::<ActivityTracker>()
                .and_then(|tracker| tracker.track_activity())
        {
            ctx.send_with_delay(
                SessionManagerMessage::ProbeIdleSession {
                    session_id,
                    instant: active_at,
                },
                self.options.session_timeout,
            );
        }
        let _ = result.send(context);
        ActorAction::Continue
    }

    pub(super) fn handle_probe_idle_session(
        &mut self,
        ctx: &mut ActorContext<Self>,
        session_id: String,
        instant: Instant,
    ) -> ActorAction {
        let session = self.sessions.get_mut(&session_id);
        if let Some(session) = session
            && let ServerSessionState::Running { context, driver_id } = &mut session.state
            && let Ok(tracker) = context.extension::<ActivityTracker>()
            && tracker.active_at().is_ok_and(|x| x <= instant)
        {
            info!("removing idle session {session_id}");
            Self::delete_session(ctx, session_id, context);
            session.deleted_at = Some(Utc::now());
            session.state = ServerSessionState::Deleting {
                driver_id: *driver_id,
            };
        }
        ActorAction::Continue
    }

    pub(super) fn handle_delete_session(
        &mut self,
        ctx: &mut ActorContext<Self>,
        session_id: String,
        result: oneshot::Sender<SessionResult<()>>,
    ) -> ActorAction {
        let session = self.sessions.get_mut(&session_id);
        let output = if let Some(session) = session {
            if let ServerSessionState::Running { context, driver_id } = &mut session.state {
                info!("removing session {session_id}");
                Self::delete_session(ctx, session_id, context);
                session.deleted_at = Some(Utc::now());
                session.state = ServerSessionState::Deleting {
                    driver_id: *driver_id,
                };
                Ok(())
            } else {
                Err(SessionError::invalid(format!(
                    "session {session_id} is not running"
                )))
            }
        } else {
            Err(SessionError::invalid(format!(
                "session not found: {session_id}"
            )))
        };
        let _ = result.send(output);
        ActorAction::Continue
    }

    pub(super) fn handle_set_session_history(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        session_id: String,
        history: SessionHistory,
    ) -> ActorAction {
        let Some(session) = self.sessions.get_mut(&session_id) else {
            warn!("session not found: {session_id}");
            return ActorAction::Continue;
        };
        match &mut session.state {
            ServerSessionState::Running { driver_id, .. }
            | ServerSessionState::Deleting { driver_id } => {
                if let Some(driver_id) = driver_id.take() {
                    self.drivers.remove(driver_id);
                }
                session.deleted_at.get_or_insert_with(Utc::now);
                session.state = ServerSessionState::Deleted {
                    history: Arc::new(history),
                };
            }
            ServerSessionState::Deleted { .. } | ServerSessionState::Failed => {
                warn!("session is not being deleted: {session_id}");
            }
        }
        ActorAction::Continue
    }

    pub(super) fn handle_set_session_failure(
        &mut self,
        ctx: &mut ActorContext<Self>,
        session_id: String,
    ) -> ActorAction {
        let Some(session) = self.sessions.get_mut(&session_id) else {
            warn!("session not found: {session_id}");
            return ActorAction::Continue;
        };
        let driver_id = match &session.state {
            ServerSessionState::Running { driver_id, .. }
            | ServerSessionState::Deleting { driver_id } => *driver_id,
            ServerSessionState::Deleted { .. } | ServerSessionState::Failed => None,
        };
        if let Some(driver_id) = driver_id
            && let Some(driver) = self.drivers.remove(driver_id)
        {
            ctx.spawn(async move {
                if let Err(e) = driver.shutdown().await {
                    warn!("failed to shut down driver {driver_id}: {e}");
                }
            });
        }
        session.state = ServerSessionState::Failed;
        ActorAction::Continue
    }

    pub(super) fn handle_observe_state(
        &mut self,
        ctx: &mut ActorContext<Self>,
        observer: SessionManagerObserver,
    ) -> ActorAction {
        match observer {
            SessionManagerObserver::Jobs {
                session_id,
                job_id,
                fetch,
                result,
            } => {
                let task = self
                    .sessions
                    .iter()
                    .predicate_filter_async_flat_map(
                        session_id,
                        |&(k, _)| k,
                        |(k, v)| {
                            v.observe_job_runner(|tx| JobRunnerObserver::Jobs {
                                session_id: k.clone(),
                                job_id: job_id.clone(),
                                fetch,
                                result: tx,
                            })
                        },
                    )
                    .into_task();
                ctx.spawn(async move {
                    let _ = result.send(task.fetch(fetch).collect().await);
                });
            }
            SessionManagerObserver::Stages {
                session_id,
                job_id,
                fetch,
                result,
            } => {
                let task = self
                    .sessions
                    .iter()
                    .predicate_filter_async_flat_map(
                        session_id,
                        |&(k, _)| k,
                        |(k, v)| {
                            v.observe_job_runner(|tx| JobRunnerObserver::Stages {
                                session_id: k.clone(),
                                job_id: job_id.clone(),
                                fetch,
                                result: tx,
                            })
                        },
                    )
                    .into_task();
                ctx.spawn(async move {
                    let _ = result.send(task.fetch(fetch).collect().await);
                });
            }
            SessionManagerObserver::Tasks {
                session_id,
                job_id,
                fetch,
                result,
            } => {
                let task = self
                    .sessions
                    .iter()
                    .predicate_filter_async_flat_map(
                        session_id,
                        |&(k, _)| k,
                        |(k, v)| {
                            v.observe_job_runner(|tx| JobRunnerObserver::Tasks {
                                session_id: k.clone(),
                                job_id: job_id.clone(),
                                fetch,
                                result: tx,
                            })
                        },
                    )
                    .into_task();
                ctx.spawn(async move {
                    let _ = result.send(task.fetch(fetch).collect().await);
                });
            }
            SessionManagerObserver::Sessions {
                session_id,
                fetch,
                result,
            } => {
                let output = self
                    .sessions
                    .iter()
                    .predicate_filter_map(
                        session_id,
                        |&(k, _)| k,
                        |(k, v)| SessionRow {
                            session_id: k.clone(),
                            user_id: v.user_id.clone(),
                            status: v.state.status().to_string(),
                            created_at: v.created_at,
                            deleted_at: v.deleted_at,
                        },
                    )
                    .fetch(fetch)
                    .collect::<Result<Vec<_>, _>>();
                let _ = result.send(output);
            }
            SessionManagerObserver::Workers {
                session_id,
                worker_id,
                fetch,
                result,
            } => {
                let task = self
                    .sessions
                    .iter()
                    .predicate_filter_async_flat_map(
                        session_id,
                        |&(k, _)| k,
                        |(k, v)| {
                            v.observe_job_runner(|tx| JobRunnerObserver::Workers {
                                session_id: k.clone(),
                                worker_id: worker_id.clone(),
                                fetch,
                                result: tx,
                            })
                        },
                    )
                    .into_task();
                ctx.spawn(async move {
                    let _ = result.send(task.fetch(fetch).collect().await);
                });
            }
            SessionManagerObserver::Options { key, fetch, result } => {
                let rows = self
                    .options
                    .options
                    .iter()
                    .predicate_filter_map(
                        key,
                        |(key, _)| key,
                        |(key, value)| OptionRow {
                            key: key.clone(),
                            value: value.clone(),
                        },
                    )
                    .fetch(fetch)
                    .collect::<Result<Vec<_>, _>>();
                let _ = result.send(rows);
            }
        }
        ActorAction::Continue
    }

    fn delete_session(ctx: &mut ActorContext<Self>, session_id: String, context: &SessionContext) {
        let Ok(service) = context.extension::<JobService>() else {
            warn!("job service not found for session {session_id}");
            return;
        };
        let checkpoint_registry = context.extension::<RemoteCheckpointRegistry>().ok();
        let runtime_env = context.runtime_env();
        ctx.spawn(async move {
            // Stop tasks before deleting the namespace so late attempts cannot recreate objects.
            service.runner().stop().await;
            if let Some(checkpoint_registry) = checkpoint_registry
                && let Err(error) = checkpoint_registry
                    .cleanup_session(runtime_env.as_ref())
                    .await
            {
                warn!("failed to clean checkpoints for session {session_id}: {error}");
            }
        });
    }
}
