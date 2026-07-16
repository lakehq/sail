use std::sync::Arc;

use chrono::Utc;
use datafusion::prelude::SessionContext;
use fastrace::Span;
use fastrace::collector::SpanContext;
use log::{info, warn};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::activity::ActivityTracker;
use sail_common_datafusion::session::job::JobService;
use sail_common_datafusion::system::catalog::{OptionRow, SessionRow};
use sail_common_datafusion::system::observable::{JobRunnerObserver, SessionManagerObserver};
use sail_common_datafusion::system::predicate::PredicateExt;
use sail_execution::DriverId;
use sail_server::actor::{ActorAction, ActorContext};
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::error::{SessionError, SessionResult};
use crate::session_factory::{ServerSessionInfo, SessionJobRunnerInfo};
use crate::session_manager::actor::SessionManagerActor;
use crate::session_manager::event::{SessionHistory, SessionManagerEvent};
use crate::session_manager::session::{ServerSession, ServerSessionState};

impl SessionManagerActor {
    pub(super) fn handle_get_or_create_session(
        &mut self,
        ctx: &mut ActorContext<Self>,
        session_id: String,
        user_id: String,
        result: oneshot::Sender<SessionResult<SessionContext>>,
    ) -> ActorAction {
        let context = if let Some(session) = self.sessions.get(&session_id) {
            if let ServerSessionState::Running { context } = &session.state {
                Ok(context.clone())
            } else {
                Err(SessionError::invalid(format!(
                    "session {session_id} is not running"
                )))
            }
        } else {
            let session_id = session_id.clone();
            info!("creating session {session_id}");
            let span = Span::root(
                "SessionManagerActor::create_session_context",
                SpanContext::random(),
            );
            let _guard = span.set_local_parent();
            let driver_id = DriverId::from(self.next_driver_id);
            let Some(next_driver_id) = self.next_driver_id.checked_add(1) else {
                let output = Err(SessionError::internal("driver ID overflow"));
                let _ = result.send(output);
                return ActorAction::Continue;
            };
            self.next_driver_id = next_driver_id;
            let runner = self.job_runner_factory.create(SessionJobRunnerInfo {
                driver_id,
                driver_server_port: self.gateway.as_ref().map(|x| x.port()),
            });
            match runner {
                Ok(runner) => {
                    let (runner, driver) = runner.into_parts();
                    let registered_driver_id = driver.as_ref().map(|_| driver_id);
                    if let Some(driver) = &driver
                        && let Err(e) = self.drivers.insert(driver_id, driver.clone())
                    {
                        let driver = driver.clone();
                        ctx.spawn(async move {
                            let _ = driver.shutdown().await;
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
                    match self.factory.create(info) {
                        Ok(context) => {
                            if let Some(driver) = driver {
                                ctx.spawn(async move {
                                    let _ = driver.activate().await;
                                });
                            }
                            let session = ServerSession {
                                user_id,
                                created_at: Utc::now(),
                                deleted_at: None,
                                state: ServerSessionState::Running {
                                    context: context.clone(),
                                },
                                driver_id: registered_driver_id,
                            };
                            self.sessions.insert(session_id, session);
                            Ok(context)
                        }
                        Err(e) => {
                            if let Some(driver_id) = registered_driver_id
                                && let Ok(Some(driver)) = self.drivers.remove(driver_id)
                            {
                                ctx.spawn(async move {
                                    let _ = driver.shutdown().await;
                                });
                            }
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
                SessionManagerEvent::ProbeIdleSession {
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
            && let ServerSessionState::Running { context } = &mut session.state
            && let Ok(tracker) = context.extension::<ActivityTracker>()
            && tracker.active_at().is_ok_and(|x| x <= instant)
        {
            info!("removing idle session {session_id}");
            Self::delete_session(ctx, session_id, context);
            session.deleted_at = Some(Utc::now());
            session.state = ServerSessionState::Deleting;
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
            if let ServerSessionState::Running { context } = &mut session.state {
                info!("removing session {session_id}");
                Self::delete_session(ctx, session_id, context);
                session.deleted_at = Some(Utc::now());
                session.state = ServerSessionState::Deleting;
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
        if let Some(driver_id) = session.driver_id.take()
            && let Err(e) = self.drivers.remove(driver_id)
        {
            warn!("failed to remove driver {driver_id}: {e}");
        }
        if matches!(session.state, ServerSessionState::Deleting) {
            session.state = ServerSessionState::Deleted {
                history: Arc::new(history),
            };
        } else {
            warn!("session is not being deleted: {session_id}");
        }
        ActorAction::Continue
    }

    pub(super) fn handle_set_session_failure(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        session_id: String,
    ) -> ActorAction {
        let Some(session) = self.sessions.get_mut(&session_id) else {
            warn!("session not found: {session_id}");
            return ActorAction::Continue;
        };
        if let Some(driver_id) = session.driver_id.take()
            && let Err(e) = self.drivers.remove(driver_id)
        {
            warn!("failed to remove driver {driver_id}: {e}");
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
        let handle = ctx.handle().clone();
        let (tx, rx) = oneshot::channel();
        ctx.spawn(async move {
            service.runner().stop(tx).await;
            let message = match rx.await {
                Ok(x) => SessionManagerEvent::SetSessionHistory {
                    session_id,
                    history: SessionHistory { job_runner: x },
                },
                Err(_) => SessionManagerEvent::SetSessionFailure { session_id },
            };
            let _ = handle.send(message).await;
        });
    }
}
