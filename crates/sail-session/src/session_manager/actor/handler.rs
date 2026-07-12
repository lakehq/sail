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
use sail_server::actor::{ActorAction, ActorContext};
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::error::{SessionError, SessionResult};
use crate::session_factory::ServerSessionInfo;
use crate::session_manager::actor::SessionManagerActor;
use crate::session_manager::event::{SessionHistory, SessionManagerEvent};
use crate::session_manager::session::{ServerSession, ServerSessionState, SessionKey};

impl SessionManagerActor {
    pub(super) fn handle_get_or_create_session(
        &mut self,
        ctx: &mut ActorContext<Self>,
        session_id: String,
        user_id: String,
        result: oneshot::Sender<SessionResult<SessionContext>>,
    ) -> ActorAction {
        let session_key = SessionKey::new(session_id, user_id);
        let context = if let Some(session) = self.sessions.get(&session_key) {
            if let ServerSessionState::Running { context } = &session.state {
                Ok(context.clone())
            } else {
                Err(SessionError::invalid(format!(
                    "session {} for user {} is not running",
                    session_key.session_id(),
                    session_key.user_id()
                )))
            }
        } else if self.shutting_down {
            Err(SessionError::invalid(
                "cannot create session: session manager is shutting down",
            ))
        } else if !self.make_room_for_session() {
            Err(SessionError::invalid(format!(
                "cannot create session {} for user {}: maximum number of sessions ({}) reached",
                session_key.session_id(),
                session_key.user_id(),
                self.options.max_sessions
            )))
        } else {
            info!(
                "creating session {} for user {}",
                session_key.session_id(),
                session_key.user_id()
            );
            let span = Span::root(
                "SessionManagerActor::create_session_context",
                SpanContext::random(),
            );
            let _guard = span.set_local_parent();
            let info = ServerSessionInfo {
                session_id: session_key.session_id().clone(),
                user_id: session_key.user_id().clone(),
                session_manager: ctx.handle().clone(),
            };
            match self.factory.create(info) {
                Ok(context) => {
                    let session = ServerSession {
                        created_at: Utc::now(),
                        deleted_at: None,
                        state: ServerSessionState::Running {
                            context: context.clone(),
                        },
                    };
                    self.sessions.insert(session_key.clone(), session);
                    Ok(context)
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
                    session_key,
                    instant: active_at,
                },
                self.options.session_timeout,
            );
        }
        let _ = result.send(context);
        ActorAction::Continue
    }

    fn make_room_for_session(&mut self) -> bool {
        if self.options.max_sessions == 0 {
            return false;
        }
        while self.sessions.len() >= self.options.max_sessions {
            let Some(index) = self.sessions.iter().position(|(_, session)| {
                matches!(
                    &session.state,
                    ServerSessionState::Deleted { .. } | ServerSessionState::Failed
                )
            }) else {
                return false;
            };
            if let Some((session_key, _)) = self.sessions.shift_remove_index(index) {
                info!(
                    "evicting terminal session {} for user {}",
                    session_key.session_id(),
                    session_key.user_id()
                );
            }
        }
        true
    }

    pub(super) fn handle_probe_idle_session(
        &mut self,
        ctx: &mut ActorContext<Self>,
        session_key: SessionKey,
        instant: Instant,
    ) -> ActorAction {
        let session = self.sessions.get_mut(&session_key);
        if let Some(session) = session
            && let ServerSessionState::Running { context } = &mut session.state
            && let Ok(tracker) = context.extension::<ActivityTracker>()
            && tracker.active_at().is_ok_and(|x| x <= instant)
        {
            info!(
                "removing idle session {} for user {}",
                session_key.session_id(),
                session_key.user_id()
            );
            let retained_context = context.clone();
            let shutdown_started =
                Self::request_session_job_shutdown(ctx, session_key, &retained_context);
            session.deleted_at = Some(Utc::now());
            session.state = if shutdown_started {
                ServerSessionState::Deleting {
                    context: retained_context,
                }
            } else {
                ServerSessionState::Failed
            };
        }
        ActorAction::Continue
    }

    pub(super) fn handle_delete_session(
        &mut self,
        ctx: &mut ActorContext<Self>,
        session_id: String,
        user_id: String,
        result: oneshot::Sender<SessionResult<()>>,
    ) -> ActorAction {
        let session_key = SessionKey::new(session_id, user_id);
        let session = self.sessions.get_mut(&session_key);
        let output = if let Some(session) = session {
            if let ServerSessionState::Running { context } = &mut session.state {
                info!(
                    "removing session {} for user {}",
                    session_key.session_id(),
                    session_key.user_id()
                );
                let retained_context = context.clone();
                let shutdown_started =
                    Self::request_session_job_shutdown(ctx, session_key, &retained_context);
                session.deleted_at = Some(Utc::now());
                session.state = if shutdown_started {
                    ServerSessionState::Deleting {
                        context: retained_context,
                    }
                } else {
                    ServerSessionState::Failed
                };
                Ok(())
            } else {
                Err(SessionError::invalid(format!(
                    "session {} for user {} is not running",
                    session_key.session_id(),
                    session_key.user_id()
                )))
            }
        } else {
            Err(SessionError::invalid(format!(
                "session {} not found for user {}",
                session_key.session_id(),
                session_key.user_id()
            )))
        };
        let _ = result.send(output);
        ActorAction::Continue
    }

    pub(super) fn handle_set_session_history(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        session_key: SessionKey,
        history: SessionHistory,
    ) -> ActorAction {
        let Some(session) = self.sessions.get_mut(&session_key) else {
            warn!(
                "session {} not found for user {}",
                session_key.session_id(),
                session_key.user_id()
            );
            return self.shutdown_action();
        };
        if matches!(session.state, ServerSessionState::Deleting { .. }) {
            session.state = ServerSessionState::Deleted {
                history: Arc::new(history),
            };
        } else {
            warn!(
                "session {} for user {} is not being deleted",
                session_key.session_id(),
                session_key.user_id()
            );
        }
        self.shutdown_action()
    }

    pub(super) fn handle_set_session_failure(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        session_key: SessionKey,
    ) -> ActorAction {
        let Some(session) = self.sessions.get_mut(&session_key) else {
            warn!(
                "session {} not found for user {}",
                session_key.session_id(),
                session_key.user_id()
            );
            return self.shutdown_action();
        };
        session.state = ServerSessionState::Failed;
        self.shutdown_action()
    }

    pub(super) fn handle_shutdown(
        &mut self,
        ctx: &mut ActorContext<Self>,
        result: oneshot::Sender<SessionResult<()>>,
    ) -> ActorAction {
        self.shutdown_results.push(result);
        if !self.shutting_down {
            self.shutting_down = true;
            let shutdown_contexts = &mut self.shutdown_contexts;
            for (session_key, session) in &mut self.sessions {
                let context = match &mut session.state {
                    ServerSessionState::Running { context } => context,
                    ServerSessionState::Deleting { context } => {
                        shutdown_contexts.push(context.clone());
                        continue;
                    }
                    ServerSessionState::Deleted { .. } | ServerSessionState::Failed => continue,
                };
                info!(
                    "shutting down session {} for user {}",
                    session_key.session_id(),
                    session_key.user_id()
                );
                let retained_context = context.clone();
                shutdown_contexts.push(retained_context.clone());
                let shutdown_started =
                    Self::request_session_job_shutdown(ctx, session_key.clone(), &retained_context);
                session.deleted_at = Some(Utc::now());
                session.state = if shutdown_started {
                    ServerSessionState::Deleting {
                        context: retained_context,
                    }
                } else {
                    ServerSessionState::Failed
                };
            }
        }
        self.shutdown_action()
    }

    fn shutdown_action(&self) -> ActorAction {
        if self.shutting_down
            && self.sessions.values().all(|session| {
                matches!(
                    &session.state,
                    ServerSessionState::Deleted { .. } | ServerSessionState::Failed
                )
            })
        {
            ActorAction::Stop
        } else {
            ActorAction::Continue
        }
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
                        |&(session_key, _)| session_key.session_id(),
                        |(session_key, session)| {
                            session.observe_job_runner(|tx| JobRunnerObserver::Jobs {
                                session_id: session_key.session_id().clone(),
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
                        |&(session_key, _)| session_key.session_id(),
                        |(session_key, session)| {
                            session.observe_job_runner(|tx| JobRunnerObserver::Stages {
                                session_id: session_key.session_id().clone(),
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
                        |&(session_key, _)| session_key.session_id(),
                        |(session_key, session)| {
                            session.observe_job_runner(|tx| JobRunnerObserver::Tasks {
                                session_id: session_key.session_id().clone(),
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
                        |&(session_key, _)| session_key.session_id(),
                        |(session_key, session)| SessionRow {
                            session_id: session_key.session_id().clone(),
                            user_id: session_key.user_id().clone(),
                            status: session.state.status().to_string(),
                            created_at: session.created_at,
                            deleted_at: session.deleted_at,
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
                        |&(session_key, _)| session_key.session_id(),
                        |(session_key, session)| {
                            session.observe_job_runner(|tx| JobRunnerObserver::Workers {
                                session_id: session_key.session_id().clone(),
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

    fn request_session_job_shutdown(
        ctx: &mut ActorContext<Self>,
        session_key: SessionKey,
        context: &SessionContext,
    ) -> bool {
        let Ok(service) = context.extension::<JobService>() else {
            warn!(
                "job service not found for session {} for user {}",
                session_key.session_id(),
                session_key.user_id()
            );
            return false;
        };
        let handle = ctx.handle().clone();
        let (tx, rx) = oneshot::channel();
        ctx.spawn(async move {
            service.runner().stop(tx).await;
            let message = match rx.await {
                Ok(x) => SessionManagerEvent::SetSessionHistory {
                    session_key,
                    history: SessionHistory { job_runner: x },
                },
                Err(_) => SessionManagerEvent::SetSessionFailure { session_key },
            };
            let _ = handle.send(message).await;
        });
        true
    }
}
