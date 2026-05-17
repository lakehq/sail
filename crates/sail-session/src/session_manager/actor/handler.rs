use std::mem;
use std::sync::Arc;

use chrono::Utc;
use datafusion::prelude::SessionContext;
use fastrace::collector::SpanContext;
use fastrace::Span;
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
            let info = ServerSessionInfo {
                session_id: session_id.clone(),
                user_id: user_id.clone(),
                session_manager: ctx.handle().clone(),
            };
            match self.factory.create(info) {
                Ok(context) => {
                    let session = ServerSession {
                        user_id,
                        created_at: Utc::now(),
                        deleted_at: None,
                        state: ServerSessionState::Running {
                            context: context.clone(),
                        },
                    };
                    self.sessions.insert(session_id, session);
                    Ok(context)
                }
                Err(e) => Err(e.into()),
            }
        };
        if let Ok(context) = &context {
            if let Ok(active_at) = context
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
        if let Some(session) = session {
            if let ServerSessionState::Running { context } = &mut session.state {
                if let Ok(tracker) = context.extension::<ActivityTracker>() {
                    if tracker.active_at().is_ok_and(|x| x <= instant) {
                        info!("removing idle session {session_id}");
                        match Self::delete_session(ctx, session_id.clone(), context) {
                            Ok(()) => {
                                session.deleted_at = Some(Utc::now());
                                session.state = ServerSessionState::Deleting { waiters: vec![] };
                            }
                            Err(error) => {
                                warn!("failed to remove idle session {session_id}: {error}");
                                session.state = ServerSessionState::Failed;
                            }
                        }
                    }
                }
            }
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
        match session {
            Some(session) => match &mut session.state {
                ServerSessionState::Running { context } => {
                    info!("removing session {session_id}");
                    match Self::delete_session(ctx, session_id.clone(), context) {
                        Ok(()) => {
                            session.deleted_at = Some(Utc::now());
                            session.state = ServerSessionState::Deleting {
                                waiters: vec![result],
                            };
                        }
                        Err(error) => {
                            session.state = ServerSessionState::Failed;
                            let _ = result.send(Err(error));
                        }
                    }
                }
                ServerSessionState::Deleting { waiters } => {
                    waiters.push(result);
                }
                ServerSessionState::Deleted { .. } => {
                    let _ = result.send(Ok(()));
                }
                ServerSessionState::Failed => {
                    let _ = result.send(Err(SessionError::internal(format!(
                        "failed to delete session: {session_id}"
                    ))));
                }
            },
            None => {
                let _ = result.send(Ok(()));
            }
        }
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
        let state = mem::replace(&mut session.state, ServerSessionState::Failed);
        match state {
            ServerSessionState::Deleting { waiters } => {
                session.state = ServerSessionState::Deleted {
                    history: Arc::new(history),
                };
                for waiter in waiters {
                    let _ = waiter.send(Ok(()));
                }
            }
            state => {
                warn!("session is not being deleted: {session_id}");
                session.state = state;
            }
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
        if let ServerSessionState::Deleting { waiters } =
            mem::replace(&mut session.state, ServerSessionState::Failed)
        {
            for waiter in waiters {
                let _ = waiter.send(Err(SessionError::internal(format!(
                    "failed to delete session: {session_id}"
                ))));
            }
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

    fn delete_session(
        ctx: &mut ActorContext<Self>,
        session_id: String,
        context: &SessionContext,
    ) -> SessionResult<()> {
        let Ok(service) = context.extension::<JobService>() else {
            warn!("job service not found for session {session_id}");
            return Err(SessionError::internal(format!(
                "job service not found for session {session_id}"
            )));
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
        Ok(())
    }
}
