use chrono::Utc;
use datafusion::prelude::SessionContext;
use fastrace::Span;
use fastrace::collector::SpanContext;
use log::{info, warn};
use sail_cache::remote_checkpoint::RemoteCheckpointRegistry;
use sail_common::actor::{ActorAction, ActorContext};
use sail_common::telemetry::SpanAttribute;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::activity::ActivityTracker;
use sail_common_datafusion::session::job::JobService;
use sail_execution::DriverId;
use sail_execution::driver::DriverHandle;
use sail_execution::error::ExecutionResult;
use sail_system_store::SystemEvent;
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::error::{SessionError, SessionResult};
use crate::session_factory::{ServerSessionInfo, SessionJobRunnerInfo};
use crate::session_manager::SessionManagerMessage;
use crate::session_manager::actor::SessionManagerActor;
use crate::session_manager::session::{ServerSession, ServerSessionState};

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
        if let Some(session) = self.sessions.get_mut(&session_id) {
            let context = match &mut session.state {
                ServerSessionState::Running { context, .. } => Some(context.clone()),
                ServerSessionState::Creating { waiters, .. } => {
                    waiters.push(result);
                    return ActorAction::Continue;
                }
                ServerSessionState::Deleted | ServerSessionState::Failed => None,
            };
            if let Some(context) = context {
                self.schedule_idle_session_probe(ctx, session_id, &context);
                let _ = result.send(Ok(context));
            } else {
                let _ = result.send(Err(SessionError::invalid(format!(
                    "session {session_id} is not running"
                ))));
            }
            return ActorAction::Continue;
        }

        // TODO: The session ID is used in various storage paths, so it is assumed to be unique
        //   across all session managers, and it should contain only valid characters for a
        //   path segment. Right now the session ID is generated as a UUID by the Spark client,
        //   so this is true in practice, but we may still want some validation here.
        info!("creating session {session_id}");
        let span = Span::root(
            "SessionManagerActor::create_session_context",
            SpanContext::random(),
        )
        .with_property(|| (SpanAttribute::SESSION_ID, session_id.clone()));
        let _guard = span.set_local_parent();
        let driver_id = match self.driver_id_generator.generate() {
            Ok(driver_id) => driver_id,
            Err(e) => {
                let _ = result.send(Err(SessionError::internal(e.to_string())));
                return ActorAction::Continue;
            }
        };
        let runner = match self.job_runner_factory.create(
            ctx.children_mut(),
            SessionJobRunnerInfo {
                session_id: session_id.clone(),
                driver_id,
                driver_server_port: self.driver_gateway.as_ref().map(|x| x.port()),
                event_reporter: self.event_reporter.clone(),
            },
        ) {
            Ok(runner) => runner,
            Err(e) => {
                let _ = result.send(Err(e));
                return ActorAction::Continue;
            }
        };
        let (runner, driver) = runner.into_parts();
        let registered_driver_id = driver.as_ref().map(|_| driver_id);
        if let Some(driver) = &driver
            && let Err(e) = self.drivers.insert(driver_id, driver.clone())
        {
            let session = ServerSession {
                state: ServerSessionState::Failed,
            };
            let status = session.state.status().to_string();
            self.sessions.insert(session_id.clone(), session);
            self.event_reporter.report(SystemEvent::SessionCreated {
                session_id,
                user_id,
                status,
                created_at: Utc::now(),
            });
            let driver = driver.clone();
            ctx.spawn(async move {
                if let Err(e) = driver.shutdown().await {
                    warn!("failed to shut down driver {driver_id}: {e}");
                }
            });
            let _ = result.send(Err(e.into()));
            return ActorAction::Continue;
        }
        let info = ServerSessionInfo {
            session_id: session_id.clone(),
            user_id: user_id.clone(),
            session_manager: ctx.handle().clone(),
            job_runner: Some(runner),
        };
        let context = match self.session_factory.create(info) {
            Ok(context) => context,
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
                    state: ServerSessionState::Failed,
                };
                let status = session.state.status().to_string();
                self.sessions.insert(session_id.clone(), session);
                self.event_reporter.report(SystemEvent::SessionCreated {
                    session_id,
                    user_id,
                    status,
                    created_at: Utc::now(),
                });
                let _ = result.send(Err(e.into()));
                return ActorAction::Continue;
            }
        };
        self.sessions.insert(
            session_id.clone(),
            ServerSession {
                state: ServerSessionState::Creating {
                    driver_id: registered_driver_id,
                    waiters: vec![result],
                },
            },
        );
        let message = move |activation| SessionManagerMessage::CompleteSessionCreation {
            session_id,
            user_id,
            context,
            driver_id: registered_driver_id,
            activation,
        };
        if let Some(driver) = driver {
            let session_manager = ctx.handle().clone();
            ctx.spawn(async move {
                let message = message(driver.activate().await);
                if session_manager.send(message).await.is_err() {
                    warn!("failed to complete session creation");
                }
            });
        } else {
            ctx.send(message(Ok(())));
        }
        ActorAction::Continue
    }

    pub(super) fn handle_complete_session_creation(
        &mut self,
        ctx: &mut ActorContext<Self>,
        session_id: String,
        user_id: String,
        context: SessionContext,
        driver_id: Option<DriverId>,
        activation: ExecutionResult<()>,
    ) -> ActorAction {
        let Some(session) = self.sessions.get_mut(&session_id) else {
            warn!("session {session_id} disappeared during creation");
            return ActorAction::Continue;
        };
        let waiters = match &mut session.state {
            ServerSessionState::Creating {
                driver_id: creating_driver_id,
                waiters,
            } if *creating_driver_id == driver_id => std::mem::take(waiters),
            _ => {
                warn!("session {session_id} creation is no longer pending");
                return ActorAction::Continue;
            }
        };
        match activation {
            Ok(()) => {
                session.state = ServerSessionState::Running {
                    context: context.clone(),
                    driver_id,
                };
                self.event_reporter.report(SystemEvent::SessionCreated {
                    session_id: session_id.clone(),
                    user_id,
                    status: session.state.status().to_string(),
                    created_at: Utc::now(),
                });
                self.schedule_idle_session_probe(ctx, session_id, &context);
                for waiter in waiters {
                    let _ = waiter.send(Ok(context.clone()));
                }
            }
            Err(e) => {
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
                self.event_reporter.report(SystemEvent::SessionCreated {
                    session_id,
                    user_id,
                    status: session.state.status().to_string(),
                    created_at: Utc::now(),
                });
                let message = e.to_string();
                for waiter in waiters {
                    let _ = waiter.send(Err(SessionError::internal(message.clone())));
                }
            }
        }
        ActorAction::Continue
    }

    fn schedule_idle_session_probe(
        &self,
        ctx: &mut ActorContext<Self>,
        session_id: String,
        context: &SessionContext,
    ) {
        if let Ok(active_at) = context
            .extension::<ActivityTracker>()
            .and_then(|tracker| tracker.track_activity())
        {
            ctx.send_with_delay(
                SessionManagerMessage::ProbeIdleSession {
                    session_id: session_id.clone(),
                    instant: active_at,
                },
                self.options.session_timeout,
            );
        }
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
            Self::delete_session(ctx, session_id.clone(), context);
            if let Some(driver_id) = *driver_id {
                self.drivers.remove(driver_id);
            }
            session.state = ServerSessionState::Deleted;
            let status = session.state.status().to_string();
            self.event_reporter.report(SystemEvent::SessionUpdated {
                session_id: session_id.clone(),
                status,
                updated_at: Utc::now(),
            });
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
                Self::delete_session(ctx, session_id.clone(), context);
                if let Some(driver_id) = *driver_id {
                    self.drivers.remove(driver_id);
                }
                session.state = ServerSessionState::Deleted;
                let status = session.state.status().to_string();
                self.event_reporter.report(SystemEvent::SessionUpdated {
                    session_id: session_id.clone(),
                    status,
                    updated_at: Utc::now(),
                });
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

    pub(super) fn handle_set_session_failure(
        &mut self,
        ctx: &mut ActorContext<Self>,
        session_id: String,
    ) -> ActorAction {
        let Some(session) = self.sessions.get_mut(&session_id) else {
            warn!("session not found: {session_id}");
            return ActorAction::Continue;
        };
        let (driver_id, waiters) = match &mut session.state {
            ServerSessionState::Creating { driver_id, waiters } => {
                (*driver_id, std::mem::take(waiters))
            }
            ServerSessionState::Running { driver_id, .. } => (*driver_id, vec![]),
            ServerSessionState::Deleted | ServerSessionState::Failed => (None, vec![]),
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
        let status = session.state.status().to_string();
        self.event_reporter.report(SystemEvent::SessionUpdated {
            session_id,
            status,
            updated_at: Utc::now(),
        });
        for waiter in waiters {
            let _ = waiter.send(Err(SessionError::internal(
                "session failed during creation",
            )));
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
