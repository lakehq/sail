use std::time::Duration;

use datafusion::prelude::SessionContext;
use fastrace::collector::SpanContext;
use fastrace::Span;
use log::info;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::{ActivityTracker, JobService};
use sail_server::actor::{ActorAction, ActorContext};
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::error::{SessionError, SessionResult};
use crate::session_factory::ServerSessionInfo;
use crate::session_manager::actor::SessionManagerActor;
use crate::session_manager::event::SessionManagerEvent;

impl SessionManagerActor {
    pub(super) fn handle_get_or_create_session(
        &mut self,
        ctx: &mut ActorContext<Self>,
        session_id: String,
        user_id: String,
        result: oneshot::Sender<SessionResult<SessionContext>>,
    ) -> ActorAction {
        let context = if let Some(context) = self.sessions.get(&session_id) {
            Ok(context.clone())
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
                user_id,
                session_manager: ctx.handle().clone(),
            };
            match self.factory.create(info) {
                Ok(context) => {
                    self.sessions.insert(session_id, context.clone());
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
                    Duration::from_secs(self.options.config.spark.session_timeout_secs),
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
        let context = self.sessions.get(&session_id);
        if let Some(context) = context {
            if let Ok(tracker) = context.extension::<ActivityTracker>() {
                if tracker.active_at().is_ok_and(|x| x <= instant) {
                    info!("removing idle session {session_id}");
                    if let Ok(service) = context.extension::<JobService>() {
                        ctx.spawn(async move { service.runner().stop().await });
                    }
                    self.sessions.remove(&session_id);
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
        let context = self.sessions.remove(&session_id);
        let output = if let Some(context) = context {
            info!("removing session {session_id}");
            if let Ok(service) = context.extension::<JobService>() {
                ctx.spawn(async move { service.runner().stop().await });
            }
            Ok(())
        } else {
            Err(SessionError::invalid(format!(
                "session not found: {session_id}"
            )))
        };
        let _ = result.send(output);
        ActorAction::Continue
    }
}
