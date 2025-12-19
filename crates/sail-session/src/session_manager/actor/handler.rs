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
use crate::session_manager::actor::SessionManagerActor;
use crate::session_manager::event::SessionManagerEvent;
use crate::session_manager::SessionKey;

impl<K: SessionKey> SessionManagerActor<K> {
    pub(super) fn handle_get_or_create_session(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: K,
        result: oneshot::Sender<SessionResult<SessionContext>>,
    ) -> ActorAction {
        let context = if let Some(context) = self.sessions.get(&key) {
            Ok(context.clone())
        } else {
            let key = key.clone();
            info!("creating session {key}");
            let span = Span::root(
                "SessionManagerActor::create_session_context",
                SpanContext::random(),
            );
            let _guard = span.set_local_parent();
            match self.factory.create(key.clone()) {
                Ok(context) => {
                    self.sessions.insert(key, context.clone());
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
                        key,
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
        key: K,
        instant: Instant,
    ) -> ActorAction {
        let context = self.sessions.get(&key);
        if let Some(context) = context {
            if let Ok(tracker) = context.extension::<ActivityTracker>() {
                if tracker.active_at().is_ok_and(|x| x <= instant) {
                    info!("removing idle session {key}");
                    if let Ok(service) = context.extension::<JobService>() {
                        ctx.spawn(async move { service.runner().stop().await });
                    }
                    self.sessions.remove(&key);
                }
            }
        }
        ActorAction::Continue
    }

    pub(super) fn handle_delete_session(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: K,
        result: oneshot::Sender<SessionResult<()>>,
    ) -> ActorAction {
        let context = self.sessions.remove(&key);
        let output = if let Some(context) = context {
            info!("removing session {key}");
            if let Ok(service) = context.extension::<JobService>() {
                ctx.spawn(async move { service.runner().stop().await });
            }
            Ok(())
        } else {
            Err(SessionError::invalid(format!("session not found: {key}")))
        };
        let _ = result.send(output);
        ActorAction::Continue
    }
}
