use std::borrow::Cow;

use datafusion::prelude::SessionContext;
use sail_telemetry::common::{SpanAssociation, SpanAttribute};
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::error::SessionResult;

#[expect(clippy::enum_variant_names)]
pub enum SessionManagerEvent {
    GetOrCreateSession {
        session_id: String,
        user_id: String,
        result: oneshot::Sender<SessionResult<SessionContext>>,
    },
    ProbeIdleSession {
        session_id: String,
        /// The time when the session was known to be active.
        instant: Instant,
    },
    DeleteSession {
        session_id: String,
        result: oneshot::Sender<SessionResult<()>>,
    },
}

impl SpanAssociation for SessionManagerEvent {
    fn name(&self) -> Cow<'static, str> {
        let name = match self {
            SessionManagerEvent::GetOrCreateSession { .. } => "GetOrCreateSession",
            SessionManagerEvent::ProbeIdleSession { .. } => "ProbeIdleSession",
            SessionManagerEvent::DeleteSession { .. } => "DeleteSession",
        };
        name.into()
    }

    fn properties(&self) -> impl IntoIterator<Item = (Cow<'static, str>, Cow<'static, str>)> {
        let mut p: Vec<(&'static str, String)> = vec![];
        match self {
            SessionManagerEvent::GetOrCreateSession {
                session_id,
                user_id: _,
                result: _,
            }
            | SessionManagerEvent::ProbeIdleSession {
                session_id,
                instant: _,
            }
            | SessionManagerEvent::DeleteSession {
                session_id,
                result: _,
            } => {
                p.push((SpanAttribute::SESSION_ID, session_id.to_string()));
            }
        }
        p.into_iter().map(|(k, v)| (k.into(), v.into()))
    }
}
