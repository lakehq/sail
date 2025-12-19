use std::borrow::Cow;
use std::fmt;

use datafusion::prelude::SessionContext;
use sail_telemetry::common::{SpanAssociation, SpanAttribute};
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::error::SessionResult;

#[expect(clippy::enum_variant_names)]
pub enum SessionManagerEvent<K> {
    GetOrCreateSession {
        key: K,
        result: oneshot::Sender<SessionResult<SessionContext>>,
    },
    ProbeIdleSession {
        key: K,
        /// The time when the session was known to be active.
        instant: Instant,
    },
    DeleteSession {
        key: K,
        result: oneshot::Sender<SessionResult<()>>,
    },
}

impl<K> SpanAssociation for SessionManagerEvent<K>
where
    K: fmt::Display,
{
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
            SessionManagerEvent::GetOrCreateSession { key, result: _ }
            | SessionManagerEvent::ProbeIdleSession { key, instant: _ }
            | SessionManagerEvent::DeleteSession { key, result: _ } => {
                p.push((SpanAttribute::SESSION_KEY, key.to_string()));
            }
        }
        p.into_iter().map(|(k, v)| (k.into(), v.into()))
    }
}
