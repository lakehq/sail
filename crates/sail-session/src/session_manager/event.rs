use std::borrow::Cow;

use datafusion::prelude::SessionContext;
use sail_common_datafusion::session::job::JobRunnerHistory;
use sail_common_datafusion::system::observable::SessionManagerObserver;
use sail_telemetry::common::{SpanAssociation, SpanAttribute};
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::error::SessionResult;

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
    SetSessionHistory {
        session_id: String,
        history: SessionHistory,
    },
    SetSessionFailure {
        session_id: String,
    },
    ObserveState {
        observer: SessionManagerObserver,
    },
}

pub struct SessionHistory {
    pub job_runner: JobRunnerHistory,
}

impl SpanAssociation for SessionManagerEvent {
    fn name(&self) -> Cow<'static, str> {
        let name = match self {
            SessionManagerEvent::GetOrCreateSession { .. } => "GetOrCreateSession",
            SessionManagerEvent::ProbeIdleSession { .. } => "ProbeIdleSession",
            SessionManagerEvent::DeleteSession { .. } => "DeleteSession",
            SessionManagerEvent::SetSessionHistory { .. } => "SetSessionHistory",
            SessionManagerEvent::SetSessionFailure { .. } => "SetSessionFailure",
            SessionManagerEvent::ObserveState { .. } => "ObserveState",
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
            }
            | SessionManagerEvent::SetSessionHistory {
                session_id,
                history: _,
            }
            | SessionManagerEvent::SetSessionFailure { session_id } => {
                p.push((SpanAttribute::SESSION_ID, session_id.to_string()));
            }
            SessionManagerEvent::ObserveState { observer: _ } => {}
        }
        p.into_iter().map(|(k, v)| (k.into(), v.into()))
    }
}
