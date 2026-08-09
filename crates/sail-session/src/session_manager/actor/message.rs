use std::borrow::Cow;

use datafusion::prelude::SessionContext;
use sail_common::telemetry::{SpanAssociation, SpanAttribute};
use sail_common_datafusion::session::job::JobRunnerHistory;
use sail_common_datafusion::system::observable::SessionManagerObserver;
use sail_execution::DriverId;
use sail_execution::driver::DriverHandle;
use sail_execution::error::ExecutionResult;
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::error::SessionResult;

pub enum SessionManagerMessage {
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
    GetDriver {
        driver_id: DriverId,
        result: oneshot::Sender<ExecutionResult<DriverHandle>>,
    },
    Shutdown {
        result: oneshot::Sender<()>,
    },
}

pub struct SessionHistory {
    pub job_runner: JobRunnerHistory,
}

impl SpanAssociation for SessionManagerMessage {
    fn name(&self) -> Cow<'static, str> {
        let name = match self {
            SessionManagerMessage::GetOrCreateSession { .. } => "GetOrCreateSession",
            SessionManagerMessage::ProbeIdleSession { .. } => "ProbeIdleSession",
            SessionManagerMessage::DeleteSession { .. } => "DeleteSession",
            SessionManagerMessage::SetSessionHistory { .. } => "SetSessionHistory",
            SessionManagerMessage::SetSessionFailure { .. } => "SetSessionFailure",
            SessionManagerMessage::ObserveState { .. } => "ObserveState",
            SessionManagerMessage::GetDriver { .. } => "GetDriver",
            SessionManagerMessage::Shutdown { .. } => "Shutdown",
        };
        name.into()
    }

    fn properties(&self) -> impl IntoIterator<Item = (Cow<'static, str>, Cow<'static, str>)> {
        let mut p: Vec<(&'static str, String)> = vec![];
        match self {
            SessionManagerMessage::GetOrCreateSession {
                session_id,
                user_id: _,
                result: _,
            }
            | SessionManagerMessage::ProbeIdleSession {
                session_id,
                instant: _,
            }
            | SessionManagerMessage::DeleteSession {
                session_id,
                result: _,
            }
            | SessionManagerMessage::SetSessionHistory {
                session_id,
                history: _,
            }
            | SessionManagerMessage::SetSessionFailure { session_id } => {
                p.push((SpanAttribute::SESSION_ID, session_id.to_string()));
            }
            SessionManagerMessage::GetDriver {
                driver_id,
                result: _,
            } => {
                p.push((SpanAttribute::CLUSTER_DRIVER_ID, driver_id.to_string()));
            }
            SessionManagerMessage::ObserveState { observer: _ }
            | SessionManagerMessage::Shutdown { .. } => {}
        }
        p.into_iter().map(|(k, v)| (k.into(), v.into()))
    }
}
