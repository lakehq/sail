use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::common::Result;
use datafusion::prelude::SessionContext;
use datafusion_common::exec_datafusion_err;
use futures::FutureExt;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::job::JobService;
use sail_common_datafusion::system::observable::{JobRunnerObserver, StateObservable};
use tokio::sync::oneshot;

use crate::session_manager::event::SessionHistory;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SessionKey {
    session_id: String,
    user_id: String,
}

impl SessionKey {
    pub(crate) fn new(session_id: String, user_id: String) -> Self {
        Self {
            session_id,
            user_id,
        }
    }

    pub(crate) fn session_id(&self) -> &String {
        &self.session_id
    }

    pub(crate) fn user_id(&self) -> &String {
        &self.user_id
    }
}

pub struct ServerSession {
    pub created_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub state: ServerSessionState,
}

impl ServerSession {
    pub fn observe_job_runner<T, F>(
        &self,
        observer: F,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<T>>> + Send>>
    where
        T: Send + 'static,
        F: FnOnce(oneshot::Sender<Result<Vec<T>>>) -> JobRunnerObserver,
    {
        let (tx, rx) = oneshot::channel();
        let observer = observer(tx);
        if let ServerSessionState::Running { context } | ServerSessionState::Deleting { context } =
            &self.state
        {
            match context.extension::<JobService>() {
                Ok(service) => async move {
                    service.runner().observe(observer).await;
                    rx.await
                        .map_err(|_| exec_datafusion_err!("failed to observe job runner"))?
                }
                .boxed(),
                _ => async {
                    Err(exec_datafusion_err!(
                        "job service not found in session context"
                    ))
                }
                .boxed(),
            }
        } else if let ServerSessionState::Deleted { history } = &self.state {
            let history = history.clone();
            async move {
                history.job_runner.observe(observer).await;
                rx.await
                    .map_err(|_| exec_datafusion_err!("failed to observe job runner history"))?
            }
            .boxed()
        } else {
            async { Ok(vec![]) }.boxed()
        }
    }
}

pub enum ServerSessionState {
    Running { context: SessionContext },
    Deleting { context: SessionContext },
    Deleted { history: Arc<SessionHistory> },
    Failed,
}

impl ServerSessionState {
    pub fn status(&self) -> &'static str {
        match self {
            ServerSessionState::Running { .. } => "RUNNING",
            ServerSessionState::Deleting { .. } => "DELETING",
            ServerSessionState::Deleted { .. } => "DELETED",
            ServerSessionState::Failed => "FAILED",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deleting_state_retains_session_context() {
        let context = SessionContext::new();
        let datafusion_session_id = context.session_id();
        let state = ServerSessionState::Deleting { context };

        assert!(matches!(
            state,
            ServerSessionState::Deleting { context }
                if context.session_id() == datafusion_session_id
        ));
    }
}
