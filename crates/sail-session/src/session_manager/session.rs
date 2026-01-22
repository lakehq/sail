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

pub struct ServerSession {
    pub user_id: String,
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
        if let ServerSessionState::Running { context } = &self.state {
            if let Ok(service) = context.extension::<JobService>() {
                async move {
                    service.runner().observe(observer).await;
                    rx.await
                        .map_err(|_| exec_datafusion_err!("failed to observe job runner"))?
                }
                .boxed()
            } else {
                async {
                    Err(exec_datafusion_err!(
                        "job service not found in session context"
                    ))
                }
                .boxed()
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
    Deleting,
    Deleted { history: Arc<SessionHistory> },
    Failed,
}

impl ServerSessionState {
    pub fn status(&self) -> &'static str {
        match self {
            ServerSessionState::Running { .. } => "RUNNING",
            ServerSessionState::Deleting => "DELETING",
            ServerSessionState::Deleted { .. } => "DELETED",
            ServerSessionState::Failed => "FAILED",
        }
    }
}
