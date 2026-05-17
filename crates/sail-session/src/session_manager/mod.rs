mod actor;
mod event;
mod options;
mod session;

use std::fmt;

use datafusion::prelude::SessionContext;
use sail_server::actor::ActorHandle;
use tokio::sync::oneshot;

use crate::error::{SessionError, SessionResult};
pub(crate) use crate::session_manager::actor::SessionManagerActor;
pub(crate) use crate::session_manager::event::SessionManagerEvent;
pub use crate::session_manager::options::SessionManagerOptions;

pub struct SessionManager {
    handle: ActorHandle<SessionManagerActor>,
}

impl fmt::Debug for SessionManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionManager").finish()
    }
}

impl SessionManager {
    pub fn try_new(options: SessionManagerOptions) -> SessionResult<Self> {
        let system = options.system.clone();
        let handle = system.lock()?.spawn::<SessionManagerActor>(options);
        Ok(Self { handle })
    }

    pub async fn get_or_create_session_context(
        &self,
        session_id: String,
        user_id: String,
    ) -> SessionResult<SessionContext> {
        let (tx, rx) = oneshot::channel();
        let event = SessionManagerEvent::GetOrCreateSession {
            session_id,
            user_id,
            result: tx,
        };
        self.handle.send(event).await?;
        rx.await
            .map_err(|e| SessionError::internal(format!("failed to get session: {e}")))?
    }

    pub async fn delete_session(&self, session_id: String) -> SessionResult<()> {
        let (tx, rx) = oneshot::channel();
        let event = SessionManagerEvent::DeleteSession {
            session_id,
            result: tx,
        };
        self.handle.send(event).await?;
        rx.await
            .map_err(|e| SessionError::internal(format!("failed to delete session: {e}")))?
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use datafusion::common::{internal_err, Result as DataFusionResult};
    use datafusion::execution::SendableRecordBatchStream;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use sail_common::runtime::RuntimeHandle;
    use sail_common_datafusion::session::job::{JobRunner, JobRunnerHistory, JobService};
    use sail_common_datafusion::system::observable::{
        JobRunnerObserver, Observer, StateObservable,
    };
    use sail_server::actor::ActorSystem;
    use tokio::sync::{oneshot, Notify};
    use tokio::time::Duration;

    use super::{SessionManager, SessionManagerOptions};
    use crate::error::{SessionError, SessionResult};
    use crate::session_factory::{ServerSessionInfo, SessionFactory};

    struct TestSessionFactory {
        state: Arc<TestJobRunnerState>,
    }

    impl SessionFactory<ServerSessionInfo> for TestSessionFactory {
        fn create(&mut self, _info: ServerSessionInfo) -> DataFusionResult<SessionContext> {
            let runner = Box::new(TestJobRunner {
                state: Arc::clone(&self.state),
            });
            let config = SessionConfig::new().with_extension(Arc::new(JobService::new(runner)));
            Ok(SessionContext::new_with_config(config))
        }
    }

    struct TestJobRunnerState {
        stop_started: Notify,
        stop_calls: AtomicUsize,
        finish: Mutex<Option<oneshot::Receiver<()>>>,
    }

    struct TestJobRunner {
        state: Arc<TestJobRunnerState>,
    }

    struct EmptySessionFactory;

    impl SessionFactory<ServerSessionInfo> for EmptySessionFactory {
        fn create(&mut self, _info: ServerSessionInfo) -> DataFusionResult<SessionContext> {
            Ok(SessionContext::new())
        }
    }

    #[tonic::async_trait]
    impl StateObservable<JobRunnerObserver> for TestJobRunner {
        async fn observe(&self, observer: JobRunnerObserver) {
            observer.nothing();
        }
    }

    #[tonic::async_trait]
    impl JobRunner for TestJobRunner {
        async fn execute(
            &self,
            _ctx: &SessionContext,
            _plan: Arc<dyn ExecutionPlan>,
        ) -> DataFusionResult<SendableRecordBatchStream> {
            internal_err!("test job runner does not execute plans")
        }

        async fn stop(&self, history: oneshot::Sender<JobRunnerHistory>) {
            self.state.stop_calls.fetch_add(1, Ordering::SeqCst);
            self.state.stop_started.notify_one();
            let finish = self.state.finish.lock().ok().and_then(|mut x| x.take());
            if let Some(finish) = finish {
                let _ = finish.await;
            }
            let _ = history.send(JobRunnerHistory {
                jobs: vec![],
                stages: vec![],
                tasks: vec![],
                workers: vec![],
            });
        }
    }

    fn test_session_manager_with_factory(
        factory: Box<dyn Fn() -> Box<dyn SessionFactory<ServerSessionInfo>> + Send>,
    ) -> SessionResult<SessionManager> {
        let handle = tokio::runtime::Handle::current();
        let runtime = RuntimeHandle::new(handle.clone(), handle);
        let system = Arc::new(Mutex::new(ActorSystem::new()));
        let options = SessionManagerOptions::new(runtime, system, factory);
        SessionManager::try_new(options)
    }

    fn test_session_manager(state: Arc<TestJobRunnerState>) -> SessionResult<SessionManager> {
        let factory_state = Arc::clone(&state);
        let factory = Box::new(move || {
            Box::new(TestSessionFactory {
                state: Arc::clone(&factory_state),
            }) as Box<dyn SessionFactory<ServerSessionInfo>>
        });
        test_session_manager_with_factory(factory)
    }

    fn test_session_manager_without_job_service() -> SessionResult<SessionManager> {
        test_session_manager_with_factory(Box::new(|| {
            Box::new(EmptySessionFactory) as Box<dyn SessionFactory<ServerSessionInfo>>
        }))
    }

    #[tokio::test]
    async fn delete_session_waits_for_job_runner_stop(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (finish_tx, finish_rx) = oneshot::channel();
        let state = Arc::new(TestJobRunnerState {
            stop_started: Notify::new(),
            stop_calls: AtomicUsize::new(0),
            finish: Mutex::new(Some(finish_rx)),
        });
        let manager = test_session_manager(Arc::clone(&state))?;
        let context = manager
            .get_or_create_session_context("session".to_string(), "user".to_string())
            .await?;
        drop(context);

        let delete =
            tokio::spawn(async move { manager.delete_session("session".to_string()).await });
        state.stop_started.notified().await;
        assert!(!delete.is_finished());

        let _ = finish_tx.send(());
        delete.await??;
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_delete_session_calls_wait_for_same_cleanup(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (finish_tx, finish_rx) = oneshot::channel();
        let state = Arc::new(TestJobRunnerState {
            stop_started: Notify::new(),
            stop_calls: AtomicUsize::new(0),
            finish: Mutex::new(Some(finish_rx)),
        });
        let manager = Arc::new(test_session_manager(Arc::clone(&state))?);
        let context = manager
            .get_or_create_session_context("session".to_string(), "user".to_string())
            .await?;
        drop(context);

        let first_manager = Arc::clone(&manager);
        let first_delete =
            tokio::spawn(async move { first_manager.delete_session("session".to_string()).await });
        state.stop_started.notified().await;

        let second_manager = Arc::clone(&manager);
        let second_delete =
            tokio::spawn(async move { second_manager.delete_session("session".to_string()).await });
        tokio::task::yield_now().await;

        assert_eq!(state.stop_calls.load(Ordering::SeqCst), 1);
        assert!(!first_delete.is_finished());
        assert!(!second_delete.is_finished());

        let _ = finish_tx.send(());
        first_delete.await??;
        second_delete.await??;
        assert_eq!(state.stop_calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn delete_missing_session_is_noop() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    {
        let (_finish_tx, finish_rx) = oneshot::channel();
        let state = Arc::new(TestJobRunnerState {
            stop_started: Notify::new(),
            stop_calls: AtomicUsize::new(0),
            finish: Mutex::new(Some(finish_rx)),
        });
        let manager = test_session_manager(state)?;
        manager.delete_session("missing".to_string()).await?;
        Ok(())
    }

    #[tokio::test]
    async fn delete_session_without_job_service_returns_error(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let manager = test_session_manager_without_job_service()?;
        let context = manager
            .get_or_create_session_context("session".to_string(), "user".to_string())
            .await?;
        drop(context);

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            manager.delete_session("session".to_string()),
        )
        .await?;

        assert!(matches!(
            result,
            Err(SessionError::InternalError(message))
                if message.contains("job service not found for session session")
        ));
        Ok(())
    }
}
