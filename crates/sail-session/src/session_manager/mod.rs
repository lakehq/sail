mod actor;
mod event;
mod options;
mod session;

use std::fmt;
use std::sync::{Arc, Mutex};

use datafusion::prelude::SessionContext;
use sail_server::actor::{ActorHandle, ActorSystem};
use tokio::sync::oneshot;

use crate::error::{SessionError, SessionResult};
pub(crate) use crate::session_manager::actor::SessionManagerActor;
pub(crate) use crate::session_manager::event::SessionManagerEvent;
pub use crate::session_manager::options::SessionManagerOptions;

#[derive(Clone)]
pub struct SessionManager {
    handle: ActorHandle<SessionManagerActor>,
    actor_system: Arc<Mutex<ActorSystem>>,
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
        Ok(Self {
            handle,
            actor_system: system,
        })
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

    pub async fn delete_session(&self, session_id: String, user_id: String) -> SessionResult<()> {
        let (tx, rx) = oneshot::channel();
        let event = SessionManagerEvent::DeleteSession {
            session_id,
            user_id,
            result: tx,
        };
        self.handle.send(event).await?;
        rx.await
            .map_err(|e| SessionError::internal(format!("failed to delete session: {e}")))?
    }

    pub async fn shutdown(&self) -> SessionResult<()> {
        let actor_system = self.actor_system.clone();
        let (tx, rx) = oneshot::channel();
        self.handle
            .send(SessionManagerEvent::Shutdown { result: tx })
            .await?;
        let result = rx.await.map_err(|error| {
            SessionError::internal(format!("failed to shut down sessions: {error}"))
        })?;
        drop(actor_system);
        result
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::panic)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex, Weak};

    use datafusion::execution::SendableRecordBatchStream;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use sail_common::runtime::RuntimeHandle;
    use sail_common_datafusion::session::job::{JobRunner, JobRunnerHistory, JobService};
    use sail_common_datafusion::system::observable::{
        JobRunnerObserver, Observer, SessionManagerObserver, StateObservable,
    };
    use sail_common_datafusion::system::predicate::{Predicate, Predicates};
    use sail_server::actor::ActorSystem;
    use tokio::sync::Notify;

    use super::*;
    use crate::session_factory::{ServerSessionInfo, SessionFactory};

    type CreationLog = Arc<Mutex<Vec<(String, String)>>>;

    struct TestSessionFactory {
        creations: CreationLog,
    }

    #[derive(Clone)]
    struct JobShutdownControl {
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    struct BlockingJobRunner {
        control: JobShutdownControl,
    }

    #[tonic::async_trait]
    impl StateObservable<JobRunnerObserver> for BlockingJobRunner {
        async fn observe(&self, observer: JobRunnerObserver) {
            observer.nothing();
        }
    }

    #[tonic::async_trait]
    impl JobRunner for BlockingJobRunner {
        async fn execute(
            &self,
            _ctx: &SessionContext,
            _plan: Arc<dyn ExecutionPlan>,
        ) -> datafusion::common::Result<SendableRecordBatchStream> {
            datafusion::common::internal_err!("blocking test job runner cannot execute plans")
        }

        async fn stop(&self, history: oneshot::Sender<JobRunnerHistory>) {
            self.control.started.notify_one();
            self.control.release.notified().await;
            let _ = history.send(JobRunnerHistory {
                jobs: vec![],
                stages: vec![],
                tasks: vec![],
                workers: vec![],
            });
        }
    }

    struct LifetimeTracker(Arc<AtomicBool>);

    impl Drop for LifetimeTracker {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    struct ShutdownSessionFactory {
        control: JobShutdownControl,
        context_states: Arc<Mutex<Vec<Weak<dyn Any + Send + Sync>>>>,
        _factory_lifetime: LifetimeTracker,
    }

    impl SessionFactory<ServerSessionInfo> for ShutdownSessionFactory {
        fn create(
            &mut self,
            _info: ServerSessionInfo,
        ) -> datafusion::common::Result<SessionContext> {
            let config = datafusion::prelude::SessionConfig::new().with_extension(Arc::new(
                JobService::new(Box::new(BlockingJobRunner {
                    control: self.control.clone(),
                })),
            ));
            let context = SessionContext::new_with_config(config);
            let state: Arc<dyn Any + Send + Sync> = context.state_ref();
            self.context_states
                .lock()
                .expect("context state lock must not be poisoned")
                .push(Arc::downgrade(&state));
            Ok(context)
        }
    }

    struct ShutdownTestResources {
        control: JobShutdownControl,
        context_states: Arc<Mutex<Vec<Weak<dyn Any + Send + Sync>>>>,
        factory_dropped: Arc<AtomicBool>,
        options_dropped: Arc<AtomicBool>,
        actor_system: Weak<Mutex<ActorSystem>>,
    }

    impl SessionFactory<ServerSessionInfo> for TestSessionFactory {
        fn create(
            &mut self,
            info: ServerSessionInfo,
        ) -> datafusion::common::Result<SessionContext> {
            self.creations
                .lock()
                .expect("creation log lock must not be poisoned")
                .push((info.session_id, info.user_id));
            Ok(SessionContext::new())
        }
    }

    fn create_session_manager(max_sessions: usize) -> (SessionManager, CreationLog) {
        let creations = Arc::new(Mutex::new(Vec::new()));
        let factory_creations = creations.clone();
        let factory = Box::new(move || {
            Box::new(TestSessionFactory {
                creations: factory_creations.clone(),
            }) as Box<dyn SessionFactory<ServerSessionInfo>>
        });
        let runtime = RuntimeHandle::new(
            tokio::runtime::Handle::current(),
            tokio::runtime::Handle::current(),
        );
        let system = Arc::new(Mutex::new(ActorSystem::new()));
        let options =
            SessionManagerOptions::new(runtime, system, factory).session_limit(max_sessions);
        (
            SessionManager::try_new(options).expect("session manager must be created"),
            creations,
        )
    }

    fn create_shutdown_test_manager() -> (SessionManager, ShutdownTestResources) {
        let control = JobShutdownControl {
            started: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        };
        let context_states = Arc::new(Mutex::new(Vec::new()));
        let factory_dropped = Arc::new(AtomicBool::new(false));
        let options_dropped = Arc::new(AtomicBool::new(false));
        let factory_control = control.clone();
        let factory_context_states = context_states.clone();
        let factory_lifetime = factory_dropped.clone();
        let options_lifetime = LifetimeTracker(options_dropped.clone());
        let factory = Box::new(move || {
            let _retain_options_lifetime = &options_lifetime;
            Box::new(ShutdownSessionFactory {
                control: factory_control.clone(),
                context_states: factory_context_states.clone(),
                _factory_lifetime: LifetimeTracker(factory_lifetime.clone()),
            }) as Box<dyn SessionFactory<ServerSessionInfo>>
        });
        let runtime = RuntimeHandle::new(
            tokio::runtime::Handle::current(),
            tokio::runtime::Handle::current(),
        );
        let system = Arc::new(Mutex::new(ActorSystem::new()));
        let actor_system = Arc::downgrade(&system);
        let options = SessionManagerOptions::new(runtime, system, factory);
        let manager = SessionManager::try_new(options).expect("session manager must be created");
        (
            manager,
            ShutdownTestResources {
                control,
                context_states,
                factory_dropped,
                options_dropped,
                actor_system,
            },
        )
    }

    async fn observe_sessions(
        manager: &SessionManager,
        session_id: Predicate<String>,
    ) -> Vec<sail_common_datafusion::system::catalog::SessionRow> {
        let (tx, rx) = oneshot::channel();
        manager
            .handle
            .send(SessionManagerEvent::ObserveState {
                observer: SessionManagerObserver::Sessions {
                    session_id,
                    fetch: usize::MAX,
                    result: tx,
                },
            })
            .await
            .expect("session observation must be sent");
        rx.await
            .expect("session observation must return")
            .expect("session observation must succeed")
    }

    #[tokio::test]
    async fn same_session_id_is_independent_across_users() {
        let (manager, creations) = create_session_manager(4);

        let user_a = manager
            .get_or_create_session_context("shared".into(), "user-a".into())
            .await
            .expect("user-a session must be created");
        let user_b = manager
            .get_or_create_session_context("shared".into(), "user-b".into())
            .await
            .expect("user-b session must be created");
        let user_a_again = manager
            .get_or_create_session_context("shared".into(), "user-a".into())
            .await
            .expect("user-a session lookup must succeed");

        assert_ne!(user_a.session_id(), user_b.session_id());
        assert_eq!(user_a.session_id(), user_a_again.session_id());
        assert_eq!(
            *creations
                .lock()
                .expect("creation log lock must not be poisoned"),
            vec![
                ("shared".into(), "user-a".into()),
                ("shared".into(), "user-b".into())
            ]
        );

        let rows = observe_sessions(
            &manager,
            Arc::new(|session_id: &String| Ok(session_id == "shared")),
        )
        .await;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].session_id, "shared");
        assert_eq!(rows[0].user_id, "user-a");
        assert_eq!(rows[1].session_id, "shared");
        assert_eq!(rows[1].user_id, "user-b");
    }

    #[tokio::test]
    async fn admission_evicts_failed_session_without_job_service() {
        let (manager, creations) = create_session_manager(2);
        manager
            .get_or_create_session_context("shared".into(), "user-a".into())
            .await
            .expect("user-a session must be created");
        let user_b = manager
            .get_or_create_session_context("shared".into(), "user-b".into())
            .await
            .expect("user-b session must be created");

        let user_b_again = manager
            .get_or_create_session_context("shared".into(), "user-b".into())
            .await
            .expect("existing session lookup must be allowed at capacity");
        assert_eq!(user_b.session_id(), user_b_again.session_id());

        let error = match manager
            .get_or_create_session_context("new".into(), "user-c".into())
            .await
        {
            Ok(_) => panic!("new session must be rejected while all slots are running"),
            Err(error) => error,
        };
        assert!(matches!(error, SessionError::InvalidArgument(_)));
        assert_eq!(
            creations
                .lock()
                .expect("creation log lock must not be poisoned")
                .len(),
            2
        );

        manager
            .delete_session("shared".into(), "user-a".into())
            .await
            .expect("session deletion request must succeed");
        let rows = observe_sessions(&manager, Predicates::always_true()).await;
        assert_eq!(rows[0].status, "FAILED");
        assert_eq!(rows[1].status, "RUNNING");

        manager
            .get_or_create_session_context("new".into(), "user-c".into())
            .await
            .expect("terminal session must be evicted for a new session");

        let rows = observe_sessions(&manager, Predicates::always_true()).await;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].session_id, "shared");
        assert_eq!(rows[0].user_id, "user-b");
        assert_eq!(rows[1].session_id, "new");
        assert_eq!(rows[1].user_id, "user-c");
        assert_eq!(
            creations
                .lock()
                .expect("creation log lock must not be poisoned")
                .len(),
            3
        );
    }

    #[tokio::test]
    async fn shutdown_waits_for_sessions_and_releases_owned_resources() {
        let (manager, resources) = create_shutdown_test_manager();
        let context = manager
            .get_or_create_session_context("session".into(), "user".into())
            .await
            .expect("session must be created");
        drop(context);
        let context_state = resources
            .context_states
            .lock()
            .expect("context state lock must not be poisoned")[0]
            .clone();

        let stop_started = resources.control.started.notified();
        let shutdown_manager = manager.clone();
        let shutdown_task = tokio::spawn(async move { shutdown_manager.shutdown().await });
        stop_started.await;
        tokio::task::yield_now().await;

        assert!(!shutdown_task.is_finished());
        assert!(context_state.upgrade().is_some());
        assert!(!resources.factory_dropped.load(Ordering::SeqCst));
        assert!(!resources.options_dropped.load(Ordering::SeqCst));
        let create_result = manager
            .get_or_create_session_context("new-session".into(), "user".into())
            .await;
        assert!(matches!(
            create_result,
            Err(SessionError::InvalidArgument(message)) if message.contains("shutting down")
        ));

        resources.control.release.notify_one();
        shutdown_task
            .await
            .expect("shutdown task must be joined")
            .expect("session manager shutdown must succeed");

        assert!(context_state.upgrade().is_none());
        assert!(resources.factory_dropped.load(Ordering::SeqCst));
        assert!(resources.options_dropped.load(Ordering::SeqCst));
        assert!(resources.actor_system.upgrade().is_some());
        drop(manager);
        assert!(resources.actor_system.upgrade().is_none());
    }
}
