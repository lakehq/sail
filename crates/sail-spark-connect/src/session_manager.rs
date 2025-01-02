use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use log::info;
use sail_common::config::{AppConfig, ExecutionMode};
use sail_execution::job::{ClusterJobRunner, JobRunner, LocalJobRunner};
use sail_plan::function::BUILT_IN_SCALAR_FUNCTIONS;
use sail_plan::new_query_planner;
use sail_plan::object_store::DynamicObjectStoreRegistry;
use sail_plan::temp_view::TemporaryViewManager;
use sail_server::actor::{Actor, ActorAction, ActorContext, ActorHandle, ActorSystem};
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::error::{SparkError, SparkResult};
use crate::session::{SparkExtension, DEFAULT_SPARK_CATALOG, DEFAULT_SPARK_SCHEMA};

#[derive(Eq, PartialEq, Hash, Clone, Debug)]
pub struct SessionKey {
    pub user_id: Option<String>,
    pub session_id: String,
}

impl Display for SessionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(user_id) = &self.user_id {
            write!(f, "{}@{}", user_id, self.session_id)
        } else {
            write!(f, "{}", self.session_id)
        }
    }
}

pub struct SessionManager {
    system: Arc<Mutex<ActorSystem>>,
    handle: ActorHandle<SessionManagerActor>,
    config: Arc<AppConfig>,
}

impl Debug for SessionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionManager").finish()
    }
}

impl SessionManager {
    pub fn new(config: Arc<AppConfig>) -> Self {
        let mut system = ActorSystem::new();
        let handle = system.spawn::<SessionManagerActor>(config.as_ref().into());
        Self {
            system: Arc::new(Mutex::new(system)),
            handle,
            config,
        }
    }

    pub async fn get_or_create_session_context(
        &self,
        key: SessionKey,
    ) -> SparkResult<SessionContext> {
        let (tx, rx) = oneshot::channel();
        let event = SessionManagerEvent::GetOrCreateSession {
            key,
            system: self.system.clone(),
            config: self.config.clone(),
            result: tx,
        };
        self.handle.send(event).await?;
        rx.await
            .map_err(|e| SparkError::internal(format!("failed to get session: {e}")))?
    }

    pub fn create_session_context(
        config: Arc<AppConfig>,
        system: Arc<Mutex<ActorSystem>>,
        key: SessionKey,
    ) -> SparkResult<SessionContext> {
        let job_runner: Box<dyn JobRunner> = match config.mode {
            ExecutionMode::Local => Box::new(LocalJobRunner::new()),
            ExecutionMode::LocalCluster | ExecutionMode::KubernetesCluster => {
                let options = config.as_ref().try_into()?;
                let mut system = system.lock()?;
                Box::new(ClusterJobRunner::new(system.deref_mut(), options))
            }
        };
        let spark = SparkExtension::new(key.user_id, key.session_id, job_runner);
        // TODO: support more systematic configuration
        // TODO: return error on invalid environment variables
        let session_config = SessionConfig::new()
            .with_create_default_catalog_and_schema(true)
            .with_default_catalog_and_schema(DEFAULT_SPARK_CATALOG, DEFAULT_SPARK_SCHEMA)
            .with_information_schema(true)
            .with_extension(Arc::new(TemporaryViewManager::default()))
            .with_extension(Arc::new(spark))
            .set_usize(
                "datafusion.execution.batch_size",
                config.execution.batch_size,
            )
            .set_usize(
                "datafusion.execution.parquet.maximum_parallel_row_group_writers",
                config.parquet.maximum_parallel_row_group_writers,
            )
            .set_usize(
                "datafusion.execution.parquet.maximum_buffered_record_batches_per_stream",
                config.parquet.maximum_buffered_record_batches_per_stream,
            )
            // Spark defaults to false:
            //  https://spark.apache.org/docs/latest/sql-data-sources-csv.html
            .set_bool("datafusion.catalog.has_header", false);
        let runtime = {
            let registry = DynamicObjectStoreRegistry::new();
            let config = RuntimeEnvBuilder::default()
                .with_object_store_registry(Arc::new(registry))
                .build()?;
            Arc::new(config)
        };
        let state = SessionStateBuilder::new()
            .with_config(session_config)
            .with_runtime_env(runtime)
            .with_default_features()
            .with_query_planner(new_query_planner())
            .build();
        let context = SessionContext::new_with_state(state);

        // TODO: This is a temp workaround to deregister all built-in functions that we define.
        //   We should deregister all context.udfs() once we have better coverage of functions.
        //   handler.rs needs to do this
        for (&name, _function) in BUILT_IN_SCALAR_FUNCTIONS.iter() {
            context.deregister_udf(name);
        }

        Ok(context)
    }
}

struct SessionManagerOptions {
    pub session_timeout: Duration,
}

impl From<&AppConfig> for SessionManagerOptions {
    fn from(config: &AppConfig) -> Self {
        Self {
            session_timeout: Duration::from_secs(config.spark.session_timeout_secs),
        }
    }
}

enum SessionManagerEvent {
    GetOrCreateSession {
        key: SessionKey,
        system: Arc<Mutex<ActorSystem>>,
        config: Arc<AppConfig>,
        result: oneshot::Sender<SparkResult<SessionContext>>,
    },
    ProbeIdleSession {
        key: SessionKey,
        /// The time when the session was known to be active.
        instant: Instant,
    },
}

struct SessionManagerActor {
    options: SessionManagerOptions,
    sessions: HashMap<SessionKey, SessionContext>,
}

#[tonic::async_trait]
impl Actor for SessionManagerActor {
    type Message = SessionManagerEvent;
    type Options = SessionManagerOptions;

    fn new(options: Self::Options) -> Self {
        Self {
            options,
            sessions: HashMap::new(),
        }
    }

    fn receive(&mut self, ctx: &mut ActorContext<Self>, message: Self::Message) -> ActorAction {
        match message {
            SessionManagerEvent::GetOrCreateSession {
                key,
                system,
                config,
                result,
            } => self.handle_get_or_create_session(ctx, key, system, config, result),
            SessionManagerEvent::ProbeIdleSession { key, instant } => {
                self.handle_probe_idle_session(ctx, key, instant)
            }
        }
    }
}

impl SessionManagerActor {
    fn handle_get_or_create_session(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: SessionKey,
        system: Arc<Mutex<ActorSystem>>,
        config: Arc<AppConfig>,
        result: oneshot::Sender<SparkResult<SessionContext>>,
    ) -> ActorAction {
        let entry = self.sessions.entry(key.clone());
        let context = match entry {
            Entry::Occupied(o) => Ok(o.get().clone()),
            Entry::Vacant(v) => {
                let key = v.key().clone();
                info!("creating session {}", key);
                match SessionManager::create_session_context(config, system, key) {
                    Ok(context) => Ok(v.insert(context).clone()),
                    Err(e) => Err(e),
                }
            }
        };
        if let Ok(context) = &context {
            if let Ok(active_at) =
                SparkExtension::get(context).and_then(|spark| spark.track_activity())
            {
                ctx.send_with_delay(
                    SessionManagerEvent::ProbeIdleSession {
                        key,
                        instant: active_at,
                    },
                    self.options.session_timeout,
                );
            }
        }
        let _ = result.send(context);
        ActorAction::Continue
    }

    fn handle_probe_idle_session(
        &mut self,
        ctx: &mut ActorContext<Self>,
        key: SessionKey,
        instant: Instant,
    ) -> ActorAction {
        let context = self.sessions.get(&key);
        if let Some(context) = context {
            if let Ok(spark) = SparkExtension::get(context) {
                if spark.active_at().is_ok_and(|x| x <= instant) {
                    info!("removing idle session {}", key);
                    ctx.spawn(async move { spark.job_runner().stop().await });
                    self.sessions.remove(&key);
                }
            }
        }
        ActorAction::Continue
    }
}
