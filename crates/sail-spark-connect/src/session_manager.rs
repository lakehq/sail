use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};

use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use sail_common::config::{AppConfig, ExecutionMode};
use sail_execution::job::{ClusterJobRunner, JobRunner, LocalJobRunner};
use sail_plan::function::BUILT_IN_SCALAR_FUNCTIONS;
use sail_plan::new_query_planner;
use sail_plan::object_store::DynamicObjectStoreRegistry;
use sail_plan::temp_view::TemporaryViewManager;
use sail_server::actor::ActorSystem;

use crate::error::SparkResult;
use crate::session::{SparkExtension, DEFAULT_SPARK_CATALOG, DEFAULT_SPARK_SCHEMA};

#[derive(Eq, PartialEq, Hash, Clone, Debug)]
pub(crate) struct SessionKey {
    pub user_id: Option<String>,
    pub session_id: String,
}

pub struct SessionManager {
    state: Mutex<SessionManagerState>,
    config: Arc<AppConfig>,
}

impl Debug for SessionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionManager").finish()
    }
}

impl SessionManager {
    pub fn new(config: Arc<AppConfig>) -> Self {
        Self {
            state: Mutex::new(SessionManagerState::new()),
            config,
        }
    }

    pub(crate) fn get_or_create_session_context(
        &self,
        key: SessionKey,
    ) -> SparkResult<SessionContext> {
        let mut state = self.state.lock()?;
        let state = state.deref_mut();
        let entry = state.sessions.entry(key);
        match entry {
            Entry::Occupied(o) => Ok(o.get().clone()),
            Entry::Vacant(v) => {
                let key = v.key().clone();
                let context = self.create_session_context(&mut state.actor_system, key)?;
                Ok(v.insert(context).clone())
            }
        }
    }

    fn create_session_context(
        &self,
        actor_system: &mut ActorSystem,
        key: SessionKey,
    ) -> SparkResult<SessionContext> {
        let job_runner: Box<dyn JobRunner> = match self.config.mode {
            ExecutionMode::Local => Box::new(LocalJobRunner::new()),
            ExecutionMode::LocalCluster | ExecutionMode::KubernetesCluster => {
                let options = self.config.as_ref().try_into()?;
                Box::new(ClusterJobRunner::new(actor_system, options))
            }
        };
        let spark = SparkExtension::new(key.user_id, key.session_id, job_runner);
        // TODO: support more systematic configuration
        // TODO: return error on invalid environment variables
        let config = SessionConfig::new()
            .with_create_default_catalog_and_schema(true)
            .with_default_catalog_and_schema(DEFAULT_SPARK_CATALOG, DEFAULT_SPARK_SCHEMA)
            .with_information_schema(true)
            .with_extension(Arc::new(TemporaryViewManager::default()))
            .with_extension(Arc::new(spark))
            .set_usize(
                "datafusion.execution.batch_size",
                self.config.execution.batch_size,
            )
            .set_usize(
                "datafusion.execution.parquet.maximum_parallel_row_group_writers",
                self.config.parquet.maximum_parallel_row_group_writers,
            )
            .set_usize(
                "datafusion.execution.parquet.maximum_buffered_record_batches_per_stream",
                self.config
                    .parquet
                    .maximum_buffered_record_batches_per_stream,
            )
            // Spark defaults to false:
            //  https://spark.apache.org/docs/latest/sql-data-sources-csv.html
            .set_bool("datafusion.catalog.has_header", false);
        let runtime = {
            let registry = DynamicObjectStoreRegistry::new();
            let config = RuntimeConfig::default().with_object_store_registry(Arc::new(registry));
            Arc::new(RuntimeEnv::try_new(config)?)
        };
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features()
            .with_query_planner(new_query_planner())
            .build();
        let context = SessionContext::new_with_state(state);

        // TODO: This is a temp workaround to deregister all built-in functions that we define.
        //  We should deregister all context.udfs() once we have better coverage of functions.
        // handler.rs needs to do this
        for (&name, _function) in BUILT_IN_SCALAR_FUNCTIONS.iter() {
            context.deregister_udf(name);
        }

        Ok(context)
    }

    #[allow(dead_code)]
    pub(crate) fn remove_session(&self, key: &SessionKey) -> SparkResult<()> {
        let mut state = self.state.lock()?;
        state.sessions.remove(key);
        Ok(())
    }
}

struct SessionManagerState {
    sessions: HashMap<SessionKey, SessionContext>,
    actor_system: ActorSystem,
}

impl SessionManagerState {
    pub fn new() -> Self {
        Self {
            sessions: HashMap::new(),
            actor_system: ActorSystem::new(),
        }
    }
}
