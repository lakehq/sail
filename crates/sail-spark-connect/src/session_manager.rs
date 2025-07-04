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
use sail_common::runtime::RuntimeHandle;
use sail_execution::driver::DriverOptions;
use sail_execution::job::{ClusterJobRunner, JobRunner, LocalJobRunner};
use sail_object_store::DynamicObjectStoreRegistry;
use sail_plan::extension::analyzer::default_analyzer_rules;
use sail_plan::extension::optimizer::default_optimizer_rules;
use sail_plan::function::{
    BUILT_IN_GENERATOR_FUNCTIONS, BUILT_IN_SCALAR_FUNCTIONS, BUILT_IN_TABLE_FUNCTIONS,
};
use sail_plan::new_query_planner;
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
}

impl Debug for SessionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionManager").finish()
    }
}

impl SessionManager {
    pub fn new(options: SessionManagerOptions) -> Self {
        let mut system = ActorSystem::new();
        let handle = system.spawn::<SessionManagerActor>(options);
        Self {
            system: Arc::new(Mutex::new(system)),
            handle,
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
            result: tx,
        };
        self.handle.send(event).await?;
        rx.await
            .map_err(|e| SparkError::internal(format!("failed to get session: {e}")))?
    }

    pub fn create_session_context(
        system: Arc<Mutex<ActorSystem>>,
        key: SessionKey,
        options: SessionManagerOptions,
    ) -> SparkResult<SessionContext> {
        let job_runner: Box<dyn JobRunner> = match options.config.mode {
            ExecutionMode::Local => Box::new(LocalJobRunner::new()),
            ExecutionMode::LocalCluster | ExecutionMode::KubernetesCluster => {
                let options = DriverOptions::try_new(&options.config, options.runtime.clone())?;
                let mut system = system.lock()?;
                Box::new(ClusterJobRunner::new(system.deref_mut(), options))
            }
        };
        // TODO: support more systematic configuration
        // TODO: return error on invalid environment variables
        let mut session_config = SessionConfig::new()
            .with_create_default_catalog_and_schema(true)
            .with_default_catalog_and_schema(DEFAULT_SPARK_CATALOG, DEFAULT_SPARK_SCHEMA)
            // We do not use the information schema since we use the catalog/schema/table providers
            // directly for catalog operations.
            .with_information_schema(false)
            .with_extension(Arc::new(TemporaryViewManager::default()))
            .with_extension(Arc::new(SparkExtension::try_new(
                key.user_id,
                key.session_id,
                job_runner,
            )?));

        // execution options
        {
            let execution = &mut session_config.options_mut().execution;

            execution.batch_size = options.config.execution.batch_size;
            execution.collect_statistics = options.config.execution.collect_statistics;
            execution.listing_table_ignore_subdirectory = false;
        }

        // execution Parquet options
        {
            let parquet = &mut session_config.options_mut().execution.parquet;

            parquet.created_by = concat!("sail version ", env!("CARGO_PKG_VERSION")).into();
            parquet.enable_page_index = options.config.parquet.enable_page_index;
            parquet.pruning = options.config.parquet.pruning;
            parquet.skip_metadata = options.config.parquet.skip_metadata;
            parquet.metadata_size_hint = options.config.parquet.metadata_size_hint;
            parquet.pushdown_filters = options.config.parquet.pushdown_filters;
            parquet.reorder_filters = options.config.parquet.reorder_filters;
            parquet.schema_force_view_types = options.config.parquet.schema_force_view_types;
            parquet.binary_as_string = options.config.parquet.binary_as_string;
            parquet.coerce_int96 = Some("us".to_string());
            parquet.data_pagesize_limit = options.config.parquet.data_page_size_limit;
            parquet.write_batch_size = options.config.parquet.write_batch_size;
            parquet.writer_version = options.config.parquet.writer_version.clone();
            parquet.skip_arrow_metadata = options.config.parquet.skip_arrow_metadata;
            parquet.compression = Some(options.config.parquet.compression.clone());
            parquet.dictionary_enabled = Some(options.config.parquet.dictionary_enabled);
            parquet.dictionary_page_size_limit = options.config.parquet.dictionary_page_size_limit;
            parquet.statistics_enabled = Some(options.config.parquet.statistics_enabled.clone());
            parquet.max_row_group_size = options.config.parquet.max_row_group_size;
            parquet.column_index_truncate_length =
                options.config.parquet.column_index_truncate_length;
            parquet.statistics_truncate_length = options.config.parquet.statistics_truncate_length;
            parquet.data_page_row_count_limit = options.config.parquet.data_page_row_count_limit;
            parquet.encoding = options.config.parquet.encoding.clone();
            parquet.bloom_filter_on_read = options.config.parquet.bloom_filter_on_read;
            parquet.bloom_filter_on_write = options.config.parquet.bloom_filter_on_write;
            parquet.bloom_filter_fpp = Some(options.config.parquet.bloom_filter_fpp);
            parquet.bloom_filter_ndv = Some(options.config.parquet.bloom_filter_ndv);
            parquet.allow_single_file_parallelism =
                options.config.parquet.allow_single_file_parallelism;
            parquet.maximum_parallel_row_group_writers =
                options.config.parquet.maximum_parallel_row_group_writers;
            parquet.maximum_buffered_record_batches_per_stream = options
                .config
                .parquet
                .maximum_buffered_record_batches_per_stream;
        }

        let runtime = {
            let registry = DynamicObjectStoreRegistry::new(options.runtime.clone());
            let builder =
                RuntimeEnvBuilder::default().with_object_store_registry(Arc::new(registry));
            Arc::new(builder.build()?)
        };
        let state = SessionStateBuilder::new()
            .with_config(session_config)
            .with_runtime_env(runtime)
            .with_default_features()
            .with_analyzer_rules(default_analyzer_rules())
            .with_optimizer_rules(default_optimizer_rules())
            .with_query_planner(new_query_planner())
            .build();
        let context = SessionContext::new_with_state(state);

        // TODO: This is a temp workaround to deregister all built-in functions that we define.
        //   We should deregister all context.udfs() once we have better coverage of functions.
        //   handler.rs needs to do this
        for (&name, _function) in BUILT_IN_SCALAR_FUNCTIONS.iter() {
            context.deregister_udf(name);
        }
        for (&name, _function) in BUILT_IN_GENERATOR_FUNCTIONS.iter() {
            context.deregister_udf(name);
        }
        for (&name, _function) in BUILT_IN_TABLE_FUNCTIONS.iter() {
            context.deregister_udtf(name);
        }

        Ok(context)
    }
}

#[derive(Debug, Clone)]
pub struct SessionManagerOptions {
    pub config: Arc<AppConfig>,
    pub runtime: RuntimeHandle,
}

enum SessionManagerEvent {
    GetOrCreateSession {
        key: SessionKey,
        system: Arc<Mutex<ActorSystem>>,
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
                result,
            } => self.handle_get_or_create_session(ctx, key, system, result),
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
        result: oneshot::Sender<SparkResult<SessionContext>>,
    ) -> ActorAction {
        let entry = self.sessions.entry(key.clone());
        let context = match entry {
            Entry::Occupied(o) => Ok(o.get().clone()),
            Entry::Vacant(v) => {
                let key = v.key().clone();
                info!("creating session {key}");
                match SessionManager::create_session_context(system, key, self.options.clone()) {
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
                    Duration::from_secs(self.options.config.spark.session_timeout_secs),
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
                    info!("removing idle session {key}");
                    ctx.spawn(async move { spark.job_runner().stop().await });
                    self.sessions.remove(&key);
                }
            }
        }
        ActorAction::Continue
    }
}
