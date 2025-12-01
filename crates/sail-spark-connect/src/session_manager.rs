use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::execution::cache::cache_manager::{
    CacheManagerConfig, FileMetadataCache, FileStatisticsCache, ListFilesCache,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use fastrace::prelude::SpanContext;
use fastrace::{func_path, Span};
use log::{debug, info};
use sail_cache::file_listing_cache::MokaFileListingCache;
use sail_cache::file_metadata_cache::MokaFileMetadataCache;
use sail_cache::file_statistics_cache::MokaFileStatisticsCache;
use sail_common::config::{AppConfig, CacheType, ExecutionMode};
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_execution::driver::DriverOptions;
use sail_execution::job::{ClusterJobRunner, JobRunner, LocalJobRunner};
use sail_logical_optimizer::{default_analyzer_rules, default_optimizer_rules};
use sail_object_store::DynamicObjectStoreRegistry;
use sail_physical_optimizer::{get_physical_optimizers, PhysicalOptimizerOptions};
use sail_plan::function::{
    BUILT_IN_GENERATOR_FUNCTIONS, BUILT_IN_SCALAR_FUNCTIONS, BUILT_IN_TABLE_FUNCTIONS,
};
use sail_plan::planner::new_query_planner;
use sail_server::actor::{Actor, ActorAction, ActorContext, ActorHandle, ActorSystem};
use sail_session::catalog::create_catalog_manager;
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::error::{SparkError, SparkResult};
use crate::session::{SparkSession, SparkSessionOptions};

#[derive(Eq, PartialEq, Hash, Clone, Debug)]
pub struct SessionKey {
    pub user_id: String,
    pub session_id: String,
}

impl Display for SessionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.user_id, self.session_id)
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
}

impl SessionManagerActor {
    pub fn create_session_context(
        &mut self,
        system: Arc<Mutex<ActorSystem>>,
        key: SessionKey,
    ) -> SparkResult<SessionContext> {
        let options = &self.options;
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
            // We do not use the DataFusion catalog and schema since we manage catalogs ourselves.
            .with_create_default_catalog_and_schema(false)
            .with_information_schema(false)
            .with_extension(Arc::new(create_catalog_manager(
                &options.config,
                options.runtime.clone(),
            )?))
            .with_extension(Arc::new(SparkSession::try_new(
                key.user_id,
                key.session_id,
                job_runner,
                SparkSessionOptions {
                    execution_heartbeat_interval: Duration::from_secs(
                        options.config.spark.execution_heartbeat_interval_secs,
                    ),
                },
            )?));

        // execution options
        {
            let execution = &mut session_config.options_mut().execution;

            execution.batch_size = options.config.execution.batch_size;
            execution.collect_statistics = options.config.execution.collect_statistics;
            execution.use_row_number_estimates_to_optimize_partitioning = options
                .config
                .execution
                .use_row_number_estimates_to_optimize_partitioning;
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

            let file_statistics_cache: Option<FileStatisticsCache> = {
                let ttl = options.config.parquet.file_statistics_cache.ttl;
                let max_entries = options.config.parquet.file_statistics_cache.max_entries;
                match &options.config.parquet.file_statistics_cache.r#type {
                    CacheType::None => {
                        debug!("Not using file statistics cache");
                        None
                    }
                    CacheType::Global => {
                        debug!("Using global file statistics cache");
                        Some(
                            self.global_file_statistics_cache
                                .get_or_insert_with(|| {
                                    Arc::new(MokaFileStatisticsCache::new(ttl, max_entries))
                                })
                                .clone(),
                        )
                    }
                    CacheType::Session => {
                        debug!("Using session file statistics cache");
                        Some(Arc::new(MokaFileStatisticsCache::new(ttl, max_entries)))
                    }
                }
            };
            let file_listing_cache: Option<ListFilesCache> = {
                let ttl = options.config.execution.file_listing_cache.ttl;
                let max_entries = options.config.execution.file_listing_cache.max_entries;
                match &options.config.execution.file_listing_cache.r#type {
                    CacheType::None => {
                        debug!("Not using file listing cache");
                        None
                    }
                    CacheType::Global => {
                        debug!("Using global file listing cache");
                        Some(
                            self.global_file_listing_cache
                                .get_or_insert_with(|| {
                                    Arc::new(MokaFileListingCache::new(ttl, max_entries))
                                })
                                .clone(),
                        )
                    }
                    CacheType::Session => {
                        debug!("Using session file listing cache");
                        Some(Arc::new(MokaFileListingCache::new(ttl, max_entries)))
                    }
                }
            };
            let file_metadata_cache: Arc<dyn FileMetadataCache> = {
                let ttl = options.config.parquet.file_metadata_cache.ttl;
                let size_limit = options.config.parquet.file_metadata_cache.size_limit;
                match options.config.parquet.file_metadata_cache.r#type {
                    CacheType::None => {
                        debug!("Not using file metadata cache");
                        Arc::new(MokaFileMetadataCache::new(ttl, Some(0)))
                    }
                    CacheType::Global => {
                        debug!("Using global file metadata cache");
                        self.global_file_metadata_cache
                            .get_or_insert_with(|| {
                                Arc::new(MokaFileMetadataCache::new(ttl, size_limit))
                            })
                            .clone()
                    }
                    CacheType::Session => {
                        debug!("Using session file metadata cache");
                        Arc::new(MokaFileMetadataCache::new(ttl, size_limit))
                    }
                }
            };

            let cache_config = CacheManagerConfig::default()
                .with_files_statistics_cache(file_statistics_cache)
                .with_list_files_cache(file_listing_cache)
                .with_file_metadata_cache(Some(file_metadata_cache));
            let builder = RuntimeEnvBuilder::default()
                .with_object_store_registry(Arc::new(registry))
                .with_cache_manager(cache_config);
            Arc::new(builder.build()?)
        };

        let state = SessionStateBuilder::new()
            .with_config(session_config)
            .with_runtime_env(runtime)
            .with_default_features()
            .with_analyzer_rules(default_analyzer_rules())
            .with_optimizer_rules(default_optimizer_rules())
            .with_physical_optimizer_rules(get_physical_optimizers(PhysicalOptimizerOptions {
                enable_join_reorder: options.config.optimizer.enable_join_reorder,
            }))
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
    global_file_listing_cache: Option<Arc<MokaFileListingCache>>,
    global_file_statistics_cache: Option<Arc<MokaFileStatisticsCache>>,
    global_file_metadata_cache: Option<Arc<MokaFileMetadataCache>>,
}

#[tonic::async_trait]
impl Actor for SessionManagerActor {
    type Message = SessionManagerEvent;
    type Options = SessionManagerOptions;

    fn new(options: Self::Options) -> Self {
        Self {
            options,
            sessions: HashMap::new(),
            global_file_listing_cache: None,
            global_file_statistics_cache: None,
            global_file_metadata_cache: None,
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
        // We cannot use `self.sessions.entry()` to perform the get-or-insert operation
        // because `self.create_session_context()` takes `&mut self`.
        let context = if let Some(context) = self.sessions.get(&key) {
            Ok(context.clone())
        } else {
            let key = key.clone();
            info!("creating session {key}");
            let span = Span::root(func_path!(), SpanContext::random());
            let _guard = span.set_local_parent();
            match self.create_session_context(system, key.clone()) {
                Ok(context) => {
                    self.sessions.insert(key, context.clone());
                    Ok(context)
                }
                Err(e) => Err(e),
            }
        };
        if let Ok(context) = &context {
            if let Ok(active_at) = context
                .extension::<SparkSession>()
                .map_err(|e| e.into())
                .and_then(|spark| spark.track_activity())
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
            if let Ok(spark) = context.extension::<SparkSession>() {
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
