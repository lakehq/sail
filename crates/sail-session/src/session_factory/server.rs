use std::ops::DerefMut;
use std::sync::{Arc, Mutex};

use datafusion::common::{internal_datafusion_err, Result};
use datafusion::execution::cache::cache_manager::{
    CacheManagerConfig, FileMetadataCache, FileStatisticsCache, ListFilesCache,
};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};
use log::debug;
use sail_cache::file_listing_cache::MokaFileListingCache;
use sail_cache::file_metadata_cache::MokaFileMetadataCache;
use sail_cache::file_statistics_cache::MokaFileStatisticsCache;
use sail_common::config::{AppConfig, CacheType, ExecutionMode};
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::session::{ActivityTracker, JobRunner, JobService};
use sail_execution::driver::DriverOptions;
use sail_execution::runner::{ClusterJobRunner, LocalJobRunner};
use sail_execution::worker_manager::{
    KubernetesWorkerManager, KubernetesWorkerManagerOptions, LocalWorkerManager,
};
use sail_object_store::DynamicObjectStoreRegistry;
use sail_physical_optimizer::{get_physical_optimizers, PhysicalOptimizerOptions};
use sail_plan::function::{
    BUILT_IN_GENERATOR_FUNCTIONS, BUILT_IN_SCALAR_FUNCTIONS, BUILT_IN_TABLE_FUNCTIONS,
};
use sail_plan::planner::new_query_planner;
use sail_server::actor::ActorSystem;

use crate::catalog::create_catalog_manager;
use crate::formats::create_table_format_registry;
use crate::optimizer::{default_analyzer_rules, default_optimizer_rules};
use crate::session_factory::{SessionFactory, WorkerSessionFactory};

pub trait ServerSessionMutator<I>: Send {
    fn mutate_config(&self, config: SessionConfig, info: &I) -> Result<SessionConfig>;
    fn mutate_state(&self, builder: SessionStateBuilder, info: &I) -> Result<SessionStateBuilder>;
    fn mutate_runtime_env(&self, builder: RuntimeEnvBuilder, info: &I)
        -> Result<RuntimeEnvBuilder>;
}

pub struct ServerSessionFactory<I> {
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
    system: Arc<Mutex<ActorSystem>>,
    mutator: Box<dyn ServerSessionMutator<I>>,
    global_file_listing_cache: Option<Arc<MokaFileListingCache>>,
    global_file_statistics_cache: Option<Arc<MokaFileStatisticsCache>>,
    global_file_metadata_cache: Option<Arc<MokaFileMetadataCache>>,
}

impl<I> ServerSessionFactory<I> {
    pub fn new(
        config: Arc<AppConfig>,
        runtime: RuntimeHandle,
        system: Arc<Mutex<ActorSystem>>,
        mutator: Box<dyn ServerSessionMutator<I>>,
    ) -> Self {
        Self {
            config,
            runtime,
            system,
            mutator,
            global_file_listing_cache: None,
            global_file_statistics_cache: None,
            global_file_metadata_cache: None,
        }
    }
}

impl<I> SessionFactory<I> for ServerSessionFactory<I> {
    fn create(&mut self, info: I) -> Result<SessionContext> {
        let state = self.create_session_state(&info)?;
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

impl<I> ServerSessionFactory<I> {
    fn create_file_statistics_cache(&mut self) -> Option<FileStatisticsCache> {
        let ttl = self.config.parquet.file_statistics_cache.ttl;
        let max_entries = self.config.parquet.file_statistics_cache.max_entries;
        match &self.config.parquet.file_statistics_cache.r#type {
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
    }

    fn create_file_listing_cache(&mut self) -> Option<ListFilesCache> {
        let ttl = self.config.execution.file_listing_cache.ttl;
        let max_entries = self.config.execution.file_listing_cache.max_entries;
        match &self.config.execution.file_listing_cache.r#type {
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
    }

    fn create_file_metadata_cache(&mut self) -> Arc<dyn FileMetadataCache> {
        let ttl = self.config.parquet.file_metadata_cache.ttl;
        let size_limit = self.config.parquet.file_metadata_cache.size_limit;
        match self.config.parquet.file_metadata_cache.r#type {
            CacheType::None => {
                debug!("Not using file metadata cache");
                Arc::new(MokaFileMetadataCache::new(ttl, Some(0)))
            }
            CacheType::Global => {
                debug!("Using global file metadata cache");
                self.global_file_metadata_cache
                    .get_or_insert_with(|| Arc::new(MokaFileMetadataCache::new(ttl, size_limit)))
                    .clone()
            }
            CacheType::Session => {
                debug!("Using session file metadata cache");
                Arc::new(MokaFileMetadataCache::new(ttl, size_limit))
            }
        }
    }

    fn create_session_config(&mut self, info: &I) -> Result<SessionConfig> {
        let job_runner = self.create_job_runner()?;
        let mut config = SessionConfig::new()
            // We do not use the DataFusion catalog and schema since we manage catalogs ourselves.
            .with_create_default_catalog_and_schema(false)
            .with_information_schema(false)
            .with_extension(create_table_format_registry()?)
            .with_extension(Arc::new(create_catalog_manager(
                &self.config,
                self.runtime.clone(),
            )?))
            .with_extension(Arc::new(ActivityTracker::new()))
            .with_extension(Arc::new(JobService::new(job_runner)));
        self.apply_execution_config(&mut config);
        self.apply_execution_parquet_config(&mut config);
        let config = self.mutator.mutate_config(config, info)?;
        Ok(config)
    }

    fn create_runtime_env(&mut self, info: &I) -> Result<Arc<RuntimeEnv>> {
        let registry = DynamicObjectStoreRegistry::new(self.runtime.clone());
        let cache_config = CacheManagerConfig::default()
            .with_files_statistics_cache(self.create_file_statistics_cache())
            .with_list_files_cache(self.create_file_listing_cache())
            .with_file_metadata_cache(Some(self.create_file_metadata_cache()));
        let builder = RuntimeEnvBuilder::default()
            .with_object_store_registry(Arc::new(registry))
            .with_cache_manager(cache_config);
        let builder = self.mutator.mutate_runtime_env(builder, info)?;
        Ok(Arc::new(builder.build()?))
    }

    fn create_session_state(&mut self, info: &I) -> Result<SessionState> {
        let config = self.create_session_config(info)?;
        let runtime = self.create_runtime_env(info)?;
        let builder = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features()
            .with_analyzer_rules(default_analyzer_rules())
            .with_optimizer_rules(default_optimizer_rules())
            .with_physical_optimizer_rules(get_physical_optimizers(PhysicalOptimizerOptions {
                enable_join_reorder: self.config.optimizer.enable_join_reorder,
            }))
            .with_query_planner(new_query_planner());
        let builder = self.mutator.mutate_state(builder, info)?;
        Ok(builder.build())
    }

    fn create_job_runner(&mut self) -> Result<Box<dyn JobRunner>> {
        let job_runner: Box<dyn JobRunner> = match self.config.mode {
            ExecutionMode::Local => Box::new(LocalJobRunner::new()),
            ExecutionMode::LocalCluster => {
                let worker_manager = Arc::new(LocalWorkerManager::new(
                    self.runtime.clone(),
                    WorkerSessionFactory::new(&self.runtime).create(())?,
                ));
                let options =
                    DriverOptions::new(&self.config, self.runtime.clone(), worker_manager);
                let mut system = self
                    .system
                    .lock()
                    .map_err(|e| internal_datafusion_err!("{e}"))?;
                Box::new(ClusterJobRunner::new(system.deref_mut(), options))
            }
            ExecutionMode::KubernetesCluster => {
                let options = KubernetesWorkerManagerOptions {
                    image: self.config.kubernetes.image.clone(),
                    image_pull_policy: self.config.kubernetes.image_pull_policy.clone(),
                    namespace: self.config.kubernetes.namespace.clone(),
                    driver_pod_name: self.config.kubernetes.driver_pod_name.clone(),
                    worker_pod_name_prefix: self.config.kubernetes.worker_pod_name_prefix.clone(),
                    worker_service_account_name: self
                        .config
                        .kubernetes
                        .worker_service_account_name
                        .clone(),
                    worker_pod_template: self.config.kubernetes.worker_pod_template.clone(),
                };
                let worker_manager = Arc::new(KubernetesWorkerManager::new(options));
                let options =
                    DriverOptions::new(&self.config, self.runtime.clone(), worker_manager);
                let mut system = self
                    .system
                    .lock()
                    .map_err(|e| internal_datafusion_err!("{e}"))?;
                Box::new(ClusterJobRunner::new(system.deref_mut(), options))
            }
        };
        Ok(job_runner)
    }

    fn apply_execution_config(&mut self, config: &mut SessionConfig) {
        let execution = &mut config.options_mut().execution;

        execution.batch_size = self.config.execution.batch_size;
        if self.config.execution.default_parallelism > 0 {
            execution.target_partitions = self.config.execution.default_parallelism;
        }
        execution.collect_statistics = self.config.execution.collect_statistics;
        execution.use_row_number_estimates_to_optimize_partitioning = self
            .config
            .execution
            .use_row_number_estimates_to_optimize_partitioning;
        execution.listing_table_ignore_subdirectory = false;
    }

    fn apply_execution_parquet_config(&mut self, config: &mut SessionConfig) {
        let parquet = &mut config.options_mut().execution.parquet;

        parquet.created_by = concat!("sail version ", env!("CARGO_PKG_VERSION")).into();
        parquet.enable_page_index = self.config.parquet.enable_page_index;
        parquet.pruning = self.config.parquet.pruning;
        parquet.skip_metadata = self.config.parquet.skip_metadata;
        parquet.metadata_size_hint = self.config.parquet.metadata_size_hint;
        parquet.pushdown_filters = self.config.parquet.pushdown_filters;
        parquet.reorder_filters = self.config.parquet.reorder_filters;
        parquet.schema_force_view_types = self.config.parquet.schema_force_view_types;
        parquet.binary_as_string = self.config.parquet.binary_as_string;
        parquet.max_predicate_cache_size = Some(self.config.parquet.max_predicate_cache_size);
        parquet.coerce_int96 = Some("us".to_string());
        parquet.data_pagesize_limit = self.config.parquet.data_page_size_limit;
        parquet.write_batch_size = self.config.parquet.write_batch_size;
        parquet.writer_version = self.config.parquet.writer_version.clone();
        parquet.skip_arrow_metadata = self.config.parquet.skip_arrow_metadata;
        parquet.compression = Some(self.config.parquet.compression.clone());
        parquet.dictionary_enabled = Some(self.config.parquet.dictionary_enabled);
        parquet.dictionary_page_size_limit = self.config.parquet.dictionary_page_size_limit;
        parquet.statistics_enabled = Some(self.config.parquet.statistics_enabled.clone());
        parquet.max_row_group_size = self.config.parquet.max_row_group_size;
        parquet.column_index_truncate_length = self.config.parquet.column_index_truncate_length;
        parquet.statistics_truncate_length = self.config.parquet.statistics_truncate_length;
        parquet.data_page_row_count_limit = self.config.parquet.data_page_row_count_limit;
        parquet.encoding = self.config.parquet.encoding.clone();
        parquet.bloom_filter_on_read = self.config.parquet.bloom_filter_on_read;
        parquet.bloom_filter_on_write = self.config.parquet.bloom_filter_on_write;
        parquet.bloom_filter_fpp = Some(self.config.parquet.bloom_filter_fpp);
        parquet.bloom_filter_ndv = Some(self.config.parquet.bloom_filter_ndv);
        parquet.allow_single_file_parallelism = self.config.parquet.allow_single_file_parallelism;
        parquet.maximum_parallel_row_group_writers =
            self.config.parquet.maximum_parallel_row_group_writers;
        parquet.maximum_buffered_record_batches_per_stream = self
            .config
            .parquet
            .maximum_buffered_record_batches_per_stream;
    }
}
