use std::ops::DerefMut;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use datafusion::common::parquet_config::DFParquetWriterVersion;
use datafusion::common::{internal_datafusion_err, Result};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};
use sail_catalog_system::service::SystemTableService;
use sail_common::config::{AppConfig, ExecutionMode};
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::session::activity::ActivityTracker;
use sail_common_datafusion::session::job::{JobRunner, JobService};
use sail_execution::driver::DriverOptions;
use sail_execution::job_runner::{ClusterJobRunner, LocalJobRunner};
use sail_execution::worker_manager::{
    KubernetesWorkerManager, KubernetesWorkerManagerOptions, LocalWorkerManager,
};
use sail_physical_optimizer::{get_physical_optimizers, PhysicalOptimizerOptions};
use sail_plan::function::{
    BUILT_IN_GENERATOR_FUNCTIONS, BUILT_IN_SCALAR_FUNCTIONS, BUILT_IN_TABLE_FUNCTIONS,
};
use sail_server::actor::{ActorHandle, ActorSystem};

use crate::catalog::create_catalog_manager;
use crate::formats::create_table_format_registry;
use crate::observable::SessionManagerHandle;
use crate::optimizer::{default_analyzer_rules, default_optimizer_rules};
use crate::planner::new_query_planner;
use crate::runtime::RuntimeEnvFactory;
use crate::session_factory::{SessionFactory, WorkerSessionFactory};
use crate::session_manager::SessionManagerActor;

pub struct ServerSessionInfo {
    pub session_id: String,
    pub user_id: String,
    pub session_manager: ActorHandle<SessionManagerActor>,
}

pub trait ServerSessionMutator: Send {
    fn mutate_config(
        &self,
        config: SessionConfig,
        info: &ServerSessionInfo,
    ) -> Result<SessionConfig>;
    fn mutate_state(
        &self,
        builder: SessionStateBuilder,
        info: &ServerSessionInfo,
    ) -> Result<SessionStateBuilder>;
    fn mutate_runtime_env(
        &self,
        builder: RuntimeEnvBuilder,
        info: &ServerSessionInfo,
    ) -> Result<RuntimeEnvBuilder>;
}

pub struct ServerSessionFactory {
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
    system: Arc<Mutex<ActorSystem>>,
    mutator: Box<dyn ServerSessionMutator>,
    runtime_env: RuntimeEnvFactory,
}

impl ServerSessionFactory {
    pub fn new(
        config: Arc<AppConfig>,
        runtime: RuntimeHandle,
        system: Arc<Mutex<ActorSystem>>,
        mutator: Box<dyn ServerSessionMutator>,
    ) -> Self {
        let runtime_env = RuntimeEnvFactory::new(config.clone(), runtime.clone());
        Self {
            config,
            runtime,
            system,
            mutator,
            runtime_env,
        }
    }
}

impl SessionFactory<ServerSessionInfo> for ServerSessionFactory {
    fn create(&mut self, info: ServerSessionInfo) -> Result<SessionContext> {
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

impl ServerSessionFactory {
    fn create_session_config(&mut self, info: &ServerSessionInfo) -> Result<SessionConfig> {
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
            .with_extension(Arc::new(JobService::new(job_runner)))
            .with_extension(Arc::new(self.create_system_table_service(info)?));
        self.apply_execution_config(&mut config);
        self.apply_execution_parquet_config(&mut config);
        let config = self.mutator.mutate_config(config, info)?;
        Ok(config)
    }

    fn create_session_state(&mut self, info: &ServerSessionInfo) -> Result<SessionState> {
        let config = self.create_session_config(info)?;
        let runtime = self
            .runtime_env
            .create(|builder| self.mutator.mutate_runtime_env(builder, info))?;
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
                    WorkerSessionFactory::new(self.config.clone(), self.runtime.clone())
                        .create(())?,
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

    fn create_system_table_service(&self, info: &ServerSessionInfo) -> Result<SystemTableService> {
        Ok(SystemTableService::new(Box::new(
            SessionManagerHandle::new(info.session_manager.clone()),
        )))
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
        parquet.writer_version =
            DFParquetWriterVersion::from_str(self.config.parquet.writer_version.as_str())
                .unwrap_or_default();
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
