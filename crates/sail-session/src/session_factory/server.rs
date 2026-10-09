use std::sync::Arc;

use datafusion::common::config::ConfigNonZeroUsize;
use datafusion::common::{Result, internal_err};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::functions_aggregate::first_last::first_value_udaf;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_expr::registry::FunctionRegistry;
use sail_cache::remote_checkpoint::RemoteCheckpointRegistry;
use sail_catalog::provider::CatalogCacheManager;
use sail_catalog_system::service::SystemTableService;
use sail_common::actor::ActorHandle;
use sail_common::config::{AppConfig, ExecutionMode};
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::session::activity::ActivityTracker;
use sail_common_datafusion::session::job::{JobRunner, JobService};
use sail_common_datafusion::session::repartition::RepartitionBufferConfig;
use sail_delta_lake::session_extension::DeltaTableCache;
use sail_physical_optimizer::{PhysicalOptimizerOptions, get_physical_optimizers};
use sail_telemetry::telemetry::global_system_store_reader;

use crate::catalog::create_catalog_manager;
use crate::formats::create_data_source_registry;
use crate::optimizer::{default_analyzer_rules, default_optimizer_rules};
use crate::planner::new_query_planner;
use crate::runtime::RuntimeEnvFactory;
use crate::session_factory::SessionFactory;
use crate::session_manager::SessionManagerActor;

pub struct ServerSessionInfo {
    pub session_id: String,
    pub user_id: String,
    pub session_manager: ActorHandle<SessionManagerActor>,
    pub job_runner: Option<Box<dyn JobRunner>>,
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
    mutator: Box<dyn ServerSessionMutator>,
    runtime_env: RuntimeEnvFactory,
    catalog_cache_manager: Arc<CatalogCacheManager>,
}

impl ServerSessionFactory {
    pub fn new(
        config: Arc<AppConfig>,
        runtime: RuntimeHandle,
        mutator: Box<dyn ServerSessionMutator>,
    ) -> Self {
        let runtime_env = RuntimeEnvFactory::new(config.clone(), runtime.clone());
        Self {
            config,
            runtime,
            mutator,
            runtime_env,
            catalog_cache_manager: Arc::new(CatalogCacheManager::new()),
        }
    }
}

impl SessionFactory<ServerSessionInfo> for ServerSessionFactory {
    fn create(&mut self, mut info: ServerSessionInfo) -> Result<SessionContext> {
        let state = self.create_session_state(&mut info)?;
        let context = SessionContext::new_with_state(state);

        // Register the `first_value` UDAF since the `replace_distinct_aggregate` optimizer rule
        // assumes that this UDAF is available in the function registry.
        // This is a hidden assumption made by the optimizer rule.
        // We have to do so because we do not add default features (including built-in functions)
        // to the session state.
        //
        // See also: https://github.com/apache/datafusion/issues/10703
        context
            .state_ref()
            .write()
            .register_udaf(first_value_udaf())?;

        Ok(context)
    }
}

impl ServerSessionFactory {
    fn create_session_config(&mut self, info: &mut ServerSessionInfo) -> Result<SessionConfig> {
        let Some(job_runner) = info.job_runner.take() else {
            return internal_err!("job runner is missing from server session information");
        };
        let mut config = SessionConfig::new()
            // We do not use the DataFusion catalog and schema since we manage catalogs ourselves.
            .with_create_default_catalog_and_schema(false)
            .with_information_schema(false)
            .with_extension(create_data_source_registry()?)
            .with_extension(Arc::new(create_catalog_manager(
                &self.config,
                self.runtime.clone(),
                self.catalog_cache_manager.clone(),
            )?))
            .with_extension(Arc::new(ActivityTracker::new()))
            .with_extension(Arc::new(JobService::new(job_runner)))
            .with_extension(Arc::new(RemoteCheckpointRegistry::new(
                self.config.execution.checkpoint.path.clone(),
                info.session_id.clone(),
            )))
            .with_extension(Arc::new(RepartitionBufferConfig::new(
                self.config.cluster.task_stream_buffer,
            )))
            .with_extension(Arc::new(self.create_system_table_service(info)?))
            .with_extension(Arc::new(DeltaTableCache::default()));
        self.apply_execution_config(&mut config)?;
        super::apply_parquet_config(
            &mut config.options_mut().execution.parquet,
            &self.config.parquet,
        );
        self.apply_optimizer_config(&mut config)?;
        let config = self.mutator.mutate_config(config, info)?;
        Ok(config)
    }

    fn create_session_state(&mut self, info: &mut ServerSessionInfo) -> Result<SessionState> {
        let config = self.create_session_config(info)?;
        let (runtime, caches) = self
            .runtime_env
            .create(|builder| self.mutator.mutate_runtime_env(builder, info))?;
        // We do not add default features to the session state,
        // since we manage data sources and functions ourselves.
        let builder = SessionStateBuilder::new()
            .with_config(config.with_extension(caches))
            .with_runtime_env(runtime)
            .with_analyzer_rules(default_analyzer_rules())
            .with_optimizer_rules(default_optimizer_rules())
            .with_physical_optimizer_rules(get_physical_optimizers(PhysicalOptimizerOptions {
                enable_join_reorder: self.config.optimizer.enable_join_reorder,
                ..Default::default()
            }))
            .with_query_planner(new_query_planner());
        let builder = self.mutator.mutate_state(builder, info)?;
        Ok(builder.build())
    }

    fn create_system_table_service(&self, _info: &ServerSessionInfo) -> Result<SystemTableService> {
        let reader = global_system_store_reader().ok_or_else(|| {
            datafusion::common::DataFusionError::Internal(
                "telemetry is not initialized for system store".to_string(),
            )
        })?;
        Ok(SystemTableService::new(
            reader,
            self.config.execution.batch_size,
        ))
    }

    fn apply_execution_config(&mut self, config: &mut SessionConfig) -> Result<()> {
        let execution = &mut config.options_mut().execution;

        execution.batch_size = ConfigNonZeroUsize::try_new(self.config.execution.batch_size)?;
        if self.config.execution.default_parallelism > 0 {
            execution.target_partitions = self.config.execution.default_parallelism;
        }
        execution.collect_statistics = self.config.execution.collect_statistics;
        execution.spill_compression =
            super::spill_compression(self.config.runtime.temporary_files.spill_compression);
        execution.use_row_number_estimates_to_optimize_partitioning = self
            .config
            .execution
            .use_row_number_estimates_to_optimize_partitioning;
        execution.listing_table_ignore_subdirectory = false;
        Ok(())
    }

    fn apply_optimizer_config(&mut self, config: &mut SessionConfig) -> Result<()> {
        let optimizer = &mut config.options_mut().optimizer;
        optimizer.join_reordering = self.config.optimizer.enable_join_swap;
        optimizer.prefer_hash_join = self.config.optimizer.prefer_hash_join;
        optimizer.enable_window_topn = self.config.optimizer.enable_window_topn;
        optimizer.expand_views_at_output = self.config.optimizer.expand_views_at_output;
        // DataFusion 55's hash-join dynamic filter assumes every plan partition reports to
        // process-local state. Cluster execution uses independently decoded task plans, so keep
        // join filters disabled while allowing task-local TopK and aggregate filters.
        if matches!(
            self.config.mode,
            ExecutionMode::LocalCluster | ExecutionMode::KubernetesCluster
        ) {
            optimizer.enable_join_dynamic_filter_pushdown = false;
        }
        Ok(())
    }
}
