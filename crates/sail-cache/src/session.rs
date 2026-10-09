use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::{CatalogProviderList, Session};
use datafusion::common::config::TableOptions;
use datafusion::common::{DFSchema, Result};
use datafusion::execution::TaskContext;
use datafusion::execution::cache::cache_manager::{CacheManager, CacheManagerConfig};
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::registry::ExtensionTypeRegistryRef;
use datafusion::logical_expr::{
    AggregateUDF, Expr, HigherOrderUDF, LogicalPlan, ScalarUDF, WindowUDF,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::operator_statistics::StatisticsRegistry;
use datafusion::prelude::SessionConfig;
use object_store::{ObjectMeta, ObjectStore};

use crate::file_caches::FileCaches;

/// A borrowed session view for DataFusion helpers that obtain their caches internally.
pub struct ObjectStoreSession<'a> {
    inner: &'a dyn Session,
    runtime: Arc<RuntimeEnv>,
    table_options: Cow<'a, TableOptions>,
}

impl<'a> ObjectStoreSession<'a> {
    pub fn new(
        inner: &'a dyn Session,
        store: &Arc<dyn ObjectStore>,
        files: &[ObjectMeta],
    ) -> Result<Self> {
        let caches = FileCaches::from_config(inner.config());
        let metadata = caches.metadata.for_store(store);
        let metadata = if files.is_empty() {
            metadata
        } else {
            metadata.for_files(files.iter().map(|meta| (meta.location.clone(), meta)))
        };
        let statistics = caches
            .statistics
            .as_ref()
            .map(|cache| Arc::new(cache.for_store(store)) as _);
        let listing = caches
            .listing
            .as_ref()
            .map(|cache| Arc::new(cache.for_store(store)) as _);
        let config = CacheManagerConfig::default()
            .with_file_metadata_cache(Some(Arc::new(metadata)))
            .with_file_statistics_cache_limit(if statistics.is_some() { usize::MAX } else { 0 })
            .with_file_statistics_cache(statistics)
            .with_list_files_cache_limit(if listing.is_some() { usize::MAX } else { 0 })
            .with_list_files_cache(listing);
        let runtime = inner.runtime_env();
        Ok(Self {
            inner,
            runtime: Arc::new(RuntimeEnv {
                memory_pool: Arc::clone(&runtime.memory_pool),
                disk_manager: Arc::clone(&runtime.disk_manager),
                object_store_registry: Arc::clone(&runtime.object_store_registry),
                cache_manager: CacheManager::try_new(&config)?,
            }),
            table_options: Cow::Borrowed(inner.table_options()),
        })
    }
}

#[async_trait::async_trait]
impl Session for ObjectStoreSession<'_> {
    fn session_id(&self) -> &str {
        self.inner.session_id()
    }
    fn config(&self) -> &SessionConfig {
        self.inner.config()
    }
    fn catalog_list(&self) -> Arc<dyn CatalogProviderList> {
        self.inner.catalog_list()
    }
    fn query_planner(&self) -> Arc<dyn QueryPlanner + Send + Sync> {
        self.inner.query_planner()
    }
    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        self.inner.optimize(plan)
    }
    fn physical_optimizers(&self) -> &[Arc<dyn PhysicalOptimizerRule + Send + Sync>] {
        self.inner.physical_optimizers()
    }
    fn statistics_registry(&self) -> Option<&StatisticsRegistry> {
        self.inner.statistics_registry()
    }
    async fn create_physical_plan(&self, plan: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.create_physical_plan(plan).await
    }
    fn create_physical_expr(&self, expr: Expr, schema: &DFSchema) -> Result<Arc<dyn PhysicalExpr>> {
        self.inner.create_physical_expr(expr, schema)
    }
    fn scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>> {
        self.inner.scalar_functions()
    }
    fn higher_order_functions(&self) -> &HashMap<String, Arc<HigherOrderUDF>> {
        self.inner.higher_order_functions()
    }
    fn aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>> {
        self.inner.aggregate_functions()
    }
    fn window_functions(&self) -> &HashMap<String, Arc<WindowUDF>> {
        self.inner.window_functions()
    }
    fn extension_type_registry(&self) -> &ExtensionTypeRegistryRef {
        self.inner.extension_type_registry()
    }
    fn runtime_env(&self) -> &Arc<RuntimeEnv> {
        &self.runtime
    }
    fn execution_props(&self) -> &ExecutionProps {
        self.inner.execution_props()
    }
    fn as_any(&self) -> &dyn Any {
        self.inner.as_any()
    }
    fn table_options(&self) -> &TableOptions {
        &self.table_options
    }
    fn table_options_mut(&mut self) -> &mut TableOptions {
        self.table_options.to_mut()
    }
    fn task_ctx(&self) -> Arc<TaskContext> {
        Arc::new(TaskContext::from(self as &dyn Session))
    }
}
