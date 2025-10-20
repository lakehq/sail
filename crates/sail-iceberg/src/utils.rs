use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::execution::context::TaskContext;
use datafusion_common::{DataFusionError, Result};
use url::Url;

pub fn get_object_store_from_context(
    context: &Arc<TaskContext>,
    table_url: &Url,
) -> Result<Arc<dyn object_store::ObjectStore>> {
    context
        .runtime_env()
        .object_store_registry
        .get_store(table_url)
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

pub fn get_object_store_from_session(
    session: &dyn Session,
    table_url: &Url,
) -> Result<Arc<dyn object_store::ObjectStore>> {
    session
        .runtime_env()
        .object_store_registry
        .get_store(table_url)
        .map_err(|e| DataFusionError::External(Box::new(e)))
}
