pub mod arrow;
pub mod expressions;
pub mod provider;
pub mod pruning;

use std::sync::Arc;

use datafusion::catalog::{Session, TableProvider};
use datafusion_common::Result as DataFusionResult;
use provider::DuckLakeTableProvider;

use crate::metadata::{DuckLakeMetaStore, PythonMetaStore};
use crate::options::DuckLakeOptions;

pub async fn create_ducklake_provider(
    ctx: &dyn Session,
    opts: DuckLakeOptions,
) -> DataFusionResult<Arc<dyn TableProvider>> {
    let meta_store: Arc<dyn DuckLakeMetaStore> = if opts.url.starts_with("sqlite://")
        || opts.url.starts_with("postgres://")
        || opts.url.starts_with("postgresql://")
    {
        Arc::new(PythonMetaStore::new(&opts.url).await?)
    } else {
        return datafusion_common::plan_err!("Unsupported metadata URL scheme: {}", opts.url);
    };

    let provider = DuckLakeTableProvider::new(ctx, meta_store, opts).await?;
    Ok(Arc::new(provider))
}
