pub mod arrow;
pub mod expressions;
pub mod provider;
pub mod pruning;

use std::sync::Arc;

use datafusion::catalog::{Session, TableProvider};
use datafusion_common::Result as DataFusionResult;
use provider::DuckLakeTableProvider;
use url::Url;

use crate::metadata::{DuckLakeMetaStore, PythonMetaStore};
use crate::options::DuckLakeOptions;

pub async fn create_ducklake_provider(
    ctx: &dyn Session,
    opts: DuckLakeOptions,
) -> DataFusionResult<Arc<dyn TableProvider>> {
    let url = Url::parse(&opts.url)
        .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;

    let meta_store: Arc<dyn DuckLakeMetaStore> = match url.scheme() {
        "sqlite" | "postgres" | "postgresql" => Arc::new(PythonMetaStore::new(&opts.url).await?),
        scheme => {
            return datafusion_common::plan_err!("Unsupported metadata URL scheme: {}", scheme)
        }
    };

    let provider = DuckLakeTableProvider::new(ctx, meta_store, opts).await?;
    Ok(Arc::new(provider))
}
