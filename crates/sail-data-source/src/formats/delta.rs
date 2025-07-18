use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion_common::{plan_err, Result};
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat};
use sail_delta_lake::create_delta_provider;
use sail_delta_lake::delta_format::DeltaFormatFactory;

#[derive(Debug, Default)]
pub struct DeltaTableFormat;

#[async_trait]
impl TableFormat for DeltaTableFormat {
    fn name(&self) -> &str {
        "delta"
    }

    async fn create_provider(&self, info: SourceInfo<'_>) -> Result<Arc<dyn TableProvider>> {
        let SourceInfo {
            ctx,
            paths,
            options,
            ..
        } = info;
        if paths.len() != 1 {
            return plan_err!("Must provide a single path for a Delta table");
        }
        let table_uri = &paths[0];
        // TODO: schema is ignored for now
        create_delta_provider(ctx, table_uri, &options).await
    }

    fn create_writer(&self, info: SinkInfo<'_>) -> Result<Arc<dyn FileFormatFactory>> {
        let SinkInfo { mode, options, partitioning_columns, .. } = info;
        Ok(Arc::new(DeltaFormatFactory::new_with_partitioning(
            mode, options, partitioning_columns,
        )))
    }
}
