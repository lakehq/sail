use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::sink::DataSinkExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{not_impl_err, plan_err, Result};
use deltalake::protocol::SaveMode;
use sail_common_datafusion::datasource::{PhysicalSinkMode, SinkInfo, SourceInfo, TableFormat};
use sail_delta_lake::create_delta_provider;
use sail_delta_lake::delta_format::DeltaDataSink;
use url::Url;

#[derive(Debug, Default)]
pub struct DeltaTableFormat;

#[async_trait]
impl TableFormat for DeltaTableFormat {
    fn name(&self) -> &str {
        "delta"
    }

    async fn create_provider(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableProvider>> {
        let SourceInfo {
            paths,
            schema: _,
            options,
        } = info;
        let table_url = Self::parse_table_url(ctx, paths).await?;
        // TODO: schema is ignored for now
        create_delta_provider(ctx, table_url.as_str(), &options).await
    }

    async fn create_writer(
        &self,
        ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let SinkInfo {
            input,
            path,
            mode,
            // TODO: support partitioning
            partition_by: _,
            bucket_by,
            sort_order,
            options,
        } = info;

        if bucket_by.is_some() {
            return not_impl_err!("bucketing for Delta format");
        }
        let table_url = Self::parse_table_url(ctx, vec![path]).await?;
        let mode = match mode {
            PhysicalSinkMode::ErrorIfExists => SaveMode::ErrorIfExists,
            PhysicalSinkMode::IgnoreIfExists => SaveMode::Ignore,
            PhysicalSinkMode::Append => SaveMode::Append,
            PhysicalSinkMode::Overwrite => SaveMode::Overwrite,
            PhysicalSinkMode::OverwriteIf { .. } | PhysicalSinkMode::OverwritePartitions => {
                return not_impl_err!("unsupported sink mode for Delta: {mode:?}")
            }
        };
        let sink = Arc::new(DeltaDataSink::new(mode, table_url, options, input.schema()));

        Ok(Arc::new(DataSinkExec::new(input, sink, sort_order)))
    }
}

impl DeltaTableFormat {
    async fn parse_table_url(ctx: &dyn Session, paths: Vec<String>) -> Result<Url> {
        let mut urls = crate::url::resolve_listing_urls(ctx, paths.clone()).await?;
        match (urls.pop(), urls.is_empty()) {
            (Some(path), true) => Ok(<ListingTableUrl as AsRef<Url>>::as_ref(&path).clone()),
            _ => plan_err!("expected a single path for Delta table sink: {paths:?}"),
        }
    }
}
