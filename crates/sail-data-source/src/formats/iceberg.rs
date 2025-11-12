use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result;
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat};
use sail_iceberg::{IcebergTableFormat, TableIcebergOptions};
use url::Url;

use crate::options::{load_default_options, load_options, IcebergReadOptions};

/// Iceberg table format implementation that delegates to sail-iceberg
#[derive(Debug)]
pub struct IcebergDataSourceFormat {
    inner: IcebergTableFormat,
}

impl Default for IcebergDataSourceFormat {
    fn default() -> Self {
        Self {
            inner: IcebergTableFormat,
        }
    }
}

#[async_trait]
impl TableFormat for IcebergDataSourceFormat {
    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn create_provider(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableProvider>> {
        let SourceInfo {
            paths,
            schema: _,
            constraints: _,
            partition_by: _,
            bucket_by: _,
            sort_order: _,
            options,
        } = info;

        let table_url = Self::parse_table_url(ctx, paths).await?;
        let iceberg_options = resolve_iceberg_read_options(options)?;

        IcebergTableFormat::create_iceberg_provider(ctx, table_url, iceberg_options).await
    }

    async fn create_writer(
        &self,
        ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.create_writer(ctx, info).await
    }
}

impl IcebergDataSourceFormat {
    async fn parse_table_url(ctx: &dyn Session, paths: Vec<String>) -> Result<Url> {
        if paths.len() != 1 {
            return datafusion::common::plan_err!(
                "Iceberg table requires exactly one path, got {}",
                paths.len()
            );
        }

        let path = &paths[0];
        let mut table_url = Url::parse(path)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        if !table_url.path().ends_with('/') {
            table_url.set_path(&format!("{}/", table_url.path()));
        }

        let _object_store = ctx
            .runtime_env()
            .object_store_registry
            .get_store(&table_url)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;

        Ok(table_url)
    }
}

fn apply_iceberg_read_options(
    from: IcebergReadOptions,
    to: &mut TableIcebergOptions,
) -> Result<()> {
    if let Some(use_ref) = from.use_ref {
        to.use_ref = Some(use_ref);
    }
    if let Some(snapshot_id) = from.snapshot_id {
        to.snapshot_id = Some(snapshot_id);
    }
    if let Some(ts) = from.timestamp_as_of {
        to.timestamp_as_of = Some(ts);
    }
    Ok(())
}

fn resolve_iceberg_read_options(
    options: Vec<std::collections::HashMap<String, String>>,
) -> Result<TableIcebergOptions> {
    let mut iceberg = TableIcebergOptions::default();
    apply_iceberg_read_options(load_default_options()?, &mut iceberg)?;
    for opt in options {
        apply_iceberg_read_options(load_options(opt)?, &mut iceberg)?;
    }
    Ok(iceberg)
}
