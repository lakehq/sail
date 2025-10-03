use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result;
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::datasource::{SinkInfo, SourceInfo, TableFormat};
use sail_iceberg::IcebergTableFormat;

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
        self.inner.create_provider(ctx, info).await
    }

    async fn create_writer(
        &self,
        ctx: &dyn Session,
        info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.create_writer(ctx, info).await
    }
}
