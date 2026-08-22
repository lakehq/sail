use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableSource};
use sail_common_datafusion::datasource::MergeCapableSource;

use crate::datasource::provider::IcebergTableProvider;

#[derive(Clone)]
pub struct IcebergTableSource {
    provider: Arc<IcebergTableProvider>,
}

impl std::fmt::Debug for IcebergTableSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergTableSource")
            .field("table_uri", &self.provider.table_uri())
            .field(
                "schema_fields",
                &self
                    .provider
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl IcebergTableSource {
    pub fn new(provider: Arc<IcebergTableProvider>) -> Self {
        Self { provider }
    }

    pub fn provider(&self) -> &Arc<IcebergTableProvider> {
        &self.provider
    }
}

impl MergeCapableSource for IcebergTableSource {
    fn file_column_name(&self) -> Option<&str> {
        self.provider.file_column_name()
    }

    fn row_index_column_name(&self) -> Option<&str> {
        self.provider.row_index_column_name()
    }

    fn with_file_column(&self, name: &str) -> Result<Arc<dyn TableSource>> {
        Ok(Arc::new(Self::new(Arc::new(
            self.provider.as_ref().clone().with_file_column(name)?,
        ))))
    }

    fn with_row_index_column(&self, name: &str) -> Result<Arc<dyn TableSource>> {
        Ok(Arc::new(Self::new(Arc::new(
            self.provider.as_ref().clone().with_row_index_column(name)?,
        ))))
    }
}

impl TableSource for IcebergTableSource {
    fn schema(&self) -> SchemaRef {
        self.provider.schema()
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.provider.supports_filters_pushdown(filter)
    }
}
