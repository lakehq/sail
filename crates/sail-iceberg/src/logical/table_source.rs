use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableSource};

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

impl TableSource for IcebergTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
