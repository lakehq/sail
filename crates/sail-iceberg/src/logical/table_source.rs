use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableSource};
use sail_common_datafusion::datasource::MergeCapableSource;

use crate::datasource::scan::{IcebergScan, IcebergScanPlan};

#[derive(Clone)]
pub struct IcebergTableSource {
    scan: Arc<IcebergScan>,
    prepared: Option<Arc<IcebergScanPlan>>,
}

impl std::fmt::Debug for IcebergTableSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergTableSource")
            .field("table_uri", &self.scan.table_uri())
            .field(
                "schema_fields",
                &self
                    .scan
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
    pub fn new(scan: Arc<IcebergScan>) -> Self {
        Self {
            scan,
            prepared: None,
        }
    }

    pub fn scan(&self) -> &Arc<IcebergScan> {
        &self.scan
    }
    pub(crate) fn prepare(&self, planned: Arc<IcebergScanPlan>) -> Self {
        Self {
            scan: Arc::clone(&self.scan),
            prepared: Some(planned),
        }
    }

    pub(crate) fn prepared(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Option<Arc<IcebergScanPlan>> {
        self.prepared
            .as_ref()
            .filter(|planned| planned.matches(filters, limit))
            .cloned()
    }
}

impl MergeCapableSource for IcebergTableSource {
    fn file_column_name(&self) -> Option<&str> {
        self.scan.file_column_name()
    }

    fn row_index_column_name(&self) -> Option<&str> {
        self.scan.row_index_column_name()
    }

    fn with_file_column(&self, name: &str) -> Result<Arc<dyn TableSource>> {
        Ok(Arc::new(Self::new(Arc::new(
            self.scan.as_ref().clone().with_file_column(name)?,
        ))))
    }

    fn with_row_index_column(&self, name: &str) -> Result<Arc<dyn TableSource>> {
        Ok(Arc::new(Self::new(Arc::new(
            self.scan.as_ref().clone().with_row_index_column(name)?,
        ))))
    }
}

impl TableSource for IcebergTableSource {
    fn schema(&self) -> SchemaRef {
        self.scan.schema()
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.scan.supports_filters_pushdown(filter)
    }
}
