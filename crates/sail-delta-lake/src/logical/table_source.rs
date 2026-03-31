use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableSource};

use crate::datasource::{df_logical_schema, get_pushdown_filters, DeltaScanConfig};
use crate::storage::LogStoreRef;
use crate::table::DeltaSnapshot;
use crate::DeltaResult;

/// Logical-only Delta table source used in DataFusion logical plans.
///
/// This avoids coupling logical planning / optimization with `TableProvider::scan`
/// behavior. Physical planning is handled by rewriting scans to an extension node.
#[derive(Clone)]
pub struct DeltaTableSource {
    snapshot: Arc<DeltaSnapshot>,
    log_store: LogStoreRef,
    config: DeltaScanConfig,
    schema: SchemaRef,
}

impl std::fmt::Debug for DeltaTableSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaTableSource")
            .field("snapshot_version", &self.snapshot.version())
            .field("config", &self.config)
            .field(
                "schema_fields",
                &self
                    .schema
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl DeltaTableSource {
    pub fn try_new(
        snapshot: Arc<DeltaSnapshot>,
        log_store: LogStoreRef,
        config: DeltaScanConfig,
    ) -> DeltaResult<Self> {
        snapshot.ensure_data_read_supported()?;
        let schema = df_logical_schema(
            snapshot.as_ref(),
            &config.file_column_name,
            &config.commit_version_column_name,
            &config.commit_timestamp_column_name,
            config.schema.clone(),
        )?;
        Ok(Self {
            snapshot,
            log_store,
            config,
            schema,
        })
    }

    pub fn snapshot(&self) -> &Arc<DeltaSnapshot> {
        &self.snapshot
    }

    pub fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }

    pub fn config(&self) -> &DeltaScanConfig {
        &self.config
    }
}

impl TableSource for DeltaTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let partition_cols = self.snapshot.metadata().partition_columns().as_slice();
        Ok(get_pushdown_filters(filter, partition_cols))
    }
}
