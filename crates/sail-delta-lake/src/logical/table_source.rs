use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableSource};
use sail_common_datafusion::datasource::MergeCapableSource;

use crate::DeltaResult;
use crate::datasource::{DeltaScanConfig, df_logical_schema, get_pushdown_filters};
use crate::delta_log::LogStoreRef;
use crate::table::DeltaSnapshot;

#[derive(Clone, Debug, Default)]
pub(crate) enum DeltaFileSelection {
    #[default]
    Snapshot,
    Selected(Arc<[usize]>),
}

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
    file_selection: DeltaFileSelection,
}

impl std::fmt::Debug for DeltaTableSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaTableSource")
            .field("snapshot_version", &self.snapshot.version())
            .field("config", &self.config)
            .field("file_selection", &self.file_selection)
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
            &config.row_index_column_name,
            &config.commit_version_column_name,
            &config.commit_timestamp_column_name,
            config.schema.clone(),
        )?;
        Ok(Self {
            snapshot,
            log_store,
            config,
            schema,
            file_selection: DeltaFileSelection::Snapshot,
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

    pub(crate) fn file_selection(&self) -> &DeltaFileSelection {
        &self.file_selection
    }

    pub(crate) fn try_select_files(&self, indices: Vec<usize>) -> DeltaResult<Self> {
        if !self.snapshot.load_config().require_files {
            return Err(crate::spec::DeltaError::generic(
                "Delta file selection requires an eagerly loaded snapshot",
            ));
        }
        let mut previous = None;
        for &index in &indices {
            if index >= self.snapshot.adds().len() {
                return Err(crate::spec::DeltaError::generic(format!(
                    "Delta file selection index {index} is out of range"
                )));
            }
            if previous.is_some_and(|previous| previous >= index) {
                return Err(crate::spec::DeltaError::generic(
                    "Delta file selection indices must be sorted and unique",
                ));
            }
            previous = Some(index);
        }
        let mut source = self.clone();
        source.file_selection = DeltaFileSelection::Selected(indices.into());
        Ok(source)
    }
}

impl TableSource for DeltaTableSource {
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

impl MergeCapableSource for DeltaTableSource {
    fn file_column_name(&self) -> Option<&str> {
        self.config.file_column_name.as_deref()
    }

    fn with_file_column(&self, name: &str) -> Result<Arc<dyn TableSource>> {
        let mut new_config = self.config.clone();
        new_config.file_column_name = Some(name.to_string());
        let mut new_source = DeltaTableSource::try_new(
            Arc::clone(&self.snapshot),
            self.log_store.clone(),
            new_config,
        )
        .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        new_source.file_selection = self.file_selection.clone();
        Ok(Arc::new(new_source))
    }

    fn row_index_column_name(&self) -> Option<&str> {
        self.config.row_index_column_name.as_deref()
    }

    fn with_row_index_column(&self, name: &str) -> Result<Arc<dyn TableSource>> {
        let mut new_config = self.config.clone();
        new_config.row_index_column_name = Some(name.to_string());
        let mut new_source = DeltaTableSource::try_new(
            Arc::clone(&self.snapshot),
            self.log_store.clone(),
            new_config,
        )
        .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        new_source.file_selection = self.file_selection.clone();
        Ok(Arc::new(new_source))
    }
}
