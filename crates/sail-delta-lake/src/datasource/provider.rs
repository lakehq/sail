use std::any::Any;
use std::borrow::Cow;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::catalog::Session;
use datafusion::common::stats::Statistics;
use datafusion::common::{Result, ToDFSchema};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use deltalake::errors::DeltaResult;
use deltalake::kernel::Add;
use deltalake::logstore::LogStoreRef;

use crate::datasource::scan::FileScanParams;
use crate::datasource::{
    build_file_scan_config, delta_to_datafusion_error, df_logical_schema, get_pushdown_filters,
    prune_files, simplify_expr, DataFusionMixins, DeltaScanConfig, DeltaTableStateExt,
};
// use crate::kernel::arrow::engine_ext::SnapshotExt as KernelSnapshotExt;
// use delta_kernel::snapshot::Snapshot as KernelSnapshot;
use delta_kernel::table_features::ColumnMappingMode;
// use deltalake::errors::DeltaTableError;
// use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use crate::table::DeltaTableState;
use sail_common_datafusion::rename::physical_plan::rename_projected_physical_plan;

// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/delta_datafusion/mod.rs>

/// A Delta table provider that enables additional metadata columns to be included during the scan
#[derive(Debug)]
pub struct DeltaTableProvider {
    snapshot: DeltaTableState,
    log_store: LogStoreRef,
    config: DeltaScanConfig,
    schema: Arc<ArrowSchema>,
    files: Option<Arc<Vec<Add>>>,
}

impl DeltaTableProvider {
    pub fn try_new(
        snapshot: DeltaTableState,
        log_store: LogStoreRef,
        config: DeltaScanConfig,
    ) -> DeltaResult<Self> {
        Ok(DeltaTableProvider {
            schema: df_logical_schema(&snapshot, &config.file_column_name, config.schema.clone())?,
            snapshot,
            log_store,
            config,
            files: None,
        })
    }

    pub fn with_files(mut self, files: Vec<Add>) -> DeltaTableProvider {
        self.files = Some(Arc::new(files));
        self
    }

    /// Separate filters into those used for pruning vs those pushed down to Parquet
    fn separate_filters(&self, filters: &[Expr]) -> (Vec<Expr>, Vec<Expr>) {
        let partition_cols = self.snapshot.metadata().partition_columns();
        let predicates: Vec<&Expr> = filters.iter().collect();
        let pushdown_filters = get_pushdown_filters(&predicates, partition_cols.as_slice());

        let mut pruning_filters = Vec::new();
        let mut parquet_pushdown_filters = Vec::new();

        for (filter, pushdown) in filters.iter().zip(pushdown_filters) {
            match pushdown {
                TableProviderFilterPushDown::Exact => {
                    pruning_filters.push(filter.clone());
                }
                TableProviderFilterPushDown::Inexact => {
                    pruning_filters.push(filter.clone());
                    parquet_pushdown_filters.push(filter.clone());
                }
                TableProviderFilterPushDown::Unsupported => {
                    // Unsupported filters are not pushed down
                }
            }
        }

        (pruning_filters, parquet_pushdown_filters)
    }
}

#[async_trait]
impl TableProvider for DeltaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let config = self.config.clone();

        let schema = match config.schema.clone() {
            Some(value) => Ok(value),
            // Change from `arrow_schema` to input_schema for Spark compatibility
            None => self.snapshot.input_schema(),
        }
        .map_err(delta_to_datafusion_error)?;

        let logical_schema = df_logical_schema(
            &self.snapshot,
            &config.file_column_name,
            Some(schema.clone()),
        )
        .map_err(delta_to_datafusion_error)?;

        let logical_schema = if let Some(used_columns) = projection {
            let mut fields = vec![];
            for idx in used_columns {
                fields.push(logical_schema.field(*idx).to_owned());
            }
            // partition filters with Exact pushdown were removed from projection by DF optimizer,
            // we need to add them back for the predicate pruning to work
            let filter_expr = conjunction(filters.iter().cloned());
            if let Some(expr) = &filter_expr {
                for c in expr.column_refs() {
                    let idx = logical_schema.index_of(c.name.as_str())?;
                    if !used_columns.contains(&idx) {
                        fields.push(logical_schema.field(idx).to_owned());
                    }
                }
            }
            // Ensure all partition columns are included in logical schema
            let table_partition_cols = self.snapshot.metadata().partition_columns();
            for partition_col in table_partition_cols.iter() {
                if let Ok(idx) = logical_schema.index_of(partition_col.as_str()) {
                    if !used_columns.contains(&idx)
                        && !fields.iter().any(|f| f.name() == partition_col)
                    {
                        fields.push(logical_schema.field(idx).to_owned());
                    }
                }
            }
            Arc::new(ArrowSchema::new(fields))
        } else {
            logical_schema
        };

        // Separate filters for pruning vs pushdown
        let (pruning_filters, pushdown_filters) = self.separate_filters(filters);

        // Use the new pruning module
        let (files, pruning_mask) = match &self.files {
            Some(files) => (files.clone(), None),
            None => {
                let result = prune_files(
                    &self.snapshot,
                    &self.log_store,
                    session,
                    &pruning_filters,
                    limit,
                    logical_schema.clone(),
                )
                .await?;
                (Arc::new(result.files), result.pruning_mask)
            }
        };

        // Prepare pushdown filter for Parquet
        let pushdown_filter = if !pushdown_filters.is_empty() {
            let df_schema = logical_schema.clone().to_dfschema()?;
            let pushdown_expr = conjunction(pushdown_filters);
            pushdown_expr.map(|expr| simplify_expr(session, &df_schema, expr))
        } else {
            None
        };

        // Build physical file schema (non-partition columns) using kernel make_physical
        let table_partition_cols = self.snapshot.metadata().partition_columns();
        let kmode: ColumnMappingMode = self
            .snapshot
            .snapshot()
            .table_configuration()
            .column_mapping_mode();
        let kschema_arc = self.snapshot.snapshot().table_configuration().schema();
        let physical_kernel = kschema_arc.make_physical(kmode);
        let physical_arrow: ArrowSchema =
            deltalake::kernel::engine::arrow_conversion::TryIntoArrow::try_into_arrow(
                &physical_kernel,
            )?;
        let file_fields = physical_arrow
            .fields()
            .iter()
            .filter(|f| !table_partition_cols.contains(f.name()))
            .cloned()
            .collect::<Vec<_>>();
        let file_schema = Arc::new(ArrowSchema::new(file_fields));

        let file_scan_config = build_file_scan_config(
            &self.snapshot,
            &self.log_store,
            &files,
            &config,
            FileScanParams {
                pruning_mask: pruning_mask.as_deref(),
                projection,
                limit,
                pushdown_filter,
            },
            session,
            file_schema,
        )?;
        // let _metrics = ExecutionPlanMetricsSet::new();
        // MetricBuilder::new(&metrics).global_counter("files_scanned").add(files_scanned);
        // MetricBuilder::new(&metrics).global_counter("files_pruned").add(files_pruned);

        // TODO: Properly expose these metrics
        let scan_exec = DataSourceExec::from_data_source(file_scan_config);
        // Rename columns from physical back to logical names expected by `schema`
        let mut logical_names = schema
            .fields()
            .iter()
            .filter(|f| !table_partition_cols.contains(f.name()))
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        // append partition column names in order
        logical_names.extend(table_partition_cols.iter().cloned());
        if let Some(file_col) = &config.file_column_name {
            logical_names.push(file_col.clone());
        }
        let renamed = rename_projected_physical_plan(scan_exec, &logical_names, projection)?;
        Ok(renamed)
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let partition_cols = self.snapshot.metadata().partition_columns().as_slice();
        Ok(get_pushdown_filters(filter, partition_cols))
    }

    fn statistics(&self) -> Option<Statistics> {
        self.snapshot.datafusion_table_statistics(None)
    }
}
