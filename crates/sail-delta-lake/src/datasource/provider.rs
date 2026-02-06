// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
//
// Copyright (2020) QP Hou and a number of other contributors.
// Portions Copyright (2025) LakeSail, Inc.
// Modified in 2025 by LakeSail, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/delta_datafusion/mod.rs>

use std::any::Any;
use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::catalog::Session;
use datafusion::common::stats::Statistics;
use datafusion::common::{Result, ToDFSchema};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use delta_kernel::table_features::ColumnMappingMode;
use sail_common_datafusion::rename::physical_plan::rename_projected_physical_plan;

use crate::datasource::scan::{build_file_scan_config, FileScanParams};
use crate::datasource::{
    df_logical_schema, get_pushdown_filters, prune_files, simplify_expr, DataFusionMixins,
    DeltaScanConfig, DeltaTableStateExt,
};
use crate::kernel::models::Add;
use crate::kernel::DeltaResult;
use crate::schema::get_physical_schema;
use crate::storage::LogStoreRef;
use crate::table::DeltaTableState;

/// A Delta table provider that enables additional metadata columns to be included during the scan
pub struct DeltaTableProvider {
    snapshot: DeltaTableState,
    log_store: LogStoreRef,
    config: DeltaScanConfig,
    schema: Arc<ArrowSchema>,
    files: Option<Arc<Vec<Add>>>,
}

impl std::fmt::Debug for DeltaTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaTableProvider")
            .field("snapshot_version", &self.snapshot.version())
            .field("config", &self.config)
            .field("schema", &self.schema)
            .field(
                "files_len",
                &self.files.as_ref().map(|files| files.len()).unwrap_or(0),
            )
            .finish()
    }
}

impl DeltaTableProvider {
    pub fn try_new(
        snapshot: DeltaTableState,
        log_store: LogStoreRef,
        config: DeltaScanConfig,
    ) -> DeltaResult<Self> {
        Ok(DeltaTableProvider {
            schema: df_logical_schema(
                &snapshot,
                &config.file_column_name,
                &config.commit_version_column_name,
                &config.commit_timestamp_column_name,
                config.schema.clone(),
            )?,
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

    pub fn snapshot(&self) -> &DeltaTableState {
        &self.snapshot
    }

    pub fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }

    pub fn config(&self) -> &DeltaScanConfig {
        &self.config
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
        }?;

        let full_logical_schema = df_logical_schema(
            &self.snapshot,
            &config.file_column_name,
            &config.commit_version_column_name,
            &config.commit_timestamp_column_name,
            Some(schema.clone()),
        )?;

        let logical_schema = if let Some(used_columns) = projection {
            let mut fields = vec![];
            for idx in used_columns {
                fields.push(full_logical_schema.field(*idx).to_owned());
            }
            // partition filters with Exact pushdown were removed from projection by DF optimizer,
            // we need to add them back for the predicate pruning to work
            let filter_expr = conjunction(filters.iter().cloned());
            if let Some(expr) = &filter_expr {
                for c in expr.column_refs() {
                    let idx = full_logical_schema.index_of(c.name.as_str())?;
                    if !used_columns.contains(&idx) {
                        fields.push(full_logical_schema.field(idx).to_owned());
                    }
                }
            }
            // Ensure all partition columns are included in logical schema
            let table_partition_cols = self.snapshot.metadata().partition_columns();
            for partition_col in table_partition_cols.iter() {
                if let Ok(idx) = full_logical_schema.index_of(partition_col.as_str()) {
                    if !used_columns.contains(&idx)
                        && !fields.iter().any(|f| f.name() == partition_col)
                    {
                        fields.push(full_logical_schema.field(idx).to_owned());
                    }
                }
            }
            Arc::new(ArrowSchema::new(fields))
        } else {
            Arc::clone(&full_logical_schema)
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
            pushdown_expr
                .map(|expr| simplify_expr(session, &df_schema, expr))
                .transpose()?
        } else {
            None
        };

        // Build physical file schema (non-partition columns)
        let table_partition_cols = self.snapshot.metadata().partition_columns();
        let kmode: ColumnMappingMode = self.snapshot.effective_column_mapping_mode();
        let kschema_arc = self.snapshot.snapshot().table_configuration().schema();
        let physical_arrow: ArrowSchema = get_physical_schema(&kschema_arc, kmode);
        log::trace!("read_kmode: {:?}", kmode);
        let phys_field_names: Vec<String> = physical_arrow
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        log::trace!("read_file_schema_fields: {:?}", &phys_field_names);
        let physical_partition_cols: HashSet<String> = table_partition_cols
            .iter()
            .map(|col| {
                kschema_arc
                    .field(col)
                    .map(|f| f.physical_name(kmode).to_string())
                    .unwrap_or_else(|| col.clone())
            })
            .collect();
        let file_fields = physical_arrow
            .fields()
            .iter()
            .filter(|f| !physical_partition_cols.contains(f.name()))
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
                sort_order: None,
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
        let logical_names = full_logical_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
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
