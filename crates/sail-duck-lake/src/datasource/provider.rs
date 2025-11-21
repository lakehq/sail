use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::catalog::Session;
use datafusion::common::stats::{ColumnStatistics, Precision, Statistics};
use datafusion::common::ToDFSchema;
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use url::Url;

use crate::datasource::arrow::columns_to_arrow_schema;
use crate::datasource::expressions::{get_pushdown_filters, simplify_expr};
use crate::datasource::pruning::prune_files;
use crate::metadata::{DuckLakeMetaStore, DuckLakeTable};
use crate::options::DuckLakeOptions;

pub struct DuckLakeTableProvider {
    table: DuckLakeTable,
    schema: ArrowSchemaRef,
    base_path: String,
    snapshot_id: Option<u64>,
    meta_store: Arc<dyn DuckLakeMetaStore>,
}

impl DuckLakeTableProvider {
    pub async fn new(
        _ctx: &dyn Session,
        meta_store: Arc<dyn DuckLakeMetaStore>,
        opts: DuckLakeOptions,
    ) -> DataFusionResult<Self> {
        let parts: Vec<&str> = opts.table.split('.').collect();
        let (schema_name, table_name) = match parts.as_slice() {
            [table] => (None, *table),
            [schema, table] => (Some(*schema), *table),
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "Invalid table name format: {}",
                    opts.table
                )))
            }
        };

        let table = meta_store.load_table(table_name, schema_name).await?;
        let schema = Arc::new(columns_to_arrow_schema(&table.columns)?);

        log::trace!(
            "Loaded DuckLake table: {}.{} with {} columns",
            table.schema_info.schema_name,
            table.table_info.table_name,
            table.columns.len()
        );

        Ok(Self {
            table,
            schema,
            base_path: opts.base_path,
            snapshot_id: opts.snapshot_id,
            meta_store,
        })
    }
}

impl std::fmt::Debug for DuckLakeTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckLakeTableProvider")
            .field("table", &self.table.table_info.table_name)
            .field("schema", &self.table.schema_info.schema_name)
            .finish()
    }
}

#[async_trait]
impl TableProvider for DuckLakeTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(get_pushdown_filters(filters, &[]))
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        log::trace!(
            "Scanning DuckLake table: {}.{}",
            self.table.schema_info.schema_name,
            self.table.table_info.table_name
        );

        let (pruning_filters, pushdown_filters) = self.separate_filters(filters);

        let files = self
            .meta_store
            .list_data_files(self.table.table_info.table_id, self.snapshot_id)
            .await?;

        log::trace!("Found {} data files", files.len());

        let prune_schema = if let Some(used_columns) = projection {
            let mut fields = vec![];
            for idx in used_columns {
                fields.push(self.schema.field(*idx).to_owned());
            }
            if let Some(expr) =
                datafusion::logical_expr::utils::conjunction(pruning_filters.iter().cloned())
            {
                for c in expr.column_refs() {
                    if let Ok(idx) = self.schema.index_of(c.name.as_str()) {
                        if !used_columns.contains(&idx)
                            && !fields.iter().any(|f| f.name() == c.name.as_str())
                        {
                            fields.push(self.schema.field(idx).to_owned());
                        }
                    }
                }
            }
            Arc::new(datafusion::arrow::datatypes::Schema::new(fields))
        } else {
            self.schema.clone()
        };

        let mut files = {
            let (kept, _mask) = prune_files(
                session,
                &pruning_filters,
                limit,
                prune_schema.clone(),
                files,
                &self.table.columns,
                &self.table.partition_fields,
            )?;
            kept
        };

        // Parse base_path URL and construct ObjectStoreUrl with only scheme + authority
        let base_url =
            Url::parse(&self.base_path).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let object_store_base = format!("{}://{}", base_url.scheme(), base_url.authority());
        let object_store_base_parsed =
            Url::parse(&object_store_base).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let object_store_url = ObjectStoreUrl::parse(object_store_base_parsed)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Build table-level prefix: {base_path}/{schema}/{table}
        let base_path_str = base_url.path();
        let mut table_prefix =
            object_store::path::Path::parse(base_path_str.trim_start_matches('/'))
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        table_prefix = table_prefix
            .child(self.table.schema_info.schema_name.as_str())
            .child(self.table.table_info.table_name.as_str());

        let mut file_groups: HashMap<Option<u64>, Vec<PartitionedFile>> = HashMap::new();

        for file in files.drain(..) {
            let object_path = if file.path_is_relative {
                let mut p = table_prefix.clone();
                for comp in file.path.split('/') {
                    if !comp.is_empty() {
                        p = p.child(comp);
                    }
                }
                p
            } else if let Ok(path_url) = Url::parse(&file.path) {
                let encoded_path = path_url.path();
                let no_leading = encoded_path.strip_prefix('/').unwrap_or(encoded_path);
                object_store::path::Path::parse(no_leading)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
            } else {
                let no_leading = file.path.strip_prefix('/').unwrap_or(&file.path);
                object_store::path::Path::parse(no_leading)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
            };

            let partitioned_file = PartitionedFile::new(object_path.clone(), file.file_size_bytes);

            let partition_key = file.partition_id.map(|p| p.0);
            file_groups
                .entry(partition_key)
                .or_default()
                .push(partitioned_file);
        }

        log::trace!(
            "Created {} file groups from {} files",
            file_groups.len(),
            file_groups.values().map(|v| v.len()).sum::<usize>()
        );

        let file_groups = if file_groups.is_empty() {
            log::warn!("No data files found for table");
            vec![FileGroup::from(vec![])]
        } else {
            file_groups.into_values().map(FileGroup::from).collect()
        };

        let parquet_options = TableParquetOptions {
            global: session.config().options().execution.parquet.clone(),
            ..Default::default()
        };

        let mut parquet_source = ParquetSource::new(parquet_options);
        let pushdown_filter: Option<Arc<dyn PhysicalExpr>> = if !pushdown_filters.is_empty() {
            let df_schema = prune_schema.clone().to_dfschema()?;
            let pushdown_expr = datafusion::logical_expr::utils::conjunction(pushdown_filters);
            pushdown_expr.map(|expr| simplify_expr(session, &df_schema, expr))
        } else {
            None
        };
        if let Some(pred) = pushdown_filter {
            parquet_source = parquet_source.with_predicate(pred);
        }
        let parquet_source = Arc::new(parquet_source);

        let table_stats = self.aggregate_statistics(self.schema.as_ref());

        let file_scan_config =
            FileScanConfigBuilder::new(object_store_url, self.schema.clone(), parquet_source)
                .with_file_groups(file_groups)
                .with_statistics(table_stats)
                .with_projection_indices(projection.cloned())
                .with_limit(limit)
                .build();

        Ok(DataSourceExec::from_data_source(file_scan_config))
    }
}

impl DuckLakeTableProvider {
    fn separate_filters(&self, filters: &[Expr]) -> (Vec<Expr>, Vec<Expr>) {
        let predicates: Vec<&Expr> = filters.iter().collect();
        let pushdown_kinds = get_pushdown_filters(&predicates, &[]);
        let mut pruning_filters = Vec::new();
        let mut parquet_pushdown_filters = Vec::new();
        for (filter, kind) in filters.iter().zip(pushdown_kinds) {
            match kind {
                TableProviderFilterPushDown::Exact => {
                    pruning_filters.push(filter.clone());
                }
                TableProviderFilterPushDown::Inexact => {
                    pruning_filters.push(filter.clone());
                    parquet_pushdown_filters.push(filter.clone());
                }
                TableProviderFilterPushDown::Unsupported => {}
            }
        }
        (pruning_filters, parquet_pushdown_filters)
    }

    fn aggregate_statistics(&self, schema: &datafusion::arrow::datatypes::Schema) -> Statistics {
        let column_statistics = (0..schema.fields().len())
            .map(|_| ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Absent,
                min_value: Precision::Absent,
                distinct_count: Precision::Absent,
                sum_value: Precision::Absent,
            })
            .collect();
        Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics,
        }
    }
}
