use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::catalog::Session;
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use object_store::ObjectMeta;
use url::Url;

use crate::datasource::arrow::columns_to_arrow_schema;
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

    fn resolve_file_path(&self, file_path: &str, is_relative: bool) -> DataFusionResult<String> {
        if is_relative {
            let base = self.base_path.trim_end_matches('/');
            let file = file_path.trim_start_matches('/');
            // DuckLake stores files in: {base_path}/{schema_name}/{table_name}/{file_name}
            Ok(format!(
                "{}/{}/{}/{}",
                base, &self.table.schema_info.schema_name, &self.table.table_info.table_name, file
            ))
        } else {
            Ok(file_path.to_string())
        }
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
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        log::trace!(
            "Scanning DuckLake table: {}.{}",
            self.table.schema_info.schema_name,
            self.table.table_info.table_name
        );

        let files = self
            .meta_store
            .list_data_files(self.table.table_info.table_id, self.snapshot_id)
            .await?;

        log::trace!("Found {} data files", files.len());

        // Parse base_path URL and extract scheme + authority only
        let base_url =
            Url::parse(&self.base_path).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let object_store_base = format!("{}://{}", base_url.scheme(), base_url.authority());
        let object_store_url = ObjectStoreUrl::parse(&object_store_base)?;

        let mut partitioned_files = Vec::new();
        for file in files {
            let full_path = self.resolve_file_path(&file.path, file.path_is_relative)?;
            let path_url = Url::parse(&full_path)
                .or_else(|_| {
                    let base = self.base_path.trim_end_matches('/');
                    Url::parse(&format!("{}/{}", base, full_path.trim_start_matches('/')))
                })
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let object_path = object_store::path::Path::from(path_url.path());

            let object_meta = ObjectMeta {
                location: object_path,
                last_modified: chrono::Utc::now(),
                size: file.file_size_bytes,
                e_tag: None,
                version: None,
            };

            partitioned_files.push(PartitionedFile::new(
                object_meta.location.clone(),
                file.file_size_bytes,
            ));
        }

        log::trace!("Created {} partitioned files", partitioned_files.len());

        if partitioned_files.is_empty() {
            log::warn!("No data files found for table");
        }

        let file_groups = partitioned_files
            .into_iter()
            .map(|f| FileGroup::new(vec![f]))
            .collect::<Vec<_>>();

        let parquet_options = TableParquetOptions {
            global: session.config().options().execution.parquet.clone(),
            ..Default::default()
        };

        let parquet_source = Arc::new(ParquetSource::new(parquet_options));

        let file_scan_config =
            FileScanConfigBuilder::new(object_store_url, self.schema.clone(), parquet_source)
                .with_file_groups(file_groups)
                .with_projection(projection.cloned())
                .with_limit(limit)
                .build();

        Ok(DataSourceExec::from_data_source(file_scan_config))
    }
}
