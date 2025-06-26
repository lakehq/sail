use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::TableProvider;
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::config::TableParquetOptions;
use datafusion_common::Result as DataFusionResult;
use datafusion_execution::object_store::ObjectStoreUrl;
use deltalake::arrow::error::ArrowError;
use deltalake::{DeltaTable, DeltaTableError};

use crate::error::DeltaResult;
use crate::scan::partitioned_file_from_action;

#[derive(Debug)]
pub struct DeltaTableProvider {
    table: DeltaTable,
    schema: SchemaRef,
}

impl DeltaTableProvider {
    pub fn new(table: DeltaTable) -> DeltaResult<Self> {
        let schema = Self::create_arrow_schema(&table)?;
        Ok(Self { table, schema })
    }

    /// Replicate the schema conversion logic from `delta-rs`.
    /// This function converts the DeltaTable's schema into an Arrow schema, correctly handling
    /// partition columns by placing them at the end.
    fn create_arrow_schema(table: &DeltaTable) -> DeltaResult<SchemaRef> {
        let metadata = table.metadata()?;
        let schema = metadata.schema()?;
        let partition_columns = &metadata.partition_columns;

        // Non-partitioned columns
        let mut fields: Vec<Arc<ArrowField>> = schema
            .fields()
            .filter(|f| !partition_columns.contains(f.name()))
            .map(|f| f.try_into().map(Arc::new))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e: ArrowError| DeltaTableError::Arrow { source: e })?;

        // Partitioned columns (at the end)
        for part_col in partition_columns {
            let field = schema
                .fields()
                .find(|f| f.name() == part_col)
                .ok_or_else(|| {
                    DeltaTableError::Generic(format!(
                        "Partition column '{}' not found in schema",
                        part_col
                    ))
                })?;
            let arrow_field: ArrowField = field
                .try_into()
                .map_err(|e: ArrowError| DeltaTableError::Arrow { source: e })?;

            // DataFusion dictionary-encodes partition columns to save memory.
            let dict_type = datafusion::datasource::physical_plan::wrap_partition_type_in_dict(
                arrow_field.data_type().clone(),
            );
            let new_arrow_field =
                ArrowField::new(arrow_field.name(), dict_type, arrow_field.is_nullable());
            fields.push(Arc::new(new_arrow_field));
        }

        Ok(Arc::new(ArrowSchema::new(fields)))
    }

    /// Get the list of partitioned files from the Delta table
    fn get_partitioned_files(&self) -> DeltaResult<Vec<PartitionedFile>> {
        let snapshot = self.table.snapshot()?;
        let partition_columns = self.table.metadata()?.partition_columns.clone();
        let schema = snapshot.schema();

        let mut files = Vec::new();

        // Get all add actions from the current snapshot
        // TODO: properly iterate through the add actions from the snapshot
        let actions = snapshot.add_actions_table(false)?;

        // Convert RecordBatch to Add actions
        // TODO: properly iterate through the add actions from the snapshot
        // iterate through the add actions from the snapshot
        for i in 0..actions.num_rows() {
            // TODO: properly iterate through the add actions from the snapshot
            let path_array = actions
                .column_by_name("path")
                .ok_or_else(|| DeltaTableError::Generic("No path column found".to_string()))?;
            let size_array = actions
                .column_by_name("size")
                .ok_or_else(|| DeltaTableError::Generic("No size column found".to_string()))?;
            let modification_time_array =
                actions.column_by_name("modificationTime").ok_or_else(|| {
                    DeltaTableError::Generic("No modificationTime column found".to_string())
                })?;

            if let (Some(path), Some(size), Some(mod_time)) = (
                path_array
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StringArray>()
                    .map(|arr| arr.value(i)),
                size_array
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .map(|arr| arr.value(i)),
                modification_time_array
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .map(|arr| arr.value(i)),
            ) {
                // Create a simple Add action for now
                let add_action = deltalake::kernel::Add {
                    path: path.to_string(),
                    size: size,
                    modification_time: mod_time,
                    partition_values: std::collections::HashMap::new(), // TODO: extract partition values
                    data_change: true,
                    stats: None,
                    stats_parsed: None,
                    tags: None,
                    deletion_vector: None,
                    base_row_id: None,
                    default_row_commit_version: None,
                    clustering_provider: None,
                };

                let partitioned_file =
                    partitioned_file_from_action(&add_action, &partition_columns, schema)?;
                files.push(partitioned_file);
            }
        }

        Ok(files)
    }
}

#[async_trait]
impl TableProvider for DeltaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Get the partitioned files from Delta table
        let files = self
            .get_partitioned_files()
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;

        // Create the object store URL from the table location
        let object_store_url = ObjectStoreUrl::parse(&self.table.table_uri())
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;

        // Create ParquetSource with default options
        let parquet_options = TableParquetOptions::default();
        let source = Arc::new(ParquetSource::new(parquet_options));

        // Create FileScanConfig using the builder
        let mut builder = FileScanConfigBuilder::new(object_store_url, self.schema.clone(), source);

        // Add files to the builder
        for file in files {
            builder = builder.with_file(file);
        }

        // Apply projection if provided
        if let Some(projection) = projection {
            builder = builder.with_projection(Some(projection.clone()));
        }

        // Apply limit if provided
        if let Some(limit) = limit {
            builder = builder.with_limit(Some(limit));
        }

        let config = builder.build();

        // Create DataSourceExec from the config
        Ok(DataSourceExec::from_data_source(config))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // For now, return Unsupported for all filters until we implement proper filter pushdown
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }
}
