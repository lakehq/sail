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

        // Use the proper iterator over Add actions from delta-rs
        for add_action in snapshot.file_actions()? {
            let partitioned_file =
                partitioned_file_from_action(&add_action, &partition_columns, schema)?;
            files.push(partitioned_file);
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

        // Parse the table URI to get the object store URL
        let object_store_url = ObjectStoreUrl::parse(&self.table.table_uri())
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;

        // Create ParquetSource with default options
        let parquet_options = TableParquetOptions::default();
        let source = Arc::new(ParquetSource::new(parquet_options));

        // Create FileScanConfig using the builder with the object store URL
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
