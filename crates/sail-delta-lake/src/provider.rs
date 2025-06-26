use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::physical_plan::wrap_partition_type_in_dict;
use datafusion::datasource::TableProvider;
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result as DataFusionResult;
use deltalake::arrow::error::ArrowError;

use deltalake::{DeltaTable, DeltaTableError};

use crate::error::DeltaResult;

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
            let dict_type = wrap_partition_type_in_dict(arrow_field.data_type().clone());
            let new_arrow_field =
                ArrowField::new(arrow_field.name(), dict_type, arrow_field.is_nullable());
            fields.push(Arc::new(new_arrow_field));
        }

        Ok(Arc::new(ArrowSchema::new(fields)))
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
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // For now, return a simple error indicating this needs to be implemented
        // with the proper DataFusion 48.0 APIs
        Err(datafusion_common::DataFusionError::NotImplemented(
            "DeltaTableProvider::scan needs to be updated for DataFusion 48.0".to_string(),
        ))
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
