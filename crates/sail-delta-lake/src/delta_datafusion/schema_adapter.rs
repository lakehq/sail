use std::fmt::Debug;
use std::sync::Arc;

/// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/delta_datafusion/schema_adapter.rs>
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::DataFusionError;
use datafusion::datasource::schema_adapter::{SchemaAdapter, SchemaAdapterFactory, SchemaMapper};
use deltalake::kernel::schema::cast::cast_record_batch;

/// A Schema Adapter Factory which provides casting record batches from parquet to meet
/// delta lake conventions.
#[derive(Debug)]
pub(crate) struct DeltaSchemaAdapterFactory {}

impl SchemaAdapterFactory for DeltaSchemaAdapterFactory {
    fn create(
        &self,
        projected_table_schema: SchemaRef,
        table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(DeltaSchemaAdapter {
            projected_table_schema,
            table_schema,
        })
    }
}

pub(crate) struct DeltaSchemaAdapter {
    /// The schema for the table, projected to include only the fields being output (projected) by
    /// the mapping.
    projected_table_schema: SchemaRef,
    /// Schema for the table
    table_schema: SchemaRef,
}

impl SchemaAdapter for DeltaSchemaAdapter {
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let field = self.table_schema.field(index);
        Some(file_schema.fields.find(field.name())?.0)
    }

    fn map_schema(
        &self,
        file_schema: &Schema,
    ) -> datafusion::common::Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        let mut projection = Vec::with_capacity(file_schema.fields().len());

        for (file_idx, file_field) in file_schema.fields.iter().enumerate() {
            if self
                .projected_table_schema
                .fields()
                .find(file_field.name())
                .is_some()
            {
                projection.push(file_idx);
            }
        }

        Ok((
            Arc::new(SchemaMapping {
                projected_schema: self.projected_table_schema.clone(),
            }),
            projection,
        ))
    }
}

#[derive(Debug)]
pub(crate) struct SchemaMapping {
    projected_schema: SchemaRef,
}

use datafusion_common::ColumnStatistics;

impl SchemaMapper for SchemaMapping {
    fn map_batch(&self, batch: RecordBatch) -> datafusion::common::Result<RecordBatch> {
        cast_record_batch(&batch, self.projected_schema.clone(), false, true)
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    fn map_column_statistics(
        &self,
        _stats: &[ColumnStatistics],
    ) -> datafusion::common::Result<Vec<ColumnStatistics>> {
        Ok(vec![])
    }
}
