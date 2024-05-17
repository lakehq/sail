use std::sync::Arc;

use crate::error::SparkResult;
use datafusion::arrow::array::{BooleanArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::MemTable;

#[derive(Debug, Clone)]
pub(crate) struct CatalogColumn {
    pub(crate) name: String,
    pub(crate) description: Option<String>,
    pub(crate) data_type: String,
    pub(crate) nullable: bool,
    pub(crate) is_partition: bool,
    pub(crate) is_bucket: bool,
}

impl CatalogColumn {
    pub fn schema() -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("nullable", DataType::Boolean, false),
            Field::new("is_partition", DataType::Boolean, false),
            Field::new("is_bucket", DataType::Boolean, false),
        ]))
    }
}

pub(crate) fn create_catalog_column_memtable(columns: Vec<CatalogColumn>) -> SparkResult<MemTable> {
    let schema_ref = CatalogColumn::schema();

    let mut names: Vec<String> = Vec::with_capacity(columns.len());
    let mut descriptions: Vec<Option<String>> = Vec::with_capacity(columns.len());
    let mut data_types: Vec<String> = Vec::with_capacity(columns.len());
    let mut nullables: Vec<bool> = Vec::with_capacity(columns.len());
    let mut is_partitions: Vec<bool> = Vec::with_capacity(columns.len());
    let mut is_buckets: Vec<bool> = Vec::with_capacity(columns.len());

    for column in columns {
        names.push(column.name);
        descriptions.push(column.description);
        data_types.push(column.data_type);
        nullables.push(column.nullable);
        is_partitions.push(column.is_partition);
        is_buckets.push(column.is_bucket);
    }

    let record_batch = RecordBatch::try_new(
        schema_ref.clone(),
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(descriptions)),
            Arc::new(StringArray::from(data_types)),
            Arc::new(BooleanArray::from(nullables)),
            Arc::new(BooleanArray::from(is_partitions)),
            Arc::new(BooleanArray::from(is_buckets)),
        ],
    )?;

    Ok(MemTable::try_new(schema_ref, vec![vec![record_batch]])?)
}
