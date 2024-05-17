use std::sync::Arc;

use crate::error::SparkResult;
use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::MemTable;

#[derive(Debug, Clone)]
pub(crate) struct CatalogMetadata {
    pub(crate) name: String,
    pub(crate) description: Option<String>,
}

impl CatalogMetadata {
    pub fn schema() -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
        ]))
    }
}

pub(crate) fn create_catalog_metadata_memtable(
    catalogs: Vec<CatalogMetadata>,
) -> SparkResult<MemTable> {
    let schema_ref = CatalogMetadata::schema();

    let mut names: Vec<String> = Vec::with_capacity(catalogs.len());
    let mut descriptions: Vec<Option<String>> = Vec::with_capacity(catalogs.len());

    for catalog in catalogs {
        names.push(catalog.name);
        descriptions.push(catalog.description);
    }

    let record_batch = RecordBatch::try_new(
        schema_ref.clone(),
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(descriptions)),
        ],
    )?;

    Ok(MemTable::try_new(schema_ref, vec![vec![record_batch]])?)
}
