use std::sync::Arc;

use crate::error::SparkResult;
use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::MemTable;

#[derive(Debug, Clone)]
pub(crate) struct CatalogDatabase {
    pub(crate) name: String,
    pub(crate) catalog: Option<String>,
    pub(crate) description: Option<String>,
    // TODO: location_uri should technically not be nullable
    pub(crate) location_uri: Option<String>,
}

impl CatalogDatabase {
    pub fn schema() -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("catalog", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
            // TODO: location_uri should technically not be nullable
            Field::new("location_uri", DataType::Utf8, true),
        ]))
    }
}

pub(crate) fn create_catalog_database_memtable(
    databases: Vec<CatalogDatabase>,
) -> SparkResult<MemTable> {
    let schema_ref = CatalogDatabase::schema();

    let mut names: Vec<String> = Vec::with_capacity(databases.len());
    let mut catalogs: Vec<Option<String>> = Vec::with_capacity(databases.len());
    let mut descriptions: Vec<Option<String>> = Vec::with_capacity(databases.len());
    let mut location_uris: Vec<Option<String>> = Vec::with_capacity(databases.len());

    for db in databases {
        names.push(db.name);
        catalogs.push(db.catalog);
        descriptions.push(db.description);
        location_uris.push(db.location_uri);
    }

    let record_batch = RecordBatch::try_new(
        schema_ref.clone(),
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(catalogs)),
            Arc::new(StringArray::from(descriptions)),
            Arc::new(StringArray::from(location_uris)),
        ],
    )?;

    Ok(MemTable::try_new(schema_ref, vec![vec![record_batch]])?)
}
